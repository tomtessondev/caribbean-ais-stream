"""Synthetic traffic in aisstream.io's exact format, for development without an API key.

A handful of ships with the habits of the real ones around Guadeloupe and Martinique: an
inter-island ferry shuttling Pointe-à-Pitre - Roseau - Fort-de-France, a cargo ship calling
at Pointe-à-Pitre, a cruise ship between Fort-de-France and Castries, a tanker at anchor
off Fort-de-France, fishing boats working off Le Moule, and one cargo ship whose AIS goes
silent for 45 minutes mid-passage (the "dark vessel" the anomaly detection must catch), and
one whose AIS reports a single spoofed position 30 nautical miles off its track (an
impossible jump, out and back). Same seed, same traffic.
"""

from __future__ import annotations

import heapq
import math
import random
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

PORTS: dict[str, tuple[float, float]] = {
    "Pointe-a-Pitre": (16.229, -61.533),
    "Basse-Terre": (15.995, -61.731),
    "Roseau": (15.297, -61.389),
    "Fort-de-France": (14.598, -61.073),
    "Castries": (14.015, -60.995),
}

UNDER_WAY_STEP = timedelta(seconds=10)
STATIONARY_STEP = timedelta(minutes=3)
STATIC_EVERY = timedelta(minutes=6)

UNDER_WAY, AT_ANCHOR, MOORED, FISHING = 0, 1, 5, 7


def go_time(ts: datetime) -> str:
    """aisstream's timestamp format (Go's default time.String())."""
    return ts.strftime("%Y-%m-%d %H:%M:%S.%f") + "000 +0000 UTC"


def _distance_nm(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat = math.radians((a[0] + b[0]) / 2)
    dy = (b[0] - a[0]) * 60
    dx = (b[1] - a[1]) * 60 * math.cos(lat)
    return math.hypot(dx, dy)


def _bearing_deg(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat = math.radians((a[0] + b[0]) / 2)
    return math.degrees(math.atan2((b[1] - a[1]) * math.cos(lat), b[0] - a[0])) % 360


@dataclass(frozen=True)
class Ship:
    mmsi: int
    name: str
    ship_type: int
    call_sign: str
    length_m: int
    beam_m: int
    draught_m: float


def position_message(
    ship: Ship,
    ts: datetime,
    at: tuple[float, float],
    sog_kn: float,
    cog_deg: float,
    heading_deg: int,
    status: int,
) -> dict[str, Any]:
    lat, lon = round(at[0], 6), round(at[1], 6)
    return {
        "MessageType": "PositionReport",
        "MetaData": {
            "MMSI": ship.mmsi,
            "ShipName": ship.name,
            "latitude": lat,
            "longitude": lon,
            "time_utc": go_time(ts),
        },
        "Message": {
            "PositionReport": {
                "MessageID": 1,
                "UserID": ship.mmsi,
                "Valid": True,
                "Latitude": lat,
                "Longitude": lon,
                "Sog": round(sog_kn, 1),
                "Cog": round(cog_deg, 1),
                "TrueHeading": heading_deg,
                "NavigationalStatus": status,
            }
        },
    }


def static_message(ship: Ship, ts: datetime, destination: str) -> dict[str, Any]:
    half_length, half_beam = ship.length_m // 2, ship.beam_m // 2
    return {
        "MessageType": "ShipStaticData",
        "MetaData": {"MMSI": ship.mmsi, "ShipName": ship.name, "time_utc": go_time(ts)},
        "Message": {
            "ShipStaticData": {
                "MessageID": 5,
                "UserID": ship.mmsi,
                "Valid": True,
                "Name": ship.name.ljust(20, "@"),  # AIS pads names with '@'
                "Type": ship.ship_type,
                "CallSign": ship.call_sign,
                "ImoNumber": 0,
                "Destination": destination.upper()[:20],
                "Dimension": {
                    "A": half_length,
                    "B": ship.length_m - half_length,
                    "C": half_beam,
                    "D": ship.beam_m - half_beam,
                },
                "MaximumStaticDraught": ship.draught_m,
            }
        },
    }


def _shuttle(
    ship: Ship,
    route: list[str],
    speed_kn: float,
    dwell: timedelta,
    start: datetime,
    end: datetime,
    rng: random.Random,
    silent: tuple[timedelta, timedelta] | None = None,
    spoofed: timedelta | None = None,
) -> Iterator[dict[str, Any]]:
    """Sails the route, dwells moored at its end, sails it back, and so on.

    `silent`: (offset from start, duration) without any position report.
    `spoofed`: offset from start of the one position reported 0.5° of latitude south of
    the true track.
    """
    ts = start
    spoof_pending = spoofed is not None
    leg_ports = route
    last_static = start - STATIC_EVERY
    while ts < end:
        for here, there in zip(leg_ports, leg_ports[1:], strict=False):
            a, b = PORTS[here], PORTS[there]
            leg_hours = _distance_nm(a, b) / speed_kn
            leg_start = ts
            course = _bearing_deg(a, b)
            while ts < end and (ts - leg_start).total_seconds() / 3600 < leg_hours:
                if ts - last_static >= STATIC_EVERY:
                    yield static_message(ship, ts, leg_ports[-1])
                    last_static = ts
                f = (ts - leg_start).total_seconds() / 3600 / leg_hours
                at = (a[0] + (b[0] - a[0]) * f, a[1] + (b[1] - a[1]) * f)
                dark = silent is not None and silent[0] <= ts - start < silent[0] + silent[1]
                if spoofed is not None and spoof_pending and ts - start >= spoofed:
                    at = (at[0] - 0.5, at[1])
                    spoof_pending = False
                if not dark:
                    sog = speed_kn + rng.gauss(0, 0.3)
                    cog = (course + rng.gauss(0, 1.5)) % 360
                    yield position_message(ship, ts, at, sog, cog, round(course) % 360, UNDER_WAY)
                ts += UNDER_WAY_STEP
        dwell_end = ts + dwell
        while ts < min(dwell_end, end):
            if ts - last_static >= STATIC_EVERY:
                yield static_message(ship, ts, leg_ports[-1])
                last_static = ts
            yield position_message(ship, ts, PORTS[leg_ports[-1]], 0.0, 0.0, 511, MOORED)
            ts += STATIONARY_STEP
        leg_ports = list(reversed(leg_ports))


def _at_anchor(
    ship: Ship, spot: tuple[float, float], start: datetime, end: datetime, rng: random.Random
) -> Iterator[dict[str, Any]]:
    ts = start
    while ts < end:
        if (ts - start) % STATIC_EVERY < STATIONARY_STEP:
            yield static_message(ship, ts, "FORT DE FRANCE")
        swing = (spot[0] + rng.gauss(0, 0.0004), spot[1] + rng.gauss(0, 0.0004))
        yield position_message(ship, ts, swing, 0.1, rng.uniform(0, 359), 511, AT_ANCHOR)
        ts += STATIONARY_STEP


def _fishing(
    ship: Ship, around: tuple[float, float], start: datetime, end: datetime, rng: random.Random
) -> Iterator[dict[str, Any]]:
    """Slow, wandering, no gyro compass (heading not available: 511)."""
    ts, at, course = start, around, rng.uniform(0, 360)
    while ts < end:
        if (ts - start) % STATIC_EVERY < UNDER_WAY_STEP:
            yield static_message(ship, ts, "LE MOULE")
        course = (course + rng.gauss(0, 12)) % 360
        speed = max(0.5, rng.gauss(3, 1))
        step_nm = speed * UNDER_WAY_STEP.total_seconds() / 3600
        at = (
            at[0] + step_nm / 60 * math.cos(math.radians(course)),
            at[1] + step_nm / 60 * math.sin(math.radians(course)) / math.cos(math.radians(at[0])),
        )
        yield position_message(ship, ts, at, speed, course, 511, FISHING)
        ts += UNDER_WAY_STEP


def stream(start: datetime, hours: float, seed: int = 7) -> Iterator[dict[str, Any]]:
    """All messages of all ships over `hours`, in time order, generated as they are read:
    each ship's messages are already in time order, so merging them needs no full list."""
    rng = random.Random(seed)
    end = start + timedelta(hours=hours)
    ferry = Ship(227000101, "ANTILLES EXPRESS", 60, "FAX101", 45, 12, 2.1)
    cargo = Ship(228000202, "CMA CGM CARIBE", 70, "FCG202", 180, 30, 9.8)
    cruise = Ship(636000303, "EMERALD PRINCESS", 60, "D5ER3", 290, 36, 8.5)
    tanker = Ship(538000404, "SEA HAWK", 80, "V7SH4", 183, 32, 11.2)
    dark = Ship(341000505, "BLUE MERIDIAN", 70, "V4BM5", 120, 20, 7.4)
    spoofer = Ship(255000707, "OCEAN PEARL", 70, "CQOP7", 140, 22, 8.1)
    fishers = [
        Ship(227000601 + i, f"KANOT {i + 1}", 30, f"FPM60{i + 1}", 9, 3, 0.8) for i in range(3)
    ]
    streams = [
        _shuttle(
            ferry,
            ["Pointe-a-Pitre", "Roseau", "Fort-de-France"],
            24,
            timedelta(hours=1),
            start,
            end,
            random.Random(rng.random()),
        ),
        _shuttle(
            cargo,
            ["Basse-Terre", "Pointe-a-Pitre"],
            13,
            timedelta(hours=6),
            start,
            end,
            random.Random(rng.random()),
        ),
        _shuttle(
            cruise,
            ["Fort-de-France", "Castries"],
            17,
            timedelta(hours=8),
            start,
            end,
            random.Random(rng.random()),
        ),
        _at_anchor(tanker, (14.565, -61.095), start, end, random.Random(rng.random())),
        _shuttle(
            dark,
            ["Castries", "Fort-de-France", "Roseau"],
            12,
            timedelta(hours=4),
            start,
            end,
            random.Random(rng.random()),
            silent=(timedelta(hours=2), timedelta(minutes=45)),
        ),
        *[
            _fishing(
                f, (16.38 + 0.03 * i, -61.25 - 0.04 * i), start, end, random.Random(rng.random())
            )
            for i, f in enumerate(fishers)
        ],
        # Last, so its random stream is drawn after the others' and leaves them unchanged.
        _shuttle(
            spoofer,
            ["Basse-Terre", "Roseau"],
            14,
            timedelta(hours=3),
            start,
            end,
            random.Random(rng.random()),
            spoofed=timedelta(hours=1, minutes=30),
        ),
    ]
    return heapq.merge(*streams, key=lambda m: m["MetaData"]["time_utc"])


def traffic(start: datetime, hours: float, seed: int = 7) -> list[dict[str, Any]]:
    """All messages of all ships over `hours`, in time order."""
    return list(stream(start, hours, seed))
