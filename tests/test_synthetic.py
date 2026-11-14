from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta

from ais_stream.messages import Position, ShipStatic, parse_aisstream_time, to_record
from ais_stream.synthetic import PORTS, traffic

START = datetime(2026, 11, 14, 6, tzinfo=UTC)
# sha256 of the eight original ships' first hour, recorded before the ninth ship existed.
EIGHT_SHIPS_ONE_HOUR = "3b11ba669f03244406fa571f30448674e43544c1cce8f27ed64bb18a1244d3fb"


def test_same_seed_same_traffic_in_time_order() -> None:
    first = traffic(START, hours=1, seed=1)
    assert first == traffic(START, hours=1, seed=1)
    assert first != traffic(START, hours=1, seed=2)
    stamps = [m["MetaData"]["time_utc"] for m in first]
    assert stamps == sorted(stamps)


def test_every_synthetic_message_parses_into_a_valid_record() -> None:
    records = [to_record(m) for m in traffic(START, hours=6)]

    ships = {r.mmsi for r in records if isinstance(r, ShipStatic)}
    assert len(ships) == 9
    # Fishing boats have no gyro compass: heading "511" became None.
    fishing = [r for r in records if isinstance(r, Position) and r.nav_status == 7]
    assert fishing and all(r.heading_deg is None for r in fishing)


def test_the_ferry_reaches_fort_de_france() -> None:
    positions = [
        r
        for r in map(to_record, traffic(START, hours=6))
        if isinstance(r, Position) and r.mmsi == 227000101
    ]
    fdf = PORTS["Fort-de-France"]
    closest = min(abs(p.lat - fdf[0]) + abs(p.lon - fdf[1]) for p in positions)
    assert closest < 0.01


def test_one_ship_goes_dark_for_45_minutes() -> None:
    times: dict[int, list[datetime]] = defaultdict(list)
    for message in traffic(START, hours=6):
        if message["MessageType"] == "PositionReport":
            times[message["MetaData"]["MMSI"]].append(
                parse_aisstream_time(message["MetaData"]["time_utc"])
            )
    longest_gap = {
        mmsi: max((b - a for a, b in zip(ts, ts[1:], strict=False)), default=timedelta())
        for mmsi, ts in times.items()
    }

    dark = [mmsi for mmsi, gap in longest_gap.items() if gap >= timedelta(minutes=40)]
    assert dark == [341000505]


def test_one_ship_reports_a_spoofed_position() -> None:
    positions = [
        r
        for r in map(to_record, traffic(START, hours=6))
        if isinstance(r, Position) and r.mmsi == 255000707
    ]
    jumps = [
        (a, b) for a, b in zip(positions, positions[1:], strict=False) if abs(b.lat - a.lat) > 0.3
    ]
    assert len(jumps) == 2  # out to the spoofed position, then back
    (out, spoofed), (_, back) = jumps
    assert spoofed.ts - START == timedelta(hours=1, minutes=30)
    assert spoofed.lat < out.lat - 0.45 and abs(back.lat - out.lat) < 0.01


def test_the_other_ships_keep_their_traffic() -> None:
    """Adding a ship must not reshuffle the others' random streams."""
    others = [m for m in traffic(START, hours=1) if m["MetaData"]["MMSI"] != 255000707]
    assert hashlib.sha256(json.dumps(others).encode()).hexdigest() == EIGHT_SHIPS_ONE_HOUR
