"""From aisstream.io messages to the records written to Kafka.

aisstream.io relays AIS, the radio system every ship over 300 gross tonnage (and every
passenger ship) must run: about every 2 to 10 seconds under way, each ship broadcasts its
position, speed and course; every 6 minutes, its name, type, size and destination.

AIS encodes "not available" as out-of-range sentinels rather than leaving fields empty:
heading 511, speed 102.3 knots, course 360, latitude 91, longitude 181. They are turned
into nulls here, once, so no downstream query ever averages a 511° heading.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

HEADING_NOT_AVAILABLE = 511
SPEED_NOT_AVAILABLE_KN = 102.3
COURSE_NOT_AVAILABLE_DEG = 360.0
LATITUDE_NOT_AVAILABLE = 91.0
LONGITUDE_NOT_AVAILABLE = 181.0


class Position(BaseModel):
    """Kafka topic `ais.positions`, keyed by MMSI."""

    model_config = ConfigDict(frozen=True)

    mmsi: int = Field(ge=100_000_000, le=999_999_999)  # 9-digit Maritime Mobile Service Identity
    ts: datetime
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    sog_kn: float | None = Field(default=None, ge=0, le=102.2)  # speed over ground
    cog_deg: float | None = Field(default=None, ge=0, lt=360)  # course over ground
    heading_deg: int | None = Field(default=None, ge=0, le=359)
    nav_status: int | None = Field(default=None, ge=0, le=15)  # 0 under way, 1 at anchor, 5 moored
    ship_name: str | None = None


class ShipStatic(BaseModel):
    """Kafka topic `ais.ships` (compacted, keyed by MMSI): the latest description of a ship."""

    model_config = ConfigDict(frozen=True)

    mmsi: int = Field(ge=100_000_000, le=999_999_999)
    ts: datetime
    name: str | None = None
    ship_type: int | None = Field(default=None, ge=0, le=99)  # 60-69 passenger, 70-79 cargo
    call_sign: str | None = None
    imo: int | None = None
    destination: str | None = None
    length_m: int | None = Field(default=None, ge=0)
    beam_m: int | None = Field(default=None, ge=0)
    draught_m: float | None = Field(default=None, ge=0)


class Rejected(BaseModel):
    """Kafka topic `ais.dlq`: a message that could not be turned into a record, kept verbatim."""

    model_config = ConfigDict(frozen=True)

    received_at: datetime
    reason: str
    raw: str


Record = Position | ShipStatic


def parse_aisstream_time(value: str) -> datetime:
    """aisstream writes Go's default format: '2026-11-14 18:22:32.318353123 +0000 UTC'."""
    stamp, fraction = value.split(" +", 1)[0], ""
    if "." in stamp:
        stamp, fraction = stamp.split(".", 1)
    parsed = datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
    micro = int((fraction + "000000")[:6]) if fraction else 0
    return parsed.replace(microsecond=micro)


def _clean_text(value: Any) -> str | None:
    """AIS pads text with '@' and spaces; an empty name is no name."""
    if not isinstance(value, str):
        return None
    cleaned = value.replace("@", " ").strip()
    return cleaned or None


def _available(value: Any, sentinel: float) -> Any:
    return None if value is None or value == sentinel else value


def to_record(message: dict[str, Any]) -> Record:
    """Normalise one aisstream message. Raises KeyError, ValueError or ValidationError on
    anything that cannot become a valid record."""
    kind: Literal["PositionReport", "ShipStaticData"] = message["MessageType"]
    meta = message["MetaData"]
    ts = parse_aisstream_time(meta["time_utc"])
    body = message["Message"][kind]

    if kind == "PositionReport":
        lat = _available(body["Latitude"], LATITUDE_NOT_AVAILABLE)
        lon = _available(body["Longitude"], LONGITUDE_NOT_AVAILABLE)
        if lat is None or lon is None:
            raise ValueError("position not available")
        return Position(
            mmsi=body["UserID"],
            ts=ts,
            lat=lat,
            lon=lon,
            sog_kn=_available(body.get("Sog"), SPEED_NOT_AVAILABLE_KN),
            cog_deg=_available(body.get("Cog"), COURSE_NOT_AVAILABLE_DEG),
            heading_deg=_available(body.get("TrueHeading"), HEADING_NOT_AVAILABLE),
            nav_status=body.get("NavigationalStatus"),
            ship_name=_clean_text(meta.get("ShipName")),
        )

    if kind == "ShipStaticData":
        dimension = body.get("Dimension") or {}
        length = (dimension.get("A") or 0) + (dimension.get("B") or 0)
        beam = (dimension.get("C") or 0) + (dimension.get("D") or 0)
        return ShipStatic(
            mmsi=body["UserID"],
            ts=ts,
            name=_clean_text(body.get("Name")),
            ship_type=body.get("Type") or None,
            call_sign=_clean_text(body.get("CallSign")),
            imo=body.get("ImoNumber") or None,
            destination=_clean_text(body.get("Destination")),
            length_m=length or None,
            beam_m=beam or None,
            draught_m=body.get("MaximumStaticDraught") or None,
        )

    raise ValueError(f"unsupported message type {kind!r}")
