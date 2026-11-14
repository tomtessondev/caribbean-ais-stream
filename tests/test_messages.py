from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from ais_stream.messages import Position, ShipStatic, parse_aisstream_time, to_record

SAMPLES = Path(__file__).parent / "fixtures" / "aisstream_samples.jsonl"


def sample(index: int) -> dict[str, Any]:
    message: dict[str, Any] = json.loads(SAMPLES.read_text().splitlines()[index])
    return message


def test_go_timestamps_with_nanoseconds_or_none() -> None:
    assert parse_aisstream_time("2026-11-14 18:22:32.318353123 +0000 UTC") == datetime(
        2026, 11, 14, 18, 22, 32, 318353, tzinfo=UTC
    )
    assert parse_aisstream_time("2026-11-14 18:22:40 +0000 UTC") == datetime(
        2026, 11, 14, 18, 22, 40, tzinfo=UTC
    )
    assert parse_aisstream_time("2026-11-14 18:23:01.5 +0000 UTC").microsecond == 500000


def test_position_report() -> None:
    record = to_record(sample(0))

    assert record == Position(
        mmsi=227000101,
        ts=datetime(2026, 11, 14, 18, 22, 32, 318353, tzinfo=UTC),
        lat=16.2101,
        lon=-61.5402,
        sog_kn=23.8,
        cog_deg=201.3,
        heading_deg=203,
        nav_status=0,
        ship_name="ANTILLES EXPRESS",
    )


def test_not_available_sentinels_become_nulls() -> None:
    record = to_record(sample(1))

    assert isinstance(record, Position)
    assert (record.sog_kn, record.cog_deg, record.heading_deg) == (None, None, None)
    assert record.ship_name == "KANOT 2"  # '@' padding removed


def test_ship_static_data() -> None:
    record = to_record(sample(2))

    assert record == ShipStatic(
        mmsi=228000202,
        ts=datetime(2026, 11, 14, 18, 23, 1, 500000, tzinfo=UTC),
        name="CMA CGM CARIBE",
        ship_type=70,
        call_sign="FCG202",
        imo=9123456,
        destination="POINTE A PITRE",
        length_m=180,  # A + B: bow and stern from the antenna
        beam_m=30,
        draught_m=9.8,
    )


@pytest.mark.parametrize(
    ("index", "error"),
    [
        (3, "position not available"),  # latitude 91, longitude 181
        (4, "mmsi"),  # not a 9-digit MMSI
        (5, "AidsToNavigationReport"),  # a buoy, not a ship
        (6, "time_utc"),  # truncated message
    ],
)
def test_unusable_messages_raise(index: int, error: str) -> None:
    with pytest.raises((KeyError, ValueError)) as caught:
        to_record(sample(index))
    assert error in str(caught.value)
