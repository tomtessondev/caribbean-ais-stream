from __future__ import annotations

from pathlib import Path

import pytest

from ais_stream.messages import Position, Rejected, ShipStatic
from ais_stream.pipeline import FeedError, handle

SAMPLES = Path(__file__).parent / "fixtures" / "aisstream_samples.jsonl"


class FakeSink:
    def __init__(self) -> None:
        self.records: list[Position | ShipStatic] = []
        self.rejected: list[Rejected] = []

    def send(self, record: Position | ShipStatic) -> None:
        self.records.append(record)

    def reject(self, rejected: Rejected) -> None:
        self.rejected.append(rejected)


def test_every_message_ends_up_somewhere() -> None:
    sink = FakeSink()
    lines = SAMPLES.read_text().splitlines()

    results = [handle(line, sink) for line in lines]

    assert results == [True, True, True, False, False, False, False, False]
    assert len(sink.records) == 3
    assert len(sink.rejected) == 5
    assert sink.rejected[-1].raw == "not json at all"  # kept verbatim for diagnosis
    assert "position not available" in sink.rejected[0].reason


def test_feed_errors_stop_the_producer() -> None:
    with pytest.raises(FeedError, match="Api Key Is Not Valid"):
        handle('{"error": "Api Key Is Not Valid"}', FakeSink())
