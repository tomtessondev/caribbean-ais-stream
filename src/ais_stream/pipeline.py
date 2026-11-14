"""One raw message in, one Kafka record out: a normalised record, or a reject in the DLQ."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Protocol

from pydantic import ValidationError

from ais_stream.messages import Position, Rejected, ShipStatic, to_record


class Sink(Protocol):
    def send(self, record: Position | ShipStatic) -> None: ...
    def reject(self, rejected: Rejected) -> None: ...


class FeedError(RuntimeError):
    """aisstream reported an error (typically an invalid API key) instead of data."""


def handle(raw: str, sink: Sink) -> bool:
    """Routes one message. Returns True for a record, False for a reject."""
    try:
        message = json.loads(raw)
    except json.JSONDecodeError as exc:
        sink.reject(Rejected(received_at=datetime.now(UTC), reason=f"not JSON: {exc}", raw=raw))
        return False
    if isinstance(message, dict) and "error" in message:
        raise FeedError(str(message["error"]))
    try:
        record = to_record(message)
    except (KeyError, TypeError, ValueError, ValidationError) as exc:
        # ValidationError is a ValueError; listed for the reader.
        reason = f"{type(exc).__name__}: {exc}"[:500]
        sink.reject(Rejected(received_at=datetime.now(UTC), reason=reason, raw=raw[:10_000]))
        return False
    sink.send(record)
    return True
