"""Where raw AIS messages come from: aisstream.io live, a recorded file, or nothing else.

Both sources yield the raw JSON text of each message, exactly as aisstream sends it, so
replaying a recording exercises the same parsing as production.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import websockets

logger = logging.getLogger(__name__)

AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"

# The Lesser Antilles, from the Virgin Islands to Grenada, as [[south, west], [north, east]].
LESSER_ANTILLES = [[11.8, -64.0], [18.8, -59.3]]
MESSAGE_TYPES = ["PositionReport", "ShipStaticData"]


def subscription(api_key: str) -> str:
    return json.dumps(
        {
            "APIKey": api_key,
            "BoundingBoxes": [LESSER_ANTILLES],
            "FilterMessageTypes": MESSAGE_TYPES,
        }
    )


async def live(
    api_key: str, url: str = AISSTREAM_URL, max_backoff_s: float = 60
) -> AsyncIterator[str]:
    """Messages from aisstream.io, forever. Reconnects with exponential backoff: the feed
    drops idle or slow clients, and a ship is not lost for a few seconds of reconnection."""
    backoff = 1.0
    while True:
        try:
            async with websockets.connect(url, open_timeout=20) as socket:
                await socket.send(subscription(api_key))
                logger.info("Subscribed to aisstream.io for the Lesser Antilles")
                backoff = 1.0
                async for raw in socket:
                    yield raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
        except (OSError, websockets.WebSocketException) as exc:
            logger.warning("aisstream connection lost (%s); retrying in %.0f s", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff_s)


def replay(path: Path, speed: float = 0) -> Iterator[str]:
    """Messages from a JSON-lines recording. With `speed` > 0, keeps the recorded pacing
    (2 = twice as fast); 0 sends everything as fast as possible."""
    previous: float | None = None
    with path.open(encoding="utf-8") as lines:
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if speed > 0:
                stamp = _stamp(line)
                if previous is not None and stamp is not None and stamp > previous:
                    time.sleep((stamp - previous) / speed)
                previous = stamp if stamp is not None else previous
            yield line


def _stamp(line: str) -> float | None:
    from ais_stream.messages import parse_aisstream_time

    try:
        return parse_aisstream_time(json.loads(line)["MetaData"]["time_utc"]).timestamp()
    except (KeyError, ValueError, TypeError):
        return None
