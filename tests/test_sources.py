from __future__ import annotations

import asyncio
import json
from pathlib import Path

import websockets

from ais_stream.sources import LESSER_ANTILLES, live, replay

SAMPLES = Path(__file__).parent / "fixtures" / "aisstream_samples.jsonl"


def test_replay_yields_every_non_empty_line(tmp_path: Path) -> None:
    recording = tmp_path / "rec.jsonl"
    recording.write_text("a\n\nb\n")
    assert list(replay(recording)) == ["a", "b"]


def test_live_subscribes_and_reconnects_after_a_drop() -> None:
    """A local WebSocket server plays aisstream: it checks the subscription, sends two
    messages and hangs up; the client must reconnect and keep going."""
    lines = SAMPLES.read_text().splitlines()[:2]
    subscriptions: list[dict[str, object]] = []

    async def server(socket: websockets.ServerConnection) -> None:
        subscriptions.append(json.loads(await socket.recv()))
        for line in lines:
            await socket.send(line)
        await socket.close()

    async def scenario() -> list[str]:
        async with websockets.serve(server, "127.0.0.1", 0) as running:
            port = running.sockets[0].getsockname()[1]
            received = []
            stream = live("test-key", url=f"ws://127.0.0.1:{port}", max_backoff_s=0.1)
            async for raw in stream:
                received.append(raw)
                if len(received) == 4:  # two connections' worth
                    break
            await stream.aclose()
            return received

    received = asyncio.run(asyncio.wait_for(scenario(), timeout=20))

    assert received == lines + lines
    assert len(subscriptions) == 2
    assert subscriptions[0] == {
        "APIKey": "test-key",
        "BoundingBoxes": [LESSER_ANTILLES],
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }
