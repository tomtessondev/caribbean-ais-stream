"""Command-line entry point: `ais-stream topics | live | replay | synthesize`."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import typer

from ais_stream.kafka import AisProducer, create_topics
from ais_stream.messages import parse_aisstream_time
from ais_stream.pipeline import handle
from ais_stream.sources import live as live_source
from ais_stream.sources import replay as replay_source
from ais_stream.synthetic import traffic

app = typer.Typer(help="Lesser Antilles ship traffic, from AIS to Kafka", no_args_is_help=True)

Bootstrap = typer.Option(
    os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"), "--bootstrap", help="Kafka bootstrap servers"
)


@app.callback()
def main(verbose: bool = typer.Option(False, "--verbose", "-v", help="Debug logging")) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


@app.command()
def topics(bootstrap: str = Bootstrap) -> None:
    """Create the Kafka topics (positions, ships, dead letters) if they are missing."""
    created = create_topics(bootstrap)
    typer.echo(f"Created {', '.join(created) or 'nothing (all topics exist)'}")


@app.command()
def live(
    bootstrap: str = Bootstrap,
    api_key: str = typer.Option(..., envvar="AISSTREAM_API_KEY", help="aisstream.io API key"),
) -> None:
    """Stream the Lesser Antilles from aisstream.io into Kafka until stopped."""
    if not api_key.strip():
        raise typer.BadParameter("get a free key at https://aisstream.io", param_hint="api_key")
    producer = AisProducer(bootstrap)

    async def run() -> None:
        async for raw in live_source(api_key):
            handle(raw, producer)

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        typer.echo(f"Delivered {producer.delivered}, failed {producer.failed}")


@app.command()
def replay(
    recording: Path = typer.Argument(
        ..., exists=True, help="JSON lines, one aisstream message each"
    ),
    bootstrap: str = Bootstrap,
    speed: float = typer.Option(0, min=0, help="1 = real pace, 10 = ten times faster, 0 = at once"),
) -> None:
    """Send a recording through the same parsing as the live feed."""
    producer = AisProducer(bootstrap)
    records = sum(handle(raw, producer) for raw in replay_source(recording, speed))
    pending = producer.flush()
    typer.echo(
        f"Records {records}, delivered {producer.delivered}, failed {producer.failed},"
        f" pending {pending}"
    )
    if producer.failed or pending:
        raise typer.Exit(1)


@app.command()
def synthesize(
    out: Path = typer.Option(Path("data/synthetic.jsonl"), help="JSON lines to write"),
    hours: float = typer.Option(6, min=0.1, help="Hours of traffic"),
    seed: int = typer.Option(7, help="Same seed, same traffic"),
    ending_now: bool = typer.Option(True, help="End at the current time (else start at it)"),
) -> None:
    """Write synthetic traffic in aisstream's format, for working without an API key."""
    now = datetime.now(UTC).replace(microsecond=0)
    start = now - timedelta(hours=hours) if ending_now else now
    messages = traffic(start, hours, seed)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("".join(json.dumps(m) + "\n" for m in messages), encoding="utf-8")
    typer.echo(f"Wrote {len(messages)} messages to {out}")


@app.command()
def demo(
    bootstrap: str = Bootstrap,
    hours: float = typer.Option(24, min=0.1, help="Hours of traffic to play"),
    seed: int = typer.Option(7, help="Same seed, same traffic"),
) -> None:
    """Play synthetic traffic into Kafka at the real pace, timestamped from now on, as if
    the ships were out there: the stand-in for `live` without an aisstream.io key."""
    producer = AisProducer(bootstrap)
    start = datetime.now(UTC).replace(microsecond=0)
    sent = 0
    try:
        for message in traffic(start, hours, seed):
            due = parse_aisstream_time(message["MetaData"]["time_utc"])
            wait = (due - datetime.now(UTC)).total_seconds()
            if wait > 0:
                producer.flush(0)  # deliver what is queued before idling
                time.sleep(wait)
            sent += handle(json.dumps(message), producer)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        typer.echo(f"Played {sent} records, delivered {producer.delivered}")
