"""The producer against a real Kafka broker."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, ConfigResource

from ais_stream.kafka import DLQ, POSITIONS, SHIPS, AisProducer, create_topics
from ais_stream.pipeline import handle
from ais_stream.synthetic import traffic

pytestmark = pytest.mark.integration


def consume_all(bootstrap: str, topic: str, expected: int) -> list[tuple[str, dict[str, Any]]]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"test-{topic}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    out: list[tuple[str, dict[str, Any]]] = []
    idle = 0  # consecutive empty polls; 5 s of silence ends the read
    while len(out) < expected and idle < 5:
        message = consumer.poll(1.0)
        if message is None or message.error():
            idle += 1
            continue
        idle = 0
        key = message.key().decode() if message.key() else ""
        out.append((key, json.loads(message.value())))
    consumer.close()
    return out


def test_topics_are_created_once_with_their_retention_policy(kafka: str) -> None:
    assert sorted(create_topics(kafka)) == sorted([POSITIONS, SHIPS, DLQ])
    assert create_topics(kafka) == []

    admin = AdminClient({"bootstrap.servers": kafka})
    resource = ConfigResource("topic", SHIPS)
    config = admin.describe_configs([resource])[resource].result()
    # Ships are a table: Kafka keeps the latest description per MMSI.
    assert config["cleanup.policy"].value == "compact"


def test_synthetic_hour_lands_in_kafka_in_per_ship_order(kafka: str) -> None:
    create_topics(kafka)
    messages = traffic(datetime(2026, 11, 14, 6, tzinfo=UTC), hours=1)
    producer = AisProducer(kafka)

    records = sum(handle(json.dumps(m), producer) for m in messages)
    handle("garbage", producer)
    expected_positions = sum(m["MessageType"] == "PositionReport" for m in messages)

    assert producer.flush() == 0
    assert producer.failed == 0
    assert producer.delivered == records + 1
    positions = consume_all(kafka, POSITIONS, expected_positions)
    ships = consume_all(kafka, SHIPS, records - expected_positions)
    assert len(positions) == expected_positions
    assert len(positions) + len(ships) == records
    assert [v["raw"] for _, v in consume_all(kafka, DLQ, 1)] == ["garbage"]

    # Keyed by MMSI: each ship's positions sit in one partition, so a consumer reads them
    # in the order they were sent.
    by_ship: dict[str, list[str]] = defaultdict(list)
    for key, value in positions:
        assert key == str(value["mmsi"])
        by_ship[key].append(value["ts"])
    assert len(by_ship) == 9
    assert all(times == sorted(times) for times in by_ship.values())
