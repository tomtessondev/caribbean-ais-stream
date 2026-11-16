"""Kafka side: topics, and a producer that never loses or reorders a ship's messages."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from ais_stream.avro import registry_client, value_serializer
from ais_stream.messages import Position, Record, Rejected, ShipStatic

logger = logging.getLogger(__name__)

POSITIONS = "ais.positions"
SHIPS = "ais.ships"
DLQ = "ais.dlq"

_DAY_MS = 24 * 3600 * 1000

# Positions are an event log, kept a week. Ships are a table: compaction keeps the latest
# description per MMSI forever, so a new consumer can rebuild the fleet from the topic.
TOPICS: dict[str, dict[str, str]] = {
    POSITIONS: {"retention.ms": str(7 * _DAY_MS)},
    SHIPS: {"cleanup.policy": "compact"},
    DLQ: {"retention.ms": str(14 * _DAY_MS)},
}


def create_topics(bootstrap: str, partitions: int = 3) -> list[str]:
    """Create the missing topics. Returns the ones created."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    missing = [
        NewTopic(name, num_partitions=partitions, replication_factor=1, config=config)
        for name, config in TOPICS.items()
        if name not in existing
    ]
    for name, future in admin.create_topics(missing).items() if missing else []:
        future.result()
        logger.info("Created topic %s", name)
    return [t.topic for t in missing]


def topic_for(record: Record) -> str:
    return POSITIONS if isinstance(record, Position) else SHIPS


@dataclass
class AisProducer:
    """Wraps a Kafka producer configured for durability and per-ship ordering:

    - key = MMSI, so all messages of one ship land in one partition, in order;
    - acks=all and idempotence: a retried send is neither lost nor duplicated;
    - small linger and zstd compression: AIS messages are tiny and numerous.
    """

    bootstrap: str
    registry_url: str
    delivered: int = 0
    failed: int = 0
    _producer: Any = field(init=False)
    _serializers: dict[str, AvroSerializer] = field(init=False)

    def __post_init__(self) -> None:
        # Positions and ships in Avro, with the schemas registered by `topics`; rejects stay
        # JSON, readable without the registry.
        client = registry_client(self.registry_url)
        self._serializers = {topic: value_serializer(client, topic) for topic in (POSITIONS, SHIPS)}
        self._producer = Producer(
            {
                "bootstrap.servers": self.bootstrap,
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 50,
                "compression.type": "zstd",
            }
        )

    def _on_delivery(self, error: KafkaError | None, _message: Message) -> None:
        if error is None:
            self.delivered += 1
        else:
            self.failed += 1
            logger.error("Delivery failed: %s", error)

    def _send(self, topic: str, key: str, value: str | bytes | None) -> None:
        while True:
            try:
                self._producer.produce(topic, key=key, value=value, on_delivery=self._on_delivery)
                break
            except BufferError:
                self._producer.poll(0.5)  # local queue full: let deliveries drain
        self._producer.poll(0)

    def send(self, record: Position | ShipStatic) -> None:
        topic = topic_for(record)
        value = self._serializers[topic](record, SerializationContext(topic, MessageField.VALUE))
        self._send(topic, str(record.mmsi), value)

    def reject(self, rejected: Rejected) -> None:
        self._send(DLQ, "", rejected.model_dump_json())

    def flush(self, timeout: float = 30) -> int:
        """Waits for outstanding deliveries. Returns how many are still pending."""
        pending: int = self._producer.flush(timeout)
        return pending
