"""Avro contract of the Kafka records: the `.avsc` files in `schemas/` are the source of
truth, registered in Confluent Schema Registry and used by the producer to serialise.

Compatibility is FULL_TRANSITIVE. The producer changes first (a new schema registered by
the `topics` command) while Flink reads with the schema fixed in its SQL, so a new schema
must stay readable by every older reader (forward); Flink also reprocesses the topic from
the earliest offset, so every older record must stay readable by a newer reader
(backward). In practice: add or remove optional fields that have a default, nothing else.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import SerializationContext
from pydantic import BaseModel

SCHEMA_DIR = Path(__file__).parent / "schemas"
COMPATIBILITY = "FULL_TRANSITIVE"

# Topic-name strategy: the value schema of topic T is subject "T-value".
_FILES = {"ais.positions": "position.avsc", "ais.ships": "ship_static.avsc"}
SUBJECTS: dict[str, str] = {
    # Stripped as the serializer strips it, so the registered text and its own match.
    f"{topic}-value": (SCHEMA_DIR / name).read_text(encoding="utf-8").strip()
    for topic, name in _FILES.items()
}


def registry_client(registry_url: str) -> SchemaRegistryClient:
    """The client has no HTTP timeout by default: a registry that accepts the connection and
    never answers would block the start (or the producer's first schema lookup) forever."""
    return SchemaRegistryClient({"url": registry_url, "timeout": 10, "max.retries": 2})


def register_schemas(registry_url: str) -> dict[str, int]:
    """Sets the compatibility of each subject and registers the repository's schemas.
    Returns subject -> schema id. Registering an unchanged schema again returns the same id;
    a schema the registry finds incompatible raises its error, before anything is produced."""
    client = registry_client(registry_url)
    ids = {}
    for subject, schema in SUBJECTS.items():
        client.set_compatibility(subject_name=subject, level=COMPATIBILITY)
        ids[subject] = int(client.register_schema(subject, Schema(schema, "AVRO")))
    return ids


def _to_dict(record: BaseModel, _ctx: SerializationContext) -> dict[str, Any]:
    return record.model_dump()


def value_serializer(client: SchemaRegistryClient, topic: str) -> AvroSerializer:
    """Serialises with the repository's schema, which must already be registered: the
    producer never registers a schema itself, so only `topics` can change the contract."""
    serializer: AvroSerializer = AvroSerializer(
        client,
        SUBJECTS[f"{topic}-value"],
        _to_dict,
        {"auto.register.schemas": False, "use.latest.version": False},
    )
    return serializer


def value_deserializer(client: SchemaRegistryClient, topic: str) -> AvroDeserializer:
    deserializer: AvroDeserializer = AvroDeserializer(client, SUBJECTS[f"{topic}-value"])
    return deserializer
