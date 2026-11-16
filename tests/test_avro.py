"""The Avro schemas describe the same records as the pydantic models, and records survive a
trip through the serializers (with an in-memory registry: no network)."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.serialization import MessageField, SerializationContext
from pydantic import BaseModel
from typer.testing import CliRunner

from ais_stream.avro import SUBJECTS, value_deserializer, value_serializer
from ais_stream.cli import app
from ais_stream.kafka import POSITIONS, SHIPS
from ais_stream.messages import Position, ShipStatic, to_record
from ais_stream.synthetic import traffic

START = datetime(2026, 11, 14, 6, tzinfo=UTC)
FISHING_BOAT = 227000601  # no gyro compass: heading not available, so None


@pytest.mark.parametrize(
    ("model", "subject"), [(Position, "ais.positions-value"), (ShipStatic, "ais.ships-value")]
)
def test_schema_and_model_describe_the_same_fields(model: type[BaseModel], subject: str) -> None:
    fields = {f["name"]: f for f in json.loads(SUBJECTS[subject])["fields"]}
    assert list(fields) == list(model.model_fields)
    for name, info in model.model_fields.items():
        nullable = isinstance(fields[name]["type"], list) and "null" in fields[name]["type"]
        assert nullable == (not info.is_required()), name
        if nullable:
            assert fields[name]["default"] is None, name


def test_records_round_trip_through_avro() -> None:
    client = SchemaRegistryClient.new_client({"url": "mock://avro-test"})  # in memory
    for subject, schema in SUBJECTS.items():
        client.register_schema(subject, Schema(schema, "AVRO"))
    records = [to_record(m) for m in traffic(START, hours=0.2)]
    fishing = next(r for r in records if isinstance(r, Position) and r.mmsi == FISHING_BOAT)
    ship = next(r for r in records if isinstance(r, ShipStatic))
    assert fishing.heading_deg is None
    for record, topic in [(fishing, POSITIONS), (ship, SHIPS)]:
        ctx = SerializationContext(topic, MessageField.VALUE)
        data = value_serializer(client, topic)(record, ctx)
        assert data is not None and data[0] == 0  # Confluent wire format: magic byte, schema id
        assert type(record)(**value_deserializer(client, topic)(data, ctx)) == record


def test_topics_fails_clearly_without_a_registry() -> None:
    result = CliRunner().invoke(
        app, ["topics", "--bootstrap", "localhost:1", "--registry", "http://localhost:1"]
    )
    assert result.exit_code == 1
    assert "schema registry at http://localhost:1" in result.output.lower()
