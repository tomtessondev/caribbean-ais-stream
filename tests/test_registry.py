"""Schema evolution rules, enforced by a real Confluent Schema Registry: FULL_TRANSITIVE
accepts an optional field with a default and refuses what would break Flink (an older
reader) or a reprocessing of the topic (old records read by a newer reader)."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pytest
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from ais_stream.avro import COMPATIBILITY, SUBJECTS, register_schemas

pytestmark = pytest.mark.integration

SUBJECT = "ais.positions-value"


def candidate(change: Callable[[list[dict[str, Any]]], None]) -> Schema:
    schema = json.loads(SUBJECTS[SUBJECT])
    change(schema["fields"])
    return Schema(json.dumps(schema), "AVRO")


def is_compatible(registry: str, schema: Schema) -> bool:
    register_schemas(registry)
    result: bool = SchemaRegistryClient({"url": registry}).test_compatibility(SUBJECT, schema)
    return result


CHANGES: dict[str, Callable[[list[dict[str, Any]]], None]] = {
    # Flink's SQL reads lat: a schema without it breaks the running job.
    "remove lat": lambda fields: fields.remove(next(f for f in fields if f["name"] == "lat")),
    # Records already in the topic have no value for it: a reprocessing could not read them.
    "required field without default": lambda fields: fields.append(
        {"name": "rot_deg_min", "type": "double"}
    ),
    "mmsi as string": lambda fields: fields[0].update(type="string"),
}


def test_registering_twice_keeps_the_same_ids(schema_registry: str) -> None:
    assert register_schemas(schema_registry) == register_schemas(schema_registry)
    client = SchemaRegistryClient({"url": schema_registry})
    assert all(client.get_compatibility(subject) == COMPATIBILITY for subject in SUBJECTS)


def test_an_optional_field_is_a_compatible_change(schema_registry: str) -> None:
    # The AIS rate of turn, which the pipeline drops today.
    rate_of_turn = {"name": "rot_deg_min", "type": ["null", "double"], "default": None}
    assert is_compatible(schema_registry, candidate(lambda fields: fields.append(rate_of_turn)))


@pytest.mark.parametrize("change", list(CHANGES))
def test_changes_that_would_break_flink_or_a_replay_are_refused(
    schema_registry: str, change: str
) -> None:
    assert not is_compatible(schema_registry, candidate(CHANGES[change]))
