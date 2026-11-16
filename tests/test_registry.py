"""Schema evolution rules, enforced by a real Confluent Schema Registry: FULL_TRANSITIVE
accepts an optional field with a default and refuses what would break Flink (an older
reader) or a reprocessing of the topic (old records read by a newer reader)."""

from __future__ import annotations

import json
import socket
import time
from collections.abc import Callable
from typing import Any

import pytest
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from typer.testing import CliRunner

from ais_stream.avro import COMPATIBILITY, SUBJECTS, register_schemas
from ais_stream.cli import app

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
    # Readers on the current schema have no default for lat: Flink would read it as null,
    # and port areas and the map would go wrong without any error.
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


def test_topics_gives_up_on_a_registry_that_never_answers() -> None:
    """A host that accepts the connection and then stays silent (wrong address behind a
    proxy, a hung registry) must not block the start forever."""
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        server.listen()
        url = f"http://127.0.0.1:{server.getsockname()[1]}"
        started = time.monotonic()
        result = CliRunner().invoke(
            app, ["topics", "--bootstrap", "localhost:1", "--registry", url]
        )
    assert result.exit_code == 1
    assert time.monotonic() - started < 60


def test_topics_stops_on_an_incompatible_schema(
    schema_registry: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Someone edits position.avsc and drops lat: the next start refuses it, with the
    registry's reason, before anything is produced."""
    register_schemas(schema_registry)
    without_lat = candidate(CHANGES["remove lat"]).schema_str
    monkeypatch.setitem(SUBJECTS, SUBJECT, without_lat)
    args = ["topics", "--bootstrap", "localhost:1", "--registry", schema_registry]
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 1
    assert "incompatible" in result.output.lower()


def test_compatibility_is_checked_against_every_version(schema_registry: str) -> None:
    """Transitive: v2 gives lat a default (compatible), v3 then drops lat. v3 is fine next
    to v2, but v1 readers (a Flink job not yet redeployed) have no default for lat."""
    client = SchemaRegistryClient({"url": schema_registry})
    v1 = json.loads(SUBJECTS[SUBJECT])
    v2 = json.loads(SUBJECTS[SUBJECT])
    next(f for f in v2["fields"] if f["name"] == "lat")["default"] = 0.0
    v3 = json.loads(SUBJECTS[SUBJECT])
    v3["fields"] = [f for f in v3["fields"] if f["name"] != "lat"]

    def accepted(level: str) -> bool:
        subject = f"transitivity-{level.lower()}-value"
        client.set_compatibility(subject_name=subject, level=level)
        for schema in (v1, v2):
            client.register_schema(subject, Schema(json.dumps(schema), "AVRO"))
        result: bool = client.test_compatibility_all_versions(
            subject, Schema(json.dumps(v3), "AVRO")
        )
        return result

    assert accepted("FULL")  # only checked against v2
    assert not accepted(COMPATIBILITY)  # checked against v1 too
