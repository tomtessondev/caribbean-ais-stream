# Avro schemas with a schema registry (roadmap step 5) — design

## Intent

Step 5 of `docs/ROADMAP.md`: "Avro with a schema registry, so producers and Flink agree on
the records and evolve them safely." Today the producer writes pydantic models as JSON
(`model_dump_json`) and Flink reads `ais.positions` with the `json` format: nothing checks
that both sides agree, and nothing stops a change that would break a consumer. The project
is a portfolio piece; this step shows schema governance as a data engineer would run it.

## Goals

- `ais.positions` and `ais.ships` values in Avro (Confluent wire format), keys unchanged
  (MMSI as a string).
- Schemas as `.avsc` files in the repository, the source of truth, registered explicitly
  in Confluent Schema Registry with a stated compatibility mode; the producer does not
  auto-register.
- Flink reads `ais.positions` with the `avro-confluent` format; every existing result
  (windows, anomalies, map) unchanged.
- Kafka UI shows the decoded Avro messages and the schemas.
- Tests prove the evolution rules: a compatible change (a new optional field, e.g. the AIS
  rate of turn) is accepted by the registry; changes that would break Flink or a
  reprocessing (removing a required field, adding one without default, changing a type)
  are refused.
- A test proves the `.avsc` files and the pydantic models describe the same fields.
- Docs: README, ROADMAP (step 5 done), CLAUDE.md, root `CLAUDE.md` ports, French page.

## Non-goals

- Changing the records' content (no new field in production now; the rate of turn is only
  the evolution test's candidate).
- Consuming `ais.ships` in Flink.
- Avro keys, Protobuf or JSON Schema.
- Schema registry security (auth, TLS).

## Anti-goals

- Two sources of truth that can drift (hand-written `.avsc` and pydantic models nobody
  compares).
- A producer that silently registers whatever it is given (auto-registration would make
  the compatibility check a formality).
- Tests that need the network or a real registry for the unit suite.

## Constraints

- Flink 1.20 needs `flink-sql-avro-confluent-registry` (version matching 1.20) in the image.
- Python: `confluent-kafka[avro,schemaregistry]` (pulls `fastavro` and an HTTP client).
  The unit tests use the client's `mock://` registry URL; integration and e2e tests use a
  real registry container.
- Event time unchanged: `ts` keeps microsecond precision in Avro
  (`timestamp-micros`), and Flink still derives `event_time` and the 30 s watermark from
  it. The builder checks how Flink 1.20 maps Avro timestamps (`TIMESTAMP` vs
  `TIMESTAMP_LTZ`) and keeps the event-time semantics identical.
- Port: Schema Registry on host **8081** (free: Flink's UI is on 8082). Add it to the root
  `CLAUDE.md` port list.
- Switching an existing stack from JSON to Avro needs empty topics (Flink cannot read a
  topic mixing both): `docker compose down -v`, said in the README.

## Approaches

| Approach | Trade-off |
| --- | --- |
| **Confluent Schema Registry** (chosen) | The reference recruiters know; native `avro-confluent` format in Flink; Kafka UI shows schemas. Confluent Community licence (not Apache), irrelevant locally. |
| Karapace (Aiven) | Same API, Apache 2 licence; less known, heavier image. |
| Apicurio Registry (Red Hat) | Apache licence, Confluent-compatible API, more formats; more setup, less common in data missions. |

The owner picked Confluent Schema Registry.

## Design

### Schemas

`schemas/position.avsc` and `schemas/ship_static.avsc`, namespace `antilles.ais`, one field
per pydantic field, nullable fields as `["null", T]` with `default: null`, `ts` as
`long` + `timestamp-micros`, a `doc` on each field (the units and AIS conventions already
written in `messages.py`). Subjects with the topic-name strategy: `ais.positions-value`,
`ais.ships-value`.

The dead-letter topic `ais.dlq` stays JSON (my call): it holds what could not be parsed,
for a human to read, and should not depend on the registry being up.

### Compatibility

`FULL_TRANSITIVE` on both subjects (my call), set explicitly when the schemas are
registered. In this project the producer changes first (a new `.avsc`, registered by
`topics`) while Flink reads with the schema fixed in its DDL, so the registry must refuse
what an *older reader* cannot read (forward compatibility): removing `lat` would pass a
backward-only check and then break the Flink job at decoding. And Flink reprocesses the
topic from the earliest offset, so a *newer reader* must also read every older record
(backward compatibility). Full, transitive: every version against every other. In practice
the allowed changes are adding or removing an optional field that has a default.

### Registration

The `topics` command (already run once per start by the `topics` Compose service) also
sets the compatibility and registers the schemas from `schemas/`. Registering the same
schema again is a no-op; a schema the registry refuses as incompatible makes the command
fail with the registry's message, which stops the stack before anything is produced.

### Producer

`AisProducer` serialises `Position` / `ShipStatic` with confluent-kafka's
`AvroSerializer`, built from the `.avsc` of the topic, `auto.register.schemas: False`,
`use.latest.version: False` (it serialises with the schema it was given, which must be
registered). The pydantic models still validate; the serializer receives
`model.model_dump()`. A registry URL becomes a setting (`SCHEMA_REGISTRY_URL`, default
`http://localhost:8081`; Compose sets `http://schema-registry:8081`).

### Flink

`positions` table: `'format' = 'avro-confluent'`, `'avro-confluent.url' =
'http://schema-registry:8081'`. Column names and types as today. No other SQL change.

### Platform

- `schema-registry` service: `confluentinc/cp-schema-registry` (a pinned release that
  supports Kafka 3.9), host port 8081, depends on Kafka healthy; `topics`, `demo`, `live`
  and `flink-jobs` depend on it.
- Kafka UI: `KAFKA_CLUSTERS_0_SCHEMAREGISTRY`.
- `make e2e-up` starts it.

### Tests

- Unit (mock registry): a `Position` and a `ShipStatic` round-trip through the
  serializer and `AvroDeserializer` unchanged; the `.avsc` fields match the pydantic
  models (names, nullability); nullable fields have a `null` default.
- Integration (Kafka + registry containers): `topics` registers both subjects with
  `FULL_TRANSITIVE`; the rate-of-turn candidate (`rot_deg_min`, `["null","double"]`,
  default null) is compatible; removing `lat` (breaks the Flink reader), adding a required
  field without default (breaks reprocessing old records) and changing `mmsi` to `string`
  are each refused; the existing "synthetic hour lands in Kafka in per-ship order" test
  decodes Avro.
- E2E: unchanged assertions pass with Avro end to end.

### Docs

README (Avro, registry, compatibility, how to evolve a schema, the reset note, port 8081),
ROADMAP step 5 done, CLAUDE.md, root CLAUDE.md port, French page (roadmap status,
a short "Schémas" section, technologies table: Schema Registry, Avro).

## Left to the builder

- Exact registry and jar versions (pinned).
- The Avro ↔ Flink timestamp mapping details, as long as event time is unchanged.
- Where the registration code lives (`kafka.py` or a new `schemas.py`).
- Test fixture layout for the registry container.
