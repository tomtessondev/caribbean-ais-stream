# Avro Schemas Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Positions and ship records in Avro, governed by Confluent Schema Registry with `FULL_TRANSITIVE` compatibility, read by Flink with `avro-confluent`.

**Architecture:** `.avsc` files packaged with `ais_stream` are the source of truth; `ais_stream.schemas` loads them, registers them (from the `topics` command) and builds the serializers the producer uses. Flink's `positions` table switches format; everything downstream is unchanged. The dead-letter topic stays JSON.

**Tech Stack:** confluent-kafka[avro,schemaregistry] (fastavro), Confluent Schema Registry, Flink 1.20 `flink-sql-avro-confluent-registry`, pytest, testcontainers.

**Spec:** `docs/superpowers/specs/2026-11-15-avro-schemas-design.md`

## Global Constraints

- Branch `feat/avro`; code, comments, commits in English; French page in French.
- Subjects `ais.positions-value`, `ais.ships-value` (topic-name strategy), namespace `antilles.ais`, compatibility `FULL_TRANSITIVE` set per subject.
- Producer: `auto.register.schemas: False`, `use.latest.version: False`; keys stay the MMSI as a UTF-8 string; `ais.dlq` stays JSON.
- `ts` as `long` with `logicalType: timestamp-micros`; nullable fields `["null", T]` with `"default": null`; `lat`, `lon`, speeds, course, draught as `double`; MMSI and IMO as `long`; other integers `int`.
- Registry URL setting `SCHEMA_REGISTRY_URL`, default `http://localhost:8081`; Compose `http://schema-registry:8081`; host port **8081**.
- Registry image pinned (`confluentinc/cp-schema-registry:7.9.0` unless it cannot talk to Kafka 3.9.1; then the nearest working 7.9.x, ledgered). Flink jar `flink-sql-avro-confluent-registry-1.20.2.jar` from Maven Central.
- Before each commit: `make check`, `make integration`; tasks touching the stack also `make e2e-up && make e2e`.

## Review Focus

- `topics` pointed at an unreachable registry must fail with a clear error, not hang or report success — Task 2 test `test_topics_fails_clearly_without_a_registry`.
- Running `topics` twice (every `docker compose up`) must be a no-op that keeps the same schema ids — Task 2 test `test_registering_twice_keeps_the_same_ids`.
- Serializing a record with a null optional field (heading 511 → None, the fishing boats) must work and read back as None — Task 1 test `test_records_round_trip_through_avro` uses a fishing-boat position.
- A rejected message must still land in `ais.dlq` as readable JSON — Task 2 keeps the existing DLQ assertion in `tests/test_kafka.py`.
- The Flink job must keep event time exactly (same windows, same anomalies) — Task 3: the full e2e suite passes unchanged.

---

### Task 1: Schemas as files, checked against the models

**Files:**
- Create: `src/ais_stream/schemas/position.avsc`, `src/ais_stream/schemas/ship_static.avsc`, `src/ais_stream/avro.py`, `tests/test_avro.py`
  (the `.avsc` live inside the package rather than at the repository root as the spec
  sketched, so the Docker image and an installed package find them)
- Modify: `pyproject.toml` (dependency `confluent-kafka[avro,schemaregistry]>=2.5`; make sure hatch includes `*.avsc` in the wheel)

**Interfaces:**
- Produces in `ais_stream.avro`:
  - `SCHEMA_DIR: Path` (the `schemas/` folder inside the package)
  - `COMPATIBILITY = "FULL_TRANSITIVE"`
  - `SUBJECTS: dict[str, str]` — `{"ais.positions-value": <position.avsc text>, "ais.ships-value": <ship_static.avsc text>}`
  - `def value_serializer(client: SchemaRegistryClient, topic: str) -> AvroSerializer` (`to_dict` = `model.model_dump()`, config per Global Constraints)
  - `def value_deserializer(client: SchemaRegistryClient, topic: str) -> AvroDeserializer`

- [ ] **Step 1: Failing tests** in `tests/test_avro.py`:

```python
@pytest.mark.parametrize(("model", "subject"), [(Position, "ais.positions-value"), (ShipStatic, "ais.ships-value")])
def test_schema_and_model_describe_the_same_fields(model, subject) -> None:
    fields = {f["name"]: f for f in json.loads(SUBJECTS[subject])["fields"]}
    assert list(fields) == list(model.model_fields)
    for name, info in model.model_fields.items():
        nullable = isinstance(fields[name]["type"], list) and "null" in fields[name]["type"]
        assert nullable == (not info.is_required()), name
        if nullable:
            assert fields[name]["default"] is None, name

def test_records_round_trip_through_avro() -> None:
    client = SchemaRegistryClient({"url": "mock://avro-test"})
    for subject, schema in SUBJECTS.items():
        client.register_schema(subject, Schema(schema, "AVRO"))
    fishing = <a Position with heading_deg=None, built from synthetic traffic's KANOT 1>
    ship = <a ShipStatic from synthetic traffic>
    for record, topic in [(fishing, POSITIONS), (ship, SHIPS)]:
        ctx = SerializationContext(topic, MessageField.VALUE)
        data = value_serializer(client, topic)(record, ctx)
        assert data[0] == 0  # Confluent wire format: magic byte, then the schema id
        assert type(record)(**value_deserializer(client, topic)(data, ctx)) == record
```

- [ ] **Step 2:** `.venv/bin/pytest tests/test_avro.py --no-cov` → FAIL (`ais_stream.avro` missing).
- [ ] **Step 3:** write the two `.avsc` (field order = model order, `doc` from the model comments) and `avro.py`; `pip install -e ".[dev]"`.
- [ ] **Step 4:** `make check` → pass.
- [ ] **Step 5:** commit "Describe positions and ships as Avro schemas".

---

### Task 2: Registry, registration and the Avro producer

**Files:**
- Modify: `src/ais_stream/avro.py` (registration), `src/ais_stream/kafka.py` (`AisProducer`), `src/ais_stream/cli.py` (`--registry` option, `topics` registers), `tests/conftest.py` (network, registry fixture), `tests/test_kafka.py`
- Create: `tests/test_registry.py`

**Interfaces:**
- Produces:
  - `ais_stream.avro.register_schemas(registry_url: str) -> dict[str, int]` — sets `COMPATIBILITY` on each subject, registers each schema, returns subject → id; raises `SchemaRegistryError` (the client's) on refusal or connection failure.
  - `AisProducer(bootstrap: str, registry_url: str)` — positions and ships via `value_serializer`, rejects unchanged (JSON).
  - CLI: `Registry = typer.Option(os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"), "--registry")` on `topics`, `live`, `replay`, `demo`; `topics` prints the registered ids.
  - Fixtures: `kafka` (now also reachable as `kafka:19092` on a Docker network) and `schema_registry -> str` (URL on the host).

- [ ] **Step 1: Failing tests**
  - `tests/test_registry.py` (marked `integration`):

```python
def test_registering_twice_keeps_the_same_ids(schema_registry: str) -> None:
    assert register_schemas(schema_registry) == register_schemas(schema_registry)
    client = SchemaRegistryClient({"url": schema_registry})
    assert all(client.get_compatibility(s) == "FULL_TRANSITIVE" for s in SUBJECTS)


def test_an_optional_field_is_a_compatible_change(schema_registry: str) -> None:
    # rate of turn, as AIS sends it and the pipeline drops today
    assert is_compatible(schema_registry, with_field("rot_deg_min", ["null", "double"], None))


@pytest.mark.parametrize(
    "change", ["remove lat", "required field without default", "mmsi as string"]
)
def test_changes_that_would_break_flink_or_a_replay_are_refused(
    schema_registry: str, change: str
) -> None:
    assert not is_compatible(schema_registry, CHANGES[change])
```

    (`is_compatible` = `client.test_compatibility("ais.positions-value", Schema(..., "AVRO"))` after `register_schemas`; `CHANGES` builds each candidate from `SUBJECTS["ais.positions-value"]`.)
  - `tests/test_kafka.py`: `AisProducer(kafka, schema_registry)`, `consume_all` decodes positions/ships with `value_deserializer` (DLQ still `json.loads`).
  - Unit, in `tests/test_avro.py`:

```python
def test_topics_fails_clearly_without_a_registry() -> None:
    result = CliRunner().invoke(
        app, ["topics", "--bootstrap", "localhost:1", "--registry", "http://localhost:1"]
    )
    assert result.exit_code != 0
```

    (Order inside `topics`: register schemas first, then create topics, so the unit test needs no broker. If that order is wrong for another reason, ledger it.)
- [ ] **Step 2:** `make check` and `make integration` → the new tests FAIL.
- [ ] **Step 3: Implement**
  - Fixtures: a `testcontainers` `Network`; Kafka gets alias `kafka` and a second listener `DOCKER://:19092` advertised as `kafka:19092` (keep the host listener as is); `schema_registry` container on the same network (`SCHEMA_REGISTRY_HOST_NAME`, `SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081`, `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://kafka:19092`), wait for `GET /subjects`.
  - `register_schemas`, `AisProducer`, CLI options as in Interfaces.
- [ ] **Step 4:** `make check`, `make integration` → pass.
- [ ] **Step 5:** commit "Register the schemas with FULL_TRANSITIVE compatibility and produce Avro".

---

### Task 3: The stack and Flink on Avro

**Files:**
- Modify: `docker-compose.yml`, `flink/Dockerfile`, `flink/sql/ais_jobs.sql`, `Makefile` (`e2e-up`), `tests/test_e2e.py`

- [ ] **Step 1: Failing test**: `tests/test_e2e.py` `play()` uses `AisProducer(KAFKA, REGISTRY)` with `REGISTRY = os.getenv("E2E_REGISTRY", "http://localhost:8081")`; add

```python
def test_the_registry_holds_both_schemas(played: datetime) -> None:
    client = SchemaRegistryClient({"url": REGISTRY})
    assert sorted(client.get_subjects()) == ["ais.positions-value", "ais.ships-value"]
```

- [ ] **Step 2:** `make e2e-up && make e2e` → FAIL (no registry).
- [ ] **Step 3: Implement**
  - `schema-registry` service (image per Global Constraints, `8081:8081`, healthcheck on `/subjects`, depends on Kafka healthy); `x-app` env `SCHEMA_REGISTRY_URL`; `topics` depends on it healthy; Kafka UI `KAFKA_CLUSTERS_0_SCHEMAREGISTRY: http://schema-registry:8081`; `make e2e-up` starts it.
  - `flink/Dockerfile`: add the jar (comment why).
  - `positions` table: `'format' = 'avro-confluent'`, `'avro-confluent.url' = 'http://schema-registry:8081'`. Check Flink 1.20's Avro timestamp mapping for `timestamp-micros` (`avro-confluent.timestamp_mapping.legacy`) so `ts` stays a `TIMESTAMP_LTZ` with the same instant; adjust the column type or the option, not the event-time logic. Comment in the SQL.
- [ ] **Step 4:** `make e2e-up && make e2e` → all e2e pass, unchanged expectations (9 ships, 45-min silence, 2 jumps, 12 port events, reprocessing identical). Open Kafka UI (browse skill) on `ais.positions`: messages shown decoded, schema listed.
- [ ] **Step 5:** commit "Read positions in Avro from the registry in Flink".

---

### Task 4: Docs and PR

**Files:**
- Modify: `README.md`, `docs/ROADMAP.md`, `CLAUDE.md`, root `../CLAUDE.md` (port 8081)

- [ ] **Step 1:** README: architecture (registry box), tooling rows (Avro, Schema Registry), a "Schemas" section (where the `.avsc` live, `FULL_TRANSITIVE` and why, how to evolve: edit the `.avsc` and the model together, `topics` registers or refuses, DLQ stays JSON), quick start port 8081, reset note (`docker compose down -v` when switching an existing stack). ROADMAP step 5 "(done)". CLAUDE.md stack lines. Root CLAUDE.md ports: Schema Registry 8081.
- [ ] **Step 2:** `make check && make integration` → pass; commit "Document the Avro schemas and their evolution rules".
- [ ] **Step 3:** after the final review: push, PR, CI green; merge only with the owner's go.

---

### Task 5: French page

**Files:**
- Modify: `antilles-data-pipeline/dashboard/src/app/projets/caribbean-ais-stream/page.tsx`

- [ ] **Step 1:** branch `docs/ais-avro`; roadmap step 5 "fait"; a short section "Schémas et évolution" (contrat Avro, registre, compatibilité totale et transitive expliquée simplement : ce qui est accepté, ce qui est refusé et pourquoi Flink en dépend); technologies rows Avro and Schema Registry.
- [ ] **Step 2:** `dashboard/`: lint, typecheck, test, build; root `make check`, `make transform` → pass.
- [ ] **Step 3:** commit, push after the final review, PR; merge only with the owner's go.
