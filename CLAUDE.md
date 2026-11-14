# CLAUDE.md

Context for Claude Code working in this repository.

## Purpose

Portfolio project (phase 4, streaming): real-time AIS ship traffic around the Lesser
Antilles with Kafka and Flink. The owner is an FPGA / embedded engineer moving toward
freelance data engineering, so code quality, tests and clear docs matter more than feature
count. Every change should be something a recruiter can read.

## Stack

- Python 3.12+ package in `src/ais_stream` (src layout, hatchling)
  - `messages.py`: aisstream message → `Position` / `ShipStatic` / `Rejected`; AIS
    "not available" sentinels become nulls here and nowhere else
  - `sources.py`: aisstream.io WebSocket (reconnects), replay of JSON-lines recordings
  - `kafka.py`: topics (`ais.positions`, compacted `ais.ships`, `ais.dlq`), keyed
    idempotent producer; `pipeline.py`: one raw message → record or reject
  - `synthetic.py`: deterministic traffic in aisstream's exact format
- Kafka 3.9 KRaft (`apache/kafka`), Kafka UI, Docker Compose
- Flink 1.20 SQL (`flink/sql/ais_jobs.sql`, image `flink/Dockerfile` with Kafka, JDBC,
  PostgreSQL and zstd jars) → PostgreSQL tables in `postgres/init.sql` (primary keys =
  upsert keys): `port_activity`, `ship_hourly`, and the anomalies `ship_sessions` +
  `ais_gaps` (view `dark_periods`), `position_jumps`, `port_events`. Event time +
  watermarks: old events are late and dropped, so e2e tests need a fresh stack
  (`make e2e-up`, which does not submit the job: the tests submit it after their traffic).
- pytest (unit; `e2e` marked tests on the Compose stack; integration marked `integration` with a real `apache/kafka` broker started
  by the `kafka` fixture), ruff, mypy --strict

## Commands

```bash
python -m venv .venv && source .venv/bin/activate
make install / check / integration
make up            # Kafka + synthetic demo + Kafka UI (localhost:8088)
make live          # real feed, needs AISSTREAM_API_KEY in .env (never commit it)
```

## Rules

- Run `make check` and `make integration` before every commit; both must pass.
- Never call aisstream.io in tests: use `tests/fixtures/` and the fake WebSocket server.
- Keep records keyed by MMSI and the producer idempotent.
- Keep mypy strict; no `type: ignore` without a comment explaining why.
- Code, comments, commits and README in English.
- Small commits with clear messages; open a PR per feature so CI runs.

## Roadmap

See `docs/ROADMAP.md`. Work through it in order unless told otherwise.
