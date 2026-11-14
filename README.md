# Caribbean AIS Stream

Real-time ship traffic around the **Lesser Antilles**, from the AIS radio broadcasts of
every large ship to **Kafka**, then **Flink**. Ships report their position every few
seconds under way and their identity every six minutes; this project turns that stream into
clean, keyed Kafka records, and (next) into port activity and anomaly detection with Flink.

Third piece of a portfolio on the same islands, after
[antilles-data-pipeline](https://github.com/tomtessondev/antilles-data-pipeline) (batch) and
[caribbean-sensor-pipeline](https://github.com/tomtessondev/caribbean-sensor-pipeline) (IoT).

## Architecture

```
 aisstream.io (WebSocket) ─┐                              ┌─► ais.positions   (keyed by MMSI, 7 days)
 recording (JSON lines) ───┼─► normalise (pydantic) ──────┼─► ais.ships       (compacted: latest per ship)
 synthetic traffic ────────┘   AIS sentinels → null       └─► ais.dlq         (rejects, verbatim)
                                                                   │
                                                          Flink SQL (roadmap step 2)
```

| Part | Tooling | Where |
| --- | --- | --- |
| Records | AIS normalisation, pydantic models | `src/ais_stream/messages.py` |
| Sources | aisstream.io client with reconnection, replay of recordings | `src/ais_stream/sources.py` |
| Producer | confluent-kafka: keyed, idempotent, acks=all, zstd | `src/ais_stream/kafka.py`, `pipeline.py` |
| Synthetic traffic | Ferries, cargo, cruise, tanker at anchor, fishing boats, a ship going dark | `src/ais_stream/synthetic.py` |
| Platform | Kafka 3.9 (KRaft, single node), Kafka UI, Docker Compose | `docker-compose.yml` |
| Quality | pytest (unit, a fake WebSocket server, a real Kafka), ruff, mypy `--strict` | `tests/`, `.github/workflows/ci.yml` |

## Quick start

```bash
docker compose up -d --build     # Kafka, topics, synthetic ships at the real pace, Kafka UI
open http://localhost:8088       # topics, partitions, messages
```

With the real feed (free key from [aisstream.io](https://aisstream.io)):

```bash
echo "AISSTREAM_API_KEY=..." > .env
docker compose --profile live up -d --build live
```

Development:

```bash
python -m venv .venv && source .venv/bin/activate
make install        # pip install -e ".[dev]"
make check          # ruff, mypy, unit tests
make integration    # tests against a real Kafka broker (needs Docker)
ais-stream synthesize --hours 6 --out data/sample.jsonl
ais-stream replay data/sample.jsonl --bootstrap localhost:9094
```

## Design choices

- **Normalise once, at the edge.** AIS says "not available" with impossible values:
  heading 511, speed 102.3 knots, course 360, latitude 91, longitude 181. They become nulls
  before anything is written, so no downstream query averages a 511° heading. Names lose
  the `@` padding; a ship's length is the sum of the antenna's distances to bow and stern.
- **Keyed by ship.** The Kafka key is the MMSI: all of a ship's messages go to one
  partition, so consumers read them in order (tested on a real broker).
- **No loss, no duplicate from the producer.** `acks=all` with idempotence: a retried send
  is written exactly once. Delivery callbacks count failures; a replay exits non-zero if any.
- **Topics shaped like their data.** Positions are an event log kept a week. Ships are a
  table: a compacted topic keeps the latest description of each ship, so a new consumer can
  rebuild the fleet from Kafka alone. Rejects go to a dead-letter topic, verbatim.
- **Development without an account.** The synthetic generator writes aisstream's exact
  format, so `demo` and `replay` go through the same parsing as the live feed. It includes
  a ship whose AIS goes silent for 45 minutes, the case the anomaly stage must catch.
- **A broken key stops the producer.** aisstream answers an invalid key with an error
  message instead of data; the producer raises instead of filling the dead-letter topic.

## Roadmap

See [docs/ROADMAP.md](docs/ROADMAP.md).

## Author

Tom Tesson, data engineer and FPGA/embedded engineer, based in Guadeloupe (UTC−4).
