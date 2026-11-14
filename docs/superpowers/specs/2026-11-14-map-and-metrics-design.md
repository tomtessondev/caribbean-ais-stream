# Map and metrics (roadmap step 4) — design

## Intent

Step 4 of `docs/ROADMAP.md`: a live map of the ships and port activity, and the health of
the stream (consumer lag, throughput), in Grafana. The project is a portfolio piece; nobody
reviewing it will run `docker compose up`, so the deliverable that matters is what they
see: **screenshots and a GIF in the README and on the French page
`/projets/caribbean-ais-stream`** (repository `antilles-data-pipeline`). Everything runs
locally; nothing is hosted.

## Goals

- Grafana in the Compose stack, provisioned as code, with two dashboards:
  - **AIS traffic**: geomap of the ships at their latest position, anomalies highlighted;
    ships per port over time; latest port arrivals and departures; open dark periods and
    recent jumps.
  - **Stream health**: messages per second per topic (positions, ships, dead letters),
    Flink consumer lag per partition, Flink records read per second, watermark delay.
- A new Flink output for the map: the latest position of each ship.
- Prometheus scraping a Kafka exporter (broker view) and Flink's Prometheus reporter (job
  view).
- Tests in the style of `caribbean-sensor-pipeline/tests/test_grafana.py`: provisioning
  files well formed, every SQL query of the dashboards runs against the real schema with
  the read-only role Grafana uses; e2e checks that Prometheus has the lag and Flink series.
- Screenshots (both dashboards) and a short GIF of ships moving, in the repo and on the
  French page.

## Non-goals

- Hosting Grafana or the stack anywhere.
- Alerting rules (the sensor project already shows Grafana alerting).
- Consuming `ais.ships` in Flink (ship type, destination on the map): later, possibly with
  step 5 (schemas).
- Loitering, per-ship thresholds (left from step 3).

## Anti-goals

- A map that looks empty or frozen in the screenshots/GIF: at real pace a 24-knot ferry
  barely moves at archipelago scale, so the GIF must come from accelerated replay.
- Queries that assume "now" is the time of the data: a replay of a recording, or the e2e
  traffic two days back, would show an empty map.
- Dashboards edited by hand in the UI and never saved back: the JSON in the repo is the
  source of truth.

## Constraints

- Ports: Grafana on **3002** (3001 is the sensor project's Grafana), Prometheus on
  **9090**. Add both to the port list in the portfolio's root `CLAUDE.md`.
- Grafana `grafana/grafana:12.3.0` (same version as the sensor project), anonymous Viewer
  access for the local demo, admin password from the environment with a local default.
- PostgreSQL access through a read-only role (`grafana_reader`), password from the
  environment, created in `postgres/init.sql`.
- The Flink image already ships `plugins/metrics-prometheus`: enabling it is configuration
  (`metrics.reporter.prom.factory.class`, port 9249) in the job and task managers.
- Flink commits Kafka offsets on checkpoints (every 30 s): the broker-side lag of group
  `flink-ais` is a committed lag, up to one checkpoint behind. Say so on the dashboard.
- Code, comments, dashboard titles and README in English; the French page in French.

## Approaches

| Approach | Trade-off |
| --- | --- |
| **Grafana + PostgreSQL + Prometheus fed by a Kafka exporter and Flink's reporter** (chosen) | Broker view and job view; lag stays visible when the job is down, which is when it matters. Three more containers. |
| Grafana + Prometheus, Flink metrics only | One container fewer; the lag disappears with the job. |
| Grafana + PostgreSQL only | Simplest; throughput derived from tables, no real lag, so the roadmap's "consumer lag" is missed. |

The owner picked the first.

## Design

### Latest position per ship (Flink)

New sink **`ship_positions`** (PK `mmsi`): `mmsi`, `ts`, `lat`, `lon`, `sog_kn`, `cog_deg`,
`heading_deg`, `nav_status`, `ship_name`, `port` (from `positions_in_port`). One
`INSERT INTO ship_positions SELECT ... FROM positions_in_port` in the statement set: a plain
upsert of every position. The JDBC sink buffers and flushes (default 1 s), keeping the last
row per key, so PostgreSQL sees one write per ship per second at most.

An out-of-order position (within the 30 s watermark) can overwrite a newer one with an
older one until the ship's next report, seconds later: harmless on a map, not worth more
machinery. A comment in the SQL says so.

### Map queries are relative to the data, not to `now()`

For the panels that show "the current state" (the map, open dark periods, latest port
events, recent jumps), "current" means relative to the **latest position in
`ship_positions`** (`max(ts)`), not to the wall clock: the map shows ships whose `ts` is
within 30 minutes of it, **plus every ship with an open dark period that started within
the last 6 hours, at its last known position** (a dark ship is exactly the one that stops
updating; with only the 30-minute rule it would show as dark for 15 minutes and then
vanish). The 6-hour bound keeps the live map from collecting every ship that ever left the
covered area. Recent jumps
and latest port events: the last 30 minutes / last 20 rows before `max(ts)`. These panels
ignore the time picker; their titles say "latest".

Panels over time (ships per port, positions per window) honour Grafana's time picker
(`$__timeFilter`), as Grafana users expect; the AIS dashboard defaults to the last 6 hours.
On the demo the data is at "now" anyway; for a replay the viewer moves the picker. The
stream health dashboard is Prometheus, so wall-clock time.

This keeps the map working on the demo, on a replay and on the e2e traffic.

Marker colour on the map: dark (an open `dark_periods` row) > recent jump (a
`position_jumps` row in the last 30 minutes of data) > moored or at anchor > under way.
Tooltip: name, MMSI, speed, status, port.

### Metrics

- **Kafka exporter** (`danielqsj/kafka-exporter`, pinned version): topic offsets per
  partition (throughput = `rate()` of the latest offset) and consumer group lag
  (`kafka_consumergroup_lag{consumergroup="flink-ais"}`).
- **Flink reporter**: records in per second of the source, `pendingRecords` /
  `records-lag-max` of the Kafka source reader, watermark delay
  (`currentEmitEventTimeLag` or the closest metric Flink 1.20 exposes; builder checks
  names on the running cluster).
- **Prometheus** (`prom/prometheus`, pinned), 15 s scrape, config in
  `prometheus/prometheus.yml`, no persistence needed beyond a volume.

### Grafana as code

`grafana/provisioning/{datasources,dashboards}/` and `grafana/dashboards/*.json`, same
layout as the sensor project. Datasources: PostgreSQL (`grafana_reader`) and Prometheus.
Home dashboard: AIS traffic.

### Capturing the screenshots and GIF

- Screenshots: from the demo stack after it has run long enough to have port activity and
  at least one anomaly (or from an accelerated replay), saved under `docs/images/`.
- GIF: `ais-stream synthesize` a few hours, `ais-stream replay --speed N` (N large enough
  that ships visibly move, e.g. 60) on a fresh stack, frames of the map captured with a
  headless browser (gstack `/browse`) and assembled with `ffmpeg`; keep it short and small
  (aim < 3 MB). The capture procedure goes in a script or a documented Make target so it can
  be redone.
- The same images are copied into the dashboard's public assets for `/projets`.

### Tests

- Unit: provisioning YAML and dashboard JSON parse; every dashboard references an existing
  datasource uid.
- Integration-style (needs the e2e stack): every SQL query of the dashboards (Grafana
  macros rendered as in the sensor project) runs as `grafana_reader` against the e2e data
  and returns rows for the map and port panels; Prometheus answers
  `kafka_consumergroup_lag{consumergroup="flink-ais"}` and at least one `flink_` series.
- E2E keeps its current assertions; `ship_positions` has 9 rows, one per ship, each at the
  ship's last position of the play (it holds because a ship's messages share one partition
  and arrive in order; no PostgreSQL-side guard). The map query returns the 9 ships.
- `make e2e-up` and CI start Prometheus, the Kafka exporter and Grafana too (my call: the
  query tests need them).

### Docs

README (dashboards, ports, screenshots, GIF), ROADMAP (step 4 done), CLAUDE.md, root
`CLAUDE.md` ports, French page (roadmap status, a section with the screenshots and GIF,
technologies table: Grafana, Prometheus).

## Left to the builder

- Exact panel layout and colours, within the panels listed above.
- Exact metric names on Flink 1.20 and the exporter version.
- The replay speed and frame rate for the GIF, as long as ships visibly move and the file
  stays small.
- How the e2e or a separate test target runs the dashboard queries (fixture reuse).
