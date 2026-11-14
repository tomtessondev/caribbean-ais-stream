# Map and Metrics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Grafana map of the ships and a stream-health dashboard (lag, throughput), provisioned as code, with screenshots and a GIF in the README and on `/projets`.

**Architecture:** Flink gains a `ship_positions` upsert sink (latest position per ship). Grafana reads PostgreSQL through a read-only role and Prometheus, which scrapes a Kafka exporter (broker view) and Flink's Prometheus reporter (job view). Dashboards live as JSON in the repo; their SQL runs in the e2e tests as the Grafana role.

**Tech Stack:** Flink 1.20 SQL, PostgreSQL 17, Grafana 12.3.0, Prometheus, danielqsj/kafka-exporter, pytest, PyYAML, ffmpeg.

**Spec:** `docs/superpowers/specs/2026-11-14-map-and-metrics-design.md`

## Global Constraints

- Branch `feat/map-metrics` in `caribbean-ais-stream`; code, comments, dashboard titles, commits in English; French page in French.
- Ports: Grafana host **3002** → container 3000; Prometheus host **9090**. Flink reporter port **9249** (internal).
- Images pinned: `grafana/grafana:12.3.0`, `prom/prometheus:v3.5.0`, `danielqsj/kafka-exporter:v1.9.0`.
- PostgreSQL role `grafana_reader` (login, select only), password from env `GRAFANA_DB_PASSWORD`, local default `grafana-local`; Grafana admin password from `GRAFANA_ADMIN_PASSWORD`, default `admin`; anonymous Viewer access on.
- Datasource uids: `postgres` (PostgreSQL), `prometheus` (Prometheus).
- "Latest" panels anchor on `max(ts)` of `ship_positions`, never `now()`; time-series SQL panels use `$__timeFilter`; AIS dashboard default range `now-6h`.
- Map: ships with `ts > max(ts) - 30 min`, plus ships with an open dark period (`silent_until is null`) whose `silent_from > max(ts) - 6 hours`. State priority: `dark` > `jump` (jump `to_time` within 30 min of `max(ts)`) > `moored` (nav_status 1 or 5) > `under way`.
- Before each commit: `make check`, `make integration`; tasks touching the stack also `make e2e-up && make e2e`.

## Review Focus

- A ship that went dark must stay on the map, coloured dark, after its last position is more than 30 minutes old — Task 2 test `test_a_dark_ship_stays_on_the_map`.
- The map must not be empty when the data is not at wall-clock "now" (e2e traffic is two days old) — Task 2 test `test_map_shows_every_ship`.
- Grafana must not be able to write: `grafana_reader` has select only — Task 2 test `test_grafana_role_cannot_write`.
- A dashboard query using a macro the test renderer does not know must fail loudly, not silently pass — Task 2 `render()` asserts no `$` left.
- Lag must be visible while the Flink job is stopped (broker-side metric) — Task 3 test `test_prometheus_sees_lag_and_flink` asserts the exporter series, which does not depend on the job.

---

### Task 1: Latest position per ship

**Files:**
- Modify: `flink/sql/ais_jobs.sql`, `postgres/init.sql`, `tests/test_e2e.py`

**Interfaces:**
- Produces: table `ship_positions(mmsi bigint primary key, ts timestamptz not null, lat double precision, lon double precision, sog_kn double precision, cog_deg double precision, heading_deg integer, nav_status integer, ship_name text, port text)`.

- [ ] **Step 1: Failing test** in `tests/test_e2e.py`; add `ship_positions` (count 9) to the fixture's `expected` dict, and:

```python
def test_ship_positions_hold_each_ships_last_report(played: datetime) -> None:
    rows = query("select mmsi, ts from ship_positions order by mmsi")
    assert len(rows) == 9
    last = {}
    for message in traffic(played, 4):
        if message["MessageType"] == "PositionReport":
            last[message["MetaData"]["MMSI"]] = parse_aisstream_time(
                message["MetaData"]["time_utc"]
            )
    assert dict(rows) == last
```

(`parse_aisstream_time` from `ais_stream.messages`.) Add `select * from ship_positions order by 1` to the reprocessing snapshots.

- [ ] **Step 2:** `make e2e-up && make e2e` → FAIL, relation `ship_positions` does not exist.
- [ ] **Step 3:** sink table + `INSERT INTO ship_positions SELECT mmsi, CAST(event_time AS TIMESTAMP(3)), lat, lon, sog_kn, cog_deg, heading_deg, nav_status, ship_name, port FROM positions_in_port` in the statement set, with a comment: upsert of every position, the JDBC sink keeps the last per key between flushes; an out-of-order position may show for seconds until the next report. Table in `init.sql`.
- [ ] **Step 4:** `make e2e-up && make e2e` → all pass.
- [ ] **Step 5:** commit "Keep the latest position of each ship for the map".

---

### Task 2: Grafana, the read-only role and the AIS traffic dashboard

**Files:**
- Create: `grafana/provisioning/datasources/datasources.yaml`, `grafana/provisioning/dashboards/ais.yaml`, `grafana/dashboards/ais-traffic.json`, `tests/test_grafana.py`
- Modify: `postgres/init.sql`, `docker-compose.yml`, `Makefile` (`e2e-up`), `pyproject.toml` (dev: `pyyaml>=6`, `types-PyYAML`), `tests/test_e2e.py`

**Interfaces:**
- Consumes: `ship_positions` (Task 1), `dark_periods`, `position_jumps`, `port_events`, `port_activity`.
- Produces: `tests/test_grafana.py` helpers `dashboard_queries() -> list[tuple[str, str]]` (name, rawSql for every panel target with a `rawSql`) and `render(sql: str) -> str`, used by Task 3 and by `tests/test_e2e.py`.

- [ ] **Step 1: Failing unit tests** in `tests/test_grafana.py` (unmarked, run by `make check`):

```python
def test_dashboards_are_well_formed() -> None:
    assert [p.stem for p in DASHBOARDS] == ["ais-traffic"]  # Task 3 adds "stream-health"
    for path in DASHBOARDS:
        board = json.loads(path.read_text(encoding="utf-8"))
        ids = [panel["id"] for panel in board["panels"]]
        assert len(ids) == len(set(ids))
        for panel in board["panels"]:
            if panel["type"] == "row":
                continue
            assert panel["datasource"]["uid"] in {"postgres", "prometheus"}, panel["title"]
            assert panel["targets"], panel["title"]


def test_provisioning_points_at_the_dashboards() -> None:
    sources = yaml.safe_load((GRAFANA / "provisioning/datasources/datasources.yaml").read_text())
    assert {s["uid"] for s in sources["datasources"]} == {"postgres", "prometheus"}
```

`render(sql)`: `$__timeFilter(col)` → `col > (select max(ts) from ship_positions) - interval '6 hours'`, `$__interval` → `5 minutes`; assert no `$` remains.

- [ ] **Step 2:** `.venv/bin/pytest tests/test_grafana.py` → FAIL (no dashboards).
- [ ] **Step 3: Implement**
  - `init.sql`, at the end: `\getenv grafana_password GRAFANA_DB_PASSWORD`, role `grafana_reader` login with that password, `grant select on all tables in schema public` (after every table and the view exist). `docker-compose.yml` postgres env `GRAFANA_DB_PASSWORD: ${GRAFANA_DB_PASSWORD:-grafana-local}`.
  - Grafana service `grafana` (env as Global Constraints, `GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH` = the AIS traffic JSON, port `3002:3000`, provisioning and dashboards mounted read-only, depends on postgres healthy).
  - Datasources: `postgres` (`postgres:5432`, db `ais`, user `grafana_reader`, `postgresVersion: 1700`, default) and `prometheus` (`http://prometheus:9090`; Task 3 adds the service). Dashboard provider like the sensor project (`allowUiUpdates: false`, folder `AIS`).
  - `ais-traffic.json`, default time `now-6h`, refresh `10s`, panels:
    1. **Ships (latest)** — geomap, markers coloured by `state` (dark red, jump orange, moored blue, under way green), tooltip name/MMSI/speed/status/port; SQL: CTE `anchor` = `max(ts)`, open dark periods with `silent_from > anchor - 6 h`, jumps with `to_time > anchor - 30 min`, `ship_positions` rows with `ts > anchor - 30 min` or dark; `state` by the priority of Global Constraints.
    2. **Ships per port** — time series from `port_activity` (`window_start` as time, `ships` by `port`), `$__timeFilter(window_start)`.
    3. **Latest port arrivals and departures** — table, last 20 `port_events` before the anchor.
    4. **Ships dark now** — table, open `dark_periods` (same 6-hour bound), with minutes since `silent_from` relative to the anchor.
    5. **Latest impossible jumps** — table, `position_jumps` with `to_time > anchor - 30 min`.
  - `Makefile` `e2e-up`: also start `grafana`.
- [ ] **Step 4: e2e tests** in `tests/test_e2e.py` (they reuse `played`), importing `dashboard_queries`, `render` from `tests.test_grafana`:

```python
READER = POSTGRES.replace("ais:ais@", "grafana_reader:grafana-local@")


@pytest.mark.parametrize(("name", "sql"), dashboard_queries())
def test_every_dashboard_query_runs_as_grafana(played: datetime, name: str, sql: str) -> None:
    with psycopg.connect(READER) as conn:
        conn.execute(render(sql).encode()).fetchall()


def test_map_shows_every_ship(played: datetime) -> None:
    rows = map_rows()  # runs the "Ships (latest)" query as grafana_reader
    assert len(rows) == 9 and {r["state"] for r in rows} <= {"moored", "under way", "jump"}


def test_a_dark_ship_stays_on_the_map(played: datetime) -> None:
    """An open dark period an hour before the latest data: the ship is shown, dark."""
    # insert (as ais) a ship_positions row and a ship_sessions row for MMSI 999000001,
    # last_seen = anchor - 1 h, no ais_gaps row; delete both in a finally block.
    assert ("999000001", "dark") in {(str(r["mmsi"]), r["state"]) for r in map_rows()}


def test_grafana_role_cannot_write() -> None:
    with pytest.raises(psycopg.errors.InsufficientPrivilege), psycopg.connect(READER) as conn:
        conn.execute(b"delete from port_events")
```

`map_rows() -> list[dict[str, Any]]`: runs the rendered "Ships (latest)" query as `grafana_reader` with `psycopg.rows.dict_row`. Make `dashboard_queries` and `render` importable from one place (`tests/__init__.py` if missing). The dark-ship test deletes its rows in a `finally`, so the reprocessing test (later in the file) never sees them; MMSI 999000001 is never in Kafka, so Flink never writes it.

- [ ] **Step 5:** `make check` (unit tests pass), `make e2e-up && make e2e` → all pass. Open `http://localhost:3002` once with the browse skill: the map shows ships around Guadeloupe/Martinique (screenshot to the scratchpad, not committed yet).
- [ ] **Step 6:** commit "Add Grafana with a live map of the ships".

---

### Task 3: Prometheus, Kafka exporter, Flink metrics, stream health dashboard

**Files:**
- Create: `prometheus/prometheus.yml`, `grafana/dashboards/stream-health.json`
- Modify: `docker-compose.yml`, `Makefile`, `tests/test_grafana.py`, `tests/test_e2e.py`, `.github/workflows/ci.yml` (if `e2e-up` alone does not cover it)

**Interfaces:**
- Consumes: `dashboard_queries` (extend to collect PromQL `expr` targets as well, or a sibling `prometheus_queries()`).

- [ ] **Step 1: Failing tests**: `test_dashboards_are_well_formed` expects `["ais-traffic", "stream-health"]`; e2e:

```python
PROMETHEUS = os.getenv("E2E_PROMETHEUS", "http://localhost:9090")


def test_prometheus_sees_lag_and_flink(played: datetime) -> None:
    wait_until(lambda: bool(prom('kafka_consumergroup_lag{consumergroup="flink-ais"}')), 120, "lag")
    wait_until(lambda: bool(prom('{__name__=~"flink_.+"}')), 120, "flink metrics")


@pytest.mark.parametrize(("name", "expr"), prometheus_queries())
def test_every_promql_query_is_valid(name: str, expr: str) -> None:
    prom(expr.replace("$__rate_interval", "1m"))  # raises on a parse error (HTTP 400)
```

`prom(expr) -> list[dict]` calls `/api/v1/query` with urllib and returns `data.result`.

- [ ] **Step 2:** run → FAIL (no Prometheus, no dashboard).
- [ ] **Step 3: Implement**
  - Services `kafka-exporter` (`--kafka.server=kafka:9092`, depends on kafka healthy) and `prometheus` (`9090:9090`, config mounted, 15 s scrape of `kafka-exporter:9308`, `flink-jobmanager:9249`, `flink-taskmanager:9249`).
  - Flink: `metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory` and `metrics.reporter.prom.port: 9249` in `FLINK_PROPERTIES` of job and task managers.
  - `stream-health.json` (Prometheus, wall clock, default `now-1h`): messages/s per topic (`sum by (topic) (rate(kafka_topic_partition_current_offset[$__rate_interval]))`), lag of `flink-ais` per partition (description: committed on checkpoints, every 30 s), Flink records in/s of the source, watermark delay / pending records (names read from the running cluster's `/metrics`), dead letters/s.
  - `make e2e-up`: also `kafka-exporter`, `prometheus`.
- [ ] **Step 4:** `make check`, `make e2e-up && make e2e` → pass; stop the Flink job via REST and check the lag series still answers (manual, note in the ledger).
- [ ] **Step 5:** commit "Monitor consumer lag and throughput with Prometheus".

---

### Task 4: Screenshots, GIF, repository docs, PR

**Files:**
- Create: `docs/images/ais-traffic.png`, `docs/images/stream-health.png`, `docs/images/ships.gif`, `scripts/frames-to-gif.sh`, `docs/images/README.md` (how to redo them)
- Modify: `README.md`, `docs/ROADMAP.md`, `CLAUDE.md`, `Makefile` (target `fast-replay`), root `../CLAUDE.md` ports line (not a git repo)

- [ ] **Step 1:** `make fast-replay`: fresh stack without `demo`, Grafana and metrics up, `ais-stream synthesize --hours 6 --no-ending-now` then `ais-stream replay ... --speed 60`. Capture frames of the map panel (Grafana `d-solo` panel URL, kiosk) with the browse skill every ~2 s; `scripts/frames-to-gif.sh <frames dir> docs/images/ships.gif` (ffmpeg palettegen/paletteuse, ~8 fps, width 900). Target < 3 MB.
- [ ] **Step 2:** screenshots of both dashboards (1600×900) from the demo stack or the replay, once port activity and an anomaly are visible.
- [ ] **Step 3:** README: section "Map and metrics" (GIF, screenshots, URLs `localhost:3002`, `localhost:9090`, what each panel shows, lag committed on checkpoints), architecture diagram line for Grafana/Prometheus, tooling table rows; ROADMAP step 4 "(done)"; CLAUDE.md stack and commands; root CLAUDE.md ports add Grafana AIS 3002, Prometheus 9090.
- [ ] **Step 4:** `make check && make integration` → pass; commit "Document the map and metrics with screenshots"; push and open the PR; CI green.

---

### Task 5: French page

**Files:**
- Modify: `antilles-data-pipeline/dashboard/src/app/projets/caribbean-ais-stream/page.tsx`, `_components/architecture.tsx`, `dashboard/public/projets/ais/` (copies of the PNGs and GIF)

- [ ] **Step 1:** branch `docs/ais-map-metrics`; roadmap step 4 "fait"; a section "Carte et métriques" with the GIF and both screenshots (`next/image` or `<img>` per the page's existing image usage; read `node_modules/next/dist/docs/` on images first, per `dashboard/AGENTS.md`), alt texts in French; technologies table rows Grafana and Prometheus; architecture box for Grafana/Prometheus if it fits.
- [ ] **Step 2:** in `dashboard/`: `npm run lint && npm run typecheck && npm test && npm run build`; at repo root `make check` and `make transform` → pass.
- [ ] **Step 3:** commit, push, PR; CI and Vercel preview green; merge only with the owner's go.
