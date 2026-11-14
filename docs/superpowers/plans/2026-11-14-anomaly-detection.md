# Anomaly Detection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Detect dark ships, impossible position jumps and port arrivals/departures in the
existing Flink SQL job, with results in PostgreSQL and proof in the e2e test.

**Architecture:** New statements in the single statement set of `flink/sql/ais_jobs.sql`:
a session-window aggregation (`ship_sessions`) and three `MATCH_RECOGNIZE` queries
(`ais_gaps`, `position_jumps`, `port_events`), each sinking to a JDBC upsert table declared
in `postgres/init.sql`, plus a PostgreSQL view `dark_periods`. The synthetic generator
gains a ninth ship with one spoofed position.

**Tech Stack:** Flink 1.20 SQL (Kafka source, JDBC sink, CEP already in `/opt/flink/lib`),
PostgreSQL 16, Python 3.12, pytest, psycopg.

**Spec:** `docs/superpowers/specs/2026-11-14-anomaly-detection-design.md`

## Global Constraints

- Work on branch `feat/anomalies` in `caribbean-ais-stream`; code, comments, commits in English.
- One statement set, pipeline name stays `ais-port-activity`; the job reads `ais.positions` once.
- Silence threshold: 15 minutes, fixed. Jump: distance > 1 nm **and** implied speed > 60 kn,
  elapsed time floored at 1 s (`GREATEST(TIMESTAMPDIFF(SECOND, ...), 1)`).
- Distance in SQL: equirectangular, same formula as `synthetic._distance_nm`
  (`dy = Δlat·60`, `dx = Δlon·60·cos(mean lat)`, `sqrt(dx² + dy²)`).
- Timestamps written to JDBC sinks as `TIMESTAMP(3)` (cast from the `TIMESTAMP_LTZ(3)`
  rowtime; `table.local-time-zone` is UTC), columns `timestamptz` in PostgreSQL, like the
  existing tables.
- Every sink table has a primary key (`NOT ENFORCED` in Flink, real in PostgreSQL) so
  reprocessing rewrites rows instead of duplicating them.
- The existing 8 ships' traffic must not change: the ninth ship's `random.Random` is drawn
  after all existing ones.
- Before each commit: `make check` and `make integration` pass. Tasks touching Flink or
  PostgreSQL also: `make e2e-up && make e2e` pass (fresh stack every run).

## Review Focus

- A ship first seen inside a port box (the tanker anchored in the Fort-de-France box) must
  get no arrival and no departure — Task 4 asserts the tanker has no port events.
- The dark ship's return after 45 min (~9 nm at 12 kn) must not count as a jump — Task 3
  asserts `position_jumps` only holds the spoofing ship.
- The anchored tanker's position jitter and the fishing boats' wandering must not count as
  jumps — same Task 3 assertion.
- Moored ships report every 3 minutes: no session may close for them — Task 2 asserts
  `ship_sessions` holds exactly one row, the dark ship's.
- At the end of the play every ship's last session is still open (watermark never passes
  it): no ship may show as dark there — same Task 2 assertion.

---

### Task 1: A ninth synthetic ship with a spoofed position

**Files:**
- Modify: `src/ais_stream/synthetic.py` (`_shuttle`, `traffic`, module docstring)
- Test: `tests/test_synthetic.py`

**Interfaces:**
- Produces: ship `Ship(255000707, "OCEAN PEARL", 70, "CQOP7", 140, 22, 8.1)` shuttling
  `["Basse-Terre", "Roseau"]` at 14 kn, dwell 3 h, with one position at
  `start + 1 h 30` (the first under-way position at or after that offset) reported
  0.5° of latitude **south** of its true track (≈ 30 nm, outside every port box), the next
  one back on track.
- `_shuttle(..., spoofed: timedelta | None = None)` — offset from `start` of the spoofed
  report.

- [ ] **Step 1: Write the failing tests**

In `tests/test_synthetic.py`: change `assert len(ships) == 8` to `9`, and add:

```python
def test_one_ship_reports_a_spoofed_position() -> None:
    positions = [
        r
        for r in map(to_record, traffic(START, hours=6))
        if isinstance(r, Position) and r.mmsi == 255000707
    ]
    jumps = [
        (a, b) for a, b in zip(positions, positions[1:], strict=False) if abs(b.lat - a.lat) > 0.3
    ]
    assert len(jumps) == 2  # out to the spoofed position, then back
    (out, spoofed), (_, back) = jumps
    assert spoofed.ts - START == timedelta(hours=1, minutes=30)
    assert spoofed.lat < out.lat - 0.45 and abs(back.lat - out.lat) < 0.01


def test_the_other_ships_keep_their_traffic() -> None:
    """Adding a ship must not reshuffle the others' random streams."""
    others = [m for m in traffic(START, hours=1) if m["MetaData"]["MMSI"] != 255000707]
    assert len(others) == EIGHT_SHIPS_ONE_HOUR
```

`EIGHT_SHIPS_ONE_HOUR`: before touching `synthetic.py`, run
`.venv/bin/python` on a scratch script in the session scratchpad that prints
`len(traffic(START, hours=1))` and also a sha256 of `json.dumps(traffic(START, hours=1))`.
Store the hash as a module constant instead of the count if you prefer a stronger check
(assert the hash of `others` equals it).

- [ ] **Step 2: Run them, expect FAIL**

Run: `.venv/bin/pytest tests/test_synthetic.py -v`
Expected: the ship-count test and `test_one_ship_reports_a_spoofed_position` fail;
`test_the_other_ships_keep_their_traffic` passes (no ship 255000707 yet).

- [ ] **Step 3: Implement**

Add the `spoofed` parameter to `_shuttle` (one flag so only the first matching report is
moved), add the ship to `traffic()` as the last stream, and describe it in the module
docstring next to the dark vessel ("the spoofed position a later stage should detect").

- [ ] **Step 4: Run, expect PASS**

Run: `make check`
Expected: all unit tests pass, ruff and mypy clean. `test_one_ship_goes_dark_for_45_minutes`
still passes (OCEAN PEARL has no gap).

- [ ] **Step 5: Commit**

```bash
git add src/ais_stream/synthetic.py tests/test_synthetic.py
git commit -m "Add a synthetic ship that reports one spoofed position"
```

---

### Task 2: Dark ships — session windows, gaps on return, `dark_periods` view

**Files:**
- Modify: `flink/sql/ais_jobs.sql`, `postgres/init.sql`, `tests/test_e2e.py`

**Interfaces:**
- Consumes: Task 1's ninth ship (e2e counts).
- Produces (PostgreSQL):
  - `ship_sessions(mmsi bigint, session_start timestamptz, last_seen timestamptz, ship_name text, positions integer, last_lat, last_lon double precision, primary key (mmsi, session_start))`
  - `ais_gaps(mmsi bigint, silent_from timestamptz, silent_until timestamptz, silent_minutes double precision, from_lat, from_lon, to_lat, to_lon double precision, ship_name text, primary key (mmsi, silent_from))`
  - view `dark_periods(mmsi, ship_name, silent_from, silent_until, silent_minutes, last_lat, last_lon)`: `ship_sessions` left join `ais_gaps` on `mmsi` and `last_seen = silent_from`.

- [ ] **Step 1: Update the e2e fixture and write the failing test**

In `tests/test_e2e.py`: fixture waits for `(27,)` hourly rows; `test_every_ship_gets_a_row_per_closed_hour` expects `len(rows) == 9`. Add:

```python
def test_only_the_dark_ship_goes_dark(played: datetime) -> None:
    sessions = query(
        f"select mmsi, count(*) from ship_sessions where session_start >= '{played}' group by mmsi"
    )
    assert sessions == [(341000505, 1)]
    ((silent_from, silent_minutes),) = query(
        f"select silent_from, silent_minutes from dark_periods where silent_from >= '{played}'"
    )
    assert timedelta(hours=1, minutes=59) < silent_from - played <= timedelta(hours=2)
    assert 44 < silent_minutes < 46
```

Add `ship_sessions` and `ais_gaps` snapshots (all columns, ordered by PK, filtered on
`played`) to `test_reprocessing_from_scratch_gives_the_same_results`.

- [ ] **Step 2: Run, expect FAIL**

Run: `make e2e-up && make e2e`
Expected: the new test fails (relation `ship_sessions` does not exist).

- [ ] **Step 3: Implement**

`postgres/init.sql`: the two tables and the view, with one-line column comments like the
existing tables. `ais_jobs.sql`: the two sink tables, and in the statement set:
- `INSERT INTO ship_sessions` from
  `TABLE(SESSION(TABLE positions PARTITION BY mmsi, DESCRIPTOR(event_time), INTERVAL '15' MINUTES))`,
  `GROUP BY mmsi, window_start, window_end`, `last_seen = CAST(MAX(event_time) AS TIMESTAMP(3))`, `LAST_VALUE` for name and position.
- `INSERT INTO ais_gaps` from `positions MATCH_RECOGNIZE (PARTITION BY mmsi ORDER BY event_time MEASURES ... ONE ROW PER MATCH AFTER MATCH SKIP TO LAST B PATTERN (A B) DEFINE B AS TIMESTAMPDIFF(SECOND, A.event_time, B.event_time) > 900)`.
A short comment above each says what it detects and when it is emitted (session: 15 min
after the last position; gap: when the ship reports again).

- [ ] **Step 4: Run, expect PASS**

Run: `make e2e-up && make e2e`
Expected: all e2e tests pass, including reprocessing.

- [ ] **Step 5: Commit**

```bash
git add flink/sql/ais_jobs.sql postgres/init.sql tests/test_e2e.py
git commit -m "Detect ships going dark with session windows and gap matching"
```

---

### Task 3: Impossible position jumps

**Files:**
- Modify: `flink/sql/ais_jobs.sql`, `postgres/init.sql`, `tests/test_e2e.py`

**Interfaces:**
- Consumes: Task 1's spoofed position at `played + 1 h 30`.
- Produces: `position_jumps(mmsi bigint, from_time timestamptz, to_time timestamptz, from_lat, from_lon, to_lat, to_lon, distance_nm, implied_speed_kn double precision, ship_name text, primary key (mmsi, to_time))`.

- [ ] **Step 1: Write the failing test**

```python
def test_only_the_spoofed_position_jumps(played: datetime) -> None:
    rows = query(
        "select mmsi, to_time, distance_nm, implied_speed_kn from position_jumps"
        f" where to_time >= '{played}' order by to_time"
    )
    assert [mmsi for mmsi, *_ in rows] == [255000707, 255000707]
    assert rows[0][1] - played == timedelta(hours=1, minutes=30)
    assert all(25 < distance < 35 and speed > 60 for _, _, distance, speed in rows)
```

Add the `position_jumps` snapshot to the reprocessing test.

- [ ] **Step 2: Run, expect FAIL**

Run: `make e2e-up && make e2e`
Expected: FAIL, relation `position_jumps` does not exist.

- [ ] **Step 3: Implement**

Table in `init.sql`; sink and `MATCH_RECOGNIZE` (`PATTERN (A B)`, `AFTER MATCH SKIP TO LAST B`,
`DEFINE B` with the distance and speed thresholds of Global Constraints) in `ais_jobs.sql`.
Write the distance expression once if Flink allows (e.g. in `MEASURES` and repeated in
`DEFINE`, since `DEFINE` cannot reference measures); a comment states the formula and why
1 nm / 60 kn.

- [ ] **Step 4: Run, expect PASS**

Run: `make e2e-up && make e2e`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add flink/sql/ais_jobs.sql postgres/init.sql tests/test_e2e.py
git commit -m "Flag physically impossible position jumps"
```

---

### Task 4: Port arrivals and departures

**Files:**
- Modify: `flink/sql/ais_jobs.sql`, `postgres/init.sql`, `tests/test_e2e.py`

**Interfaces:**
- Consumes: the existing `positions_in_port` view.
- Produces: `port_events(mmsi bigint, event_time timestamptz, port text, event text check (event in ('arrival', 'departure')), ship_name text, sog_kn double precision, primary key (mmsi, event_time))`.

- [ ] **Step 1: Write the failing test**

```python
def test_port_arrivals_and_departures(played: datetime) -> None:
    rows = query(
        "select mmsi, event, port from port_events"
        f" where event_time >= '{played}' order by mmsi, event_time"
    )
    by_ship: dict[int, list[tuple[str, str]]] = defaultdict(list)
    for mmsi, event, port in rows:
        by_ship[mmsi].append((event, port))
    assert 538000404 not in by_ship  # the tanker never leaves the Fort-de-France box
    assert by_ship[227000101][:2] == [("departure", "Pointe-a-Pitre"), ("arrival", "Roseau")]
    for events in by_ship.values():  # departures and arrivals alternate, same port in/out
        for (e1, p1), (e2, p2) in zip(events, events[1:], strict=False):
            assert e1 != e2
            assert e1 == "departure" or p1 == p2
```

Import `defaultdict` from `collections`. Before relying on the ferry line, check it against the generator (scratch script: replay
`traffic(played, 4)` through the port boxes of `ais_jobs.sql`) and replace it with the
ferry's actual first two events if they differ. Add the `port_events` snapshot to the
reprocessing test.

- [ ] **Step 2: Run, expect FAIL**

Run: `make e2e-up && make e2e`
Expected: FAIL, relation `port_events` does not exist.

- [ ] **Step 3: Implement**

Table in `init.sql`; in `ais_jobs.sql`, two `MATCH_RECOGNIZE` on `positions_in_port`
(arrival `PATTERN (OUTSIDE INSIDE)`, departure `PATTERN (INSIDE OUTSIDE)`, `port IS NULL`
/ `IS NOT NULL`), `UNION ALL` into one `INSERT INTO port_events`. Arrival: time, port and
speed of the INSIDE row. Departure: time and speed of the OUTSIDE row, port of the INSIDE
row.

- [ ] **Step 4: Run, expect PASS**

Run: `make e2e-up && make e2e`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add flink/sql/ais_jobs.sql postgres/init.sql tests/test_e2e.py
git commit -m "Detect port arrivals and departures with pattern matching"
```

---

### Task 5: Repository docs and pull request

**Files:**
- Modify: `README.md`, `docs/ROADMAP.md`, `CLAUDE.md`
- Add: `docs/superpowers/specs/2026-11-14-anomaly-detection-design.md`, `docs/superpowers/plans/2026-11-14-anomaly-detection.md`

- [ ] **Step 1: Update the docs**

README: a section on the four detections (what, which Flink feature, which table/view,
the 15 min / 1 nm / 60 kn thresholds) and the known limitation (a ship leaving coverage
shows as dark). ROADMAP step 3: "(done)", loitering explicitly left for later. CLAUDE.md
Stack: the new tables and view.

- [ ] **Step 2: Verify**

Run: `make check && make integration`
Expected: pass.

- [ ] **Step 3: Commit, push, open the PR**

```bash
git add README.md docs/ROADMAP.md CLAUDE.md docs/superpowers
git commit -m "Document anomaly detection"
git push -u origin feat/anomalies
gh pr create --title "Detect dark ships, impossible jumps and port calls" --body "..."
```

Wait for CI green (`gh pr checks --watch`). Do not merge without the owner's go.

---

### Task 6: French project page

**Files:**
- Modify: `antilles-data-pipeline/dashboard/src/app/projets/caribbean-ais-stream/page.tsx` (and `_components/` if the roadmap lives there)

- [ ] **Step 1: Update the page**

On a branch `docs/ais-anomalies` of `antilles-data-pipeline`: mark the anomaly step as done,
describe the three detections in French in the page's existing tone, loitering as "plus
tard". Add any new term (fenêtre de session, MATCH_RECOGNIZE) to the glossary page if the
page links glossary terms.

- [ ] **Step 2: Verify**

Run in `dashboard/`: `npm run lint && npm run typecheck && npm test && npm run build`
Expected: all pass.

- [ ] **Step 3: Commit, push, PR**

```bash
git commit -am "Mark AIS anomaly detection as done on the project page"
git push -u origin docs/ais-anomalies
gh pr create --title "AIS: anomaly detection done" --body "..."
```

CI green; merge only with the owner's go.
