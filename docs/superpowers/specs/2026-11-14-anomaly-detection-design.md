# Anomaly detection (roadmap step 3) — design

## Intent

Step 3 of `docs/ROADMAP.md`. The Flink job already turns `ais.positions` into port activity
and hourly ship statistics. It should now also detect, in real time:

1. ships going dark (AIS silence),
2. physically impossible position jumps,
3. port arrivals and departures.

The project is a portfolio piece for a move into freelance data engineering: each detection
must be short, readable SQL that shows a distinct Flink feature, and the end-to-end test must
prove it on the synthetic traffic.

## Goals

- Three new results in PostgreSQL, computed by Flink SQL from `ais.positions`.
- Synthetic traffic that exercises every detection (it already has the dark ship; it needs a
  ship that jumps).
- E2E assertions on exact, known events of the synthetic traffic.
- Docs updated in the same piece of work: README, ROADMAP, CLAUDE.md, and the French page
  `antilles-data-pipeline/dashboard/src/app/projets/caribbean-ais-stream/` (separate repo,
  separate PR).

## Non-goals

- Loitering (listed in the roadmap, left for later; ROADMAP keeps it as not done).
- A silence threshold adapted to each ship's own reporting rate (needs DataStream keyed state
  and timers, ruled out below).
- Maps, dashboards, notifications (step 4).
- Using `ais.ships` (static data) in the detections.

## Anti-goals

- False alerts on the normal synthetic traffic: the dark ship coming back after 45 minutes
  must not count as a jump, the anchored tanker's swinging must not count as anything, a ship
  that starts inside a port must not get an arrival.
- Leaving Flink SQL for a second language or build tool in this repository.

## Constraints

- Flink 1.20 SQL, one statement set (the job reads `ais.positions` once). Keep the pipeline
  name `ais-port-activity`, which the e2e tests look up.
- Event time and watermarks as today (30 s out of order, 30 s idle timeout): every result is
  final when emitted, so the JDBC sinks stay append/upsert on a primary key and reprocessing
  from scratch gives the same rows (existing e2e test).
- `flink-cep-1.20.2.jar` is already in `/opt/flink/lib` of the image (checked), so
  `MATCH_RECOGNIZE` needs no new jar.
- No `MATCH_RECOGNIZE` on a regular join (it would lose the time attribute): the port boxes
  stay in the `positions_in_port` CASE view.

## Approaches

| Approach | Trade-off |
| --- | --- |
| **All Flink SQL** (chosen) | Continuity with step 2, each anomaly shows one Flink feature (session windows, `MATCH_RECOGNIZE`). The silence threshold is a fixed gap, not each ship's own rate. |
| Java DataStream job | Per-ship state and timers, alert exactly at a ship-specific threshold. Adds Maven and Java to a Python repo. |
| PyFlink | Same power in Python, but a much heavier Flink image and rare in production. |

The owner picked all Flink SQL.

## Design

### 1. Ships going dark — session windows

`SESSION(TABLE positions PARTITION BY mmsi, DESCRIPTOR(event_time), INTERVAL '15' MINUTES)`,
grouped per ship and session, written to a new table **`ship_sessions`**:

| column | meaning |
| --- | --- |
| `mmsi`, `session_start` | primary key |
| `last_seen` | event time of the last position (= `window_end` − 15 min) |
| `ship_name`, `positions` | |
| `last_lat`, `last_lon` | where the ship went silent |

A session is emitted when the watermark passes `last_seen + 15 min`, i.e. 15 minutes after
the ship fell silent: that row is the "went dark" alert.

The return is a second detection, because the session the ship starts on its return is only
emitted when *that* session ends (the next silence, possibly days later; in the 4-hour e2e
play, never). A `MATCH_RECOGNIZE` on `positions` (`PARTITION BY mmsi ORDER BY event_time`,
`PATTERN (A B)`, `B` defined as `B.event_time - A.event_time > 15 min`) writes a new table
**`ais_gaps`** the moment the ship reports again: `mmsi`, `silent_from` (= `A.event_time`,
PK with `mmsi`), `silent_until` (= `B.event_time`), `silent_minutes`, both positions,
`ship_name`.

A PostgreSQL **view `dark_periods`** (in `postgres/init.sql`) joins the two:
`ship_sessions` left join `ais_gaps` on `mmsi` and `last_seen = silent_from` (both are the
event time of the same last position). `silent_until` is null while the ship is still dark.
Two Flink tables written by different queries, rather than two queries upserting partial
rows into one table, so neither write can overwrite the other.

Why 15 minutes, fixed: ships under way report every 10 s, ships moored or at anchor every
3 minutes (in the synthetic traffic and in AIS class A practice). 15 minutes is five missed
reports for a moored ship and far below the dark ship's 45-minute silence.

Known limitation (document it): on the live feed, a ship leaving the coverage area also ends
its session and shows as dark.

### 2. Impossible jumps — `MATCH_RECOGNIZE`

On `positions`, `PARTITION BY mmsi ORDER BY event_time`, `PATTERN (A B)`,
`AFTER MATCH SKIP TO LAST B` (so a spoofed position's jump out and jump back are both
caught), `B` defined as: distance(A, B) > 1 nm **and** distance / elapsed time > 60 kn.

- Distance: equirectangular approximation in SQL (`RADIANS`, `COS`, `SQRT`, same formula as
  `synthetic._distance_nm`); exact enough at these distances.
- The 1 nm floor keeps GPS jitter from short intervals out. 60 kn is above any ship in the
  area (fast ferries ~40 kn).
- Two positions with the same timestamp: use a minimum elapsed time of 1 s (builder's call on
  the exact form).

New table **`position_jumps`**: `mmsi`, `from_time`, `to_time` (PK `mmsi, to_time`),
`from_lat`, `from_lon`, `to_lat`, `to_lon`, `distance_nm`, `implied_speed_kn`, `ship_name`.

### 3. Port arrivals and departures — `MATCH_RECOGNIZE`

On `positions_in_port` (the existing CASE view), per ship in event-time order:

- arrival: `PATTERN (OUTSIDE INSIDE)`, outside = `port IS NULL`, inside = `port IS NOT NULL`;
- departure: `PATTERN (INSIDE OUTSIDE)`.

Two queries `UNION ALL` into one new table **`port_events`**: `mmsi`, `event_time`
(PK `mmsi, event_time`), `port`, `event` (`'arrival'` / `'departure'`), `ship_name`, `sog_kn`.
A ship first seen inside a port gets no arrival (there is no outside row before it). Ships
passing through a box without stopping get an arrival and a departure: they did enter and
leave the port area; that is what the event means.

No debouncing for now: boxes are several miles wide and the synthetic traffic has no edge
noise. Add it (e.g. `INSIDE{2}`) only if the e2e test shows flapping.

### 4. Synthetic traffic

Add a ninth ship that jumps: under way on a shuttle route, with one position, at a fixed
offset from the start (inside the 4-hour e2e window, e.g. at 1 h 30), reported about 30 nm
away from its true track, then back on track: two jumps expected. Implement it as an option
of `_shuttle` (like `silent`), document it in the module docstring, unit-test it in
`tests/test_synthetic.py`. Same seed, same traffic for the other ships: the new ship gets its
own `random.Random` drawn **after** the existing ones so their streams do not change.

### 5. Tests

- Unit: the jumping ship's spoofed position and its return (`test_synthetic.py`).
- E2E (`tests/test_e2e.py`), on the existing 4-hour play:
  - row counts move from 8 ships / 24 hourly rows to 9 / 27;
  - `dark_periods`: BLUE MERIDIAN has exactly one period starting around 2 h, closed by
    `ais_gaps` after about 45 minutes; no other ship has one (moored ships report every
    3 minutes, so no gap reaches 15);
  - `position_jumps`: exactly the two jumps of the new ship; none for BLUE MERIDIAN;
  - `port_events`: a few exact events computed from the generator (e.g. the ferry's
    departure from Pointe-a-Pitre and arrival at Roseau, the cruise ship's arrival at
    Castries), and no arrival for the anchored tanker;
  - the reprocessing test also snapshots the four new tables.
- `make check`, `make integration`, `make e2e-up e2e` pass.

### 6. Docs

README (what is detected and how), ROADMAP (step 3 done, loitering left for later),
CLAUDE.md (the new tables), and the French AIS page with the anomaly step marked done
(separate PR in `antilles-data-pipeline`, CI green, then merge).

## Left to the builder

- Exact SQL shapes (CTE vs views, column order), within the design above.
- The jumping ship's name, MMSI, route and spoof offset.
- The exact expected port events in the e2e test, read from the generator's output.
- Commit split, as long as each commit passes `make check`.
