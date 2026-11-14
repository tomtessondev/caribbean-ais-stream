"""End to end on the Compose stack: producer → Kafka → Flink SQL → PostgreSQL.

Needs the stack running (`make e2e-up`, as CI does) and is skipped otherwise; the tests
submit the Flink job themselves, after the traffic. Synthetic traffic is aligned on whole
hours so the expected windows are exact.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest
from grafana_queries import dashboard_queries, prometheus_queries, render
from psycopg.rows import dict_row

from ais_stream.kafka import AisProducer
from ais_stream.messages import parse_aisstream_time
from ais_stream.pipeline import handle
from ais_stream.synthetic import traffic

pytestmark = pytest.mark.e2e

KAFKA = os.getenv("E2E_KAFKA", "localhost:9094")
FLINK = os.getenv("E2E_FLINK", "http://localhost:8082")
POSTGRES = os.getenv("E2E_POSTGRES", "postgresql://ais:ais@localhost:5434/ais")
PROMETHEUS = os.getenv("E2E_PROMETHEUS", "http://localhost:9090")
# Grafana's read-only role (postgres/init.sql), with the local default password.
READER = POSTGRES.replace("ais:ais@", "grafana_reader:grafana-local@")
REPOSITORY = Path(__file__).resolve().parents[1]
PORTS = {"Basse-Terre", "Castries", "Fort-de-France", "Pointe-a-Pitre", "Roseau"}


def query(sql: str) -> list[tuple[Any, ...]]:
    with psycopg.connect(POSTGRES) as conn:
        return conn.execute(sql.encode()).fetchall()


def wait_until(condition: Callable[[], bool], timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(2)
    raise AssertionError(f"timed out waiting for {what}")


def flink_job_running() -> bool:
    try:
        with urllib.request.urlopen(f"{FLINK}/jobs/overview", timeout=5) as response:
            jobs = json.load(response)["jobs"]
    except OSError:
        return False  # the cluster is still starting
    return any(j["name"] == "ais-port-activity" and j["state"] == "RUNNING" for j in jobs)


def flink_cluster_up() -> bool:
    try:
        with urllib.request.urlopen(f"{FLINK}/overview", timeout=5) as response:
            return bool(json.load(response)["taskmanagers"])
    except OSError:
        return False


def submit_job() -> None:
    subprocess.run(
        ["docker", "compose", "run", "--rm", "--no-deps", "flink-jobs"],
        cwd=REPOSITORY,
        check=True,
        capture_output=True,
    )
    wait_until(flink_job_running, 120, "the Flink job")


def play(start: datetime, hours: float) -> int:
    producer = AisProducer(KAFKA)
    records = sum(handle(json.dumps(m), producer) for m in traffic(start, hours))
    assert producer.flush() == 0 and producer.failed == 0
    return records


@pytest.fixture(scope="module")
def played() -> datetime:
    try:
        wait_until(flink_cluster_up, 180, "the Flink cluster")
    except AssertionError:
        if os.getenv("CI"):
            raise  # in CI the stack must be there: never pass by skipping
        pytest.skip("the Compose stack is not running (make e2e-up)")
    # Two days back, on a whole hour: windows are aligned and no live data overlaps.
    start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(days=2)
    play(start, hours=4)
    # The job starts once the traffic is in Kafka, so it reads a backlog on every partition
    # at once. Started first, it would wait with all partitions marked idle, the first
    # partition read would push the watermark hours ahead alone, and the others' early
    # positions would arrive late and be dropped.
    if not flink_job_running():
        submit_job()
    # The last hour stays open (the watermark never passes its end): three closed hours per
    # ship. The anomalies of the fourth hour are written a moment later: wait for those too.
    expected = {
        f"select count(*) from ship_hourly where window_start >= '{start}'": 27,
        f"select count(*) from port_events where event_time >= '{start}'": 12,
        f"select count(*) from position_jumps where to_time >= '{start}'": 2,
        f"select count(*) from ais_gaps where silent_from >= '{start}'": 1,
        # Every ship reports until the end of the play: its row holds one of the last minutes.
        f"select count(*) from ship_positions where ts > '{start + timedelta(minutes=235)}'": 9,
    }
    wait_until(
        lambda: all(query(sql) == [(count,)] for sql, count in expected.items()),
        180,
        "every result table",
    )
    return start


def test_every_ship_gets_a_row_per_closed_hour(played: datetime) -> None:
    rows = query(
        "select mmsi, count(*), min(window_start) from ship_hourly"
        f" where window_start >= '{played}' group by mmsi"
    )
    assert len(rows) == 9
    assert all(count == 3 and first == played for _, count, first in rows)


def test_ferry_speed_and_the_ship_that_went_dark(played: datetime) -> None:
    ferry = query(
        "select max(max_sog_kn) from ship_hourly"
        f" where mmsi = 227000101 and window_start >= '{played}'"
    )
    assert 23 < ferry[0][0] < 26  # the 24-knot inter-island ferry
    # BLUE MERIDIAN switches its AIS off for 45 minutes at the start of the third hour.
    dark = dict(
        query(
            "select extract(hour from window_start - "
            f"'{played}'::timestamptz)::int, positions from ship_hourly"
            f" where mmsi = 341000505 and window_start >= '{played}'"
        )
    )
    assert dark[0] > 300 and dark[2] < dark[0] / 2


def test_port_activity_sees_every_harbour(played: datetime) -> None:
    rows = query(
        "select port, max(ships), max(moored) from port_activity"
        f" where window_start >= '{played}' group by port"
    )
    ports = {port: (ships, moored) for port, ships, moored in rows}
    assert set(ports) <= PORTS and "Fort-de-France" in ports
    assert ports["Fort-de-France"][1] >= 1  # the tanker at anchor in the bay


def test_only_the_dark_ship_goes_dark(played: datetime) -> None:
    """Moored ships report every 3 minutes and every ship's last session is still open
    when the play ends: the only closed session is BLUE MERIDIAN's, before its silence."""
    sessions = query(
        f"select mmsi, count(*) from ship_sessions where session_start >= '{played}' group by mmsi"
    )
    assert sessions == [(341000505, 1)]
    ((silent_from, silent_minutes),) = query(
        f"select silent_from, silent_minutes from dark_periods where silent_from >= '{played}'"
    )
    assert timedelta(hours=1, minutes=59) < silent_from - played <= timedelta(hours=2)
    assert 44 < silent_minutes < 46


def test_only_the_spoofed_position_jumps(played: datetime) -> None:
    """OCEAN PEARL reports one position 30 nm off its track, at 1 h 30: a jump out and a
    jump back. The dark ship's return and the anchored tanker's swinging are not jumps."""
    rows = query(
        "select mmsi, to_time, distance_nm, implied_speed_kn from position_jumps"
        f" where to_time >= '{played}' order by to_time"
    )
    assert [mmsi for mmsi, *_ in rows] == [255000707, 255000707]
    assert rows[0][1] - played == timedelta(hours=1, minutes=30)
    assert all(25 < distance < 35 and speed > 60 for _, _, distance, speed in rows)


def test_port_arrivals_and_departures(played: datetime) -> None:
    rows = query(
        "select mmsi, event, port from port_events"
        f" where event_time >= '{played}' order by mmsi, event_time"
    )
    assert len(rows) == 12  # replayed from the generator through the same port boxes
    by_ship: dict[int, list[tuple[str, str]]] = defaultdict(list)
    for mmsi, event, port in rows:
        by_ship[mmsi].append((event, port))
    assert 538000404 not in by_ship  # the tanker never leaves the Fort-de-France box
    assert by_ship[227000101][:2] == [("departure", "Pointe-a-Pitre"), ("arrival", "Roseau")]
    for events in by_ship.values():  # departures and arrivals alternate, same port in/out
        for (e1, p1), (e2, p2) in zip(events, events[1:], strict=False):
            assert e1 != e2
            assert e1 == "departure" or p1 == p2


def test_ship_positions_hold_each_ships_last_report(played: datetime) -> None:
    rows = query("select mmsi, ts from ship_positions order by mmsi")
    last = {}
    for message in traffic(played, 4):
        if message["MessageType"] == "PositionReport":
            last[message["MetaData"]["MMSI"]] = parse_aisstream_time(
                message["MetaData"]["time_utc"]
            )
    assert dict(rows) == last


@pytest.mark.parametrize(("name", "sql"), dashboard_queries())
def test_every_dashboard_query_runs_as_grafana(played: datetime, name: str, sql: str) -> None:
    with psycopg.connect(READER) as conn:
        conn.execute(render(sql).encode()).fetchall()


def map_rows() -> list[dict[str, Any]]:
    (sql,) = [sql for name, sql in dashboard_queries() if name == "ais-traffic/Ships (latest)"]
    with psycopg.connect(READER, row_factory=dict_row) as conn:
        return conn.execute(render(sql).encode()).fetchall()


def test_map_shows_every_ship(played: datetime) -> None:
    """The traffic is two days old: the map is anchored on the data, not on the clock."""
    rows = map_rows()
    assert len(rows) == 9
    assert {row["state"] for row in rows} <= {"moored", "under way", "jump"}


def test_a_dark_ship_stays_on_the_map(played: datetime) -> None:
    """A ship silent for an hour is past the map's 30-minute window, but an open dark
    period keeps it there, in red. Its rows are removed afterwards; MMSI 999000001 is
    never in Kafka, so Flink never touches them."""
    ((anchor,),) = query("select max(ts) from ship_positions")
    last_seen = anchor - timedelta(hours=1)
    with psycopg.connect(POSTGRES) as conn:
        conn.execute(
            "insert into ship_positions (mmsi, ts, lat, lon, ship_name)"
            " values (%s, %s, %s, %s, %s)",
            (999000001, last_seen, 15.6, -61.5, "TEST DARK"),
        )
        conn.execute(
            "insert into ship_sessions (mmsi, session_start, last_seen, ship_name, last_lat,"
            " last_lon) values (%s, %s, %s, %s, %s, %s)",
            (999000001, last_seen - timedelta(hours=1), last_seen, "TEST DARK", 15.6, -61.5),
        )
    try:
        states = {row["mmsi"]: row["state"] for row in map_rows()}
        assert states[999000001] == "dark"
    finally:
        with psycopg.connect(POSTGRES) as conn:
            conn.execute("delete from ship_positions where mmsi = 999000001")
            conn.execute("delete from ship_sessions where mmsi = 999000001")


def prom(expr: str) -> list[dict[str, Any]]:
    """Result of an instant PromQL query; an invalid query raises (HTTP 400)."""
    url = f"{PROMETHEUS}/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    with urllib.request.urlopen(url, timeout=5) as response:
        result: list[dict[str, Any]] = json.load(response)["data"]["result"]
    return result


def test_prometheus_sees_lag_and_flink(played: datetime) -> None:
    """The lag comes from the broker (Kafka exporter), so it stays visible when the job is
    down; Flink's own metrics come from its Prometheus reporter."""
    wait_until(lambda: bool(prom('kafka_consumergroup_lag{consumergroup="flink-ais"}')), 120, "lag")
    wait_until(lambda: bool(prom('{__name__=~"flink_.+"}')), 120, "Flink metrics")


@pytest.mark.parametrize(("name", "expr"), prometheus_queries())
def test_every_promql_query_is_valid(played: datetime, name: str, expr: str) -> None:
    prom(expr)


def test_grafana_role_cannot_write(played: datetime) -> None:
    with pytest.raises(psycopg.errors.InsufficientPrivilege), psycopg.connect(READER) as conn:
        conn.execute("delete from port_events")


def test_reprocessing_from_scratch_gives_the_same_results(played: datetime) -> None:
    """Cancel the job and submit it again: it re-reads Kafka from the earliest offset and
    recomputes every window. The sinks upsert on their primary key, so the tables come out
    identical, with no duplicate. (Re-sending the same traffic instead would prove nothing:
    events older than the watermark are late and dropped before any window.)"""
    snapshots = [
        "select port, window_start, ships, moored, positions from port_activity"
        f" where window_start >= '{played}' order by 1, 2",
        f"select * from ship_sessions where session_start >= '{played}' order by 1, 2",
        f"select * from ais_gaps where silent_from >= '{played}' order by 1, 2",
        f"select * from position_jumps where to_time >= '{played}' order by 1, 3",
        f"select * from port_events where event_time >= '{played}' order by 1, 2",
        "select * from ship_positions order by 1",
    ]
    before = [query(snapshot) for snapshot in snapshots]

    with urllib.request.urlopen(f"{FLINK}/jobs/overview", timeout=5) as response:
        running = [j["jid"] for j in json.load(response)["jobs"] if j["state"] == "RUNNING"]
    for jid in running:
        urllib.request.urlopen(
            urllib.request.Request(f"{FLINK}/jobs/{jid}?mode=cancel", method="PATCH"), timeout=5
        )
    wait_until(lambda: not flink_job_running(), 60, "the job to stop")
    submit_job()
    time.sleep(45)  # the new job re-reads the topic and rewrites the closed windows

    assert [query(snapshot) for snapshot in snapshots] == before
