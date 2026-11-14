"""End to end on the Compose stack: producer → Kafka → Flink SQL → PostgreSQL.

Needs the stack running (`make e2e-up`, as CI does) and is skipped otherwise. Synthetic
traffic is aligned on whole hours so the expected windows are exact.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest

from ais_stream.kafka import AisProducer
from ais_stream.pipeline import handle
from ais_stream.synthetic import traffic

pytestmark = pytest.mark.e2e

KAFKA = os.getenv("E2E_KAFKA", "localhost:9094")
FLINK = os.getenv("E2E_FLINK", "http://localhost:8082")
POSTGRES = os.getenv("E2E_POSTGRES", "postgresql://ais:ais@localhost:5434/ais")
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


def play(start: datetime, hours: float) -> int:
    producer = AisProducer(KAFKA)
    records = sum(handle(json.dumps(m), producer) for m in traffic(start, hours))
    assert producer.flush() == 0 and producer.failed == 0
    return records


@pytest.fixture(scope="module")
def played() -> datetime:
    try:
        wait_until(flink_job_running, 180, "the Flink job")
    except AssertionError:
        if os.getenv("CI"):
            raise  # in CI the stack must be there: never pass by skipping
        pytest.skip("the Compose stack is not running (make e2e-up)")
    # Two days back, on a whole hour: windows are aligned and no live data overlaps.
    start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(days=2)
    play(start, hours=4)
    # The last hour stays open (the watermark never passes its end): three closed hours per ship.
    wait_until(
        lambda: (
            query(f"select count(*) from ship_hourly where window_start >= '{start}'") == [(27,)]
        ),
        180,
        "27 hourly rows",
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
    ]
    before = [query(snapshot) for snapshot in snapshots]

    with urllib.request.urlopen(f"{FLINK}/jobs/overview", timeout=5) as response:
        running = [j["jid"] for j in json.load(response)["jobs"] if j["state"] == "RUNNING"]
    for jid in running:
        urllib.request.urlopen(
            urllib.request.Request(f"{FLINK}/jobs/{jid}?mode=cancel", method="PATCH"), timeout=5
        )
    wait_until(lambda: not flink_job_running(), 60, "the job to stop")
    subprocess.run(
        ["docker", "compose", "run", "--rm", "--no-deps", "flink-jobs"],
        cwd=REPOSITORY,
        check=True,
        capture_output=True,
    )
    wait_until(flink_job_running, 120, "the resubmitted job")
    time.sleep(45)  # the new job re-reads the topic and rewrites the closed windows

    assert [query(snapshot) for snapshot in snapshots] == before
