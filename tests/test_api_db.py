"""The site's endpoint on a real PostgreSQL, schema from postgres/init.sql, read as the
`site_reader` role the deployed service uses. Rows are written as the Flink job would."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from fastapi.testclient import TestClient

from ais_stream import api
from ais_stream.api import SITE_ORIGINS, create_app

pytestmark = pytest.mark.integration


def position(
    conn: psycopg.Connection,
    now: datetime,
    mmsi: int,
    minutes_ago: float,
    lat: float | None = 16.24,
    lon: float | None = -61.53,
    name: str | None = "KARUKERA EXPRESS",
) -> None:
    conn.execute(
        "insert into ship_positions"
        " (mmsi, ts, lat, lon, sog_kn, cog_deg, heading_deg, nav_status, ship_name, port)"
        " values (%s, %s, %s, %s, 12.5, 287.0, 286, 0, %s, 'Pointe-à-Pitre')",
        (mmsi, now - timedelta(minutes=minutes_ago), lat, lon, name),
    )


@pytest.fixture
def now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


@pytest.fixture
def client(site_dsn: str) -> TestClient:
    return TestClient(create_app(site_dsn))


@pytest.fixture
def owner(postgres: str) -> psycopg.Connection:
    return psycopg.connect(postgres, autocommit=True)


def test_only_ships_seen_within_the_window(
    client: TestClient, owner: psycopg.Connection, now: datetime
) -> None:
    with owner:
        position(owner, now, 227000001, minutes_ago=5)
        position(owner, now, 227000002, minutes_ago=28)
        position(owner, now, 227000003, minutes_ago=32)
        position(owner, now, 227000004, minutes_ago=90)

    default = client.get("/v1/ships")
    wider = client.get("/v1/ships", params={"since_minutes": 120})

    assert default.status_code == 200
    assert [s["mmsi"] for s in default.json()["ships"]] == [227000001, 227000002]
    assert [s["mmsi"] for s in wider.json()["ships"]] == [
        227000001,
        227000002,
        227000003,
        227000004,
    ]


def test_ship_shape(client: TestClient, owner: psycopg.Connection, now: datetime) -> None:
    with owner:
        position(owner, now, 227000001, minutes_ago=2)

    body = client.get("/v1/ships").json()

    assert datetime.fromisoformat(body["generated_at"]) >= now
    assert body["ships"] == [
        {
            "mmsi": 227000001,
            "name": "KARUKERA EXPRESS",
            "lat": 16.24,
            "lon": -61.53,
            "sog_kn": 12.5,
            "cog_deg": 287.0,
            "heading_deg": 286,
            "ts": body["ships"][0]["ts"],
            "port": "Pointe-à-Pitre",
        }
    ]
    assert datetime.fromisoformat(body["ships"][0]["ts"]) == now - timedelta(minutes=2)


def test_ship_without_position_is_left_out(
    client: TestClient, owner: psycopg.Connection, now: datetime
) -> None:
    """AIS "not available" coordinates become nulls; the map must not draw them at (0, 0)."""
    with owner:
        position(owner, now, 227000001, minutes_ago=1)
        position(owner, now, 227000002, minutes_ago=1, lat=None, lon=None)
        position(owner, now, 227000003, minutes_ago=1, lat=None)
        position(owner, now, 227000004, minutes_ago=1, lon=None)

    ships = client.get("/v1/ships").json()["ships"]

    assert [s["mmsi"] for s in ships] == [227000001]


def test_responses_are_cacheable_for_five_seconds(client: TestClient) -> None:
    for path in ("/v1/ships", "/v1/anomalies"):
        assert client.get(path).headers["cache-control"] == "public, max-age=5"


def test_cors_on_a_real_response(client: TestClient) -> None:
    allowed = client.get("/v1/ships", headers={"Origin": SITE_ORIGINS[0]})
    other = client.get("/v1/ships", headers={"Origin": "https://evil.example"})

    assert allowed.headers["access-control-allow-origin"] == SITE_ORIGINS[0]
    assert "access-control-allow-origin" not in other.headers


def test_anomalies_shape_and_window(
    client: TestClient, owner: psycopg.Connection, now: datetime
) -> None:
    dark_since = now - timedelta(minutes=40)
    back_from = now - timedelta(hours=3)
    with owner:
        # Dark now: a closed session, no gap yet.
        owner.execute(
            "insert into ship_sessions values (227000001, %s, %s, 'BLUE MERIDIAN', 16.0, -61.7)",
            (dark_since - timedelta(hours=2), dark_since),
        )
        # Dark for 45 minutes, then back.
        owner.execute(
            "insert into ship_sessions values (227000002, %s, %s, 'OCEAN PEARL', 14.6, -61.1)",
            (back_from - timedelta(hours=1), back_from),
        )
        owner.execute(
            "insert into ais_gaps values"
            " (227000002, %s, %s, 45, 14.6, -61.1, 14.7, -61.2, 'OCEAN PEARL')",
            (back_from, back_from + timedelta(minutes=45)),
        )
        # Went dark long before the window.
        owner.execute(
            "insert into ship_sessions values (227000003, %s, %s, 'OLD NEWS', 15.0, -61.0)",
            (now - timedelta(hours=30), now - timedelta(hours=20)),
        )
        owner.execute(
            "insert into position_jumps values"
            " (227000004, %s, %s, 16.2, -61.5, 16.7, -61.5, 30.0, 900.0, 'SPOOFED')",
            (now - timedelta(minutes=11), now - timedelta(minutes=10)),
        )
        owner.execute(
            "insert into position_jumps values"
            " (227000005, %s, %s, 16.2, -61.5, 16.7, -61.5, 30.0, 900.0, 'TOO OLD')",
            (now - timedelta(hours=8, minutes=1), now - timedelta(hours=8)),
        )

    body = client.get("/v1/anomalies").json()
    wider = client.get("/v1/anomalies", params={"hours": 48}).json()

    assert datetime.fromisoformat(body["generated_at"]) >= now
    dark = body["dark_periods"]
    assert [d["mmsi"] for d in dark] == [227000001, 227000002]  # latest first
    assert dark[0] == {
        "mmsi": 227000001,
        "name": "BLUE MERIDIAN",
        "silent_from": dark[0]["silent_from"],
        "silent_until": None,
        "silent_minutes": None,
        "lat": 16.0,
        "lon": -61.7,
    }
    assert datetime.fromisoformat(dark[0]["silent_from"]) == dark_since
    assert dark[1]["silent_minutes"] == 45
    assert datetime.fromisoformat(dark[1]["silent_until"]) == back_from + timedelta(minutes=45)

    assert body["position_jumps"] == [
        {
            "mmsi": 227000004,
            "name": "SPOOFED",
            "from_time": body["position_jumps"][0]["from_time"],
            "to_time": body["position_jumps"][0]["to_time"],
            "from_lat": 16.2,
            "from_lon": -61.5,
            "to_lat": 16.7,
            "to_lon": -61.5,
            "distance_nm": 30.0,
            "implied_speed_kn": 900.0,
        }
    ]
    assert [d["mmsi"] for d in wider["dark_periods"]] == [227000001, 227000002, 227000003]
    assert [j["mmsi"] for j in wider["position_jumps"]] == [227000004, 227000005]


def test_health_with_the_database_up(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert "cache-control" not in response.headers


def test_site_role_can_only_read_what_it_serves(site_dsn: str) -> None:
    with psycopg.connect(site_dsn, autocommit=True) as conn:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            conn.execute("delete from ship_positions")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            conn.execute("select * from port_events")


def test_a_200_varies_on_origin(client: TestClient) -> None:
    response = client.get("/v1/ships")
    assert response.status_code == 200
    assert "Origin" in [v.strip() for v in response.headers["vary"].split(",")]


def test_slow_queries_are_cancelled(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api, "SHIPS_SQL", "select pg_sleep(3), %s")
    with pytest.raises(psycopg.errors.QueryCanceled):
        client.get("/v1/ships")


def test_site_password_follows_the_environment(
    postgres: str, site_dsn: str, rerun_init_sql: Callable[[str], None]
) -> None:
    rerun_init_sql("rotated")

    with psycopg.connect(postgres.replace("ais:ais@", "site_reader:rotated@", 1)) as conn:
        assert conn.execute("select current_user").fetchone() == ("site_reader",)
    with pytest.raises(psycopg.OperationalError):
        psycopg.connect(site_dsn).close()
