"""The site's endpoint without a database: what FastAPI and the CORS middleware decide before
a query runs. The queries themselves are tested on a real PostgreSQL (test_api_db.py)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from ais_stream.api import SITE_ORIGINS, create_app

# Nothing listens on port 1: a request that reached the database would fail with a 500.
UNREACHABLE = "postgresql://site_reader:x@127.0.0.1:1/ais?connect_timeout=1"


@pytest.fixture
def client() -> TestClient:
    return TestClient(create_app(UNREACHABLE))


@pytest.mark.parametrize("since", [0, 181, -5])
def test_since_minutes_out_of_range_is_refused(client: TestClient, since: int) -> None:
    assert client.get("/v1/ships", params={"since_minutes": since}).status_code == 422


@pytest.mark.parametrize("hours", [0, 49])
def test_hours_out_of_range_is_refused(client: TestClient, hours: int) -> None:
    assert client.get("/v1/anomalies", params={"hours": hours}).status_code == 422


@pytest.mark.parametrize("origin", SITE_ORIGINS)
def test_the_site_origins_may_call(client: TestClient, origin: str) -> None:
    response = client.options(
        "/v1/ships", headers={"Origin": origin, "Access-Control-Request-Method": "GET"}
    )
    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == origin


@pytest.mark.parametrize(
    "origin", ["https://evil.example", "https://antilles-data-pipeline.vercel.app.evil.example"]
)
def test_other_origins_may_not(client: TestClient, origin: str) -> None:
    response = client.options(
        "/v1/ships", headers={"Origin": origin, "Access-Control-Request-Method": "GET"}
    )
    assert "access-control-allow-origin" not in response.headers


def test_only_get_is_allowed(client: TestClient) -> None:
    preflight = client.options(
        "/v1/ships",
        headers={"Origin": SITE_ORIGINS[0], "Access-Control-Request-Method": "POST"},
    )
    assert preflight.status_code == 400
    assert client.post("/v1/ships").status_code == 405


def test_health_reports_an_unreachable_database(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 503
    assert response.json() == {"status": "unavailable"}
