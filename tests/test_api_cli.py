from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from typer.testing import CliRunner

from ais_stream import cli


def test_api_command_serves_the_app(monkeypatch: pytest.MonkeyPatch) -> None:
    served: list[tuple[FastAPI, str, int]] = []

    def fake_run(app: FastAPI, host: str, port: int) -> None:
        served.append((app, host, port))

    monkeypatch.setattr(cli.uvicorn, "run", fake_run)
    monkeypatch.setenv("SITE_DATABASE_URL", "postgresql://site_reader:x@db/ais")

    result = CliRunner().invoke(cli.app, ["api", "--host", "0.0.0.0", "--port", "8000"])

    assert result.exit_code == 0, result.output
    [(app, host, port)] = served
    assert (host, port) == ("0.0.0.0", 8000)
    paths = {route.path for route in app.routes if isinstance(route, APIRoute)}
    assert paths == {"/v1/ships", "/v1/anomalies", "/health"}


def test_api_command_needs_a_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SITE_DATABASE_URL", raising=False)
    assert CliRunner().invoke(cli.app, ["api"]).exit_code != 0
