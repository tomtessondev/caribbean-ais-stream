"""Read-only HTTP endpoint for the portfolio site's live map: the latest position of each
ship and the recent anomalies, from the tables the Flink job writes. It connects as
`site_reader`, which can only select those tables (postgres/init.sql)."""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg
from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from psycopg.rows import class_row
from pydantic import BaseModel

SITE_ORIGINS = ("https://antilles-data-pipeline.vercel.app", "http://localhost:3000")
# The site polls every 5 seconds; a shared cache in front can answer in between.
CACHE_CONTROL = "public, max-age=5"


class Ship(BaseModel):
    mmsi: int
    name: str | None
    lat: float
    lon: float
    sog_kn: float | None
    cog_deg: float | None
    heading_deg: int | None
    ts: datetime
    port: str | None


class Ships(BaseModel):
    generated_at: datetime
    ships: list[Ship]


class DarkPeriod(BaseModel):
    """A ship's AIS silence: `silent_until` and `silent_minutes` are null while it lasts."""

    mmsi: int
    name: str | None
    silent_from: datetime
    silent_until: datetime | None
    silent_minutes: float | None
    lat: float | None
    lon: float | None


class PositionJump(BaseModel):
    mmsi: int
    name: str | None
    from_time: datetime
    to_time: datetime
    from_lat: float | None
    from_lon: float | None
    to_lat: float | None
    to_lon: float | None
    distance_nm: float
    implied_speed_kn: float


class Anomalies(BaseModel):
    generated_at: datetime
    dark_periods: list[DarkPeriod]
    position_jumps: list[PositionJump]


# A position without coordinates (AIS "not available") would be drawn at (0, 0).
SHIPS_SQL = """
select mmsi, ship_name as name, lat, lon, sog_kn, cog_deg, heading_deg, ts, port
from ship_positions
where ts > now() - make_interval(mins => %s) and lat is not null and lon is not null
order by mmsi
"""

# Silences that started within the window, like the Grafana panel: on the live feed a ship
# that left the covered area stays dark forever and would otherwise never drop off.
DARK_SQL = """
select mmsi, ship_name as name, silent_from, silent_until, silent_minutes,
       last_lat as lat, last_lon as lon
from dark_periods
where silent_from > now() - make_interval(hours => %s)
order by silent_from desc, mmsi
"""

JUMPS_SQL = """
select mmsi, ship_name as name, from_time, to_time, from_lat, from_lon, to_lat, to_lon,
       distance_nm, implied_speed_kn
from position_jumps
where to_time > now() - make_interval(hours => %s)
order by to_time desc, mmsi
"""


def create_app(dsn: str) -> FastAPI:
    app = FastAPI(title="Caribbean AIS stream", docs_url=None, redoc_url=None)
    app.add_middleware(CORSMiddleware, allow_origins=SITE_ORIGINS, allow_methods=["GET"])

    @app.get("/v1/ships")
    def ships(response: Response, since_minutes: int = Query(30, ge=1, le=180)) -> Ships:
        """Ships seen in the last `since_minutes`, at their latest position."""
        with psycopg.connect(dsn) as conn, conn.cursor(row_factory=class_row(Ship)) as cur:
            rows = cur.execute(SHIPS_SQL, (since_minutes,)).fetchall()
        response.headers["Cache-Control"] = CACHE_CONTROL
        return Ships(generated_at=datetime.now(UTC), ships=rows)

    @app.get("/v1/anomalies")
    def anomalies(response: Response, hours: int = Query(6, ge=1, le=48)) -> Anomalies:
        """Dark periods and impossible position jumps of the last `hours`."""
        with psycopg.connect(dsn) as conn:
            with conn.cursor(row_factory=class_row(DarkPeriod)) as cur:
                dark = cur.execute(DARK_SQL, (hours,)).fetchall()
            with conn.cursor(row_factory=class_row(PositionJump)) as cur:
                jumps = cur.execute(JUMPS_SQL, (hours,)).fetchall()
        response.headers["Cache-Control"] = CACHE_CONTROL
        return Anomalies(generated_at=datetime.now(UTC), dark_periods=dark, position_jumps=jumps)

    @app.get("/health")
    def health() -> JSONResponse:
        try:
            with psycopg.connect(dsn) as conn:
                conn.execute("select 1")
        except psycopg.OperationalError:
            return JSONResponse({"status": "unavailable"}, status_code=503)
        return JSONResponse({"status": "ok"})

    return app
