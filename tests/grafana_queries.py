"""The queries held by the Grafana dashboards, for the tests that run them."""

from __future__ import annotations

import json
import re
from pathlib import Path

GRAFANA = Path(__file__).resolve().parents[1] / "grafana"
DASHBOARDS = sorted((GRAFANA / "dashboards").glob("*.json"))


def _targets(key: str) -> list[tuple[str, str]]:
    queries = []
    for path in DASHBOARDS:
        board = json.loads(path.read_text(encoding="utf-8"))
        for panel in board["panels"]:
            for target in panel.get("targets", []):
                if key in target:
                    queries.append((f"{path.stem}/{panel['title']}", target[key]))
    return queries


def dashboard_queries() -> list[tuple[str, str]]:
    """(dashboard/panel, SQL) of every PostgreSQL panel."""
    return _targets("rawSql")


def prometheus_queries() -> list[tuple[str, str]]:
    """(dashboard/panel, PromQL) of every Prometheus panel, with Grafana's rate interval
    replaced by a fixed one."""
    return [(name, expr.replace("$__rate_interval", "1m")) for name, expr in _targets("expr")]


def render(sql: str) -> str:
    """Expand the Grafana macros the queries use, as Grafana would. The time range is the
    six hours before the latest data, so the tests do not depend on the wall clock."""
    sql = re.sub(
        r"\$__timeFilter\(([^)]+)\)",
        r"\1 > (select max(ts) from ship_positions) - interval '6 hours'",
        sql,
    )
    sql = sql.replace("$__interval", "5 minutes")
    assert "$" not in sql, f"unexpanded macro in {sql}"
    return sql
