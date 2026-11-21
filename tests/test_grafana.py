"""Grafana as code: the provisioning files are well formed and point at each other. The
queries themselves run against the real tables in the e2e tests."""

from __future__ import annotations

import json

import yaml
from grafana_queries import DASHBOARDS, GRAFANA, dashboard_queries, render

DATASOURCES = {"postgres", "prometheus"}


def test_dashboards_are_well_formed() -> None:
    assert [p.stem for p in DASHBOARDS] == ["ais-traffic", "stream-health"]
    for path in DASHBOARDS:
        board = json.loads(path.read_text(encoding="utf-8"))
        ids = [panel["id"] for panel in board["panels"]]
        assert len(ids) == len(set(ids)), f"duplicate panel id in {path.name}"
        for panel in board["panels"]:
            if panel["type"] == "row":  # a section header, no query
                continue
            assert panel["datasource"]["uid"] in DATASOURCES, panel["title"]
            assert panel["targets"], panel["title"]


def test_provisioning_points_at_the_dashboards() -> None:
    sources = yaml.safe_load(
        (GRAFANA / "provisioning" / "datasources" / "datasources.yaml").read_text()
    )
    assert {s["uid"] for s in sources["datasources"]} == DATASOURCES
    providers = yaml.safe_load((GRAFANA / "provisioning" / "dashboards" / "ais.yaml").read_text())
    # Outside /var/lib/grafana: that directory is the data volume, which would keep the
    # dashboards of the first deployment and hide newer ones.
    assert providers["providers"][0]["options"]["path"] == "/etc/grafana/dashboards"


def test_every_query_renders() -> None:
    queries = dashboard_queries()
    assert queries
    for name, sql in queries:
        assert "$" not in render(sql), name
