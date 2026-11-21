"""Checks on the production Compose file that Coolify deploys."""

from pathlib import Path
from typing import Any

import yaml

COMPOSE = Path(__file__).parent.parent / "docker-compose.vps.yml"


def services() -> dict[str, dict[str, Any]]:
    loaded: dict[str, dict[str, Any]] = yaml.safe_load(COMPOSE.read_text())["services"]
    return loaded


def test_one_shot_services_are_not_restarted() -> None:
    # Coolify gives a service without a restart policy "unless-stopped": a one-shot that
    # exits 0 would then run forever, and the services waiting for it would never start.
    awaited = {
        name
        for svc in services().values()
        if isinstance(svc.get("depends_on"), dict)
        for name, dep in svc["depends_on"].items()
        if dep.get("condition") == "service_completed_successfully"
    }
    assert awaited
    assert {name: services()[name].get("restart") for name in awaited} == dict.fromkeys(
        awaited, "no"
    )


def test_every_service_states_its_restart_policy() -> None:
    assert [name for name, svc in services().items() if "restart" not in svc] == []


def test_no_service_publishes_a_host_port() -> None:
    assert [name for name, svc in services().items() if "ports" in svc] == []
