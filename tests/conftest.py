from __future__ import annotations

import socket
import time
import urllib.request
from collections.abc import Callable, Iterator
from pathlib import Path

import psycopg
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.waiting_utils import wait_for_logs

KAFKA_IMAGE = "apache/kafka:3.9.1"
REGISTRY_IMAGE = "confluentinc/cp-schema-registry:7.9.0"
POSTGRES_IMAGE = "postgres:17.6"
INIT_SQL = Path(__file__).parent.parent / "postgres" / "init.sql"
SITE_PASSWORD = "site-test"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
        return port


@pytest.fixture(scope="session")
def network() -> Iterator[Network]:
    with Network() as net:
        yield net


@pytest.fixture(scope="session")
def kafka(network: Network) -> Iterator[str]:
    """A single-node KRaft broker, the same image as docker-compose.yml. Kafka tells clients
    which address to reconnect to (advertised listener), so the host port is chosen before
    start and advertised as is. A second listener serves the containers of the test network
    (the schema registry) as kafka:19092."""
    port = _free_port()
    container = (
        DockerContainer(KAFKA_IMAGE)
        .with_network(network)
        .with_network_aliases("kafka")
        .with_bind_ports(9092, port)
        .with_env("KAFKA_NODE_ID", "1")
        .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env("KAFKA_LISTENERS", "PLAINTEXT://:9092,DOCKER://:19092,CONTROLLER://:9093")
        .with_env(
            "KAFKA_ADVERTISED_LISTENERS", f"PLAINTEXT://localhost:{port},DOCKER://kafka:19092"
        )
        .with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,DOCKER:PLAINTEXT",
        )
        .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
    )
    with container:
        wait_for_logs(container, "Kafka Server started", timeout=60)
        yield f"localhost:{port}"


@pytest.fixture(scope="session")
def schema_registry(network: Network, kafka: str) -> Iterator[str]:
    """Confluent Schema Registry on the test network, storing its schemas in the broker."""
    container = (
        DockerContainer(REGISTRY_IMAGE)
        .with_network(network)
        .with_exposed_ports(8081)
        .with_env("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .with_env("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
        .with_env("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
    )
    with container:
        wait_for_logs(container, "Server started, listening for requests", timeout=90)
        url = f"http://localhost:{container.get_exposed_port(8081)}"
        with urllib.request.urlopen(f"{url}/subjects", timeout=10) as response:
            assert response.status == 200
        yield url


def apply_init_sql(container: DockerContainer, site_password: str = SITE_PASSWORD) -> None:
    """Run postgres/init.sql through psql as the db-schema service does."""
    code, output = container.exec(
        [
            "env",
            f"SITE_DB_PASSWORD={site_password}",
            "psql",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "ais",
            "-d",
            "ais",
            "-f",
            "/init.sql",
        ]
    )
    assert code == 0, output.decode()


@pytest.fixture
def rerun_init_sql(postgres_container: DockerContainer) -> Iterator[Callable[[str], None]]:
    """Applies postgres/init.sql again with another site password; the test's password is
    back afterwards for the other tests."""
    yield lambda site_password: apply_init_sql(postgres_container, site_password)
    apply_init_sql(postgres_container)


@pytest.fixture(scope="session")
def postgres(postgres_container: DockerContainer) -> str:
    """The owner's connection string; the site's role is `site_reader`."""
    port = postgres_container.get_exposed_port(5432)
    return f"postgresql://ais:ais@localhost:{port}/ais?connect_timeout=2"


@pytest.fixture(scope="session")
def postgres_container() -> Iterator[DockerContainer]:
    """The same PostgreSQL as docker-compose.yml, its schema applied by postgres/init.sql
    through psql exactly as the db-schema service does, twice (it must be idempotent)."""
    container = (
        DockerContainer(POSTGRES_IMAGE)
        .with_env("POSTGRES_USER", "ais")
        .with_env("POSTGRES_PASSWORD", "ais")
        .with_env("POSTGRES_DB", "ais")
        .with_env("GRAFANA_DB_PASSWORD", "grafana-test")
        .with_env("SITE_DB_PASSWORD", SITE_PASSWORD)
        .with_volume_mapping(str(INIT_SQL), "/init.sql", "ro")
        .with_exposed_ports(5432)
    )
    with container:
        dsn = (
            f"postgresql://ais:ais@localhost:{container.get_exposed_port(5432)}/ais"
            "?connect_timeout=2"
        )
        # The image's first start runs a temporary server on the socket only: TCP answers
        # once the real one is up.
        deadline = time.monotonic() + 60
        while True:
            try:
                psycopg.connect(dsn).close()
                break
            except psycopg.OperationalError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.5)
        for _ in range(2):
            apply_init_sql(container)
        yield container


@pytest.fixture
def site_dsn(postgres: str) -> Iterator[str]:
    """The site's read-only connection, on tables emptied before each test."""
    with psycopg.connect(postgres) as conn:
        conn.execute("truncate ship_positions, ship_sessions, ais_gaps, position_jumps")
    yield postgres.replace("ais:ais@", f"site_reader:{SITE_PASSWORD}@", 1)
