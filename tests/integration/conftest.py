import asyncio
import socket
from collections.abc import AsyncIterator, Iterator
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from finguard.entrypoints.migrate import alembic_config
from finguard.infrastructure.db.engine import create_database_engine, create_session_factory

# Same images as deploy/compose/compose.yaml.
POSTGRES_IMAGE = "pgvector/pgvector:0.8.7-pg18-trixie"
KAFKA_IMAGE = "apache/kafka:4.3.2"


@pytest.fixture(scope="session")
def postgres() -> Iterator[PostgresContainer]:
    with PostgresContainer(POSTGRES_IMAGE, driver="asyncpg") as container:
        yield container


@pytest.fixture(scope="session")
def database_url(postgres: PostgresContainer) -> str:
    url = postgres.get_connection_url()
    command.upgrade(alembic_config(url), "head")
    return url


@pytest.fixture
def empty_database_url(postgres: PostgresContainer) -> str:
    name = f"empty_{uuid4().hex[:12]}"
    server_url = postgres.get_connection_url()

    async def create_database() -> None:
        engine = create_async_engine(server_url, isolation_level="AUTOCOMMIT")
        async with engine.connect() as connection:
            await connection.execute(text(f'CREATE DATABASE "{name}"'))
        await engine.dispose()

    asyncio.run(create_database())
    return make_url(server_url).set(database=name).render_as_string(hide_password=False)


@pytest.fixture
async def session_factory(database_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_database_engine(database_url, pool_size=2)
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE platform.outbox, platform.inbox"))
    yield create_session_factory(engine)
    await engine.dispose()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
        return port


@pytest.fixture(scope="session")
def kafka_bootstrap_servers() -> Iterator[str]:
    # The broker must advertise the host port it is reached on, so that port is picked
    # up front instead of letting Docker assign one after the broker has started.
    port = _free_port()
    container = (
        DockerContainer(KAFKA_IMAGE)
        .with_bind_ports(9092, port)
        .with_env("KAFKA_NODE_ID", "1")
        .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env("KAFKA_LISTENERS", "INTERNAL://:19092,EXTERNAL://:9092,CONTROLLER://:9093")
        .with_env(
            "KAFKA_ADVERTISED_LISTENERS", f"INTERNAL://localhost:19092,EXTERNAL://localhost:{port}"
        )
        .with_env(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
        )
        .with_env("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
        .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .waiting_for(LogMessageWaitStrategy("Kafka Server started").with_startup_timeout(120))
    )
    with container:
        yield f"localhost:{port}"
