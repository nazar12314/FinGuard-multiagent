import asyncio
import socket
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import Any
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
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
    engine = create_database_engine(database_url, pool_size=4, idle_in_transaction_timeout_s=60)
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
        .with_env("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")  # as in deploy/compose
        .waiting_for(LogMessageWaitStrategy("Kafka Server started").with_startup_timeout(120))
    )
    with container:
        yield f"localhost:{port}"


CreateTopics = Callable[..., Awaitable[None]]
ReadTopic = Callable[..., Awaitable[list[Any]]]


@pytest.fixture
def create_topics(kafka_bootstrap_servers: str) -> CreateTopics:
    async def create(*topics: NewTopic) -> None:
        admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap_servers)
        await admin.start()
        try:
            await admin.create_topics(list(topics))
        finally:
            await admin.close()

    return create


@pytest.fixture
def read_topic(kafka_bootstrap_servers: str) -> ReadTopic:
    async def read(topic: str, count: int, *, timeout_s: float = 10) -> list[Any]:
        """Read up to `count` records from the start of `topic`, waiting at most `timeout_s`."""
        consumer = AIOKafkaConsumer(
            topic, bootstrap_servers=kafka_bootstrap_servers, auto_offset_reset="earliest"
        )
        await consumer.start()
        try:
            records: list[Any] = []
            deadline = time.monotonic() + timeout_s
            while len(records) < count and time.monotonic() < deadline:
                batches = await consumer.getmany(timeout_ms=500)
                records.extend(record for batch in batches.values() for record in batch)
            return records
        finally:
            await consumer.stop()

    return read


TopicSize = Callable[[str], Awaitable[int]]


@pytest.fixture
def topic_size(kafka_bootstrap_servers: str) -> TopicSize:
    async def size(topic: str) -> int:
        """How many records the topic holds, read from its end offsets, without waiting."""
        admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap_servers)
        await admin.start()
        try:
            [description] = await admin.describe_topics([topic])
        finally:
            await admin.close()
        partitions = [
            TopicPartition(topic, partition["partition"]) for partition in description["partitions"]
        ]
        assert partitions, f"topic {topic} does not exist"  # an empty sum would read as 0
        consumer = AIOKafkaConsumer(bootstrap_servers=kafka_bootstrap_servers)
        await consumer.start()
        try:
            offsets: dict[TopicPartition, int] = await consumer.end_offsets(partitions)
            return sum(offsets.values())
        finally:
            await consumer.stop()

    return size
