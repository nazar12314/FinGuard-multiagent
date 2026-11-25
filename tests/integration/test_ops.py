from collections.abc import AsyncIterator, Awaitable, Callable
from uuid import uuid4

import pytest
from aiokafka.admin import NewTopic
from prometheus_client import REGISTRY
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from finguard.core.ops import Readiness
from finguard.infrastructure.db.engine import (
    check_database,
    create_database_engine,
    create_session_factory,
)
from finguard.infrastructure.kafka.publisher import (
    KafkaPublisher,
    OutgoingMessage,
    check_kafka,
    create_kafka_producer,
)
from finguard.infrastructure.outbox.postgres import PostgresOutbox
from finguard.infrastructure.outbox.relay import OutboxRelay
from finguard.infrastructure.outbox.tables import outbox
from finguard.platform.envelope import Envelope

SessionFactory = async_sessionmaker[AsyncSession]
CreateTopics = Callable[..., Awaitable[None]]


@pytest.fixture
async def producer(kafka_bootstrap_servers: str) -> AsyncIterator[KafkaPublisher]:
    kafka = create_kafka_producer(kafka_bootstrap_servers)
    await kafka.start()
    yield KafkaPublisher(kafka)
    await kafka.stop()


async def test_readiness_reflects_whether_the_database_answers(
    session_factory: SessionFactory,
) -> None:
    # Nothing listens on port 1, so connecting fails at once.
    unreachable = create_database_engine(
        "postgresql+asyncpg://finguard:finguard@127.0.0.1:1/finguard",
        pool_size=1,
        idle_in_transaction_timeout_s=60,
    )
    readiness = Readiness()
    readiness.require("database", lambda: check_database(session_factory))
    readiness.require("other_database", lambda: check_database(create_session_factory(unreachable)))

    ready, checks = await readiness.report()
    await unreachable.dispose()

    assert not ready
    assert checks["database"] == "ok"
    assert checks["other_database"] != "ok"


async def test_kafka_is_reported_reachable_through_the_producer(
    kafka_bootstrap_servers: str,
) -> None:
    kafka = create_kafka_producer(kafka_bootstrap_servers)
    await kafka.start()
    try:
        await check_kafka(kafka)
    finally:
        await kafka.stop()


async def test_the_outbox_backlog_and_its_age_are_measured(
    session_factory: SessionFactory, producer: KafkaPublisher, create_topics: CreateTopics
) -> None:
    topic = f"test.ops.{uuid4().hex}"
    await create_topics(NewTopic(topic, 1, 1))
    async with session_factory.begin() as session:
        for _ in range(2):
            await PostgresOutbox(session, topics={topic}).add(
                Envelope.new("cases.case.opened", {}), topic=topic, key="case-1"
            )
        await session.execute(
            update(outbox).values(created_at=func.now() - text("interval '1 minute'"))
        )
    relay = OutboxRelay(session_factory, producer, batch_size=10, publish_timeout_s=10)

    await relay.measure_backlog()
    pending = REGISTRY.get_sample_value("finguard_outbox_pending_messages")
    oldest = REGISTRY.get_sample_value("finguard_outbox_oldest_pending_age_seconds")
    published_before = REGISTRY.get_sample_value(
        "finguard_outbox_published_total", {"topic": topic}
    )
    await relay.publish_pending()
    await relay.measure_backlog()

    assert pending == 2
    assert oldest is not None
    assert oldest >= 59
    assert published_before is None
    assert REGISTRY.get_sample_value("finguard_outbox_published_total", {"topic": topic}) == 2
    assert REGISTRY.get_sample_value("finguard_outbox_pending_messages") == 0
    assert REGISTRY.get_sample_value("finguard_outbox_oldest_pending_age_seconds") == 0


class PublisherThatLosesTheTransaction:
    """Publishes, then ends the relay's database session, so the relay's commit fails."""

    def __init__(self, session_factory: SessionFactory) -> None:
        self._session_factory = session_factory

    async def publish(self, message: OutgoingMessage) -> None:
        async with self._session_factory() as session:
            await session.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
                    " WHERE state = 'idle in transaction' AND pid <> pg_backend_pid()"
                )
            )


async def test_messages_are_counted_as_published_only_once_committed(
    session_factory: SessionFactory, create_topics: CreateTopics
) -> None:
    topic = f"test.ops.{uuid4().hex}"
    await create_topics(NewTopic(topic, 1, 1))
    async with session_factory.begin() as session:
        await PostgresOutbox(session, topics={topic}).add(
            Envelope.new("cases.case.opened", {}), topic=topic, key="case-1"
        )
    relay = OutboxRelay(
        session_factory,
        PublisherThatLosesTheTransaction(session_factory),
        batch_size=10,
        publish_timeout_s=10,
    )

    with pytest.raises(DBAPIError):
        await relay.publish_pending()

    assert REGISTRY.get_sample_value("finguard_outbox_published_total", {"topic": topic}) is None
    async with session_factory() as session:
        pending = await session.scalar(
            select(func.count()).select_from(outbox).where(outbox.c.published_at.is_(None))
        )
    assert pending == 1
