import json
from collections.abc import AsyncIterator, Sequence
from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from finguard.infrastructure.kafka.publisher import (
    KafkaPublisher,
    OutgoingMessage,
    create_kafka_producer,
)
from finguard.infrastructure.outbox.postgres import PostgresOutbox
from finguard.infrastructure.outbox.relay import RELAY_LOCK_ID, OutboxRelay
from finguard.infrastructure.outbox.tables import outbox
from finguard.platform.envelope import Envelope

SessionFactory = async_sessionmaker[AsyncSession]
TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


@pytest.fixture
async def publisher(kafka_bootstrap_servers: str) -> AsyncIterator[KafkaPublisher]:
    producer = create_kafka_producer(kafka_bootstrap_servers)
    await producer.start()
    yield KafkaPublisher(producer)
    await producer.stop()


@pytest.fixture
def topic() -> str:
    return f"test.outbox.{uuid4().hex}"


class FailingPublisher:
    async def publish(self, messages: Sequence[OutgoingMessage]) -> None:
        raise ConnectionError("kafka unavailable")


async def write(session_factory: SessionFactory, topic: str, *envelopes: Envelope) -> None:
    async with session_factory.begin() as session:
        for envelope in envelopes:
            await PostgresOutbox(session).add(envelope, topic=topic, key="case-1")


async def consume(bootstrap_servers: str, topic: str, count: int) -> list[Any]:
    consumer = AIOKafkaConsumer(
        topic, bootstrap_servers=bootstrap_servers, auto_offset_reset="earliest"
    )
    await consumer.start()
    try:
        records: list[Any] = []
        while len(records) < count:
            batches = await consumer.getmany(timeout_ms=10_000)
            if not batches:
                break
            records.extend(record for batch in batches.values() for record in batch)
        return records
    finally:
        await consumer.stop()


async def pending_count(session_factory: SessionFactory) -> int:
    async with session_factory() as session:
        count = await session.scalar(
            select(func.count()).select_from(outbox).where(outbox.c.published_at.is_(None))
        )
        return count or 0


def new_envelope(index: int = 0) -> Envelope:
    return Envelope.new("cases.case.opened", {"index": index})


async def test_a_committed_message_is_published_once_with_its_key_and_trace(
    session_factory: SessionFactory,
    publisher: KafkaPublisher,
    kafka_bootstrap_servers: str,
    topic: str,
) -> None:
    envelope = new_envelope().model_copy(update={"traceparent": TRACEPARENT})
    await write(session_factory, topic, envelope)
    relay = OutboxRelay(session_factory, publisher, batch_size=10)

    assert await relay.publish_pending() == 1
    assert await relay.publish_pending() == 0

    [record] = await consume(kafka_bootstrap_servers, topic, 1)
    assert record.key == b"case-1"
    assert json.loads(record.value) == envelope.model_dump(mode="json")
    assert ("traceparent", TRACEPARENT.encode()) in record.headers
    assert await pending_count(session_factory) == 0


async def test_a_failed_transaction_leaves_nothing_to_publish(
    session_factory: SessionFactory, topic: str
) -> None:
    async def add_then_fail() -> None:
        async with session_factory.begin() as session:
            await PostgresOutbox(session).add(new_envelope(), topic=topic, key="case-1")
            raise RuntimeError("state change failed")

    with pytest.raises(RuntimeError):
        await add_then_fail()

    assert await pending_count(session_factory) == 0


async def test_messages_are_published_in_the_order_they_were_written(
    session_factory: SessionFactory,
    publisher: KafkaPublisher,
    kafka_bootstrap_servers: str,
    topic: str,
) -> None:
    envelopes = [new_envelope(index) for index in range(5)]
    for envelope in envelopes:
        await write(session_factory, topic, envelope)
    relay = OutboxRelay(session_factory, publisher, batch_size=2)

    assert [await relay.publish_pending() for _ in range(4)] == [2, 2, 1, 0]

    records = await consume(kafka_bootstrap_servers, topic, 5)
    assert [json.loads(record.value)["id"] for record in records] == [
        str(envelope.id) for envelope in envelopes
    ]


async def test_messages_stay_pending_when_publishing_fails(
    session_factory: SessionFactory, publisher: KafkaPublisher, topic: str
) -> None:
    await write(session_factory, topic, new_envelope())

    with pytest.raises(ConnectionError):
        await OutboxRelay(session_factory, FailingPublisher(), batch_size=10).publish_pending()

    assert await pending_count(session_factory) == 1
    assert await OutboxRelay(session_factory, publisher, batch_size=10).publish_pending() == 1


async def test_only_one_relay_publishes_at_a_time(
    session_factory: SessionFactory, publisher: KafkaPublisher, topic: str
) -> None:
    await write(session_factory, topic, new_envelope())
    relay = OutboxRelay(session_factory, publisher, batch_size=10)

    async with session_factory.begin() as other_relay:
        await other_relay.execute(select(func.pg_advisory_xact_lock(RELAY_LOCK_ID)))
        assert await relay.publish_pending() == 0

    assert await relay.publish_pending() == 1


async def test_published_messages_are_pruned_after_the_retention_period(
    session_factory: SessionFactory, topic: str
) -> None:
    old_published, recent_published, old_pending = new_envelope(1), new_envelope(2), new_envelope(3)
    await write(session_factory, topic, old_published, recent_published, old_pending)
    async with session_factory.begin() as session:
        for envelope, published_ago in ((old_published, "8 days"), (recent_published, "1 day")):
            await session.execute(
                update(outbox)
                .where(outbox.c.id == envelope.id)
                .values(published_at=func.now() - text(f"interval '{published_ago}'"))
            )
        await session.execute(
            update(outbox)
            .where(outbox.c.id == old_pending.id)
            .values(created_at=func.now() - text("interval '30 days'"))
        )
    relay = OutboxRelay(session_factory, FailingPublisher(), batch_size=10)

    assert await relay.prune_published(timedelta(days=7)) == 1

    async with session_factory() as session:
        remaining = set(await session.scalars(select(outbox.c.id)))
    assert remaining == {recent_published.id, old_pending.id}
