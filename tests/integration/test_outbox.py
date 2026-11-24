import asyncio
import json
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from finguard.infrastructure.kafka.publisher import (
    KafkaPublisher,
    MessageRejected,
    OutgoingMessage,
    create_kafka_producer,
)
from finguard.infrastructure.outbox.postgres import PostgresOutbox
from finguard.infrastructure.outbox.relay import RELAY_LOCK_ID, MessageParked, OutboxRelay
from finguard.infrastructure.outbox.tables import outbox
from finguard.platform.envelope import Envelope
from finguard.platform.errors import ValidationFailed

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


class RecordingPublisher:
    """Refuses messages keyed "poison", hangs on "slow", and records the rest."""

    def __init__(self) -> None:
        self.published: list[OutgoingMessage] = []

    async def publish(self, message: OutgoingMessage) -> None:
        if message.key == "poison":
            raise MessageRejected("MessageSizeTooLargeError: too large")
        if message.key == "slow":
            await asyncio.sleep(3600)
        self.published.append(message)


class FailingPublisher:
    async def publish(self, message: OutgoingMessage) -> None:
        raise ConnectionError("kafka unavailable")


def relay_for(
    session_factory: SessionFactory,
    publisher: KafkaPublisher | RecordingPublisher | FailingPublisher,
    *,
    batch_size: int = 10,
    publish_timeout_s: float = 10,
) -> OutboxRelay:
    return OutboxRelay(
        session_factory, publisher, batch_size=batch_size, publish_timeout_s=publish_timeout_s
    )


async def write(
    session_factory: SessionFactory, topic: str, *envelopes: Envelope, key: str = "case-1"
) -> None:
    async with session_factory.begin() as session:
        for envelope in envelopes:
            await PostgresOutbox(session).add(envelope, topic=topic, key=key)


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
            select(func.count())
            .select_from(outbox)
            .where(outbox.c.published_at.is_(None), outbox.c.failed_at.is_(None))
        )
        return count or 0


def new_envelope(index: int = 0) -> Envelope:
    return Envelope.new("cases.case.opened", {"index": index})


def indexes(messages: list[OutgoingMessage]) -> list[int]:
    return [json.loads(message.value)["data"]["index"] for message in messages]


async def test_a_committed_message_is_published_once_with_its_key_and_trace(
    session_factory: SessionFactory,
    publisher: KafkaPublisher,
    kafka_bootstrap_servers: str,
    topic: str,
) -> None:
    envelope = new_envelope().model_copy(update={"traceparent": TRACEPARENT})
    await write(session_factory, topic, envelope)
    relay = relay_for(session_factory, publisher)

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


async def test_a_message_too_large_for_kafka_is_refused_by_the_writer(
    session_factory: SessionFactory, topic: str
) -> None:
    async with session_factory() as session:
        small_outbox = PostgresOutbox(session, max_message_bytes=200)

        with pytest.raises(ValidationFailed, match="over the 200 limit"):
            await small_outbox.add(
                Envelope.new("cases.case.opened", {"memo": "x" * 500}), topic=topic, key="case-1"
            )


async def test_messages_are_published_in_the_order_they_were_written(
    session_factory: SessionFactory,
    publisher: KafkaPublisher,
    kafka_bootstrap_servers: str,
    topic: str,
) -> None:
    envelopes = [new_envelope(index) for index in range(5)]
    for envelope in envelopes:
        await write(session_factory, topic, envelope)
    relay = relay_for(session_factory, publisher, batch_size=2)

    assert [await relay.publish_pending() for _ in range(4)] == [2, 2, 1, 0]

    records = await consume(kafka_bootstrap_servers, topic, 5)
    assert [json.loads(record.value)["id"] for record in records] == [
        str(envelope.id) for envelope in envelopes
    ]


async def test_order_follows_inserts_not_envelope_ids_within_a_transaction(
    session_factory: SessionFactory, topic: str
) -> None:
    first, second, third = new_envelope(1), new_envelope(2), new_envelope(3)
    await write(session_factory, topic, third, first, second)
    publisher = RecordingPublisher()

    await relay_for(session_factory, publisher).publish_pending()

    assert indexes(publisher.published) == [3, 1, 2]


async def test_order_follows_inserts_when_an_earlier_transaction_commits_later(
    session_factory: SessionFactory, topic: str
) -> None:
    async with session_factory() as slow_writer:
        # The slow writer's transaction starts first but adds its message last, the way a
        # writer queued behind another one's row lock does.
        await slow_writer.execute(text("SELECT 1"))
        await write(session_factory, topic, new_envelope(1))
        await PostgresOutbox(slow_writer).add(new_envelope(2), topic=topic, key="case-1")
        await slow_writer.commit()
    publisher = RecordingPublisher()

    await relay_for(session_factory, publisher).publish_pending()

    assert indexes(publisher.published) == [1, 2]


async def test_messages_stay_pending_when_publishing_fails(
    session_factory: SessionFactory, publisher: KafkaPublisher, topic: str
) -> None:
    await write(session_factory, topic, new_envelope())

    with pytest.raises(ConnectionError):
        await relay_for(session_factory, FailingPublisher()).publish_pending()

    assert await pending_count(session_factory) == 1
    assert await relay_for(session_factory, publisher).publish_pending() == 1


async def test_a_publish_that_hangs_times_out_and_leaves_messages_pending(
    session_factory: SessionFactory, topic: str
) -> None:
    await write(session_factory, topic, new_envelope(), key="slow")

    with pytest.raises(TimeoutError):
        await relay_for(
            session_factory, RecordingPublisher(), publish_timeout_s=0.2
        ).publish_pending()

    assert await pending_count(session_factory) == 1


async def test_a_refused_message_is_parked_and_later_messages_still_flow(
    session_factory: SessionFactory, topic: str
) -> None:
    before, poison, after = new_envelope(1), new_envelope(2), new_envelope(3)
    await write(session_factory, topic, before)
    await write(session_factory, topic, poison, key="poison")
    await write(session_factory, topic, after)
    publisher = RecordingPublisher()
    relay = relay_for(session_factory, publisher)

    with pytest.raises(MessageParked) as parked:
        await relay.publish_pending()
    assert parked.value.message_id == poison.id
    assert await relay.publish_pending() == 1

    assert indexes(publisher.published) == [1, 3]
    async with session_factory() as session:
        last_error = await session.scalar(
            select(outbox.c.last_error).where(outbox.c.failed_at.is_not(None))
        )
    assert last_error is not None
    assert "MessageSizeTooLargeError" in last_error


async def test_kafka_refusing_an_oversized_message_parks_it(
    session_factory: SessionFactory,
    publisher: KafkaPublisher,
    kafka_bootstrap_servers: str,
    topic: str,
) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap_servers)
    await admin.start()
    try:
        await admin.create_topics(
            [NewTopic(topic, 1, 1, topic_configs={"max.message.bytes": "1000"})]
        )
    finally:
        await admin.close()
    oversized = Envelope.new("cases.case.opened", {"memo": "x" * 5000})
    await write(session_factory, topic, oversized)

    with pytest.raises(MessageParked) as parked:
        await relay_for(session_factory, publisher).publish_pending()

    assert parked.value.message_id == oversized.id
    assert "MessageSizeTooLarge" in parked.value.reason
    assert await pending_count(session_factory) == 0


async def test_only_one_relay_publishes_at_a_time(
    session_factory: SessionFactory, publisher: KafkaPublisher, topic: str
) -> None:
    await write(session_factory, topic, new_envelope())
    relay = relay_for(session_factory, publisher)

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

    assert (
        await relay_for(session_factory, FailingPublisher()).prune_published(timedelta(days=7)) == 1
    )

    async with session_factory() as session:
        remaining = set(await session.scalars(select(outbox.c.id)))
    assert remaining == {recent_published.id, old_pending.id}


async def test_sessions_that_idle_inside_a_transaction_are_ended_by_postgres(
    session_factory: SessionFactory,
) -> None:
    async with session_factory() as session:
        timeout = await session.scalar(text("SHOW idle_in_transaction_session_timeout"))

    assert timeout == "1min"
