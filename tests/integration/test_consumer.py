import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack
from typing import Any
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from pydantic import SecretStr
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from finguard.core.container import build_message_consumer
from finguard.core.settings import ConsumerSettings, DatabaseSettings, KafkaSettings, Settings
from finguard.infrastructure.kafka.consumer import CONSUMER_HEADER, ERROR_HEADER, ORIGIN_HEADER
from finguard.infrastructure.kafka.publisher import (
    KafkaPublisher,
    create_kafka_admin,
    create_kafka_producer,
)
from finguard.infrastructure.kafka.topics import dead_letter_topic, ensure_topics, retry_topic
from finguard.infrastructure.outbox.postgres import PostgresOutbox
from finguard.infrastructure.outbox.relay import OutboxRelay
from finguard.infrastructure.outbox.tables import inbox
from finguard.platform.envelope import Envelope
from finguard.platform.errors import ValidationFailed
from finguard.platform.topics import Topic

SessionFactory = async_sessionmaker[AsyncSession]
ReadTopic = Callable[..., Awaitable[list[Any]]]
HANDLED_TYPE = "cases.case.opened"
RETRY_DELAY_S = 0.5


class RecordingHandler:
    """Raises the queued failures one per call, then records what it handles."""

    def __init__(self, *failures: Exception) -> None:
        self.failures = list(failures)
        self.handled: list[Envelope] = []
        self.call_times: list[float] = []

    async def __call__(self, envelope: Envelope, session: AsyncSession) -> None:
        self.call_times.append(time.monotonic())
        if self.failures:
            raise self.failures.pop(0)
        self.handled.append(envelope)

    def handled_ids(self) -> list[Any]:
        return [envelope.id for envelope in self.handled]


@pytest.fixture
async def source(kafka_bootstrap_servers: str) -> Topic:
    topic = Topic(f"test.consumer.{uuid4().hex}", partitions=1)
    admin = create_kafka_admin(kafka_bootstrap_servers)
    await admin.start()
    try:
        await ensure_topics(admin, [topic], retry_tiers=2, replication_factor=1)
    finally:
        await admin.close()
    return topic


@pytest.fixture
async def producer(kafka_bootstrap_servers: str) -> AsyncIterator[AIOKafkaProducer]:
    producer = create_kafka_producer(kafka_bootstrap_servers)
    await producer.start()
    yield producer
    await producer.stop()


@pytest.fixture
def settings_for(database_url: str, kafka_bootstrap_servers: str) -> Callable[..., Settings]:
    def build(**consumer: Any) -> Settings:
        return Settings(
            env="test",
            database=DatabaseSettings(url=SecretStr(database_url)),
            kafka=KafkaSettings(
                bootstrap_servers=kafka_bootstrap_servers,
                retry_delays_s=(RETRY_DELAY_S, RETRY_DELAY_S),
            ),
            consumer=ConsumerSettings(**{"attempts": 1, "attempt_backoff_s": 0.01, **consumer}),
        )

    return build


async def consume_until(
    settings: Settings,
    source: Topic,
    handler: RecordingHandler,
    done: Callable[[], bool],
    *,
    name: str = "test-consumer",
) -> None:
    stop = asyncio.Event()
    async with AsyncExitStack() as resources:
        consumer = await build_message_consumer(
            settings, resources, name=name, topics=[source], handlers={HANDLED_TYPE: handler}
        )
        task = asyncio.create_task(consumer.run(stop))
        try:
            async with asyncio.timeout(30):
                while not done():
                    if task.done():
                        await task
                    await asyncio.sleep(0.05)
        finally:
            stop.set()
            await task


async def send(
    producer: AIOKafkaProducer,
    topic: str,
    envelope: Envelope | bytes,
    headers: list[tuple[str, bytes]] | None = None,
) -> None:
    value = envelope if isinstance(envelope, bytes) else envelope.model_dump_json().encode()
    await producer.send_and_wait(topic, value, key=b"case-1", headers=headers or [])


def new_envelope(type: str = HANDLED_TYPE) -> Envelope:
    return Envelope.new(type, {"case": "c-1"})


def header(record: Any, name: str) -> str:
    return str(dict(record.headers)[name].decode())


async def test_a_message_from_the_outbox_reaches_its_handler_and_is_marked_processed(
    session_factory: SessionFactory,
    settings_for: Callable[..., Settings],
    source: Topic,
    kafka_bootstrap_servers: str,
) -> None:
    envelope = new_envelope()
    async with session_factory.begin() as session:
        await PostgresOutbox(session, topics={source.name}).add(
            envelope, topic=source.name, key="case-1"
        )
    producer = create_kafka_producer(kafka_bootstrap_servers)
    await producer.start()
    try:
        relay = OutboxRelay(
            session_factory, KafkaPublisher(producer), batch_size=10, publish_timeout_s=10
        )
        assert await relay.publish_pending() == 1
    finally:
        await producer.stop()
    handler = RecordingHandler()

    await consume_until(settings_for(), source, handler, lambda: bool(handler.handled))

    assert handler.handled == [envelope]
    async with session_factory() as session:
        consumers = list(
            await session.scalars(select(inbox.c.consumer).where(inbox.c.message_id == envelope.id))
        )
    assert consumers == ["test-consumer"]


async def test_a_redelivered_message_is_handled_once(
    session_factory: SessionFactory,
    settings_for: Callable[..., Settings],
    source: Topic,
    producer: AIOKafkaProducer,
) -> None:
    envelope, last = new_envelope(), new_envelope()
    for message in (envelope, envelope, last):
        await send(producer, source.name, message)
    handler = RecordingHandler()

    await consume_until(settings_for(), source, handler, lambda: last in handler.handled)

    assert handler.handled_ids() == [envelope.id, last.id]


async def test_a_transient_failure_is_retried_in_place(
    session_factory: SessionFactory,
    settings_for: Callable[..., Settings],
    source: Topic,
    producer: AIOKafkaProducer,
    read_topic: ReadTopic,
) -> None:
    envelope = new_envelope()
    await send(producer, source.name, envelope)
    handler = RecordingHandler(ConnectionError("db down"), ConnectionError("db down"))

    await consume_until(settings_for(attempts=3), source, handler, lambda: bool(handler.handled))

    assert handler.handled == [envelope]
    assert len(handler.call_times) == 3
    assert await read_topic(retry_topic(source.name, 1), 1, timeout_s=2) == []


async def test_a_message_recovers_on_a_retry_topic_after_its_delay(
    session_factory: SessionFactory,
    settings_for: Callable[..., Settings],
    source: Topic,
    producer: AIOKafkaProducer,
    read_topic: ReadTopic,
) -> None:
    envelope = new_envelope()
    await send(producer, source.name, envelope)
    handler = RecordingHandler(ConnectionError("db down"))

    await consume_until(settings_for(), source, handler, lambda: bool(handler.handled))

    first_call, retry_call = handler.call_times
    assert retry_call - first_call >= RETRY_DELAY_S * 0.9
    [copy] = await read_topic(retry_topic(source.name, 1), 1)
    assert header(copy, CONSUMER_HEADER) == "test-consumer"
    assert header(copy, ORIGIN_HEADER) == f"{source.name}:0:0"


async def test_a_message_that_keeps_failing_ends_in_the_dead_letter_topic(
    session_factory: SessionFactory,
    settings_for: Callable[..., Settings],
    source: Topic,
    producer: AIOKafkaProducer,
    read_topic: ReadTopic,
) -> None:
    envelope = new_envelope()
    await send(producer, source.name, envelope)
    handler = RecordingHandler(*[ConnectionError("db down")] * 3)

    await consume_until(settings_for(), source, handler, lambda: len(handler.call_times) == 3)

    [dead] = await read_topic(dead_letter_topic(source.name), 1)
    assert Envelope.model_validate_json(dead.value) == envelope
    assert dead.key == b"case-1"
    assert header(dead, ERROR_HEADER) == "ConnectionError: db down"
    assert header(dead, ORIGIN_HEADER) == f"{source.name}:0:0"
    assert handler.handled == []


async def test_a_permanent_error_goes_straight_to_the_dead_letter_topic(
    session_factory: SessionFactory,
    settings_for: Callable[..., Settings],
    source: Topic,
    producer: AIOKafkaProducer,
    read_topic: ReadTopic,
) -> None:
    await send(producer, source.name, new_envelope())
    handler = RecordingHandler(ValidationFailed("case already closed"))

    await consume_until(settings_for(attempts=3), source, handler, lambda: bool(handler.call_times))

    [dead] = await read_topic(dead_letter_topic(source.name), 1)
    assert header(dead, ERROR_HEADER) == "ValidationFailed: case already closed"
    assert len(handler.call_times) == 1
    assert await read_topic(retry_topic(source.name, 1), 1, timeout_s=2) == []


async def test_a_malformed_message_is_dead_lettered_and_consumption_continues(
    session_factory: SessionFactory,
    settings_for: Callable[..., Settings],
    source: Topic,
    producer: AIOKafkaProducer,
    read_topic: ReadTopic,
) -> None:
    last = new_envelope()
    await send(producer, source.name, b"{not json")
    await send(producer, source.name, last)
    handler = RecordingHandler()

    await consume_until(settings_for(), source, handler, lambda: bool(handler.handled))

    assert handler.handled == [last]
    [dead] = await read_topic(dead_letter_topic(source.name), 1)
    assert dead.value == b"{not json"
    assert header(dead, ERROR_HEADER).startswith("ValidationError")


async def test_messages_of_other_types_are_skipped(
    session_factory: SessionFactory,
    settings_for: Callable[..., Settings],
    source: Topic,
    producer: AIOKafkaProducer,
) -> None:
    other, last = new_envelope("cases.case.closed"), new_envelope()
    await send(producer, source.name, other)
    await send(producer, source.name, last)
    handler = RecordingHandler()

    await consume_until(settings_for(), source, handler, lambda: bool(handler.handled))

    assert handler.handled == [last]
    async with session_factory() as session:
        assert (
            await session.scalar(select(inbox.c.message_id).where(inbox.c.message_id == other.id))
            is None
        )


async def test_retry_copies_for_another_consumer_group_are_ignored(
    session_factory: SessionFactory,
    settings_for: Callable[..., Settings],
    source: Topic,
    producer: AIOKafkaProducer,
) -> None:
    theirs, ours = new_envelope(), new_envelope()
    tier = retry_topic(source.name, 1)
    await send(producer, tier, theirs, [(CONSUMER_HEADER, b"another-consumer")])
    await send(producer, tier, ours, [(CONSUMER_HEADER, b"test-consumer")])
    handler = RecordingHandler()

    await consume_until(settings_for(), source, handler, lambda: bool(handler.handled))

    assert handler.handled == [ours]


async def test_handled_messages_are_committed_so_a_restart_resumes_after_them(
    session_factory: SessionFactory,
    settings_for: Callable[..., Settings],
    source: Topic,
    producer: AIOKafkaProducer,
    kafka_bootstrap_servers: str,
) -> None:
    await send(producer, source.name, new_envelope())
    await send(producer, source.name, new_envelope())
    handler = RecordingHandler()

    await consume_until(settings_for(), source, handler, lambda: len(handler.handled) == 2)

    group = AIOKafkaConsumer(bootstrap_servers=kafka_bootstrap_servers, group_id="test-consumer")
    await group.start()
    try:
        assert await group.committed(TopicPartition(source.name, 0)) == 2
    finally:
        await group.stop()
