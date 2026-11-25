import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode
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
from finguard.infrastructure.tracing import current_traceparent, tracer
from finguard.platform.envelope import Envelope
from finguard.platform.errors import ValidationFailed
from finguard.platform.topics import Topic

SessionFactory = async_sessionmaker[AsyncSession]
ReadTopic = Callable[..., Awaitable[list[Any]]]
TopicSize = Callable[[str], Awaitable[int]]
SettingsFor = Callable[..., Settings]
HANDLED_TYPE = "cases.case.opened"
RETRY_DELAY_S = 0.5


class RecordingHandler:
    """Sleeps and raises as scripted, one entry per call, then records what it handles."""

    def __init__(self, *failures: Exception, delays: tuple[float, ...] = ()) -> None:
        self.failures = list(failures)
        self.delays = list(delays)
        self.handled: list[Envelope] = []
        self.call_times: list[float] = []

    async def __call__(self, envelope: Envelope, session: AsyncSession) -> None:
        self.call_times.append(time.monotonic())
        if self.delays:
            await asyncio.sleep(self.delays.pop(0))
        if self.failures:
            raise self.failures.pop(0)
        self.handled.append(envelope)

    def handled_ids(self) -> list[Any]:
        return [envelope.id for envelope in self.handled]


async def create(kafka_bootstrap_servers: str, topic: Topic) -> Topic:
    admin = create_kafka_admin(kafka_bootstrap_servers)
    await admin.start()
    try:
        await ensure_topics(admin, [topic], retry_tiers=2, replication_factor=1)
    finally:
        await admin.close()
    return topic


@pytest.fixture
async def source(kafka_bootstrap_servers: str) -> Topic:
    return await create(kafka_bootstrap_servers, Topic(f"test.consumer.{uuid4().hex}", 1))


@pytest.fixture
def group() -> str:
    return f"test-consumer-{uuid4().hex[:8]}"


@pytest.fixture
async def producer(kafka_bootstrap_servers: str) -> AsyncIterator[AIOKafkaProducer]:
    producer = create_kafka_producer(kafka_bootstrap_servers)
    await producer.start()
    yield producer
    await producer.stop()


@pytest.fixture
def settings_for(database_url: str, kafka_bootstrap_servers: str) -> SettingsFor:
    def build(
        retry_delays_s: tuple[float, ...] = (RETRY_DELAY_S, RETRY_DELAY_S), **consumer: Any
    ) -> Settings:
        return Settings(
            env="test",
            database=DatabaseSettings(url=SecretStr(database_url)),
            kafka=KafkaSettings(
                bootstrap_servers=kafka_bootstrap_servers, retry_delays_s=retry_delays_s
            ),
            consumer=ConsumerSettings(**{"attempts": 1, "attempt_backoff_s": 0.01, **consumer}),
        )

    return build


@asynccontextmanager
async def running(
    settings: Settings, source: Topic, handler: RecordingHandler, name: str
) -> AsyncIterator[tuple[asyncio.Task[None], asyncio.Event]]:
    """Run a consumer in the background; on exit, stop it and surface any crash."""
    stop = asyncio.Event()
    async with AsyncExitStack() as resources:
        consumer = await build_message_consumer(
            settings, resources, name=name, topics=[source], handlers={HANDLED_TYPE: handler}
        )
        task = asyncio.create_task(consumer.run(stop))
        try:
            yield task, stop
        finally:
            stop.set()
            await task


async def wait_until(done: Callable[[], bool], *tasks: asyncio.Task[None]) -> None:
    async with asyncio.timeout(30):
        while not done():
            for task in tasks:
                if task.done():
                    await task  # surfaces a crash
            await asyncio.sleep(0.05)


async def consume_until(
    settings: Settings,
    source: Topic,
    handler: RecordingHandler,
    done: Callable[[], bool],
    name: str,
) -> None:
    async with running(settings, source, handler, name) as (task, _):
        await wait_until(done, task)


async def send(
    producer: AIOKafkaProducer,
    topic: str,
    envelope: Envelope | bytes,
    headers: list[tuple[str, bytes]] | None = None,
    partition: int | None = None,
) -> None:
    value = envelope if isinstance(envelope, bytes) else envelope.model_dump_json().encode()
    await producer.send_and_wait(
        topic, value, key=b"case-1", headers=headers or [], partition=partition
    )


async def committed(kafka_bootstrap_servers: str, group: str, partition: TopicPartition) -> Any:
    consumer = AIOKafkaConsumer(bootstrap_servers=kafka_bootstrap_servers, group_id=group)
    await consumer.start()
    try:
        return await consumer.committed(partition)
    finally:
        await consumer.stop()


def new_envelope(type: str = HANDLED_TYPE) -> Envelope:
    return Envelope.new(type, {"case": "c-1"})


def header(record: Any, name: str) -> str:
    return str(dict(record.headers)[name].decode())


async def test_a_message_from_the_outbox_reaches_its_handler_and_is_marked_processed(
    session_factory: SessionFactory,
    settings_for: SettingsFor,
    source: Topic,
    group: str,
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

    await consume_until(settings_for(), source, handler, lambda: bool(handler.handled), group)

    assert handler.handled == [envelope]
    async with session_factory() as session:
        consumers = list(
            await session.scalars(select(inbox.c.consumer).where(inbox.c.message_id == envelope.id))
        )
    assert consumers == [group]


async def test_a_redelivered_message_is_handled_once(
    session_factory: SessionFactory,
    settings_for: SettingsFor,
    source: Topic,
    group: str,
    producer: AIOKafkaProducer,
) -> None:
    envelope, last = new_envelope(), new_envelope()
    for message in (envelope, envelope, last):
        await send(producer, source.name, message)
    handler = RecordingHandler()

    await consume_until(settings_for(), source, handler, lambda: last in handler.handled, group)

    assert handler.handled_ids() == [envelope.id, last.id]


async def test_a_transient_failure_is_retried_in_place(
    session_factory: SessionFactory,
    settings_for: SettingsFor,
    source: Topic,
    group: str,
    producer: AIOKafkaProducer,
    topic_size: TopicSize,
) -> None:
    envelope = new_envelope()
    await send(producer, source.name, envelope)
    handler = RecordingHandler(ConnectionError("db down"), ConnectionError("db down"))

    await consume_until(
        settings_for(attempts=3), source, handler, lambda: bool(handler.handled), group
    )

    assert handler.handled == [envelope]
    assert len(handler.call_times) == 3
    assert await topic_size(retry_topic(source.name, 1)) == 0


async def test_a_handler_that_runs_too_long_is_cut_off_and_retried(
    session_factory: SessionFactory,
    settings_for: SettingsFor,
    source: Topic,
    group: str,
    producer: AIOKafkaProducer,
) -> None:
    envelope = new_envelope()
    await send(producer, source.name, envelope)
    handler = RecordingHandler(delays=(5.0,))
    settings = settings_for(attempts=2, handler_timeout_s=0.3)

    await consume_until(settings, source, handler, lambda: bool(handler.handled), group)

    assert handler.handled == [envelope]
    first_call, second_call = handler.call_times
    assert second_call - first_call < 2


async def test_a_message_recovers_on_a_retry_topic_after_its_delay(
    session_factory: SessionFactory,
    settings_for: SettingsFor,
    source: Topic,
    group: str,
    producer: AIOKafkaProducer,
    read_topic: ReadTopic,
) -> None:
    envelope = new_envelope()
    await send(producer, source.name, envelope)
    handler = RecordingHandler(ConnectionError("db down"))

    await consume_until(settings_for(), source, handler, lambda: bool(handler.handled), group)

    first_call, retry_call = handler.call_times
    assert retry_call - first_call >= RETRY_DELAY_S * 0.9
    [copy] = await read_topic(retry_topic(source.name, 1), 1)
    assert header(copy, CONSUMER_HEADER) == group
    assert header(copy, ORIGIN_HEADER) == f"{source.name}:0:0"


async def test_a_waiting_retry_does_not_hold_up_new_messages(
    session_factory: SessionFactory,
    settings_for: SettingsFor,
    source: Topic,
    group: str,
    producer: AIOKafkaProducer,
) -> None:
    failing, later = new_envelope(), new_envelope()
    await send(producer, source.name, failing)
    handler = RecordingHandler(ConnectionError("db down"))
    settings = settings_for(retry_delays_s=(3.0, 3.0))

    async with running(settings, source, handler, group) as (task, _):
        await wait_until(lambda: bool(handler.call_times), task)
        await send(producer, source.name, later)
        await wait_until(lambda: len(handler.handled) == 2, task)

    assert handler.handled_ids() == [later.id, failing.id]


async def test_a_message_that_keeps_failing_ends_in_the_dead_letter_topic(
    session_factory: SessionFactory,
    settings_for: SettingsFor,
    source: Topic,
    group: str,
    producer: AIOKafkaProducer,
    read_topic: ReadTopic,
) -> None:
    envelope = new_envelope()
    await send(producer, source.name, envelope)
    handler = RecordingHandler(*[ConnectionError("password=hunter2 rejected")] * 3)

    await consume_until(
        settings_for(), source, handler, lambda: len(handler.call_times) == 3, group
    )

    [dead] = await read_topic(dead_letter_topic(source.name), 1)
    assert Envelope.model_validate_json(dead.value) == envelope
    assert dead.key == b"case-1"
    # Only the type: driver error text can quote message data.
    assert header(dead, ERROR_HEADER) == "ConnectionError"
    assert header(dead, ORIGIN_HEADER) == f"{source.name}:0:0"
    assert handler.handled == []


async def test_without_retry_tiers_a_failure_goes_straight_to_the_dead_letter_topic(
    session_factory: SessionFactory,
    settings_for: SettingsFor,
    source: Topic,
    group: str,
    producer: AIOKafkaProducer,
    topic_size: TopicSize,
) -> None:
    await send(producer, source.name, new_envelope())
    handler = RecordingHandler(ConnectionError("db down"))

    await consume_until(
        settings_for(retry_delays_s=()), source, handler, lambda: bool(handler.call_times), group
    )

    assert await topic_size(retry_topic(source.name, 1)) == 0
    assert await topic_size(dead_letter_topic(source.name)) == 1


async def test_a_permanent_error_goes_straight_to_the_dead_letter_topic(
    session_factory: SessionFactory,
    settings_for: SettingsFor,
    source: Topic,
    group: str,
    producer: AIOKafkaProducer,
    read_topic: ReadTopic,
    topic_size: TopicSize,
) -> None:
    await send(producer, source.name, new_envelope())
    handler = RecordingHandler(ValidationFailed("case already closed"))

    await consume_until(
        settings_for(attempts=3), source, handler, lambda: bool(handler.call_times), group
    )

    [dead] = await read_topic(dead_letter_topic(source.name), 1)
    assert header(dead, ERROR_HEADER) == "ValidationFailed: case already closed"
    assert len(handler.call_times) == 1
    assert await topic_size(retry_topic(source.name, 1)) == 0


async def test_a_malformed_message_is_dead_lettered_and_consumption_continues(
    session_factory: SessionFactory,
    settings_for: SettingsFor,
    source: Topic,
    group: str,
    producer: AIOKafkaProducer,
    read_topic: ReadTopic,
) -> None:
    last = new_envelope()
    await send(producer, source.name, b"{not json")
    await send(producer, source.name, last)
    handler = RecordingHandler()

    await consume_until(settings_for(), source, handler, lambda: bool(handler.handled), group)

    assert handler.handled == [last]
    [dead] = await read_topic(dead_letter_topic(source.name), 1)
    assert dead.value == b"{not json"
    assert header(dead, ERROR_HEADER) == "ValidationError: value: json_invalid"


async def test_messages_of_other_types_are_skipped(
    session_factory: SessionFactory,
    settings_for: SettingsFor,
    source: Topic,
    group: str,
    producer: AIOKafkaProducer,
) -> None:
    other, last = new_envelope("cases.case.closed"), new_envelope()
    await send(producer, source.name, other)
    await send(producer, source.name, last)
    handler = RecordingHandler()

    await consume_until(settings_for(), source, handler, lambda: bool(handler.handled), group)

    assert handler.handled == [last]
    async with session_factory() as session:
        recorded = await session.scalar(
            select(inbox.c.message_id).where(inbox.c.message_id == other.id)
        )
    assert recorded is None


async def test_retry_copies_not_addressed_to_the_consumer_are_ignored(
    session_factory: SessionFactory,
    settings_for: SettingsFor,
    source: Topic,
    group: str,
    producer: AIOKafkaProducer,
) -> None:
    unaddressed, theirs, ours = new_envelope(), new_envelope(), new_envelope()
    tier = retry_topic(source.name, 1)
    await send(producer, tier, unaddressed)
    await send(producer, tier, theirs, [(CONSUMER_HEADER, b"another-consumer")])
    await send(producer, tier, ours, [(CONSUMER_HEADER, group.encode())])
    handler = RecordingHandler()

    await consume_until(settings_for(), source, handler, lambda: bool(handler.handled), group)

    assert handler.handled == [ours]


async def test_handled_messages_are_committed_so_a_restart_resumes_after_them(
    session_factory: SessionFactory,
    settings_for: SettingsFor,
    source: Topic,
    group: str,
    producer: AIOKafkaProducer,
    kafka_bootstrap_servers: str,
) -> None:
    await send(producer, source.name, new_envelope())
    await send(producer, source.name, new_envelope())
    handler = RecordingHandler()

    await consume_until(settings_for(), source, handler, lambda: len(handler.handled) == 2, group)

    assert await committed(kafka_bootstrap_servers, group, TopicPartition(source.name, 0)) == 2


async def test_stopping_during_retries_abandons_the_message_uncommitted(
    session_factory: SessionFactory,
    settings_for: SettingsFor,
    source: Topic,
    group: str,
    producer: AIOKafkaProducer,
    kafka_bootstrap_servers: str,
    topic_size: TopicSize,
) -> None:
    await send(producer, source.name, new_envelope())
    handler = RecordingHandler(*[ConnectionError("db down")] * 3)
    settings = settings_for(attempts=3, attempt_backoff_s=60)

    async with running(settings, source, handler, group) as (task, stop):
        await wait_until(lambda: bool(handler.call_times), task)
        stop.set()
        # The one-minute backoff would time this out if shutdown waited for it.
        await asyncio.wait_for(asyncio.shield(task), timeout=5)

    assert len(handler.call_times) == 1
    assert await committed(kafka_bootstrap_servers, group, TopicPartition(source.name, 0)) is None
    assert await topic_size(retry_topic(source.name, 1)) == 0


async def test_a_rebalance_during_handling_neither_crashes_nor_duplicates_work(
    session_factory: SessionFactory,
    settings_for: SettingsFor,
    group: str,
    producer: AIOKafkaProducer,
    kafka_bootstrap_servers: str,
) -> None:
    wide = await create(kafka_bootstrap_servers, Topic(f"test.consumer.{uuid4().hex}", 2))
    envelopes = [new_envelope() for _ in range(6)]
    for index, envelope in enumerate(envelopes):
        await send(producer, wide.name, envelope, partition=index % 2)
    slow, fast = RecordingHandler(delays=(1.0,) * 6), RecordingHandler()
    settings = settings_for()

    async with running(settings, wide, slow, group) as (first, _):
        await wait_until(lambda: bool(slow.call_times), first)
        # A second member joins while the first is mid-handler, so a partition is revoked
        # from under it.
        async with running(settings, wide, fast, group) as (second, _):
            await wait_until(lambda: len(slow.handled) + len(fast.handled) == 6, first, second)

    handled = slow.handled_ids() + fast.handled_ids()
    assert sorted(handled) == sorted(envelope.id for envelope in envelopes)


async def test_a_retried_message_stays_in_one_trace(
    session_factory: SessionFactory,
    settings_for: SettingsFor,
    source: Topic,
    group: str,
    producer: AIOKafkaProducer,
    spans: InMemorySpanExporter,
) -> None:
    with tracer.start_as_current_span("publish") as publish:
        traceparent = current_traceparent()
        assert traceparent is not None
        await send(producer, source.name, new_envelope(), [("traceparent", traceparent.encode())])
    handler = RecordingHandler(ConnectionError("db down"))

    await consume_until(settings_for(), source, handler, lambda: bool(handler.handled), group)

    finished = sorted(spans.get_finished_spans(), key=lambda span: span.start_time or 0)
    first, retried = [span for span in finished if span.name.startswith("process ")]
    failed, succeeded = [span for span in finished if span.name.startswith("handle ")]
    assert first.name == f"process {source.name}"
    assert retried.name == f"process {source.name}.retry.1"
    assert first.parent is not None
    assert first.parent.span_id == publish.get_span_context().span_id
    # The retry copy continues from the delivery that failed.
    assert first.context is not None
    assert retried.parent is not None
    assert retried.parent.span_id == first.context.span_id
    assert failed.parent is not None
    assert failed.parent.span_id == first.context.span_id
    assert failed.status.status_code is StatusCode.ERROR
    assert retried.context is not None
    assert succeeded.parent is not None
    assert succeeded.parent.span_id == retried.context.span_id
    assert {span.context.trace_id for span in finished if span.context} == {
        publish.get_span_context().trace_id
    }
