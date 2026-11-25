import asyncio
import time
import traceback
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.errors import CommitFailedError, IllegalStateError
from opentelemetry import trace
from opentelemetry.trace import SpanKind
from prometheus_client import Counter, Gauge, Histogram
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from finguard.infrastructure.kafka.topics import (
    dead_letter_topic,
    retry_topic,
    split_retry_topic,
)
from finguard.infrastructure.outbox.postgres import PostgresInbox
from finguard.infrastructure.tracing import (
    context_from,
    current_traceparent,
    record_failure,
    tracer,
)
from finguard.platform.envelope import Envelope
from finguard.platform.errors import ApplicationError, DependencyUnavailable, describe_error

MessageHandler = Callable[[Envelope, AsyncSession], Awaitable[None]]

# Headers on retried and dead-lettered copies.
CONSUMER_HEADER = "finguard-consumer"
ERROR_HEADER = "finguard-error"
ORIGIN_HEADER = "finguard-origin"

logger = structlog.stdlib.get_logger(__name__)

# outcome: handled, duplicate, ignored (another type or another group's copy), retried or
# dead_lettered.
DELIVERIES = Counter(
    "finguard_consumer_deliveries", "Deliveries by outcome", ["consumer", "topic", "outcome"]
)
HANDLER_FAILURES = Counter(
    "finguard_consumer_handler_failures",
    "Failed handling attempts",
    ["consumer", "message_type", "permanent"],
)
HANDLING_DURATION = Histogram(
    "finguard_consumer_handling_seconds",
    "Time to handle one delivery, in-place retries included",
    ["consumer", "message_type"],
)
LAG = Gauge(
    "finguard_consumer_lag",
    "Messages between the consumer's position and the end of the partition",
    ["consumer", "topic", "partition"],
)
_LAG_INTERVAL_S = 5.0


class ConsumerEvent(StrEnum):
    HANDLER_FAILED = "consumer.handler_failed"
    DUPLICATE_SKIPPED = "consumer.duplicate_skipped"
    RETRY_SCHEDULED = "consumer.retry_scheduled"
    DEAD_LETTERED = "consumer.dead_lettered"
    UNADDRESSED_COPY_SKIPPED = "consumer.unaddressed_copy_skipped"
    COMMIT_LOST_TO_REBALANCE = "consumer.commit_lost_to_rebalance"


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    attempts: int
    attempt_backoff_s: float
    handler_timeout_s: float
    # One delay per retry topic tier, after which the copy on that tier is handled again.
    retry_delays_s: tuple[float, ...]

    @property
    def longest_handling_s(self) -> float:
        backoff = sum(self.attempt_backoff_s * 2.0**attempt for attempt in range(self.attempts - 1))
        return self.attempts * self.handler_timeout_s + backoff


def is_permanent(error: Exception) -> bool:
    # A business-rule outcome or malformed data comes out the same however often it is
    # retried; only a dependency outage is worth another attempt among application errors.
    if isinstance(error, ValidationError):
        return True
    return isinstance(error, ApplicationError) and not isinstance(error, DependencyUnavailable)


def _header(record: Any, name: str) -> str | None:
    value = dict(record.headers).get(name)
    return value.decode(errors="replace") if value is not None else None


def _frames(error: Exception) -> list[str]:
    return [
        f"{frame.filename}:{frame.lineno} in {frame.name}"
        for frame in traceback.extract_tb(error.__traceback__)
    ]


class _Stopping(Exception):
    """Raised to abandon a message, uncommitted, when shutdown interrupts its retries."""


def create_kafka_consumer(
    bootstrap_servers: str, *, group_id: str, topics: list[str], policy: RetryPolicy
) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        # Offsets are committed only after a message is handled or handed on.
        enable_auto_commit=False,
        # A new consumer group starts from the oldest retained message, so nothing is missed.
        auto_offset_reset="earliest",
        # The broker would drop the consumer from the group if handling one message,
        # retries included, took longer than this.
        max_poll_interval_ms=int(max(300.0, policy.longest_handling_s + 60) * 1000),
    )


class KafkaMessageConsumer:
    """Runs handlers for the envelope types it knows, at least once and in order per key.

    A failing handler is retried in place, then moved through the retry topic tiers, then
    to the dead-letter topic. Permanent errors and malformed messages go straight there.
    Messages of other types are skipped.
    """

    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        producer: AIOKafkaProducer,
        session_factory: async_sessionmaker[AsyncSession],
        handlers: Mapping[str, MessageHandler],
        *,
        name: str,
        policy: RetryPolicy,
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._session_factory = session_factory
        self._handlers = handlers
        self._name = name
        self._policy = policy
        self._paused_until: dict[TopicPartition, float] = {}
        self._stop = asyncio.Event()
        self._lag_partitions: set[TopicPartition] = set()
        self._lag_measured_at = 0.0

    async def run(self, stop: asyncio.Event) -> None:
        """Consume until `stop` is set. Raises if a copy can't be handed on to Kafka;
        the offset isn't committed then, so the message is redelivered after a restart.
        """
        self._stop = stop
        while not stop.is_set():
            self._resume_due_partitions()
            if time.monotonic() - self._lag_measured_at >= _LAG_INTERVAL_S:
                await self._measure_lag()
            # One record per poll, so max.poll.interval.ms only has to cover handling one
            # message. aiokafka still fetches ahead in the background.
            batches = await self._consumer.getmany(timeout_ms=200, max_records=1)
            for partition, records in batches.items():
                for record in records:
                    try:
                        await self._consume(partition, record)
                    except _Stopping:
                        return

    async def _consume(self, partition: TopicPartition, record: Any) -> None:
        # Kafka can take the partition away during any await (a rebalance). Calls without
        # an await in between see a stable assignment.
        if not self._owns(partition):
            return
        if not self._addressed_to_us(record):
            DELIVERIES.labels(self._name, record.topic, "ignored").inc()
            await self._commit(partition, record)
            return
        due_at = self._due_at(record)
        if due_at > time.time():
            # Not due yet: rewind and pause this partition instead of sleeping, so the
            # other partitions keep flowing and the consumer keeps polling.
            self._consumer.seek(partition, record.offset)
            self._consumer.pause(partition)
            self._paused_until[partition] = due_at
            return
        with tracer.start_as_current_span(
            f"process {record.topic}",
            context=context_from(_header(record, "traceparent")),
            kind=SpanKind.CONSUMER,
            attributes={
                "messaging.system": "kafka",
                "messaging.operation.name": "process",
                "messaging.operation.type": "process",
                "messaging.destination.name": record.topic,
                "messaging.destination.partition.id": str(record.partition),
                "messaging.kafka.offset": record.offset,
                "messaging.consumer.group.name": self._name,
            },
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            try:
                await self._process(record)
            except _Stopping:
                raise
            except Exception as error:
                # Handing a copy on failed, so the message stays uncommitted.
                record_failure(span, error)
                raise
        await self._commit(partition, record)

    async def _measure_lag(self) -> None:
        self._lag_measured_at = time.monotonic()
        assigned = self._consumer.assignment()
        for partition in self._lag_partitions - assigned:
            LAG.remove(self._name, partition.topic, str(partition.partition))
        self._lag_partitions = set(assigned)
        for partition in assigned:
            end = self._consumer.highwater(partition)
            if end is None:
                continue  # nothing fetched from this partition yet
            position = await self._consumer.position(partition)
            LAG.labels(self._name, partition.topic, str(partition.partition)).set(
                max(end - position, 0)
            )

    def _owns(self, partition: TopicPartition) -> bool:
        return partition in self._consumer.assignment()

    async def _commit(self, partition: TopicPartition, record: Any) -> None:
        if not self._owns(partition):
            return
        try:
            await self._consumer.commit({partition: record.offset + 1})
        except (CommitFailedError, IllegalStateError):
            # The partition moved to another consumer, which receives this message again;
            # the inbox makes that harmless.
            logger.info(
                ConsumerEvent.COMMIT_LOST_TO_REBALANCE,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
            )

    def _resume_due_partitions(self) -> None:
        assigned = self._consumer.assignment()
        now = time.time()
        for partition, due_at in list(self._paused_until.items()):
            if partition not in assigned:
                del self._paused_until[partition]  # reassigned elsewhere; pausing ended with it
            elif due_at <= now:
                self._consumer.resume(partition)
                del self._paused_until[partition]

    def _addressed_to_us(self, record: Any) -> bool:
        if split_retry_topic(record.topic)[1] == 0:
            return True
        consumer = dict(record.headers).get(CONSUMER_HEADER)
        if consumer is None:
            logger.warning(
                ConsumerEvent.UNADDRESSED_COPY_SKIPPED,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
            )
        return consumer == self._name.encode()

    def _due_at(self, record: Any) -> float:
        _, tier = split_retry_topic(record.topic)
        if tier == 0:
            return 0.0
        delay = self._policy.retry_delays_s[tier - 1]
        # Capped at now + delay, so a producer clock running ahead can't stretch the wait.
        handed_on_at = min(record.timestamp / 1000, time.time())
        return float(handed_on_at + delay)

    async def _process(self, record: Any) -> None:
        source, tier = split_retry_topic(record.topic)
        try:
            envelope = Envelope.model_validate_json(record.value)
        except ValidationError as error:
            record_failure(trace.get_current_span(), error)
            await self._hand_on(record, dead_letter_topic(source), error)
            return

        handler = self._handlers.get(envelope.type)
        if handler is None:
            DELIVERIES.labels(self._name, record.topic, "ignored").inc()
            return

        with HANDLING_DURATION.labels(self._name, envelope.type).time():
            failure = await self._handle(envelope, handler, record.topic)
        if failure is None:
            return
        if is_permanent(failure) or tier >= len(self._policy.retry_delays_s):
            await self._hand_on(record, dead_letter_topic(source), failure)
        else:
            await self._hand_on(record, retry_topic(source, tier + 1), failure)

    async def _handle(
        self, envelope: Envelope, handler: MessageHandler, topic: str
    ) -> Exception | None:
        attempt = 1
        while True:
            error = await self._attempt(envelope, handler, attempt, topic)
            if error is None:
                return None
            if is_permanent(error) or attempt >= self._policy.attempts:
                return error
            backoff = self._policy.attempt_backoff_s * 2 ** (attempt - 1)
            with suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=backoff)
            if self._stop.is_set():
                raise _Stopping from error
            attempt += 1

    async def _attempt(
        self, envelope: Envelope, handler: MessageHandler, attempt: int, topic: str
    ) -> Exception | None:
        with tracer.start_as_current_span(
            f"handle {envelope.type}",
            attributes={"messaging.message.id": str(envelope.id), "finguard.attempt": attempt},
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            try:
                async with (
                    asyncio.timeout(self._policy.handler_timeout_s),
                    self._session_factory.begin() as session,
                ):
                    if not await PostgresInbox(session).try_mark_processed(envelope.id, self._name):
                        span.add_event("duplicate_skipped")
                        logger.info(ConsumerEvent.DUPLICATE_SKIPPED, message_id=str(envelope.id))
                        DELIVERIES.labels(self._name, topic, "duplicate").inc()
                        return None
                    await handler(envelope, session)
                DELIVERIES.labels(self._name, topic, "handled").inc()
                return None
            except Exception as error:
                record_failure(span, error)
                permanent = is_permanent(error)
                HANDLER_FAILURES.labels(self._name, envelope.type, str(permanent).lower()).inc()
                # Logged inside the attempt's span, so the line points at the failed attempt.
                logger.warning(
                    ConsumerEvent.HANDLER_FAILED,
                    message_id=str(envelope.id),
                    message_type=envelope.type,
                    attempt=attempt,
                    permanent=permanent,
                    error=describe_error(error),
                    frames=_frames(error),
                )
                return error

    async def _hand_on(self, record: Any, topic: str, error: Exception) -> None:
        own_headers = dict(record.headers)
        # Where the message was first delivered, kept unchanged across retry tiers.
        origin: bytes = own_headers.get(ORIGIN_HEADER) or (
            f"{record.topic}:{record.partition}:{record.offset}".encode()
        )
        headers = [
            (key, value)
            for key, value in record.headers
            if not key.startswith("finguard-") and key != "traceparent"
        ]
        headers += [
            (CONSUMER_HEADER, self._name.encode()),
            (ERROR_HEADER, describe_error(error)[:1000].encode()),
            (ORIGIN_HEADER, origin),
        ]
        # The copy continues the trace from this delivery, so retries appear as a chain.
        if traceparent := current_traceparent():
            headers.append(("traceparent", traceparent.encode()))
        trace.get_current_span().add_event("handed_on", {"messaging.destination.name": topic})
        await self._producer.send_and_wait(topic, record.value, key=record.key, headers=headers)
        dead_lettered = topic == dead_letter_topic(split_retry_topic(record.topic)[0])
        outcome = "dead_lettered" if dead_lettered else "retried"
        DELIVERIES.labels(self._name, record.topic, outcome).inc()
        logger.warning(
            (ConsumerEvent.DEAD_LETTERED if dead_lettered else ConsumerEvent.RETRY_SCHEDULED),
            topic=topic,
            origin=origin.decode(),
            error=describe_error(error),
        )
