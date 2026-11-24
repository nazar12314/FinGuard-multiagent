import asyncio
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from finguard.infrastructure.kafka.topics import (
    dead_letter_topic,
    retry_topic,
    split_retry_topic,
)
from finguard.infrastructure.outbox.postgres import PostgresInbox
from finguard.platform.envelope import Envelope
from finguard.platform.errors import ApplicationError, DependencyUnavailable

# Runs in the transaction that also marks the message processed, so a handler's writes,
# including outbox messages, commit or roll back together with that mark.
MessageHandler = Callable[[Envelope, AsyncSession], Awaitable[None]]

# Headers on retried and dead-lettered copies.
CONSUMER_HEADER = "finguard-consumer"
ERROR_HEADER = "finguard-error"
ORIGIN_HEADER = "finguard-origin"

logger = structlog.stdlib.get_logger(__name__)


class ConsumerEvent(StrEnum):
    HANDLER_FAILED = "consumer.handler_failed"
    DUPLICATE_SKIPPED = "consumer.duplicate_skipped"
    RETRY_SCHEDULED = "consumer.retry_scheduled"
    DEAD_LETTERED = "consumer.dead_lettered"


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

    async def run(self, stop: asyncio.Event) -> None:
        """Consume until `stop` is set. Raises if a copy can't be handed on to Kafka;
        the offset isn't committed then, so the message is redelivered after a restart."""
        while not stop.is_set():
            self._resume_due_partitions()
            batches = await self._consumer.getmany(timeout_ms=200, max_records=100)
            for partition, records in batches.items():
                for record in records:
                    if stop.is_set():
                        return
                    due_at = self._due_at(record)
                    if due_at > time.time():
                        # Not due yet: rewind and pause this partition instead of sleeping,
                        # so the other partitions keep flowing and the consumer stays polled.
                        self._consumer.seek(partition, record.offset)
                        self._consumer.pause(partition)
                        self._paused_until[partition] = due_at
                        break
                    await self._process(record)
                    await self._consumer.commit({partition: record.offset + 1})

    def _resume_due_partitions(self) -> None:
        assigned = self._consumer.assignment()
        now = time.time()
        for partition, due_at in list(self._paused_until.items()):
            if partition not in assigned:
                del self._paused_until[partition]  # reassigned elsewhere; pausing ended with it
            elif due_at <= now:
                self._consumer.resume(partition)
                del self._paused_until[partition]

    def _due_at(self, record: Any) -> float:
        _, tier = split_retry_topic(record.topic)
        if tier == 0:
            return 0.0
        delays = self._policy.retry_delays_s
        timestamp_s: float = record.timestamp / 1000
        return timestamp_s + delays[min(tier, len(delays)) - 1]

    async def _process(self, record: Any) -> None:
        source, tier = split_retry_topic(record.topic)
        headers = dict(record.headers)
        if tier and headers.get(CONSUMER_HEADER) != self._name.encode():
            return  # a retry copy meant for another consumer group

        try:
            envelope = Envelope.model_validate_json(record.value)
        except ValidationError as error:
            await self._hand_on(record, dead_letter_topic(source), error)
            return

        handler = self._handlers.get(envelope.type)
        if handler is None:
            return

        failure = await self._handle(envelope, handler)
        if failure is None:
            return
        if is_permanent(failure) or tier >= len(self._policy.retry_delays_s):
            await self._hand_on(record, dead_letter_topic(source), failure)
        else:
            await self._hand_on(record, retry_topic(source, tier + 1), failure)

    async def _handle(self, envelope: Envelope, handler: MessageHandler) -> Exception | None:
        attempt = 1
        while True:
            try:
                async with (
                    asyncio.timeout(self._policy.handler_timeout_s),
                    self._session_factory.begin() as session,
                ):
                    if not await PostgresInbox(session).try_mark_processed(envelope.id, self._name):
                        logger.info(ConsumerEvent.DUPLICATE_SKIPPED, message_id=str(envelope.id))
                        return None
                    await handler(envelope, session)
                return None
            except Exception as error:
                permanent = is_permanent(error)
                logger.warning(
                    ConsumerEvent.HANDLER_FAILED,
                    message_id=str(envelope.id),
                    message_type=envelope.type,
                    attempt=attempt,
                    permanent=permanent,
                    exc_info=error,
                )
                if permanent or attempt >= self._policy.attempts:
                    return error
                await asyncio.sleep(self._policy.attempt_backoff_s * 2 ** (attempt - 1))
                attempt += 1

    async def _hand_on(self, record: Any, topic: str, error: Exception) -> None:
        own_headers = dict(record.headers)
        # Where the message was first delivered, kept unchanged across retry tiers.
        origin: bytes = own_headers.get(ORIGIN_HEADER) or (
            f"{record.topic}:{record.partition}:{record.offset}".encode()
        )
        headers = [(key, value) for key, value in record.headers if not key.startswith("finguard-")]
        headers += [
            (CONSUMER_HEADER, self._name.encode()),
            (ERROR_HEADER, f"{type(error).__name__}: {error}"[:1000].encode()),
            (ORIGIN_HEADER, origin),
        ]
        await self._producer.send_and_wait(topic, record.value, key=record.key, headers=headers)
        dead_lettered = topic == dead_letter_topic(split_retry_topic(record.topic)[0])
        logger.warning(
            ConsumerEvent.DEAD_LETTERED if dead_lettered else ConsumerEvent.RETRY_SCHEDULED,
            topic=topic,
            origin=origin.decode(),
            error=type(error).__name__,
        )
