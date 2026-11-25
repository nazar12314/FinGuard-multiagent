import asyncio
import json
from collections.abc import Mapping
from datetime import timedelta
from functools import cache
from typing import Any, Protocol, cast
from uuid import UUID

from opentelemetry.trace import SpanKind
from prometheus_client import Counter, Gauge
from sqlalchemy import CursorResult, delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from finguard.infrastructure.kafka.publisher import MessageRejected, OutgoingMessage
from finguard.infrastructure.outbox.tables import outbox
from finguard.infrastructure.tracing import (
    context_from,
    record_failure,
    traceparent_of,
    tracer,
)

# Any fixed bigint works, as long as every relay uses the same one ("outbox" in ASCII).
RELAY_LOCK_ID = 0x6F7574626F78

PUBLISHED = Counter("finguard_outbox_published", "Messages the relay published", ["topic"])
PARKED = Counter("finguard_outbox_parked", "Messages Kafka refused for good", ["topic"])


@cache
def _backlog_gauges() -> tuple[Gauge, Gauge]:
    # Lazy, or every process importing this module would report a backlog of 0.
    pending = Gauge("finguard_outbox_pending_messages", "Messages waiting in the outbox")
    # The signal to alert on: it grows while publishing is stuck.
    oldest_age = Gauge(
        "finguard_outbox_oldest_pending_age_seconds",
        "Age of the oldest message waiting in the outbox",
    )
    return pending, oldest_age


class Publisher(Protocol):
    async def publish(self, message: OutgoingMessage) -> None: ...


class MessageParked(Exception):
    """Kafka refused a message for good; it was set aside so later messages can flow."""

    def __init__(self, message_id: UUID, topic: str, reason: str) -> None:
        super().__init__(f"message {message_id} for {topic} was parked: {reason}")
        self.message_id = message_id
        self.topic = topic
        self.reason = reason


class OutboxRelay:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: Publisher,
        *,
        batch_size: int,
        publish_timeout_s: float,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._publish_timeout_s = publish_timeout_s

    async def publish_pending(self) -> int:
        """Return how many were published; 0 while another relay holds the lock.

        After TimeoutError or MessageParked the producer may be unusable; start a fresh one.
        """
        parked: MessageParked | None = None
        async with self._session_factory.begin() as session:
            locked = await session.scalar(select(func.pg_try_advisory_xact_lock(RELAY_LOCK_ID)))
            if not locked:
                return 0

            rows = (
                await session.execute(
                    select(outbox.c.id, outbox.c.destination, outbox.c.key, outbox.c.payload)
                    .where(outbox.c.published_at.is_(None), outbox.c.failed_at.is_(None))
                    .order_by(outbox.c.position)
                    .limit(self._batch_size)
                )
            ).all()

            # One at a time, so the messages before a refused one count as sent.
            published: list[tuple[UUID, str]] = []
            async with asyncio.timeout(self._publish_timeout_s):
                for row in rows:
                    with tracer.start_as_current_span(
                        f"send {row.destination}",
                        context=context_from(row.payload.get("traceparent")),
                        kind=SpanKind.PRODUCER,
                        attributes={
                            "messaging.system": "kafka",
                            "messaging.operation.name": "send",
                            "messaging.operation.type": "send",
                            "messaging.destination.name": row.destination,
                            "messaging.message.id": str(row.id),
                        },
                        record_exception=False,
                        set_status_on_exception=False,
                    ) as span:
                        try:
                            await self._publisher.publish(
                                _to_outgoing(
                                    row.destination,
                                    row.key,
                                    row.payload,
                                    traceparent=traceparent_of(span),
                                )
                            )
                        except MessageRejected as rejection:
                            record_failure(span, rejection)
                            parked = MessageParked(row.id, row.destination, str(rejection))
                            await session.execute(
                                update(outbox)
                                .where(outbox.c.id == row.id)
                                .values(failed_at=func.now(), last_error=str(rejection))
                            )
                            break
                        # CancelledError too: the batch timeout cancels a stuck publish.
                        except (Exception, asyncio.CancelledError) as error:
                            record_failure(span, error)
                            raise
                    published.append((row.id, row.destination))

            if published:
                await session.execute(
                    update(outbox)
                    .where(outbox.c.id.in_([message_id for message_id, _ in published]))
                    .values(published_at=func.now())
                )

        # Counted once committed: a failed commit leaves the messages pending, to be sent again.
        for _, topic in published:
            PUBLISHED.labels(topic).inc()
        if parked:
            PARKED.labels(parked.topic).inc()
            raise parked
        return len(published)

    async def measure_backlog(self) -> None:
        async with self._session_factory() as session:
            count, oldest_age_s = (
                await session.execute(
                    select(
                        func.count(),
                        func.extract("epoch", func.now() - func.min(outbox.c.created_at)),
                    ).where(outbox.c.published_at.is_(None), outbox.c.failed_at.is_(None))
                )
            ).one()
        pending, oldest_age = _backlog_gauges()
        pending.set(count)
        oldest_age.set(float(oldest_age_s or 0))

    async def prune_published(self, older_than: timedelta) -> int:
        async with self._session_factory.begin() as session:
            result = await session.execute(
                delete(outbox).where(outbox.c.published_at < func.now() - older_than)
            )
            return cast(CursorResult[Any], result).rowcount


def _to_outgoing(
    topic: str, key: str, envelope: Mapping[str, Any], *, traceparent: str | None
) -> OutgoingMessage:
    # The header carries the send span, so the consumer's span can start before parsing.
    return OutgoingMessage(
        topic=topic,
        key=key,
        value=json.dumps(envelope, separators=(",", ":")).encode(),
        headers=(("traceparent", traceparent.encode()),) if traceparent else (),
    )
