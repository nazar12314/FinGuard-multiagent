import asyncio
import json
from collections.abc import Mapping
from datetime import timedelta
from typing import Any, Protocol, cast
from uuid import UUID

from opentelemetry.trace import SpanKind
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
        """Publish the oldest pending messages in order and mark them sent. Return how many.

        Only one relay publishes at a time, which keeps per-key order. If another relay
        holds the lock, this returns 0 without reading anything. A crash between publishing
        and committing re-sends the batch, which consumers deduplicate.

        Raises TimeoutError when the batch takes longer than the publish timeout, and
        MessageParked after setting aside a message Kafka refused. The producer may be
        unusable after either, so the caller should start a fresh one.
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

            # One message at a time, so a refused message is known exactly and the ones
            # before it count as sent.
            published: list[UUID] = []
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
                    published.append(row.id)

            if published:
                await session.execute(
                    update(outbox).where(outbox.c.id.in_(published)).values(published_at=func.now())
                )

        if parked:
            raise parked
        return len(published)

    async def prune_published(self, older_than: timedelta) -> int:
        async with self._session_factory.begin() as session:
            result = await session.execute(
                delete(outbox).where(outbox.c.published_at < func.now() - older_than)
            )
            return cast(CursorResult[Any], result).rowcount


def _to_outgoing(
    topic: str, key: str, envelope: Mapping[str, Any], *, traceparent: str | None
) -> OutgoingMessage:
    # The header carries the send span, so the consumer's span becomes its child and can
    # start before the payload is parsed. The payload keeps the writer's context.
    return OutgoingMessage(
        topic=topic,
        key=key,
        value=json.dumps(envelope, separators=(",", ":")).encode(),
        headers=(("traceparent", traceparent.encode()),) if traceparent else (),
    )
