import json
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any, Protocol, cast

from sqlalchemy import CursorResult, delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from finguard.infrastructure.kafka.publisher import OutgoingMessage
from finguard.infrastructure.outbox.tables import outbox

# Any fixed bigint works, as long as every relay uses the same one ("outbox" in ASCII).
RELAY_LOCK_ID = 0x6F7574626F78


class Publisher(Protocol):
    async def publish(self, messages: Sequence[OutgoingMessage]) -> None: ...


class OutboxRelay:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: Publisher,
        *,
        batch_size: int,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._batch_size = batch_size

    async def publish_pending(self) -> int:
        """Publish the oldest unsent messages and mark them sent. Return how many.

        Only one relay publishes at a time, which keeps per-key order. If another relay
        holds the lock, this returns 0 without reading anything. A crash between publishing
        and committing re-sends the batch, which consumers deduplicate.
        """
        async with self._session_factory.begin() as session:
            locked = await session.scalar(select(func.pg_try_advisory_xact_lock(RELAY_LOCK_ID)))
            if not locked:
                return 0

            rows = (
                await session.execute(
                    select(outbox.c.id, outbox.c.destination, outbox.c.key, outbox.c.payload)
                    .where(outbox.c.published_at.is_(None))
                    .order_by(outbox.c.created_at, outbox.c.id)
                    .limit(self._batch_size)
                )
            ).all()
            if not rows:
                return 0

            await self._publisher.publish(
                [_to_outgoing(row.destination, row.key, row.payload) for row in rows]
            )
            await session.execute(
                update(outbox)
                .where(outbox.c.id.in_([row.id for row in rows]))
                .values(published_at=func.now())
            )
            return len(rows)

    async def prune_published(self, older_than: timedelta) -> int:
        async with self._session_factory.begin() as session:
            result = await session.execute(
                delete(outbox).where(outbox.c.published_at < func.now() - older_than)
            )
            return cast(CursorResult[Any], result).rowcount


def _to_outgoing(topic: str, key: str, envelope: Mapping[str, Any]) -> OutgoingMessage:
    # The trace context also travels as a Kafka header, so consumers can continue the
    # trace before they parse the payload.
    traceparent = envelope.get("traceparent")
    return OutgoingMessage(
        topic=topic,
        key=key,
        value=json.dumps(envelope, separators=(",", ":")).encode(),
        headers=(("traceparent", traceparent.encode()),) if traceparent else (),
    )
