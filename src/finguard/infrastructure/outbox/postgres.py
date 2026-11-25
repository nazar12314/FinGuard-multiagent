from collections.abc import Collection
from uuid import UUID

from sqlalchemy import insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from finguard.infrastructure.outbox.tables import inbox, outbox
from finguard.infrastructure.tracing import current_traceparent
from finguard.platform.envelope import Envelope
from finguard.platform.errors import ValidationFailed
from finguard.platform.topics import TOPIC_NAMES

# Below Kafka's default max.message.bytes (1 MiB), leaving room for the key and headers.
MAX_MESSAGE_BYTES = 1_000_000


class PostgresOutbox:
    def __init__(
        self,
        session: AsyncSession,
        *,
        max_message_bytes: int = MAX_MESSAGE_BYTES,
        topics: Collection[str] = TOPIC_NAMES,
    ) -> None:
        self._session = session
        self._max_message_bytes = max_message_bytes
        self._topics = topics

    async def add(self, envelope: Envelope, *, topic: str, key: str) -> None:
        # Refused here, while the writer can still roll back, instead of parking in the relay.
        if topic not in self._topics:
            raise ValidationFailed(f"unknown topic {topic!r} for message {envelope.id}")
        size = len(envelope.model_dump_json().encode())
        if size > self._max_message_bytes:
            raise ValidationFailed(
                f"message {envelope.id} is {size} bytes, over the {self._max_message_bytes} limit"
            )
        payload = envelope.model_dump(mode="json")
        if payload["traceparent"] is None:
            payload["traceparent"] = current_traceparent()
        await self._session.execute(
            insert(outbox).values(id=envelope.id, destination=topic, key=key, payload=payload)
        )


class PostgresInbox:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def try_mark_processed(self, message_id: UUID, consumer: str) -> bool:
        # A parallel duplicate blocks on the primary key, then gets False if the first
        # delivery committed, or True if it rolled back.
        statement = (
            pg_insert(inbox)
            .values(message_id=message_id, consumer=consumer)
            .on_conflict_do_nothing()
            .returning(inbox.c.message_id)
        )
        return (await self._session.execute(statement)).first() is not None
