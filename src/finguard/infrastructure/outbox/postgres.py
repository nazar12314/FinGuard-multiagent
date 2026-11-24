from uuid import UUID

from sqlalchemy import insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from finguard.infrastructure.outbox.tables import inbox, outbox
from finguard.platform.envelope import Envelope
from finguard.platform.errors import ValidationFailed

# Below Kafka's default max.message.bytes (1 MiB), leaving room for the key and headers.
MAX_MESSAGE_BYTES = 1_000_000


class PostgresOutbox:
    def __init__(
        self, session: AsyncSession, *, max_message_bytes: int = MAX_MESSAGE_BYTES
    ) -> None:
        self._session = session
        self._max_message_bytes = max_message_bytes

    async def add(self, envelope: Envelope, *, topic: str, key: str) -> None:
        # Refusing here fails the writer's own transaction, instead of parking the message
        # in the relay after the state change has already committed.
        size = len(envelope.model_dump_json().encode())
        if size > self._max_message_bytes:
            raise ValidationFailed(
                f"message {envelope.id} is {size} bytes, over the {self._max_message_bytes} limit"
            )
        await self._session.execute(
            insert(outbox).values(
                id=envelope.id,
                destination=topic,
                key=key,
                payload=envelope.model_dump(mode="json"),
            )
        )


class PostgresInbox:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def try_mark_processed(self, message_id: UUID, consumer: str) -> bool:
        # A parallel duplicate waits on the primary key until the first transaction ends.
        # If that one committed, the duplicate inserts nothing and gets False; if it rolled
        # back, the duplicate takes over and gets True.
        statement = (
            pg_insert(inbox)
            .values(message_id=message_id, consumer=consumer)
            .on_conflict_do_nothing()
            .returning(inbox.c.message_id)
        )
        return (await self._session.execute(statement)).first() is not None
