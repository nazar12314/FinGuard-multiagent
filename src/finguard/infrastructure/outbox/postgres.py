from uuid import UUID

from sqlalchemy import insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from finguard.infrastructure.outbox.tables import inbox, outbox
from finguard.platform.envelope import Envelope


class PostgresOutbox:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, envelope: Envelope, *, topic: str, key: str) -> None:
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

    async def record_first_delivery(self, message_id: UUID, consumer: str) -> bool:
        # A parallel duplicate blocks on the primary key until the first one commits,
        # then inserts nothing, so only one of them ever sees True.
        statement = (
            pg_insert(inbox)
            .values(message_id=message_id, consumer=consumer)
            .on_conflict_do_nothing()
            .returning(inbox.c.message_id)
        )
        return (await self._session.execute(statement)).first() is not None
