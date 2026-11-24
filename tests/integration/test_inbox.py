from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from finguard.infrastructure.outbox.postgres import PostgresInbox

SessionFactory = async_sessionmaker[AsyncSession]


async def record(session_factory: SessionFactory, message_id: UUID, consumer: str) -> bool:
    async with session_factory.begin() as session:
        return await PostgresInbox(session).record_first_delivery(message_id, consumer)


async def test_a_redelivered_message_is_recognised_per_consumer(
    session_factory: SessionFactory,
) -> None:
    message_id = uuid4()

    assert await record(session_factory, message_id, "cases") is True
    assert await record(session_factory, message_id, "cases") is False
    assert await record(session_factory, message_id, "orchestrator") is True


async def test_a_failed_handler_leaves_the_message_unprocessed(
    session_factory: SessionFactory,
) -> None:
    message_id = uuid4()

    async def record_then_fail() -> None:
        async with session_factory.begin() as session:
            await PostgresInbox(session).record_first_delivery(message_id, "cases")
            raise RuntimeError("handler failed")

    with pytest.raises(RuntimeError):
        await record_then_fail()

    assert await record(session_factory, message_id, "cases") is True
