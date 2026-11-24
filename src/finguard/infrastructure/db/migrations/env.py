import asyncio

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

if context.is_offline_mode():
    raise SystemExit("Offline (--sql) migrations are not supported.")

attributes = context.config.attributes


def run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=attributes["target_metadata"],
        include_schemas=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    engine = create_async_engine(attributes["database_url"], poolclass=NullPool)
    async with engine.connect() as connection:
        await connection.run_sync(run_migrations)
    await engine.dispose()


# A caller that is already inside an event loop passes its own connection instead
# (`await connection.run_sync(...)`), since asyncio.run can't be nested.
if (connection := attributes.get("connection")) is not None:
    run_migrations(connection)
else:
    asyncio.run(run_async_migrations())
