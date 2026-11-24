import asyncio

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from finguard.infrastructure.db.metadata import metadata
from finguard.infrastructure.outbox import tables as _outbox_tables  # noqa: F401  registers tables

if context.is_offline_mode():
    raise SystemExit("Offline (--sql) migrations are not supported.")


def run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=metadata, include_schemas=True)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    # The URL is passed through attributes rather than the ini options, which would
    # treat a "%" in a URL-encoded password as interpolation.
    engine = create_async_engine(context.config.attributes["database_url"], poolclass=NullPool)
    async with engine.connect() as connection:
        await connection.run_sync(run_migrations)
    await engine.dispose()


asyncio.run(run_async_migrations())
