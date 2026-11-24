from alembic import command
from sqlalchemy import Connection, text
from sqlalchemy.ext.asyncio import create_async_engine

from finguard.entrypoints.migrate import alembic_config


def test_migrations_upgrade_downgrade_and_upgrade_again(empty_database_url: str) -> None:
    config = alembic_config(empty_database_url)

    command.upgrade(config, "head")
    command.downgrade(config, "base")
    command.upgrade(config, "head")


def test_the_table_definitions_match_the_migrations(empty_database_url: str) -> None:
    config = alembic_config(empty_database_url)
    command.upgrade(config, "head")

    command.check(config)


async def test_migrations_run_on_a_connection_from_async_code(empty_database_url: str) -> None:
    config = alembic_config(empty_database_url)
    engine = create_async_engine(empty_database_url)

    def upgrade(connection: Connection) -> None:
        config.attributes["connection"] = connection
        command.upgrade(config, "head")

    async with engine.begin() as connection:
        await connection.run_sync(upgrade)
        tables = await connection.scalar(
            text("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'platform'")
        )
    await engine.dispose()

    assert tables == 2
