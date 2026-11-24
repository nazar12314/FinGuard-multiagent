from alembic import command

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
