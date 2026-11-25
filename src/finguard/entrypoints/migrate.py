from collections.abc import Sequence

from alembic.config import CommandLine, Config

# Import every module that defines tables, so autogenerate and `alembic check` see them all.
from finguard.core import diagnostics as _diagnostics_tables  # noqa: F401
from finguard.core.logging import configure_logging
from finguard.core.settings import load_settings_or_exit
from finguard.infrastructure.db.metadata import metadata
from finguard.infrastructure.outbox import tables as _outbox_tables  # noqa: F401
from finguard.modules.screening.adapters import tables as _screening_tables  # noqa: F401

MIGRATIONS = "finguard.infrastructure.db:migrations"


def alembic_config(database_url: str) -> Config:
    config = Config()
    config.set_main_option("script_location", MIGRATIONS)
    # Not ini options, which would read a "%" in a URL-encoded password as interpolation.
    config.attributes["database_url"] = database_url
    config.attributes["target_metadata"] = metadata
    return config


def main(argv: Sequence[str] | None = None) -> None:
    """The Alembic command line, configured from FinGuard's settings instead of alembic.ini.

    For example: `finguard-migrate upgrade head`, `finguard-migrate history`.
    """
    command_line = CommandLine(prog="finguard-migrate")
    options = command_line.parser.parse_args(argv)
    if not hasattr(options, "cmd"):
        command_line.parser.error("too few arguments")

    settings = load_settings_or_exit()
    configure_logging(settings)
    config = alembic_config(settings.database.url.get_secret_value())
    config.cmd_opts = options
    command_line.run_cmd(config, options)


if __name__ == "__main__":
    main()
