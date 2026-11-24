from collections.abc import Sequence

from alembic.config import CommandLine, Config

from finguard.core.logging import configure_logging
from finguard.core.settings import load_settings_or_exit

MIGRATIONS = "finguard.infrastructure.db:migrations"


def alembic_config(database_url: str) -> Config:
    config = Config()
    config.set_main_option("script_location", MIGRATIONS)
    config.attributes["database_url"] = database_url
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
