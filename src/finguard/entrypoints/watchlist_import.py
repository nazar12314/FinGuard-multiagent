import argparse
import asyncio
from collections.abc import Sequence
from contextlib import AsyncExitStack
from pathlib import Path

from finguard.core.container import build_watchlist_import
from finguard.core.logging import LogEvent, configure_logging, get_logger
from finguard.core.settings import Settings, load_settings_or_exit
from finguard.modules.screening.adapters.opensanctions import read_opensanctions
from finguard.modules.screening.service import WatchlistImported

logger = get_logger(__name__)

OPENSANCTIONS_DIR = Path("data/raw/opensanctions")


async def import_watchlist(settings: Settings, sanctions: Path, peps: Path) -> WatchlistImported:
    async with AsyncExitStack() as resources:
        watchlist_import = build_watchlist_import(settings, resources)
        return await watchlist_import.replace_watchlist(read_opensanctions(sanctions, peps))


def main(argv: Sequence[str] | None = None) -> None:
    """Replace the screening watchlist with OpenSanctions' sanctions and PEP collections."""
    parser = argparse.ArgumentParser(prog="finguard-watchlist-import")
    parser.add_argument(
        "--sanctions", type=Path, default=OPENSANCTIONS_DIR / "sanctions.targets.simple.csv"
    )
    parser.add_argument("--peps", type=Path, default=OPENSANCTIONS_DIR / "peps.targets.simple.csv")
    arguments = parser.parse_args(argv)
    for path in (arguments.sanctions, arguments.peps):
        if not path.is_file():
            parser.error(
                f"{path} not found; download targets.simple.csv from "
                "https://data.opensanctions.org/datasets/latest/<collection>/"
            )

    settings = load_settings_or_exit()
    configure_logging(settings)
    imported = asyncio.run(import_watchlist(settings, arguments.sanctions, arguments.peps))
    logger.info(
        LogEvent.WATCHLIST_IMPORTED,
        entities=imported.entities,
        names=imported.names,
        sanctioned=imported.sanctioned,
        peps=imported.peps,
    )


if __name__ == "__main__":
    main()
