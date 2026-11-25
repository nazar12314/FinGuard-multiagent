from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from typing import Protocol

from finguard.modules.screening.domain.watchlist import WatchlistEntity, WatchlistName


class Transliterator(Protocol):
    def latin_spellings(self, text: str) -> list[str]:
        """The ways a name is commonly written in Latin letters, most generic first."""
        ...


class WatchlistWriter(Protocol):
    async def add(
        self, entities: Sequence[WatchlistEntity], names: Sequence[WatchlistName]
    ) -> None: ...


class WatchlistRepository(Protocol):
    def replace(self) -> AbstractAsyncContextManager[WatchlistWriter]:
        """Replace the whole watchlist: what's added inside becomes visible together on exit,
        and nothing changes if the block raises."""
        ...
