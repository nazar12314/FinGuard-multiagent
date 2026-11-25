from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager

from finguard.modules.screening.domain.watchlist import WatchlistEntity, WatchlistName


class InMemoryWatchlist:
    def __init__(self) -> None:
        self.entities: dict[str, WatchlistEntity] = {}
        self.names: list[WatchlistName] = []

    @asynccontextmanager
    async def replace(self) -> AsyncIterator["_Writer"]:
        writer = _Writer()
        yield writer
        self.entities = {entity.entity_id: entity for entity in writer.entities}
        self.names = writer.names


class _Writer:
    def __init__(self) -> None:
        self.entities: list[WatchlistEntity] = []
        self.names: list[WatchlistName] = []

    async def add(
        self, entities: Sequence[WatchlistEntity], names: Sequence[WatchlistName]
    ) -> None:
        self.entities.extend(entities)
        self.names.extend(names)
