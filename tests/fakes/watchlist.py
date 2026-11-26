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
        # Postgres enforces both with its primary and foreign keys.
        known = {entity.entity_id for entity in self.entities}
        for entity in entities:
            if entity.entity_id in known:
                raise ValueError(f"{entity.entity_id} is already on the list")
            known.add(entity.entity_id)
        if dangling := {name.entity_id for name in names} - known:
            raise ValueError(f"names for unknown entities {sorted(dangling)}")
        self.entities.extend(entities)
        self.names.extend(names)
