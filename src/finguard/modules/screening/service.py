from collections.abc import Iterable
from dataclasses import dataclass
from itertools import batched

from finguard.modules.screening.domain.watchlist import WatchlistEntity, WatchlistName, name_forms
from finguard.modules.screening.ports import Transliterator, WatchlistRepository

BATCH_SIZE = 5000


@dataclass(frozen=True, slots=True)
class WatchlistImported:
    entities: int
    names: int
    sanctioned: int
    peps: int


class WatchlistImport:
    def __init__(self, repository: WatchlistRepository, transliterator: Transliterator) -> None:
        self._repository = repository
        self._transliterator = transliterator

    async def replace_watchlist(self, entities: Iterable[WatchlistEntity]) -> WatchlistImported:
        counts = {"entities": 0, "names": 0, "sanctioned": 0, "peps": 0}
        async with self._repository.replace() as writer:
            for batch in batched(entities, BATCH_SIZE):
                names = [name for entity in batch for name in self.searchable_names(entity)]
                await writer.add(batch, names)
                counts["entities"] += len(batch)
                counts["names"] += len(names)
                counts["sanctioned"] += sum(entity.sanctioned for entity in batch)
                counts["peps"] += sum(entity.pep for entity in batch)
        return WatchlistImported(**counts)

    def searchable_names(self, entity: WatchlistEntity) -> list[WatchlistName]:
        """One row per distinct Latin form of each of the entity's names and aliases."""
        names: list[WatchlistName] = []
        seen: set[str] = set()
        for position, name in enumerate(entity.names()):
            for latin in self._transliterator.latin_spellings(name):
                forms = name_forms(latin)
                if not forms.normalised or forms.normalised in seen:
                    continue
                seen.add(forms.normalised)
                names.append(
                    WatchlistName(
                        entity_id=entity.entity_id,
                        name=name,
                        latin=latin,
                        forms=forms,
                        is_alias=position > 0,
                    )
                )
        return names
