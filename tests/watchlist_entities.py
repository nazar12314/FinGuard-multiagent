from dataclasses import replace

from finguard.modules.screening.domain.watchlist import WatchlistEntity

PERSON = WatchlistEntity(
    entity_id="NK-1",
    schema="Person",
    name="Petr Ivanov",
    aliases=("Ivanov, Petr", "Petr Ivanov"),
    birth_dates=("1965-03-02",),
    countries=("ru",),
    program_ids=("EU-RUS",),
    datasets=("EU Financial Sanctions Files",),
    sanctioned=True,
    pep=False,
)


def entity(entity_id: str, name: str, **changes: object) -> WatchlistEntity:
    fields: dict[str, object] = {"aliases": (), **changes}
    return replace(PERSON, entity_id=entity_id, name=name, **fields)  # type: ignore[arg-type]
