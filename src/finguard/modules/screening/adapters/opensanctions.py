import csv
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

from finguard.modules.screening.domain.watchlist import SCREENED_SCHEMAS, WatchlistEntity
from finguard.platform.errors import ValidationFailed

# Columns of OpenSanctions' targets.simple.csv that the watchlist keeps.
REQUIRED_COLUMNS = frozenset(
    {"id", "schema", "name", "aliases", "birth_date", "countries", "program_ids", "dataset"}
)
# Some free-text columns (sanction details) run far past csv's default 128 KiB field limit.
FIELD_SIZE_LIMIT = 16 * 1024 * 1024


def read_opensanctions(sanctions: Path, peps: Path) -> Iterator[WatchlistEntity]:
    """The entities of both collections, each once. An entity on both lists is flagged as both.

    The sanctions collection is the smaller one, so it's held in memory while the PEPs stream.
    """
    sanctioned = {e.entity_id: e for e in _read(sanctions, sanctioned=True, pep=False)}
    for entity in _read(peps, sanctioned=False, pep=True):
        listed = sanctioned.pop(entity.entity_id, None)
        yield entity if listed is None else _merge(listed, entity)
    yield from sanctioned.values()


def _read(path: Path, *, sanctioned: bool, pep: bool) -> Iterator[WatchlistEntity]:
    csv.field_size_limit(FIELD_SIZE_LIMIT)
    with path.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        if missing := REQUIRED_COLUMNS - set(reader.fieldnames or ()):
            raise ValidationFailed(f"{path.name} lacks the columns {sorted(missing)}")
        for row in reader:
            if row["schema"] not in SCREENED_SCHEMAS or not row["name"]:
                continue
            yield WatchlistEntity(
                entity_id=row["id"],
                schema=row["schema"],
                name=row["name"],
                aliases=_values(row["aliases"]),
                birth_dates=_values(row["birth_date"]),
                countries=_values(row["countries"]),
                program_ids=_values(row["program_ids"]),
                datasets=_values(row["dataset"]),
                sanctioned=sanctioned,
                pep=pep,
            )


def _values(field: str) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value for value in field.split(";") if value))


def _merge(listed: WatchlistEntity, pep: WatchlistEntity) -> WatchlistEntity:
    def union(first: tuple[str, ...], second: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(dict.fromkeys(first + second))

    return replace(
        listed,
        aliases=union(listed.aliases, pep.aliases),
        birth_dates=union(listed.birth_dates, pep.birth_dates),
        countries=union(listed.countries, pep.countries),
        program_ids=union(listed.program_ids, pep.program_ids),
        datasets=union(listed.datasets, pep.datasets),
        pep=True,
    )
