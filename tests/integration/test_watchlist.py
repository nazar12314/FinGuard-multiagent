from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from pydantic import SecretStr
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from tests.fakes.watchlist import InMemoryWatchlist
from tests.watchlist_entities import PERSON, entity

from finguard.core.settings import DatabaseSettings, Settings
from finguard.entrypoints.watchlist_import import import_watchlist
from finguard.modules.screening.adapters.postgres import PostgresWatchlist
from finguard.modules.screening.adapters.tables import watchlist_entities, watchlist_names
from finguard.modules.screening.domain.watchlist import (
    NameForms,
    WatchlistEntity,
    WatchlistName,
    name_forms,
)
from finguard.modules.screening.ports import WatchlistRepository

SessionFactory = async_sessionmaker[AsyncSession]


@dataclass
class Subject:
    repository: WatchlistRepository
    entity_ids: Callable[[], Awaitable[set[str]]]
    names_of: Callable[[str], Awaitable[set[str]]]
    rows: Callable[[], Awaitable[tuple[set[tuple[Any, ...]], set[tuple[Any, ...]]]]]


def entity_row(e: WatchlistEntity) -> tuple[Any, ...]:
    return (
        e.entity_id,
        e.schema,
        e.name,
        e.birth_dates,
        e.countries,
        e.program_ids,
        e.datasets,
        e.sanctioned,
        e.pep,
    )


def name_tuple(n: WatchlistName) -> tuple[Any, ...]:
    forms = n.forms
    return (
        n.entity_id,
        n.name,
        n.latin,
        forms.normalised,
        forms.token_sorted,
        forms.phonetic,
        n.is_alias,
    )


def name_row(entity: WatchlistEntity, latin: str) -> WatchlistName:
    return WatchlistName(entity.entity_id, latin, latin, name_forms(latin), is_alias=False)


@pytest.fixture(params=["memory", "postgres"])
async def subject(
    request: pytest.FixtureRequest, session_factory: SessionFactory
) -> AsyncIterator[Subject]:
    # One suite for the fake and the real adapter, so the fake can't drift from Postgres.
    if request.param == "memory":
        fake = InMemoryWatchlist()

        async def fake_ids() -> set[str]:
            return set(fake.entities)

        async def fake_names(entity_id: str) -> set[str]:
            return {n.forms.normalised for n in fake.names if n.entity_id == entity_id}

        async def fake_rows() -> tuple[set[tuple[Any, ...]], set[tuple[Any, ...]]]:
            return (
                {entity_row(e) for e in fake.entities.values()},
                {name_tuple(n) for n in fake.names},
            )

        yield Subject(fake, fake_ids, fake_names, fake_rows)
        return

    async def ids() -> set[str]:
        async with session_factory() as session:
            return set(await session.scalars(select(watchlist_entities.c.entity_id)))

    async def names(entity_id: str) -> set[str]:
        async with session_factory() as session:
            query = select(watchlist_names.c.normalised).where(
                watchlist_names.c.entity_id == entity_id
            )
            return set(await session.scalars(query))

    async def rows() -> tuple[set[tuple[Any, ...]], set[tuple[Any, ...]]]:
        async with session_factory() as session:
            entities = (await session.execute(select(watchlist_entities))).all()
            names = (await session.execute(select(*NAME_COLUMNS))).all()
        return (
            {tuple(tuple(v) if isinstance(v, list) else v for v in row) for row in entities},
            {tuple(row) for row in names},
        )

    yield Subject(PostgresWatchlist(session_factory), ids, names, rows)


NAME_COLUMNS = [column for column in watchlist_names.columns if column.name != "id"]


async def test_every_field_survives_the_round_trip(subject: Subject) -> None:
    listed = WatchlistEntity(
        entity_id="NK-9",
        schema="Person",
        name="Petr Ivanov",
        aliases=(),
        birth_dates=("1965-03-02", "1965"),
        countries=("ru", "by"),
        program_ids=("EU-RUS",),
        datasets=("EU Sanctions", "UN"),
        sanctioned=True,
        pep=False,
    )
    # Every form differs, so two columns swapped in the adapter can't go unnoticed.
    name = WatchlistName(
        "NK-9", "Пётр Иванов", "Petr Ivanov", NameForms("a", "b", "c"), is_alias=True
    )

    async with subject.repository.replace() as writer:
        await writer.add([listed], [name])

    assert await subject.rows() == ({entity_row(listed)}, {name_tuple(name)})


async def test_a_duplicate_entity_or_a_name_without_one_fails_the_replace(
    subject: Subject,
) -> None:
    async def add(entities: list[WatchlistEntity], names: list[WatchlistName]) -> None:
        async with subject.repository.replace() as writer:
            await writer.add(entities, names)

    with pytest.raises((ValueError, asyncpg.PostgresError)):
        await add([PERSON, PERSON], [])
    with pytest.raises((ValueError, asyncpg.PostgresError)):
        await add([], [name_row(entity("NK-404", "Nobody"), "Nobody")])


async def test_replacing_loads_what_was_added(subject: Subject) -> None:
    async with subject.repository.replace() as writer:
        await writer.add([PERSON], [name_row(PERSON, "Petr Ivanov")])
        await writer.add([entity("NK-2", "Anna")], [])

    assert await subject.entity_ids() == {"NK-1", "NK-2"}
    assert await subject.names_of("NK-1") == {"petr ivanov"}


async def test_replacing_drops_the_previous_list(subject: Subject) -> None:
    async with subject.repository.replace() as writer:
        await writer.add([PERSON], [name_row(PERSON, "Petr Ivanov")])
    async with subject.repository.replace() as writer:
        await writer.add([entity("NK-3", "Jan Nowak")], [])

    assert await subject.entity_ids() == {"NK-3"}
    assert await subject.names_of("NK-1") == set()


async def test_a_failed_replace_keeps_the_previous_list(subject: Subject) -> None:
    async with subject.repository.replace() as writer:
        await writer.add([PERSON], [])

    async def import_that_breaks_halfway() -> None:
        async with subject.repository.replace() as writer:
            await writer.add([entity("NK-3", "Jan Nowak")], [])
            raise RuntimeError("source file broke halfway")

    with pytest.raises(RuntimeError):
        await import_that_breaks_halfway()

    assert await subject.entity_ids() == {"NK-1"}


async def test_the_import_command_loads_both_collections(
    tmp_path: Path, database_url: str, session_factory: SessionFactory
) -> None:
    header = '"id","schema","name","aliases","birth_date","countries","program_ids","dataset"\n'
    sanctions = tmp_path / "sanctions.csv"
    sanctions.write_text(header + '"NK-1","Person","Шевченко Юлія","","1980","ua","EU-UKR","EU"\n')
    peps = tmp_path / "peps.csv"
    peps.write_text(header + '"Q-1","Person","Anna Kowalska","","","pl","","Wikidata"\n')
    settings = Settings(env="test", database=DatabaseSettings(url=SecretStr(database_url)))

    imported = await import_watchlist(settings, sanctions, peps)

    assert (imported.entities, imported.sanctioned, imported.peps) == (2, 1, 1)
    async with session_factory() as session:
        spellings = set(
            await session.scalars(
                select(watchlist_names.c.latin).where(watchlist_names.c.entity_id == "NK-1")
            )
        )
    assert "Shevchenko Yuliya" in spellings
