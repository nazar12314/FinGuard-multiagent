from collections.abc import AsyncIterator, Iterable, Sequence
from contextlib import asynccontextmanager
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from finguard.modules.screening.adapters.tables import (
    SCREENING_SCHEMA,
    watchlist_entities,
    watchlist_names,
)
from finguard.modules.screening.domain.watchlist import WatchlistEntity, WatchlistName
from finguard.modules.screening.ports import WatchlistWriter

ENTITY_COLUMNS = [column.name for column in watchlist_entities.columns]
NAME_COLUMNS = [column.name for column in watchlist_names.columns if column.name != "id"]


class PostgresWatchlist:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    @asynccontextmanager
    async def replace(self) -> AsyncIterator[WatchlistWriter]:
        # One transaction, so a failed import leaves the old list in place. TRUNCATE locks both
        # tables until the commit: screening waits during an import rather than seeing a
        # half-loaded list.
        async with self._session_factory.begin() as session:
            await session.execute(
                text(
                    f"TRUNCATE {SCREENING_SCHEMA}.{watchlist_names.name}, "
                    f"{SCREENING_SCHEMA}.{watchlist_entities.name}"
                )
            )
            connection = await session.connection()
            raw = await connection.get_raw_connection()
            yield _CopyWriter(raw.driver_connection)


class _CopyWriter:
    def __init__(self, connection: Any) -> None:
        # The asyncpg connection under the session, inside its transaction; COPY loads the
        # ~2M name rows far faster than INSERTs.
        self._connection = connection

    async def add(
        self, entities: Sequence[WatchlistEntity], names: Sequence[WatchlistName]
    ) -> None:
        await self._copy(watchlist_entities.name, ENTITY_COLUMNS, map(_entity_row, entities))
        await self._copy(watchlist_names.name, NAME_COLUMNS, map(_name_row, names))

    async def _copy(
        self, table: str, columns: list[str], rows: Iterable[dict[str, object]]
    ) -> None:
        # Rows are dicts and records follow the table's columns, so the two can't drift apart.
        await self._connection.copy_records_to_table(
            table,
            schema_name=SCREENING_SCHEMA,
            columns=columns,
            records=[tuple(row[column] for column in columns) for row in rows],
        )


def _entity_row(entity: WatchlistEntity) -> dict[str, object]:
    return {
        "entity_id": entity.entity_id,
        "entity_schema": entity.schema,
        "name": entity.name,
        "birth_dates": list(entity.birth_dates),
        "countries": list(entity.countries),
        "program_ids": list(entity.program_ids),
        "datasets": list(entity.datasets),
        "sanctioned": entity.sanctioned,
        "pep": entity.pep,
    }


def _name_row(name: WatchlistName) -> dict[str, object]:
    return {
        "entity_id": name.entity_id,
        "name": name.name,
        "latin": name.latin,
        "normalised": name.forms.normalised,
        "token_sorted": name.forms.token_sorted,
        "phonetic": name.forms.phonetic,
        "is_alias": name.is_alias,
    }
