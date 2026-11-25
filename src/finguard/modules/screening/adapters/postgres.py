from collections.abc import AsyncIterator, Sequence
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
        await self._connection.copy_records_to_table(
            watchlist_entities.name,
            schema_name=SCREENING_SCHEMA,
            columns=ENTITY_COLUMNS,
            records=[
                (
                    e.entity_id,
                    e.schema,
                    e.name,
                    list(e.birth_dates),
                    list(e.countries),
                    list(e.program_ids),
                    list(e.datasets),
                    e.sanctioned,
                    e.pep,
                )
                for e in entities
            ],
        )
        await self._connection.copy_records_to_table(
            watchlist_names.name,
            schema_name=SCREENING_SCHEMA,
            columns=NAME_COLUMNS,
            records=[
                (
                    n.entity_id,
                    n.name,
                    n.latin,
                    n.forms.normalised,
                    n.forms.token_sorted,
                    n.forms.phonetic,
                    n.is_alias,
                )
                for n in names
            ],
        )
