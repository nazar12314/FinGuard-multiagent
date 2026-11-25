# ruff: noqa: RUF001  (Cyrillic names on purpose)
from tests.fakes.watchlist import InMemoryWatchlist
from tests.watchlist_entities import PERSON, entity

from finguard.modules.screening.service import WatchlistImport


class TwoSpellings:
    """Spells every non-ASCII name two ways, like the ICU adapter does for Cyrillic."""

    def latin_spellings(self, text: str) -> list[str]:
        if text.isascii():
            return [text]
        return ["Sevcenko", "Shevchenko"]


async def test_replaces_the_watchlist_with_every_entity() -> None:
    repository = InMemoryWatchlist()
    watchlist = WatchlistImport(repository, TwoSpellings())

    imported = await watchlist.replace_watchlist(
        [PERSON, entity("NK-2", "Шевченко", sanctioned=False, pep=True)]
    )

    assert set(repository.entities) == {"NK-1", "NK-2"}
    assert (imported.entities, imported.sanctioned, imported.peps) == (2, 1, 1)
    assert imported.names == len(repository.names)


async def test_drops_entities_from_the_previous_import() -> None:
    repository = InMemoryWatchlist()
    watchlist = WatchlistImport(repository, TwoSpellings())
    await watchlist.replace_watchlist([PERSON, entity("NK-2", "Anna Kowalska")])

    await watchlist.replace_watchlist([entity("NK-3", "Jan Nowak")])

    assert set(repository.entities) == {"NK-3"}


def test_keeps_each_distinct_latin_form_of_every_name_once() -> None:
    watchlist = WatchlistImport(InMemoryWatchlist(), TwoSpellings())

    names = watchlist.searchable_names(
        entity("NK-2", "Шевченко", aliases=("Shevchenko", "SHEVCHENKO", "Тарас"))
    )

    assert [(n.name, n.latin, n.is_alias) for n in names] == [
        ("Шевченко", "Sevcenko", False),
        ("Шевченко", "Shevchenko", False),
    ]


def test_skips_a_name_with_nothing_to_search_on() -> None:
    watchlist = WatchlistImport(InMemoryWatchlist(), TwoSpellings())

    assert [n.latin for n in watchlist.searchable_names(entity("NK-4", "--", aliases=("Ok",)))] == [
        "Ok"
    ]
