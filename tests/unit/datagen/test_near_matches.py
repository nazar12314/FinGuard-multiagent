import random
from datetime import date
from pathlib import Path

import polars as pl
import pytest
from tests.amlworld_files import write_opensanctions

from datagen.near_matches import (
    NearMatches,
    _Listed,
    _without_a_middle_name,
    plant_near_matches,
    read_listed,
)
from finguard.modules.screening.domain.watchlist import name_forms


@pytest.fixture(scope="module")
def listed(tmp_path_factory: pytest.TempPathFactory) -> pl.DataFrame:
    directory = tmp_path_factory.mktemp("opensanctions")
    write_opensanctions(directory)
    return read_listed(directory)


def persons(count: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "person_id": [f"per-{n:05d}" for n in range(count)],
            "customer_id": [None] * count,
            "full_name": ["Jan Nowak"] * count,
            "full_name_latin": ["Jan Nowak"] * count,
            "birth_date": [date(1990, 6, 1)] * count,
            "nationality": ["PL"] * count,
        },
        schema_overrides={"customer_id": pl.String},
    )


def companies(count: int) -> pl.DataFrame:
    return pl.DataFrame(
        {"customer_id": [f"cus-{n:05d}" for n in range(count)], "legal_form": ["GmbH"] * count}
    )


ACTIVITY_STARTS = date(2022, 9, 1)


def planted(listed: pl.DataFrame, laundering: int, seed: int = 9) -> NearMatches:
    people = {f"per-{n:05d}" for n in range(laundering)}
    return plant_near_matches(persons(5000), companies(2000), people, listed, ACTIVITY_STARTS, seed)


def listed_people(*rows: tuple[str, str, str, str]) -> pl.DataFrame:
    """People as read_listed returns them, from (id, name, birth_date, countries), each on both
    lists so either pool draws them, plus one organisation for company namesakes."""
    people = [
        (entity_id, "Person", name, birth, countries, collection)
        for entity_id, name, birth, countries in rows
        for collection in ["sanctions", "peps"]
    ]
    return pl.DataFrame(
        [*people, ("NK-ORG", "Organization", "Acme Trading", "", "ru", "sanctions")],
        schema=["id", "schema", "name", "birth_date", "countries", "collection"],
        orient="row",
    )


def matched(near: NearMatches, label: str) -> pl.DataFrame:
    return near.labels.filter(pl.col("label") == label).join(
        near.person_changes, left_on="subject_id", right_on="person_id"
    )


def test_reads_listed_people_and_organisations_from_both_collections(
    listed: pl.DataFrame,
) -> None:
    assert dict(listed.select("id", "collection").iter_rows()) == {
        "NK-1": "sanctions",
        "NK-2": "sanctions",
        "NK-3": "sanctions",
        "Q-1": "peps",
    }


def test_reports_missing_opensanctions_files(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="opensanctions"):
        read_listed(tmp_path)


def test_true_matches_cluster_around_laundering(listed: pl.DataFrame) -> None:
    labels = planted(listed, laundering=1000).labels.filter(pl.col("subject_type") == "person")
    true_matches = labels.filter(pl.col("label") == "true_match")
    linked = true_matches.filter(pl.col("subject_id") < "per-01000").height

    assert linked == pytest.approx(100, abs=30)
    assert true_matches.height - linked == pytest.approx(8, abs=8)
    namesakes = labels.filter(pl.col("label") == "false_positive").height
    assert namesakes == pytest.approx(0.02 * 5000, rel=0.3)


def test_a_true_match_carries_the_listed_identity(listed: pl.DataFrame) -> None:
    near = planted(listed, laundering=1000)
    matched = near.labels.filter(pl.col("label") == "true_match").join(
        near.person_changes, left_on="subject_id", right_on="person_id"
    )
    identities = {
        "NK-1": (date(1965, 3, 2), "RU"),
        "Q-1": (date(1980, 5, 1), "PL"),
    }

    for row in matched.rows(named=True):
        birth_date, nationality = row["birth_date"], row["nationality"]
        if row["watchlist_entity_id"] in identities:
            assert (birth_date, nationality) == identities[row["watchlist_entity_id"]]
        else:  # NK-2 lists only a birth year
            assert (birth_date.year, nationality) == (1970, "UA")


def test_a_namesake_is_born_years_apart(listed: pl.DataFrame) -> None:
    near = planted(listed, laundering=0)
    namesakes = near.labels.filter(
        (pl.col("watchlist_entity_id") == "NK-1") & (pl.col("label") == "false_positive")
    ).join(near.person_changes, left_on="subject_id", right_on="person_id")

    assert namesakes.height > 0
    assert all(abs(year - 1965) >= 3 for year in namesakes.get_column("birth_date").dt.year())


def test_planted_names_are_close_variants_of_the_listed_name(listed: pl.DataFrame) -> None:
    labels = planted(listed, laundering=500).labels

    for row in labels.rows(named=True):
        planted_tokens = set(name_forms(row["planted_name"]).normalised.split())
        assert planted_tokens, row


def test_company_namesakes_drop_listed_legal_words_and_keep_their_own(
    listed: pl.DataFrame,
) -> None:
    near = planted(listed, laundering=0)

    assert near.company_changes.height == pytest.approx(20, abs=12)
    assert all(name.endswith(" GmbH") for name in near.company_changes.get_column("name"))
    assert set(near.labels.filter(pl.col("subject_type") == "company").get_column("label")) == {
        "false_positive"
    }


def test_is_deterministic_for_a_seed(listed: pl.DataFrame) -> None:
    assert planted(listed, 100).labels.equals(planted(listed, 100).labels)
    assert not planted(listed, 100).labels.equals(planted(listed, 100, seed=10).labels)


def test_a_year_and_month_birth_date_stays_within_that_month() -> None:
    near = planted(listed_people(("NK-1", "Petr Sidorov", "1970-02", "ru")), laundering=1000)

    births = matched(near, "true_match").get_column("birth_date")
    assert births.len() > 0
    assert {(birth.year, birth.month) for birth in births} == {(1970, 2)}


def test_true_matches_skip_listings_without_a_valid_country_or_a_plausible_age() -> None:
    listed = listed_people(
        ("NK-1", "Petr Sidorov", "1965-03-02", "ru"),
        ("NK-2", "Ivan Petrov", "1960-01-01", "suhh"),
        ("NK-3", "Oleg Popov", "1900-01-01", "ru"),
        ("NK-4", "Pavel Orlov", "2010-01-01", "ru"),
        ("NK-5", "Boris Volkov", "1962-01-01", ""),
    )

    true_matches = matched(planted(listed, laundering=1000), "true_match")

    assert set(true_matches.get_column("watchlist_entity_id")) == {"NK-1"}
    assert set(true_matches.get_column("nationality")) == {"RU"}


def test_a_namesake_without_a_listed_birth_date_has_another_nationality() -> None:
    # Our people are Polish; a Polish listing with no birth date would be indistinguishable.
    listed = listed_people(
        ("NK-1", "Anna Kowalska", "", "pl"),
        ("NK-2", "Anna Kowalski", "", "de"),
        ("NK-3", "Petr Sidorov", "1965-03-02", "ru"),
    )

    namesakes = matched(planted(listed, laundering=0), "false_positive")

    people = namesakes.filter(pl.col("subject_type") == "person")
    assert "NK-2" in set(people.get_column("watchlist_entity_id"))
    assert "NK-1" not in set(people.get_column("watchlist_entity_id"))


def test_a_namesake_is_born_years_apart_from_every_listed_date() -> None:
    listed = listed_people(
        ("NK-1", "Anna Kowalska", "1986;1990-01-01", "de"),
        ("NK-2", "Petr Sidorov", "1965-03-02", "ru"),
    )

    namesakes = matched(planted(listed, laundering=0), "false_positive")
    births = namesakes.filter(pl.col("watchlist_entity_id") == "NK-1").get_column("birth_date")

    assert births.len() > 0
    assert all(abs(birth.year - 1986) >= 4 and abs(birth.year - 1990) >= 4 for birth in births)


def test_titles_and_initials_never_reach_the_planted_name() -> None:
    listed = listed_people(("NK-1", "Dr Anna K. Voss", "1965-03-02", "de"))

    labels = planted(listed, laundering=1000).labels
    names = labels.filter(pl.col("subject_type") == "person").get_column("planted_name")

    assert names.len() > 0
    assert not any({"Dr", "K"} & set(name.split()) for name in names)


def test_variants_are_recorded_only_when_the_name_changed() -> None:
    listed = listed_people(
        ("NK-1", "Li Bo", "1965-03-02", "cn"), ("NK-2", "Anna Ott", "1965-03-02", "de")
    )

    labels = planted(listed, laundering=1000).labels
    for row in labels.filter(pl.col("subject_type") == "person").rows(named=True):
        unchanged = row["planted_name"] == row["listed_name"]
        assert unchanged == (row["variants"] == []), row


def listing(name: str, *, syllabic: bool = False) -> _Listed:
    return _Listed("NK-1", "sanctions", name, [name], [], [], syllabic)


@pytest.mark.parametrize(
    ("tokens", "kept"),
    [
        (["Nazarova", "Kateryna", "Viktorivna"], ["Nazarova", "Kateryna"]),
        (["Petr", "Ivanovich", "Sidorov"], ["Petr", "Sidorov"]),
        (["Anna", "Maria", "Kowalska"], ["Anna", "Kowalska"]),
        (["Abramovich", "Roman", "Arkadyevich"], ["Abramovich", "Roman"]),
        (["Friedrich", "Wilhelm", "Muller"], ["Friedrich", "Muller"]),
        (["Anna", "Kowalska"], None),
    ],
)
def test_a_missing_middle_name_never_takes_the_given_name_or_surname(
    tokens: list[str], kept: list[str] | None
) -> None:
    assert _without_a_middle_name(random.Random(1), listing(" ".join(tokens)), tokens) == kept


def test_syllabic_names_keep_every_syllable() -> None:
    tokens = ["Yu", "Shi", "Yuan"]

    assert (
        _without_a_middle_name(random.Random(1), listing("余世袁", syllabic=True), tokens) is None
    )
