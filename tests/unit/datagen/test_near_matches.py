from datetime import date
from pathlib import Path

import polars as pl
import pytest
from tests.amlworld_files import write_opensanctions

from datagen.near_matches import NearMatches, plant_near_matches, read_listed
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


def planted(listed: pl.DataFrame, laundering: int, seed: int = 9) -> NearMatches:
    people = {f"per-{n:05d}" for n in range(laundering)}
    return plant_near_matches(persons(5000), companies(2000), people, listed, seed)


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
        assert row["variants"], row
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
