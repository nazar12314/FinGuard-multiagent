import random
from datetime import date

import polars as pl
import pytest

from datagen.companies import (
    CIRCULAR_OWNERSHIP,
    NOMINEE_DIRECTOR,
    OFFSHORE,
    OFFSHORE_HOLDING,
    STRUCTURE_MIXES,
    Companies,
    generate_companies,
    split_shares,
)

ACTIVITY_STARTS = date(2022, 9, 1)


def companies_for(count: int, laundering: int, seed: int = 5) -> Companies:
    ids = [f"cus-E{n:05d}" for n in range(count)]
    customers = pl.DataFrame(
        {
            "customer_id": ids,
            "customer_type": ["company"] * count,
            "name": [f"Test {n} GmbH" for n in range(count)],
            "country": ["DE"] * count,
            "legal_form": ["GmbH"] * count,
            "onboarded_at": [date(2020, 1, 1)] * count,
        }
    )
    labels = pl.DataFrame(
        {"customer_id": ids, "laundering_linked": [n < laundering for n in range(count)]}
    )
    return generate_companies(customers, labels, ACTIVITY_STARTS, seed)


@pytest.fixture(scope="module")
def generated() -> Companies:
    return companies_for(3000, 600)


def companies_flagged(generated: Companies, flag: str) -> list[str]:
    return (
        generated.labels.filter(pl.col("flags").list.contains(flag))
        .get_column("company_id")
        .to_list()
    )


def test_every_company_is_fully_owned_and_every_customer_has_a_director(
    generated: Companies,
) -> None:
    totals = generated.shareholdings.group_by("company_id").agg(pl.col("share_percent").sum())

    assert set(totals.get_column("share_percent")) == {100}
    assert set(totals.get_column("company_id")) == set(generated.companies.get_column("company_id"))
    customers = generated.companies.filter(pl.col("customer_id").is_not_null())
    assert customers.height == 3000
    assert customers.join(generated.officers, on="company_id", how="anti").is_empty()


def test_every_holder_and_officer_exists(generated: Companies) -> None:
    persons = set(generated.persons.get_column("person_id"))
    companies = set(generated.companies.get_column("company_id"))
    holders = generated.shareholdings

    assert set(holders.filter(pl.col("holder_type") == "person").get_column("holder_id")) <= persons
    assert (
        set(holders.filter(pl.col("holder_type") == "company").get_column("holder_id")) <= companies
    )
    assert set(generated.officers.get_column("person_id")) <= persons


def test_structures_are_label_aware_but_noisy(generated: Companies) -> None:
    odd = dict(
        generated.labels.group_by("laundering_linked")
        .agg((pl.col("structure") != "plain").mean())
        .rows()
    )

    assert odd[True] == pytest.approx(0.5, abs=0.06)
    assert odd[False] == pytest.approx(0.08, abs=0.02)
    mixes = {tuple(flags) for flags in generated.labels.get_column("flags").to_list() if flags}
    assert mixes <= {mix for mix, _ in STRUCTURE_MIXES}


def test_an_offshore_holding_owns_the_company_from_abroad(generated: Companies) -> None:
    offshore = {jurisdiction.code for jurisdiction in OFFSHORE}
    countries = dict(generated.companies.select("company_id", "country").iter_rows())
    holders = generated.shareholdings.filter(pl.col("holder_type") == "company")

    for company_id in companies_flagged(generated, OFFSHORE_HOLDING):
        parents = holders.filter(pl.col("company_id") == company_id).get_column("holder_id")
        assert any(countries[parent] in offshore for parent in parents)


def test_a_nominee_director_sits_on_many_boards(generated: Companies) -> None:
    boards = dict(generated.officers.group_by("person_id").len().iter_rows())
    directors = generated.officers.group_by("company_id").agg("person_id")
    director_of = dict(directors.iter_rows())

    for company_id in companies_flagged(generated, NOMINEE_DIRECTOR):
        assert all(boards[person] >= 5 for person in director_of[company_id])


def test_circular_ownership_leads_back_to_the_company(generated: Companies) -> None:
    parents: dict[str, list[str]] = {}
    for company, holder in generated.shareholdings.select("company_id", "holder_id").iter_rows():
        parents.setdefault(company, []).append(holder)

    def reaches(start: str, target: str, depth: int = 4) -> bool:
        frontier = parents.get(start, [])
        for _ in range(depth):
            if target in frontier:
                return True
            frontier = [p for holder in frontier for p in parents.get(holder, [])]
        return False

    flagged = companies_flagged(generated, CIRCULAR_OWNERSHIP)
    assert flagged
    assert all(reaches(company_id, company_id) for company_id in flagged)


def test_hard_negatives_carry_their_innocent_story(generated: Companies) -> None:
    hard = generated.labels.filter(pl.col("structure") == "hard_negative")

    assert hard.height > 0
    assert hard.get_column("hard_negative_story").null_count() == 0


def test_is_deterministic_for_a_seed() -> None:
    first, again = companies_for(200, 20), companies_for(200, 20)

    assert first.shareholdings.equals(again.shareholdings)
    assert first.persons.equals(again.persons)
    assert not first.persons.equals(companies_for(200, 20, seed=6).persons)


@pytest.mark.parametrize(("total", "parts"), [(100, 1), (100, 3), (70, 2), (20, 4)])
def test_shares_add_up(total: int, parts: int) -> None:
    shares = split_shares(random.Random(1), total, parts)

    assert len(shares) == parts
    assert sum(shares) == total
    assert min(shares) > 0
