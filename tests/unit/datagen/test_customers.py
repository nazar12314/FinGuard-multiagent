import os
import subprocess
import sys
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest
from tests.amlworld_files import OUR_BANK, csv_row, write_accounts_csv, write_transactions_csv

from datagen.amlworld import AmlworldFormatError, map_bank_slice, read_bank_rows
from datagen.customers import (
    ANOMALY_MIXES,
    VOLUME_BELOW_ACTIVITY,
    Customers,
    Industry,
    generate_customers,
    monthly_volumes,
    read_account_holders,
    risk_rating,
)

ACTIVITY_STARTS = date(2022, 9, 1)


def our_accounts(tmp_path: Path, rows: list[str]) -> pl.DataFrame:
    csv = write_transactions_csv(tmp_path, rows)
    return map_bank_slice(read_bank_rows(csv, OUR_BANK), OUR_BANK).accounts


def test_finds_the_customer_behind_each_of_our_accounts(tmp_path: Path) -> None:
    accounts = our_accounts(
        tmp_path,
        [csv_row(OUR_BANK, "C1", "002", "B1"), csv_row(OUR_BANK, "C2", OUR_BANK, "C3")],
    )
    entities = write_accounts_csv(
        tmp_path,
        [
            ("12", "C1", "E1", "Sole Proprietorship #1"),
            ("12", "C2", "E2", "Partnership #2"),
            ("12", "C3", "E2", "Partnership #2"),
            ("2", "B1", "E3", "Corporation #3"),
        ],
    )

    holders = read_account_holders(entities, accounts)

    assert holders.select("account_id", "customer_id", "customer_type").rows() == [
        ("12:C1", "cus-E1", "sole_trader"),
        ("12:C2", "cus-E2", "company"),
        ("12:C3", "cus-E2", "company"),
    ]


def test_rejects_an_account_without_an_entity(tmp_path: Path) -> None:
    accounts = our_accounts(tmp_path, [csv_row(OUR_BANK, "C1", "002", "B1")])
    entities = write_accounts_csv(tmp_path, [("2", "B1", "E3", "Corporation #3")])

    with pytest.raises(AmlworldFormatError, match="1 of our accounts have no entity"):
        read_account_holders(entities, accounts)


def test_rejects_an_unknown_entity_type(tmp_path: Path) -> None:
    accounts = our_accounts(tmp_path, [csv_row(OUR_BANK, "C1", "002", "B1")])
    entities = write_accounts_csv(tmp_path, [("12", "C1", "E1", "Trust #1")])

    with pytest.raises(AmlworldFormatError, match="Trust"):
        read_account_holders(entities, accounts)


def test_monthly_volume_leaves_out_moves_between_own_accounts() -> None:
    holders = pl.DataFrame({"account_id": ["12:A", "12:B"], "customer_id": ["cus-1", "cus-1"]})
    transactions = pl.DataFrame(
        {
            "booked_at": [datetime(2022, 9, 1, tzinfo=UTC), datetime(2022, 10, 1, tzinfo=UTC)],
            "from_account_id": ["12:A", "12:A"],
            "to_account_id": ["2:X", "12:B"],
            "amount_paid_eur": [600.0, 1_000_000.0],
            "amount_received_eur": [600.0, 1_000_000.0],
        }
    )

    volumes = monthly_volumes(transactions, holders)

    # 600 out over a 30-day span, averaged over money in and out.
    assert volumes.rows() == [("cus-1", 300.0)]


def customers_for(
    count: int, laundering: int, seed: int = 7, rings: list[list[str]] | None = None
) -> Customers:
    ids = [f"E{number}" for number in range(count)]
    holders = pl.DataFrame(
        {
            "account_id": [f"12:{entity}" for entity in ids],
            "entity_id": ids,
            "entity_type": [
                "Sole Proprietorship" if n % 2 else "Partnership" for n in range(count)
            ],
            "customer_id": [f"cus-{entity}" for entity in ids],
            "customer_type": ["sole_trader" if n % 2 else "company" for n in range(count)],
        }
    )
    volumes = pl.DataFrame(
        {
            "customer_id": [f"cus-{entity}" for entity in ids],
            "monthly_volume_eur": [500_000.0] * count,
        }
    )
    account_labels = pl.DataFrame(
        {
            "account_id": [f"12:{entity}" for entity in ids],
            "laundering_transaction_count": [1] * laundering + [0] * (count - laundering),
        }
    )
    return generate_customers(holders, volumes, account_labels, ACTIVITY_STARTS, seed, rings or [])


def test_is_deterministic_for_a_seed() -> None:
    first, again, other = customers_for(50, 5), customers_for(50, 5), customers_for(50, 5, seed=8)

    assert first.customers.equals(again.customers)
    assert first.persons.equals(again.persons)
    assert not first.customers.equals(other.customers)


def test_every_customer_gets_a_profile_and_sole_traders_a_person() -> None:
    generated = customers_for(40, 4)

    assert generated.customers.height == generated.kyc_profiles.height == 40
    sole_traders = generated.customers.filter(pl.col("customer_type") == "sole_trader")
    assert set(generated.persons.get_column("customer_id")) == set(
        sole_traders.get_column("customer_id")
    )


def test_partnerships_get_a_partnership_legal_form() -> None:
    companies = customers_for(40, 0).customers.filter(pl.col("customer_type") == "company")

    forms = {"GbR", "sp.j.", "VOF", "SNC", "Partnership", "S.C."}
    assert all(name.split()[-1] in forms for name in companies.get_column("name"))


def test_romanises_ukrainian_names() -> None:
    persons = customers_for(400, 0).persons.filter(pl.col("nationality") == "UA")

    assert persons.height > 0
    assert all(name.isascii() for name in persons.get_column("full_name_latin"))
    assert not any(name.isascii() for name in persons.get_column("full_name"))


def test_profiles_are_label_aware_but_noisy() -> None:
    labels = customers_for(4000, 1000).labels
    by_label = labels.group_by("laundering_linked").agg(
        odd=(pl.col("profile") != "consistent").mean()
    )
    odd = dict(by_label.rows())

    assert odd[True] == pytest.approx(0.6, abs=0.05)
    assert odd[False] == pytest.approx(0.1, abs=0.02)
    # Laundering and innocent oddities come from the same mix of anomalies.
    mixes = {
        tuple(anomalies) for anomalies in labels.get_column("anomalies").to_list() if anomalies
    }
    assert mixes <= {mix for mix, _ in ANOMALY_MIXES}
    assert (
        labels.filter(pl.col("profile") == "hard_negative")
        .get_column("hard_negative_story")
        .null_count()
        == 0
    )


def test_a_volume_anomaly_declares_far_less_than_the_account_moves() -> None:
    generated = customers_for(2000, 1000)
    declared = generated.kyc_profiles.join(generated.labels, on="customer_id").with_columns(
        below=pl.col("anomalies").list.contains(VOLUME_BELOW_ACTIVITY)
    )

    for below, ceiling in [(True, 0.26), (False, 3.0)]:
        ratios = (
            declared.filter(pl.col("below") == below).get_column("expected_monthly_volume_eur")
            / 500_000
        )
        assert ratios.max() <= ceiling  # type: ignore[operator]
    assert declared.filter(~pl.col("below")).get_column(
        "expected_monthly_volume_eur"
    ).median() == pytest.approx(500_000, rel=0.1)


def test_customers_onboard_before_their_activity_starts() -> None:
    onboarded = customers_for(500, 100).customers.get_column("onboarded_at")

    assert onboarded.max() < ACTIVITY_STARTS  # type: ignore[operator]


@pytest.mark.parametrize(
    ("industry_risk", "onboarded_at", "expected_monthly", "purpose", "rating"),
    [
        (0, date(2018, 1, 1), 10_000, "paying suppliers", "low"),
        (2, date(2018, 1, 1), 10_000, "paying suppliers", "medium"),
        (1, date(2022, 8, 1), 2_000_000, "paying suppliers", "high"),
        (0, date(2022, 8, 1), 10_000, "general purposes", "medium"),
    ],
)
def test_risk_rating_follows_the_onboarding_rule(
    industry_risk: int, onboarded_at: date, expected_monthly: int, purpose: str, rating: str
) -> None:
    industry = Industry("test", "Test", industry_risk)

    assert risk_rating(industry, onboarded_at, ACTIVITY_STARTS, expected_monthly, purpose) == rating


def test_seeded_fakers_draw_the_same_cities_whatever_the_hash_seed() -> None:
    program = (
        "from datagen.customers import locale_fakers\n"
        "fakers = locale_fakers(7, 'test')\n"
        "print([faker.city() for faker in fakers.values() for _ in range(5)])"
    )

    def cities(hash_seed: str) -> str:
        environment = {**os.environ, "PYTHONHASHSEED": hash_seed}
        command = [sys.executable, "-c", program]
        return subprocess.run(
            command, env=environment, capture_output=True, check=True
        ).stdout.decode()

    assert cities("1") == cities("2")


def test_most_members_of_a_ring_are_registered_in_its_home_country() -> None:
    rings = [[f"cus-E{n}" for n in range(start, start + 5)] for start in range(0, 500, 5)]

    countries = dict(
        customers_for(500, 500, rings=rings).customers.select("customer_id", "country").rows()
    )

    at_home = [
        max(Counter(countries[member] for member in ring).values()) / len(ring) for ring in rings
    ]
    assert sum(at_home) / len(at_home) == pytest.approx(0.8, abs=0.1)


def test_rings_leave_other_customers_in_their_countries() -> None:
    without = customers_for(50, 5).customers
    with_ring = customers_for(50, 5, rings=[["cus-E0", "cus-E1"]]).customers

    others = pl.col("customer_id").is_in(["cus-E0", "cus-E1"]).not_()
    columns = ["customer_id", "country"]
    assert without.filter(others).select(columns).equals(with_ring.filter(others).select(columns))
