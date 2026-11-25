from datetime import date

import polars as pl
import pytest

from datagen.contacts import Contacts, generate_contacts, laundering_rings


def customers(count: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "customer_id": [f"cus-{n:04d}" for n in range(count)],
            "customer_type": [("company", "sole_trader")[n % 2] for n in range(count)],
            "country": [("DE", "PL")[n % 3 == 0] for n in range(count)],
            "onboarded_at": [date(2020, 1, 1)] * count,
        }
    )


def users_of(contacts: Contacts) -> pl.DataFrame:
    return pl.concat(
        [
            contacts.customer_addresses.select("customer_id", attribute_id="address_id"),
            contacts.customer_devices.select("customer_id", "device_id").rename(
                {"device_id": "attribute_id"}
            ),
        ]
    ).unique()


def test_groups_our_customers_by_the_attempt_they_share() -> None:
    holders = pl.DataFrame(
        {"account_id": ["12:A", "12:B", "12:C", "12:D"], "customer_id": ["c1", "c2", "c3", "c3"]}
    )
    account_labels = pl.DataFrame(
        {
            "account_id": ["12:A", "12:B", "12:C", "12:D"],
            "attempt_ids": [["attempt-1"], ["attempt-1", "attempt-2"], ["attempt-2"], []],
        }
    )

    assert laundering_rings(holders, account_labels) == [["c1", "c2"], ["c2", "c3"]]


def test_every_customer_has_an_address_and_a_device() -> None:
    contacts = generate_contacts(customers(60), [], seed=3)

    roles = contacts.customer_addresses.group_by("customer_id").agg(pl.col("role").sort())
    for customer_id, customer_roles in roles.rows():
        sole_trader = int(customer_id[-4:]) % 2 == 1
        assert customer_roles == (["owner_home", "registered"] if sole_trader else ["registered"])
    assert contacts.customer_devices.get_column("customer_id").n_unique() == 60
    linked = set(contacts.customer_addresses.get_column("address_id"))
    assert linked == set(contacts.addresses.get_column("address_id"))


def test_every_shared_address_or_device_is_labelled_with_why() -> None:
    contacts = generate_contacts(customers(1500), [["cus-0001", "cus-0002"]] * 20, seed=3)

    shared = users_of(contacts).group_by("attribute_id").len().filter(pl.col("len") > 1)

    assert set(shared.get_column("attribute_id")) == set(contacts.labels.get_column("attribute_id"))
    assert set(contacts.labels.get_column("reason")) == {
        "ring",
        "virtual_office",
        "family",
        "bookkeeper",
    }


def test_rings_often_but_not_always_share_a_device() -> None:
    rings = [[f"cus-{2 * n:04d}", f"cus-{2 * n + 1:04d}"] for n in range(200)]

    contacts = generate_contacts(customers(400), rings, seed=3)

    ring_devices = contacts.labels.filter(
        (pl.col("attribute") == "device") & (pl.col("reason") == "ring")
    )
    assert ring_devices.height / len(rings) == pytest.approx(0.6, abs=0.1)


def test_is_deterministic_for_a_seed() -> None:
    first = generate_contacts(customers(200), [["cus-0001", "cus-0003"]], seed=3)
    again = generate_contacts(customers(200), [["cus-0001", "cus-0003"]], seed=3)

    assert first.addresses.equals(again.addresses)
    assert first.customer_devices.equals(again.customer_devices)
    assert first.labels.equals(again.labels)
