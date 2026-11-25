import hashlib
import json
import shutil
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from datagen.amlworld import map_bank_slice, read_bank_rows, transactions_csv
from datagen.companies import Companies, generate_companies
from datagen.contacts import generate_contacts, laundering_rings
from datagen.customers import (
    Customers,
    accounts_csv,
    generate_customers,
    monthly_volumes,
    read_account_holders,
)
from datagen.fx import EUR_PER_UNIT
from datagen.ground_truth import build_ground_truth, patterns_file, read_attempts
from datagen.near_matches import opensanctions_files, plant_near_matches, read_listed


@dataclass(frozen=True)
class WorldConfig:
    seed: int
    amlworld_variant: str = "HI-Small"
    our_bank: str = "12"


def world_dir(worlds_dir: Path, seed: int) -> Path:
    return worlds_dir / f"seed-{seed}"


def build_world(config: WorldConfig, raw_dir: Path, worlds_dir: Path) -> Path:
    """Build the world from raw_dir/amlworld and raw_dir/opensanctions."""
    variant = config.amlworld_variant
    amlworld = raw_dir / "amlworld"
    opensanctions = raw_dir / "opensanctions"
    listed = read_listed(opensanctions)
    attempt_rows = read_attempts(patterns_file(amlworld, variant))
    bank_rows = read_bank_rows(transactions_csv(amlworld, variant), config.our_bank)
    bank_slice = map_bank_slice(bank_rows, config.our_bank)
    truth = build_ground_truth(bank_rows, attempt_rows, config.our_bank)
    holders = read_account_holders(accounts_csv(amlworld, variant), bank_slice.accounts)
    first_booking: datetime = bank_slice.transactions.get_column("booked_at").min()  # type: ignore[assignment]
    customers = generate_customers(
        holders,
        monthly_volumes(bank_slice.transactions, holders),
        truth.account_labels,
        activity_starts=first_booking.date(),
        seed=config.seed,
    )
    contacts = generate_contacts(
        customers.customers, laundering_rings(holders, truth.account_labels), config.seed
    )
    companies = generate_companies(
        customers.customers, customers.labels, first_booking.date(), config.seed
    )
    near_matches = plant_near_matches(
        pl.concat([customers.persons, companies.persons]),
        customers.customers.filter(pl.col("customer_type") == "company"),
        laundering_people(customers, companies),
        listed,
        config.seed,
    )
    persons = (
        pl.concat([customers.persons, companies.persons])
        .update(near_matches.person_changes, on="person_id")
        .sort("person_id")
    )
    customer_names = _renamed_customers(customers, persons, near_matches.company_changes)
    accounts = bank_slice.accounts.join(
        holders.select("account_id", "customer_id"), on="account_id", how="left"
    ).sort("account_id")

    files = {
        "transactions.parquet": bank_slice.transactions,
        "accounts.parquet": accounts,
        "customers.parquet": customers.customers.update(customer_names, on="customer_id"),
        "kyc_profiles.parquet": customers.kyc_profiles,
        "persons.parquet": persons,
        "companies.parquet": companies.companies.update(customer_names, on="customer_id"),
        "officers.parquet": companies.officers,
        "shareholdings.parquet": companies.shareholdings,
        "addresses.parquet": contacts.addresses,
        "customer_addresses.parquet": contacts.customer_addresses,
        "devices.parquet": contacts.devices,
        "customer_devices.parquet": contacts.customer_devices,
        "ground_truth/transaction_labels.parquet": truth.transaction_labels,
        "ground_truth/laundering_attempts.parquet": truth.laundering_attempts,
        "ground_truth/account_labels.parquet": truth.account_labels,
        "ground_truth/customer_labels.parquet": customers.labels,
        "ground_truth/shared_attribute_labels.parquet": contacts.labels,
        "ground_truth/company_labels.parquet": companies.labels,
        "ground_truth/screening_labels.parquet": near_matches.labels,
    }

    # Build next to the target and swap it in at the end, so a failed build never leaves a
    # half-written world that looks complete.
    target = world_dir(worlds_dir, config.seed)
    staging = worlds_dir / f".{target.name}.building"
    if staging.exists():
        shutil.rmtree(staging)
    (staging / "ground_truth").mkdir(parents=True)
    for name, frame in files.items():
        frame.write_parquet(staging / name)
    manifest = world_manifest(config, files, opensanctions_files(opensanctions).values())
    (staging / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    if target.exists():
        shutil.rmtree(target)
    staging.rename(target)
    return target


def laundering_people(customers: Customers, companies: Companies) -> set[str]:
    """Sole traders, directors and direct owners of laundering-linked customers."""
    linked = customers.labels.filter("laundering_linked").get_column("customer_id").implode()
    company_ids = (
        companies.companies.filter(pl.col("customer_id").is_in(linked))
        .get_column("company_id")
        .implode()
    )
    sole_traders = customers.persons.filter(pl.col("customer_id").is_in(linked))
    directors = companies.officers.filter(pl.col("company_id").is_in(company_ids))
    owners = companies.shareholdings.filter(
        pl.col("company_id").is_in(company_ids) & (pl.col("holder_type") == "person")
    )
    return (
        set(sole_traders.get_column("person_id"))
        | set(directors.get_column("person_id"))
        | set(owners.get_column("holder_id"))
    )


def _renamed_customers(
    customers: Customers, persons: pl.DataFrame, company_changes: pl.DataFrame
) -> pl.DataFrame:
    # A sole trader trades under the owner's name, so a renamed owner renames the business.
    sole_traders = (
        customers.persons.select("person_id", "customer_id", old="full_name_latin")
        .join(persons.select("person_id", new="full_name_latin"), on="person_id")
        .filter(pl.col("old") != pl.col("new"))
        .join(customers.customers.select("customer_id", "name"), on="customer_id")
        .select(
            "customer_id",
            name=pl.col("name").str.replace(pl.col("old"), pl.col("new"), literal=True),
        )
    )
    return pl.concat([sole_traders, company_changes.select("customer_id", "name")]).sort(
        "customer_id"
    )


def world_manifest(
    config: WorldConfig, files: dict[str, pl.DataFrame], sources: Iterable[Path]
) -> dict[str, Any]:
    inputs = {
        **asdict(config),
        "fx_eur_per_unit": {currency: str(rate) for currency, rate in EUR_PER_UNIT.items()},
        # OpenSanctions publishes daily, so the world records which files it was built from.
        "opensanctions_sha256": {path.name: _sha256(path) for path in sources},
    }
    canonical = json.dumps(inputs, sort_keys=True, separators=(",", ":"))
    accounts = files["accounts.parquet"]
    labels = files["ground_truth/transaction_labels.parquet"]
    customer_labels = files["ground_truth/customer_labels.parquet"]
    return {
        **inputs,
        "config_hash": hashlib.sha256(canonical.encode()).hexdigest(),
        "counts": {
            "transactions": files["transactions.parquet"].height,
            "accounts": accounts.height,
            "accounts_at_our_bank": int(accounts.get_column("held_at_our_bank").sum()),
            "laundering_transactions": int(labels.get_column("is_laundering").sum()),
            "laundering_transactions_in_attempts": labels.get_column("attempt_id").count(),
            "laundering_attempts": files["ground_truth/laundering_attempts.parquet"].height,
            "customers": customer_labels.height,
            "laundering_linked_customers": int(
                customer_labels.get_column("laundering_linked").sum()
            ),
        },
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()
