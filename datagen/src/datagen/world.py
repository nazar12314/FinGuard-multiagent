import hashlib
import json
import shutil
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from datagen.amlworld import canonical_bank_id, map_bank_slice, read_bank_rows, transactions_csv
from datagen.companies import Companies, generate_companies
from datagen.contacts import generate_contacts, laundering_rings
from datagen.customers import (
    Customers,
    accounts_csv,
    generate_customers,
    monthly_volumes,
    read_account_holders,
)
from datagen.documents.content import document_specs, sample_customers
from datagen.documents.render import write_documents
from datagen.fx import EUR_PER_UNIT
from datagen.ground_truth import build_ground_truth, patterns_file, read_attempts
from datagen.media import assign_media
from datagen.media_corpus import CORPUS, read_corpus
from datagen.near_matches import opensanctions_files, plant_near_matches, read_listed
from datagen.people import customers_of_people, laundering_linked_people
from datagen.quality import DataQualityError, quality_report, write_quality_report


@dataclass(frozen=True)
class WorldConfig:
    seed: int
    amlworld_variant: str = "HI-Small"
    our_bank: str = "12"
    # Benign customers who get a document pack, besides every laundering-linked one.
    document_sample: int = 300
    documents: bool = True

    def __post_init__(self) -> None:
        # "012" and "12" are one bank, and must give one config hash.
        object.__setattr__(self, "our_bank", canonical_bank_id(self.our_bank))


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
    rings = laundering_rings(holders, truth.account_labels)
    customers = generate_customers(
        holders,
        monthly_volumes(bank_slice.transactions, holders),
        truth.account_labels,
        activity_starts=first_booking.date(),
        seed=config.seed,
        rings=rings,
    )
    contacts = generate_contacts(customers.customers, rings, config.seed)
    companies = generate_companies(
        customers.customers, customers.labels, first_booking.date(), config.seed
    )
    near_matches = plant_near_matches(
        pl.concat([customers.persons, companies.persons]),
        customers.customers.filter(pl.col("customer_type") == "company"),
        laundering_people(customers, companies),
        listed,
        first_booking.date(),
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
    (staging / "media").mkdir()
    media = assign_media(files, read_corpus(), first_booking.date(), config.seed)
    files["media/articles.parquet"] = media.articles
    files["ground_truth/media_labels.parquet"] = media.labels
    sample = (
        sample_customers(customers.labels, companies.labels, config.document_sample, config.seed)
        if config.documents
        else []
    )
    specs = document_specs(files, sample, config.seed)
    files["documents.parquet"] = write_documents(
        specs, staging / "documents", staging / "ground_truth" / "documents"
    )
    files["ground_truth/document_labels.parquet"] = pl.DataFrame(
        {
            "document_id": [spec.document_id for spec in specs],
            "anomalies": [[a["type"] for a in spec.anomalies] for spec in specs],
            "injection": [
                spec.injection["technique"] if spec.injection else None for spec in specs
            ],
            "injection_goal": [
                spec.injection["goal"] if spec.injection else None for spec in specs
            ],
            "scanned": [spec.scan is not None for spec in specs],
        },
        schema={
            "document_id": pl.String,
            "anomalies": pl.List(pl.String),
            "injection": pl.String,
            "injection_goal": pl.String,
            "scanned": pl.Boolean,
        },
    ).sort("document_id")
    for name, frame in files.items():
        frame.write_parquet(staging / name)
    manifest = world_manifest(
        config,
        files,
        {
            "opensanctions": opensanctions_files(opensanctions).values(),
            "amlworld": [
                transactions_csv(amlworld, variant),
                patterns_file(amlworld, variant),
                accounts_csv(amlworld, variant),
            ],
            "adverse_media": [CORPUS] if CORPUS.is_file() else [],
        },
    )
    (staging / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    report = quality_report(staging, files)
    write_quality_report(report, staging)
    if failed := report.failed_checks:
        raise DataQualityError(
            f"{len(failed)} data-quality checks failed; see {staging / 'quality_report.md'}"
        )

    # Two renames, so an interrupted swap leaves either world whole, never a half-deleted one.
    previous = worlds_dir / f".{target.name}.previous"
    if previous.exists():
        shutil.rmtree(previous)
    if target.exists():
        target.rename(previous)
    staging.rename(target)
    shutil.rmtree(previous, ignore_errors=True)
    return target


def laundering_people(customers: Customers, companies: Companies) -> set[str]:
    people = customers_of_people(
        pl.concat([customers.persons, companies.persons]),
        companies.companies,
        companies.officers,
        companies.shareholdings,
    )
    return laundering_linked_people(people, customers.labels)


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
    config: WorldConfig, files: dict[str, pl.DataFrame], sources: dict[str, Iterable[Path]]
) -> dict[str, Any]:
    inputs = {
        **asdict(config),
        "fx_eur_per_unit": {currency: str(rate) for currency, rate in EUR_PER_UNIT.items()},
        # A world depends on these files' exact contents (transaction ids are CSV row numbers,
        # and OpenSanctions publishes daily), so they're part of its config hash.
        **{
            f"{source}_sha256": {path.name: _sha256(path) for path in paths}
            for source, paths in sources.items()
        },
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
            "documents": files["documents.parquet"].height,
            "media_articles": files["media/articles.parquet"].height,
        },
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()
