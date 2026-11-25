import hashlib
import json
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from datagen.amlworld import map_bank_slice, read_bank_rows, transactions_csv
from datagen.customers import (
    accounts_csv,
    generate_customers,
    monthly_volumes,
    read_account_holders,
)
from datagen.fx import EUR_PER_UNIT
from datagen.ground_truth import build_ground_truth, patterns_file, read_attempts


@dataclass(frozen=True)
class WorldConfig:
    seed: int
    amlworld_variant: str = "HI-Small"
    our_bank: str = "12"


def world_dir(worlds_dir: Path, seed: int) -> Path:
    return worlds_dir / f"seed-{seed}"


def build_world(config: WorldConfig, raw_dir: Path, worlds_dir: Path) -> Path:
    variant = config.amlworld_variant
    attempt_rows = read_attempts(patterns_file(raw_dir, variant))
    bank_rows = read_bank_rows(transactions_csv(raw_dir, variant), config.our_bank)
    bank_slice = map_bank_slice(bank_rows, config.our_bank)
    truth = build_ground_truth(bank_rows, attempt_rows, config.our_bank)
    holders = read_account_holders(accounts_csv(raw_dir, variant), bank_slice.accounts)
    first_booking: datetime = bank_slice.transactions.get_column("booked_at").min()  # type: ignore[assignment]
    customers = generate_customers(
        holders,
        monthly_volumes(bank_slice.transactions, holders),
        truth.account_labels,
        activity_starts=first_booking.date(),
        seed=config.seed,
    )
    accounts = bank_slice.accounts.join(
        holders.select("account_id", "customer_id"), on="account_id", how="left"
    ).sort("account_id")

    files = {
        "transactions.parquet": bank_slice.transactions,
        "accounts.parquet": accounts,
        "customers.parquet": customers.customers,
        "kyc_profiles.parquet": customers.kyc_profiles,
        "persons.parquet": customers.persons,
        "ground_truth/transaction_labels.parquet": truth.transaction_labels,
        "ground_truth/laundering_attempts.parquet": truth.laundering_attempts,
        "ground_truth/account_labels.parquet": truth.account_labels,
        "ground_truth/customer_labels.parquet": customers.labels,
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
    manifest = world_manifest(config, files)
    (staging / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    if target.exists():
        shutil.rmtree(target)
    staging.rename(target)
    return target


def world_manifest(config: WorldConfig, files: dict[str, pl.DataFrame]) -> dict[str, Any]:
    inputs = {
        **asdict(config),
        "fx_eur_per_unit": {currency: str(rate) for currency, rate in EUR_PER_UNIT.items()},
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
