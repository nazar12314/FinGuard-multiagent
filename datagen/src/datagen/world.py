import hashlib
import json
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from datagen.amlworld import BankSlice, map_bank_slice, read_bank_rows, transactions_csv
from datagen.fx import EUR_PER_UNIT
from datagen.ground_truth import GroundTruth, build_ground_truth, patterns_file, read_attempts


@dataclass(frozen=True)
class WorldConfig:
    seed: int
    amlworld_variant: str = "HI-Small"
    our_bank: str = "012"


def world_dir(worlds_dir: Path, seed: int) -> Path:
    return worlds_dir / f"seed-{seed}"


def build_world(config: WorldConfig, raw_dir: Path, worlds_dir: Path) -> Path:
    attempt_rows = read_attempts(patterns_file(raw_dir, config.amlworld_variant))
    bank_rows = read_bank_rows(transactions_csv(raw_dir, config.amlworld_variant), config.our_bank)
    bank_slice = map_bank_slice(bank_rows, config.our_bank)
    truth = build_ground_truth(bank_rows, attempt_rows, config.our_bank)

    # Build next to the target and swap it in at the end, so a failed build never leaves a
    # half-written world that looks complete.
    target = world_dir(worlds_dir, config.seed)
    staging = worlds_dir / f".{target.name}.building"
    if staging.exists():
        shutil.rmtree(staging)
    (staging / "ground_truth").mkdir(parents=True)

    bank_slice.transactions.write_parquet(staging / "transactions.parquet")
    bank_slice.accounts.write_parquet(staging / "accounts.parquet")
    truth.transaction_labels.write_parquet(staging / "ground_truth" / "transaction_labels.parquet")
    truth.laundering_attempts.write_parquet(
        staging / "ground_truth" / "laundering_attempts.parquet"
    )
    truth.account_labels.write_parquet(staging / "ground_truth" / "account_labels.parquet")
    manifest = world_manifest(config, bank_slice, truth)
    (staging / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    if target.exists():
        shutil.rmtree(target)
    staging.rename(target)
    return target


def world_manifest(
    config: WorldConfig, bank_slice: BankSlice, truth: GroundTruth
) -> dict[str, Any]:
    inputs = {
        **asdict(config),
        "fx_eur_per_unit": {currency: str(rate) for currency, rate in EUR_PER_UNIT.items()},
    }
    canonical = json.dumps(inputs, sort_keys=True, separators=(",", ":"))
    labels = truth.transaction_labels
    return {
        **inputs,
        "config_hash": hashlib.sha256(canonical.encode()).hexdigest(),
        "counts": {
            "transactions": bank_slice.transactions.height,
            "accounts": bank_slice.accounts.height,
            "accounts_at_our_bank": int(bank_slice.accounts.get_column("held_at_our_bank").sum()),
            "laundering_transactions": int(labels.get_column("is_laundering").sum()),
            "laundering_transactions_in_attempts": labels.get_column("attempt_id").count(),
            "laundering_attempts": truth.laundering_attempts.height,
        },
    }
