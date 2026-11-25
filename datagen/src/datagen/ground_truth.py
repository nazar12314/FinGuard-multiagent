import re
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from datagen.amlworld import CSV_SCHEMA, AmlworldFormatError, touches_bank

PATTERN_TYPES = {
    "BIPARTITE": "bipartite",
    "CYCLE": "cycle",
    "FAN-IN": "fan_in",
    "FAN-OUT": "fan_out",
    "GATHER-SCATTER": "gather_scatter",
    "RANDOM": "random",
    "SCATTER-GATHER": "scatter_gather",
    "STACK": "stack",
}

# Bipartite, scatter-gather and stack attempts have no description after the pattern name.
BEGIN_ATTEMPT = re.compile(
    r"BEGIN LAUNDERING ATTEMPT - (?P<pattern>[A-Z-]+)(?::\s*(?P<description>.*))?"
)
END_ATTEMPT = re.compile(r"END LAUNDERING ATTEMPT - (?P<pattern>[A-Z-]+)")

ATTEMPT_ROW_SCHEMA = {
    "attempt_id": pl.String,
    "pattern_type": pl.String,
    "description": pl.String,
    **CSV_SCHEMA,
}


@dataclass(frozen=True)
class GroundTruth:
    transaction_labels: pl.DataFrame
    laundering_attempts: pl.DataFrame
    account_labels: pl.DataFrame


def patterns_file(raw_dir: Path, variant: str) -> Path:
    return raw_dir / f"{variant}_Patterns.txt"


def read_attempts(path: Path) -> pl.DataFrame:
    """One row per transaction in the pattern file, as text, with the attempt it belongs to."""
    if not path.is_file():
        raise FileNotFoundError(
            f"{path} not found; download it from the IBM AMLworld dataset on Kaggle"
        )
    rows: list[tuple[str | None, ...]] = []
    attempt: tuple[str, str, str | None] | None = None
    attempts_seen = 0
    for number, line in enumerate(path.read_text().splitlines(), start=1):
        line = line.strip()
        where = f"{path.name}:{number}"
        if not line:
            continue
        if begin := BEGIN_ATTEMPT.fullmatch(line):
            if attempt is not None:
                raise AmlworldFormatError(f"{where}: an attempt starts before {attempt[0]} ends")
            if begin["pattern"] not in PATTERN_TYPES:
                raise AmlworldFormatError(f"{where}: unknown pattern {begin['pattern']}")
            attempts_seen += 1
            attempt_id = f"attempt-{attempts_seen:05d}"
            attempt = (attempt_id, PATTERN_TYPES[begin["pattern"]], begin["description"])
        elif end := END_ATTEMPT.fullmatch(line):
            if attempt is None or PATTERN_TYPES.get(end["pattern"]) != attempt[1]:
                raise AmlworldFormatError(f"{where}: {line} doesn't close the open attempt")
            attempt = None
        else:
            values = line.split(",")
            if attempt is None or len(values) != len(CSV_SCHEMA):
                raise AmlworldFormatError(f"{where}: expected a transaction inside an attempt")
            rows.append((*attempt, *values))
    if attempt is not None:
        raise AmlworldFormatError(f"{path.name}: {attempt[0]} never ends")
    return pl.DataFrame(rows, schema=ATTEMPT_ROW_SCHEMA, orient="row")


def build_ground_truth(
    bank_rows: pl.DataFrame, attempt_rows: pl.DataFrame, our_bank: str
) -> GroundTruth:
    # The pattern file has no transaction ids, so its rows are matched to the CSV rows by their
    # exact text, which is why both frames keep the CSV columns as strings.
    ours = attempt_rows.filter(touches_bank(our_bank))
    matched = ours.join(
        bank_rows.select(*CSV_SCHEMA, "transaction_id"), on=list(CSV_SCHEMA), how="left"
    )
    if matched.height != ours.height:
        raise AmlworldFormatError("a pattern-file transaction matches more than one CSV row")
    if unmatched := matched.filter(pl.col("transaction_id").is_null()).height:
        raise AmlworldFormatError(
            f"{unmatched} pattern-file transactions of bank {our_bank} match no laundering row "
            "in the CSV"
        )
    if matched.get_column("transaction_id").is_duplicated().any():
        raise AmlworldFormatError("a transaction belongs to more than one laundering attempt")

    transaction_labels = (
        bank_rows.select("transaction_id", is_laundering=pl.col("is_laundering") == "1")
        .join(matched.select("transaction_id", "attempt_id"), on="transaction_id", how="left")
        .sort("transaction_id")
    )

    # Attempts usually span several banks, so we only ever see part of one. Only attempts that
    # reach our bank are kept, with both their full size and the part we see.
    laundering_attempts = (
        attempt_rows.group_by("attempt_id", "pattern_type", "description")
        .agg(transaction_count=pl.len().cast(pl.UInt32))
        .join(
            matched.group_by("attempt_id").agg(visible_transaction_count=pl.len().cast(pl.UInt32)),
            on="attempt_id",
        )
        .sort("attempt_id")
    )

    account_transactions = pl.concat(
        [
            bank_rows.select("transaction_id", account_id="from_account_id"),
            bank_rows.select("transaction_id", account_id="to_account_id"),
        ]
    ).unique()
    account_labels = (
        account_transactions.join(transaction_labels, on="transaction_id")
        .group_by("account_id")
        .agg(
            laundering_transaction_count=pl.col("is_laundering").sum().cast(pl.UInt32),
            attempt_ids=pl.col("attempt_id").drop_nulls().unique().sort(),
        )
        .sort("account_id")
    )
    return GroundTruth(
        transaction_labels=transaction_labels,
        laundering_attempts=laundering_attempts,
        account_labels=account_labels,
    )
