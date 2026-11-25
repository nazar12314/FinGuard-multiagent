from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from datagen.fx import to_eur

CSV_HEADER = (
    "Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,"
    "Amount Paid,Payment Currency,Payment Format,Is Laundering"
)

# The CSV names both account columns "Account", so columns are renamed by position.
CSV_SCHEMA = {
    "timestamp": pl.String,
    "from_bank": pl.String,
    "from_account": pl.String,
    "to_bank": pl.String,
    "to_account": pl.String,
    "amount_received": pl.String,
    "receiving_currency": pl.String,
    "amount_paid": pl.String,
    "payment_currency": pl.String,
    "payment_format": pl.String,
    "is_laundering": pl.String,
}

CURRENCY_CODES = {
    "Australian Dollar": "AUD",
    "Bitcoin": "BTC",
    "Brazil Real": "BRL",
    "Canadian Dollar": "CAD",
    "Euro": "EUR",
    "Mexican Peso": "MXN",
    "Ruble": "RUB",
    "Rupee": "INR",
    "Saudi Riyal": "SAR",
    "Shekel": "ILS",
    "Swiss Franc": "CHF",
    "UK Pound": "GBP",
    "US Dollar": "USD",
    "Yen": "JPY",
    "Yuan": "CNY",
}

PAYMENT_FORMATS = {
    "ACH": "ach",
    "Bitcoin": "bitcoin",
    "Cash": "cash",
    "Cheque": "cheque",
    "Credit Card": "credit_card",
    "Reinvestment": "reinvestment",
    "Wire": "wire",
}

# Bitcoin amounts carry six decimals; the largest amounts in HI-Small have 13 integer digits.
ORIGINAL_AMOUNT = pl.Decimal(24, 6)


class AmlworldFormatError(ValueError):
    pass


@dataclass(frozen=True)
class BankSlice:
    transactions: pl.DataFrame
    accounts: pl.DataFrame


def transactions_csv(raw_dir: Path, variant: str) -> Path:
    return raw_dir / f"{variant}_Trans.csv"


def transaction_id(row: pl.Expr) -> pl.Expr:
    # The id is the row's position in the full CSV, so it stays the same whichever bank is ours
    # and lets the pattern file's rows be matched back to it.
    return pl.format("tx-{}", row.cast(pl.String).str.zfill(9))


def canonical_bank_id(bank: str) -> str:
    if not bank.isdigit():
        raise AmlworldFormatError(f"bank ids are numbers, got {bank!r}")
    return str(int(bank))


def bank_id(bank: pl.Expr) -> pl.Expr:
    # The CSV pads one bank's id with varying zeros ("012" and "0012" are both bank 12, as the
    # accounts file confirms), so ids are compared and stored without the padding.
    return bank.cast(pl.Int64).cast(pl.String)


def account_id(bank: pl.Expr, account: pl.Expr) -> pl.Expr:
    # Account numbers repeat across banks, so an account is only unique together with its bank.
    return pl.format("{}:{}", bank, account)


def touches_bank(bank: str) -> pl.Expr:
    bank = canonical_bank_id(bank)
    return (pl.col("from_bank") == bank) | (pl.col("to_bank") == bank)


def read_bank_rows(csv_path: Path, our_bank: str) -> pl.DataFrame:
    """The CSV rows our bank is party to, as text, with their transaction and account ids."""
    if not csv_path.is_file():
        raise FileNotFoundError(
            f"{csv_path} not found; download it from the IBM AMLworld dataset on Kaggle"
        )
    with csv_path.open() as csv_file:
        header = csv_file.readline().strip()
    if header != CSV_HEADER:
        raise AmlworldFormatError(f"{csv_path.name} has an unexpected header: {header}")
    rows = (
        pl.scan_csv(
            csv_path, has_header=True, new_columns=list(CSV_SCHEMA), schema_overrides=CSV_SCHEMA
        )
        .with_row_index("row")
        .with_columns(from_bank=bank_id(pl.col("from_bank")), to_bank=bank_id(pl.col("to_bank")))
        .filter(touches_bank(our_bank))
        .collect()
    )
    if rows.is_empty():
        raise AmlworldFormatError(
            f"bank {canonical_bank_id(our_bank)} has no transactions in {csv_path.name}"
        )
    _reject_unknown(rows, ["payment_currency", "receiving_currency"], CURRENCY_CODES)
    _reject_unknown(rows, ["payment_format"], PAYMENT_FORMATS)
    _reject_unknown(rows, ["is_laundering"], {"0", "1"})
    return rows.with_columns(
        transaction_id=transaction_id(pl.col("row")),
        from_account_id=account_id(pl.col("from_bank"), pl.col("from_account")),
        to_account_id=account_id(pl.col("to_bank"), pl.col("to_account")),
    )


def map_bank_slice(rows: pl.DataFrame, our_bank: str) -> BankSlice:
    amounts = rows.with_columns(
        payment_currency=pl.col("payment_currency").replace_strict(CURRENCY_CODES),
        receiving_currency=pl.col("receiving_currency").replace_strict(CURRENCY_CODES),
        amount_paid=pl.col("amount_paid").cast(ORIGINAL_AMOUNT),
        amount_received=pl.col("amount_received").cast(ORIGINAL_AMOUNT),
    )
    transactions = amounts.select(
        "transaction_id",
        booked_at=pl.col("timestamp").str.to_datetime("%Y/%m/%d %H:%M", time_zone="UTC"),
        from_account_id="from_account_id",
        to_account_id="to_account_id",
        amount_paid="amount_paid",
        payment_currency="payment_currency",
        amount_paid_eur=to_eur(pl.col("amount_paid"), pl.col("payment_currency")),
        amount_received="amount_received",
        receiving_currency="receiving_currency",
        amount_received_eur=to_eur(pl.col("amount_received"), pl.col("receiving_currency")),
        payment_format=pl.col("payment_format").replace_strict(PAYMENT_FORMATS),
    ).sort("booked_at", "transaction_id")

    accounts = (
        pl.concat(
            [
                rows.select(
                    account_id="from_account_id", bank_id="from_bank", account_number="from_account"
                ),
                rows.select(
                    account_id="to_account_id", bank_id="to_bank", account_number="to_account"
                ),
            ]
        )
        .unique()
        .with_columns(held_at_our_bank=pl.col("bank_id") == canonical_bank_id(our_bank))
        .sort("account_id")
    )
    return BankSlice(transactions=transactions, accounts=accounts)


def _reject_unknown(rows: pl.DataFrame, columns: list[str], known: Collection[str]) -> None:
    seen = pl.concat([rows.get_column(column) for column in columns]).unique()
    if unknown := sorted(set(seen.to_list()) - set(known)):
        raise AmlworldFormatError(f"unknown values in {', '.join(columns)}: {unknown}")
