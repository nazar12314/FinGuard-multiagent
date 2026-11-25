from pathlib import Path

from datagen.amlworld import CSV_HEADER

OUR_BANK = "012"


def csv_row(
    from_bank: str,
    from_account: str,
    to_bank: str,
    to_account: str,
    *,
    amount: str = "100.00",
    currency: str = "Euro",
    received_currency: str | None = None,
    payment_format: str = "ACH",
    laundering: bool = False,
    timestamp: str = "2022/09/01 10:00",
) -> str:
    received = received_currency or currency
    return (
        f"{timestamp},{from_bank},{from_account},{to_bank},{to_account},{amount},{received},"
        f"{amount},{currency},{payment_format},{int(laundering)}"
    )


def write_transactions_csv(raw_dir: Path, rows: list[str], variant: str = "HI-Small") -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / f"{variant}_Trans.csv"
    path.write_text("\n".join([CSV_HEADER, *rows]) + "\n")
    return path


def attempt(pattern: str, rows: list[str], description: str | None = None) -> list[str]:
    header = f"BEGIN LAUNDERING ATTEMPT - {pattern}"
    if description is not None:
        header += f":  {description}"
    return [header, *rows, f"END LAUNDERING ATTEMPT - {pattern}", ""]


def write_patterns_file(
    raw_dir: Path, attempts: list[list[str]], variant: str = "HI-Small"
) -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / f"{variant}_Patterns.txt"
    path.write_text("\n".join(line for lines in attempts for line in lines) + "\n")
    return path
