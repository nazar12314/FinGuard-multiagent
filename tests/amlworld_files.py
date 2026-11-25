from pathlib import Path

from datagen.amlworld import CSV_HEADER
from datagen.customers import ACCOUNTS_HEADER
from datagen.world import WorldConfig, build_world

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


def write_accounts_csv(
    raw_dir: Path, entities: list[tuple[str, str, str, str]], variant: str = "HI-Small"
) -> Path:
    """Write the accounts file from (bank id, account number, entity id, entity name) rows."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / f"{variant}_accounts.csv"
    lines = [
        f"Bank #{bank},{bank},{account},{entity},{name}" for bank, account, entity, name in entities
    ]
    path.write_text("\n".join([ACCOUNTS_HEADER, *lines]) + "\n")
    return path


def one_entity_per_account(rows: list[str]) -> list[tuple[str, str, str, str]]:
    accounts = sorted(
        {(str(int(fields[1])), fields[2]) for fields in (row.split(",") for row in rows)}
        | {(str(int(fields[3])), fields[4]) for fields in (row.split(",") for row in rows)}
    )
    return [
        (bank, account, f"E{number}", f"Corporation #{number}")
        for number, (bank, account) in enumerate(accounts)
    ]


def write_raw_files(
    raw_dir: Path, rows: list[str], attempts: list[list[str]] | None = None
) -> None:
    write_transactions_csv(raw_dir, rows)
    write_patterns_file(raw_dir, attempts or [])
    write_accounts_csv(raw_dir, one_entity_per_account(rows))


def build_small_world(directory: Path, rows: list[str]) -> Path:
    raw = directory / "raw"
    write_raw_files(raw, rows)
    return build_world(WorldConfig(seed=1), raw, directory / "worlds")
