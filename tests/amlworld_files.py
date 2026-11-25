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


OPENSANCTIONS_HEADER = (
    '"id","schema","name","aliases","birth_date","countries","program_ids","dataset"'
)
LISTED = [
    '"NK-1","Person","Petr Ivanovich Sidorov","","1965-03-02","ru","EU-RUS","EU"',
    '"NK-2","Person","Шевченко Олександр","","1970","ua","","EU"',
    '"NK-3","Organization","Acme Trading","","","ru","","EU"',
]
PEPS = ['"Q-1","Person","Anna Maria Kowalska","","1980-05-01","pl","","Wikidata"']


def write_opensanctions(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for name, rows in [("sanctions", LISTED), ("peps", PEPS)]:
        lines = [OPENSANCTIONS_HEADER, *rows]
        (directory / f"{name}.targets.simple.csv").write_text("\n".join(lines) + "\n")


def write_raw_files(
    raw_dir: Path, rows: list[str], attempts: list[list[str]] | None = None
) -> None:
    """Lay out raw_dir as the world build expects: amlworld/ and opensanctions/."""
    amlworld = raw_dir / "amlworld"
    write_transactions_csv(amlworld, rows)
    write_patterns_file(amlworld, attempts or [])
    write_accounts_csv(amlworld, one_entity_per_account(rows))
    write_opensanctions(raw_dir / "opensanctions")


def build_small_world(directory: Path, rows: list[str]) -> Path:
    raw = directory / "raw"
    write_raw_files(raw, rows)
    return build_world(WorldConfig(seed=1, documents=False), raw, directory / "worlds")
