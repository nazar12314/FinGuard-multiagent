import json
from collections.abc import Iterable
from pathlib import Path

from finguard.core.diagnostics import PING_REQUESTED_CONTRACT
from finguard.modules.ingestion.messages import TRANSACTION_BOOKED_CONTRACT
from finguard.platform.contracts import MessageContract, message_schema

# A module adds its contracts here when it starts publishing.
CONTRACTS: tuple[MessageContract, ...] = (PING_REQUESTED_CONTRACT, TRANSACTION_BOOKED_CONTRACT)

# Subjects no longer produced; their messages stay in topics until retention expires.
RETIRED: frozenset[str] = frozenset()


def render(contracts: Iterable[MessageContract]) -> dict[str, str]:
    return {
        f"{contract.subject}.json": json.dumps(message_schema(contract), indent=2, sort_keys=True)
        + "\n"
        for contract in contracts
    }


def write(directory: Path, contracts: Iterable[MessageContract]) -> list[str]:
    """Return the names of the files that changed."""
    expected = render(contracts)
    changed = []
    for name in sorted(_schema_files(directory) - expected.keys()):
        (directory / name).unlink()
        changed.append(name)
    for name, text in sorted(expected.items()):
        path = directory / name
        if not path.exists() or path.read_text() != text:
            path.write_text(text)
            changed.append(name)
    return sorted(changed)


def stale(directory: Path, contracts: Iterable[MessageContract]) -> list[str]:
    """The schema files that are missing, outdated or no longer backed by a contract."""
    expected = render(contracts)
    present = _schema_files(directory)
    outdated = [
        name
        for name, text in expected.items()
        if name not in present or (directory / name).read_text() != text
    ]
    return sorted(outdated + list(present - expected.keys()))


def removed_without_retiring(
    base: Path, contracts: Iterable[MessageContract], retired: frozenset[str]
) -> list[str]:
    return sorted(read(base).keys() - {contract.subject for contract in contracts} - retired)


def read(directory: Path) -> dict[str, str]:
    return {
        name.removesuffix(".json"): (directory / name).read_text()
        for name in sorted(_schema_files(directory))
    }


def _schema_files(directory: Path) -> set[str]:
    return {path.name for path in directory.glob("*.json")} if directory.exists() else set()
