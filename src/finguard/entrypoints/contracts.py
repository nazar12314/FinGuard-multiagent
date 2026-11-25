import argparse
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import NoReturn

from finguard.core.contracts import CONTRACTS, RETIRED, read, removed_without_retiring, stale, write
from finguard.core.settings import load_settings_or_exit
from finguard.infrastructure.schema_registry import SchemaRegistry, SchemaRegistryError

# Exit codes: 1 when the contracts need fixing, 2 when the command couldn't do its job.
CONTRACTS_NEED_FIXING = 1
COMMAND_FAILED = 2


def main(argv: Sequence[str] | None = None) -> None:
    """Keep contracts/ in line with the message models, and the schema registry in line with
    contracts/."""
    parser = argparse.ArgumentParser(prog="finguard-contracts")
    parser.add_argument("--directory", type=Path, default=Path("contracts"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("generate", help="write the schema files from the message models")
    commands.add_parser("check", help="fail if the schema files differ from the message models")
    commands.add_parser("register", help="register the schema files in the schema registry")
    compatibility = commands.add_parser(
        "check-compatibility",
        help="fail if a schema file isn't backward compatible with the registered one",
    )
    compatibility.add_argument(
        "--base",
        type=Path,
        help="the base branch's contracts/; fail if a contract there was removed undeclared",
    )
    arguments = parser.parse_args(argv)
    directory: Path = arguments.directory
    if not directory.is_dir():
        _fail(f"{directory} is not a directory; run from the repository root or pass --directory")

    try:
        match arguments.command:
            case "generate":
                for name in write(directory, CONTRACTS):
                    print(f"updated {directory / name}")
            case "check":
                if outdated := stale(directory, CONTRACTS):
                    for name in outdated:
                        print(f"out of date: {directory / name}")
                    print("run `uv run finguard-contracts generate` and commit the result")
                    raise SystemExit(CONTRACTS_NEED_FIXING)
            case "register":
                registry = SchemaRegistry(load_settings_or_exit().schema_registry.url)
                for subject, schema in read(directory).items():
                    print(f"registered {subject} as schema {registry.register(subject, schema)}")
            case "check-compatibility":
                _check_compatibility(directory, arguments.base)
    except SchemaRegistryError as error:
        _fail(str(error))


def _check_compatibility(directory: Path, base: Path | None) -> None:
    problems = []
    if base is not None:
        for subject in removed_without_retiring(base, CONTRACTS, RETIRED):
            print(f"REMOVED: {subject}")
            problems.append("a removed contract must be listed in RETIRED in core/contracts.py")

    registry = SchemaRegistry(load_settings_or_exit().schema_registry.url)
    for subject, schema in read(directory).items():
        compatible = registry.is_compatible(subject, schema)
        verdict = {None: "new", True: "compatible", False: "INCOMPATIBLE"}[compatible]
        print(f"{verdict}: {subject}")
        if compatible is False:
            problems.append("a breaking change needs a new contract version instead")

    for problem in dict.fromkeys(problems):
        print(problem)
    if problems:
        raise SystemExit(CONTRACTS_NEED_FIXING)


def _fail(message: str) -> NoReturn:
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(COMMAND_FAILED)


if __name__ == "__main__":
    main()
