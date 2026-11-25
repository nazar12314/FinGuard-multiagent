import argparse
from collections.abc import Sequence
from pathlib import Path

from finguard.core.contracts import CONTRACTS, read, stale, write
from finguard.core.settings import load_settings_or_exit
from finguard.infrastructure.schema_registry import SchemaRegistry


def main(argv: Sequence[str] | None = None) -> None:
    """Keep contracts/ in line with the message models, and the schema registry in line with
    contracts/."""
    parser = argparse.ArgumentParser(prog="finguard-contracts")
    parser.add_argument("--directory", type=Path, default=Path("contracts"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("generate", help="write the schema files from the message models")
    commands.add_parser("check", help="fail if the schema files differ from the message models")
    commands.add_parser("register", help="register the schema files in the schema registry")
    commands.add_parser(
        "check-compatibility",
        help="fail if a schema file isn't backward compatible with the registered one",
    )
    arguments = parser.parse_args(argv)
    directory: Path = arguments.directory

    match arguments.command:
        case "generate":
            for name in write(directory, CONTRACTS):
                print(f"updated {directory / name}")
        case "check":
            if outdated := stale(directory, CONTRACTS):
                for name in outdated:
                    print(f"out of date: {directory / name}")
                print("run `uv run finguard-contracts generate` and commit the result")
                raise SystemExit(1)
        case "register":
            registry = SchemaRegistry(load_settings_or_exit().schema_registry.url)
            for subject, schema in read(directory).items():
                print(f"registered {subject} as schema {registry.register(subject, schema)}")
        case "check-compatibility":
            registry = SchemaRegistry(load_settings_or_exit().schema_registry.url)
            incompatible = []
            for subject, schema in read(directory).items():
                compatible = registry.is_compatible(subject, schema)
                verdict = {None: "new", True: "compatible", False: "INCOMPATIBLE"}[compatible]
                print(f"{verdict}: {subject}")
                if compatible is False:
                    incompatible.append(subject)
            if incompatible:
                print("a breaking change needs a new contract version instead")
                raise SystemExit(1)


if __name__ == "__main__":
    main()
