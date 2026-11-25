import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from datagen.amlworld import AmlworldFormatError
from datagen.world import WorldConfig, build_world


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="finguard-datagen")
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build", help="build a seeded world under --worlds-dir")
    build.add_argument("--seed", type=int, required=True)
    build.add_argument("--variant", default="HI-Small", help="AMLworld variant, e.g. HI-Small")
    build.add_argument("--our-bank", default="012", help="AMLworld bank id that plays FinGuard")
    build.add_argument("--raw-dir", type=Path, default=Path("data/raw/amlworld"))
    build.add_argument("--worlds-dir", type=Path, default=Path("data/worlds"))
    arguments = parser.parse_args(argv)

    config = WorldConfig(
        seed=arguments.seed, amlworld_variant=arguments.variant, our_bank=arguments.our_bank
    )
    try:
        world = build_world(config, arguments.raw_dir, arguments.worlds_dir)
    except (FileNotFoundError, AmlworldFormatError) as error:
        sys.exit(f"finguard-datagen: {error}")
    counts = json.loads((world / "manifest.json").read_text())["counts"]
    print(f"built {world}: " + ", ".join(f"{name} {value}" for name, value in counts.items()))
