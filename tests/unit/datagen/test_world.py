import json
from pathlib import Path

import polars as pl
import pytest
from tests.unit.datagen.amlworld_files import OUR_BANK, csv_row, write_transactions_csv

from datagen.cli import main
from datagen.world import WorldConfig, build_world


@pytest.fixture
def raw_dir(tmp_path: Path) -> Path:
    raw = tmp_path / "raw"
    write_transactions_csv(
        raw,
        [
            csv_row(OUR_BANK, "C1", "002", "B1", laundering=True),
            csv_row("001", "A1", OUR_BANK, "C2"),
            csv_row("001", "A1", "002", "B1"),
        ],
    )
    return raw


def test_writes_the_world_files_and_a_manifest(tmp_path: Path, raw_dir: Path) -> None:
    world = build_world(WorldConfig(seed=7), raw_dir, tmp_path / "worlds")

    assert world == tmp_path / "worlds" / "seed-7"
    assert pl.read_parquet(world / "transactions.parquet").height == 2
    assert pl.read_parquet(world / "ground_truth" / "transaction_labels.parquet").height == 2
    manifest = json.loads((world / "manifest.json").read_text())
    assert manifest["seed"] == 7
    assert manifest["our_bank"] == OUR_BANK
    assert manifest["fx_eur_per_unit"]["JPY"] == "0.007"
    assert manifest["counts"] == {
        "transactions": 2,
        "accounts": 4,
        "accounts_at_our_bank": 2,
        "laundering_transactions": 1,
    }


def test_the_config_hash_changes_with_the_config(tmp_path: Path, raw_dir: Path) -> None:
    def config_hash(config: WorldConfig) -> str:
        world = build_world(config, raw_dir, tmp_path / "worlds")
        return str(json.loads((world / "manifest.json").read_text())["config_hash"])

    first = config_hash(WorldConfig(seed=7))

    assert config_hash(WorldConfig(seed=7)) == first
    assert config_hash(WorldConfig(seed=8)) != first
    assert config_hash(WorldConfig(seed=7, our_bank="001")) != first


def test_rebuilding_replaces_the_previous_world(tmp_path: Path, raw_dir: Path) -> None:
    world = build_world(WorldConfig(seed=7), raw_dir, tmp_path / "worlds")
    (world / "left_over.parquet").write_text("stale")

    build_world(WorldConfig(seed=7), raw_dir, tmp_path / "worlds")

    assert not (world / "left_over.parquet").exists()
    assert (world / "transactions.parquet").exists()


def test_a_failed_build_keeps_the_previous_world(tmp_path: Path, raw_dir: Path) -> None:
    world = build_world(WorldConfig(seed=7), raw_dir, tmp_path / "worlds")

    with pytest.raises(FileNotFoundError):
        build_world(WorldConfig(seed=7), tmp_path / "missing", tmp_path / "worlds")

    assert (world / "transactions.parquet").exists()


def test_command_builds_a_world(
    tmp_path: Path, raw_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    worlds = tmp_path / "worlds"

    main(["build", "--seed", "7", "--raw-dir", str(raw_dir), "--worlds-dir", str(worlds)])

    assert (worlds / "seed-7" / "manifest.json").exists()
    assert "transactions 2" in capsys.readouterr().out


def test_command_exits_with_the_reason_when_input_is_missing(tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match="not found"):
        main(["build", "--seed", "7", "--raw-dir", str(tmp_path), "--worlds-dir", str(tmp_path)])
