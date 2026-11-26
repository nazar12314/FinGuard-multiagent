import json
import os
import subprocess
import sys
from pathlib import Path

import polars as pl
import pytest
from tests.amlworld_files import OUR_BANK, attempt, csv_row, write_raw_files

from datagen.cli import main
from datagen.world import WorldConfig, build_world


@pytest.fixture
def raw_dir(tmp_path: Path) -> Path:
    raw = tmp_path / "raw"
    laundering = csv_row(OUR_BANK, "C1", "002", "B1", laundering=True)
    write_raw_files(
        raw,
        [laundering, csv_row("001", "A1", OUR_BANK, "C2"), csv_row("001", "A1", "002", "B1")],
        [attempt("CYCLE", [laundering], "Max 1 hops")],
    )
    return raw


def test_writes_the_world_files_and_a_manifest(tmp_path: Path, raw_dir: Path) -> None:
    world = build_world(WorldConfig(seed=7, documents=False), raw_dir, tmp_path / "worlds")

    assert world == tmp_path / "worlds" / "seed-7"
    assert pl.read_parquet(world / "transactions.parquet").height == 2
    for name in [
        "customers",
        "kyc_profiles",
        "persons",
        "addresses",
        "customer_addresses",
        "devices",
        "customer_devices",
        "companies",
        "officers",
        "shareholdings",
    ]:
        assert (world / f"{name}.parquet").exists()
    for name in [
        "transaction_labels",
        "laundering_attempts",
        "account_labels",
        "customer_labels",
        "shared_attribute_labels",
        "company_labels",
        "screening_labels",
    ]:
        assert (world / "ground_truth" / f"{name}.parquet").exists()
    accounts = pl.read_parquet(world / "accounts.parquet")
    assert accounts.filter("held_at_our_bank").get_column("customer_id").null_count() == 0
    manifest = json.loads((world / "manifest.json").read_text())
    assert manifest["seed"] == 7
    assert manifest["our_bank"] == "12"
    assert manifest["fx_eur_per_unit"]["JPY"] == "0.007"
    assert set(manifest["opensanctions_sha256"]) == {
        "sanctions.targets.simple.csv",
        "peps.targets.simple.csv",
    }
    assert manifest["counts"] == {
        "transactions": 2,
        "accounts": 4,
        "accounts_at_our_bank": 2,
        "laundering_transactions": 1,
        "laundering_transactions_in_attempts": 1,
        "laundering_attempts": 1,
        "customers": 2,
        "laundering_linked_customers": 1,
        "documents": 0,
        "media_articles": manifest["counts"]["media_articles"],
    }
    assert set(manifest["amlworld_sha256"]) == {
        "HI-Small_Trans.csv",
        "HI-Small_Patterns.txt",
        "HI-Small_accounts.csv",
    }
    assert set(manifest["adverse_media_sha256"]) == {"adverse_media.jsonl"}


def test_the_config_hash_changes_with_the_config(tmp_path: Path, raw_dir: Path) -> None:
    def config_hash(config: WorldConfig) -> str:
        world = build_world(config, raw_dir, tmp_path / "worlds")
        return str(json.loads((world / "manifest.json").read_text())["config_hash"])

    first = config_hash(WorldConfig(seed=7, documents=False))

    assert config_hash(WorldConfig(seed=7, documents=False)) == first
    assert config_hash(WorldConfig(seed=8, documents=False)) != first
    assert config_hash(WorldConfig(seed=7, our_bank="001", documents=False)) != first
    assert config_hash(WorldConfig(seed=7, our_bank="012", documents=False)) == first


def test_the_raw_files_are_part_of_the_config_hash(tmp_path: Path, raw_dir: Path) -> None:
    def config_hash() -> str:
        world = build_world(WorldConfig(seed=7, documents=False), raw_dir, tmp_path / "worlds")
        return str(json.loads((world / "manifest.json").read_text())["config_hash"])

    first = config_hash()
    patterns = raw_dir / "amlworld" / "HI-Small_Patterns.txt"
    patterns.write_text(patterns.read_text() + "\n")

    assert config_hash() != first


def test_rebuilding_replaces_the_previous_world(tmp_path: Path, raw_dir: Path) -> None:
    world = build_world(WorldConfig(seed=7, documents=False), raw_dir, tmp_path / "worlds")
    (world / "left_over.parquet").write_text("stale")

    build_world(WorldConfig(seed=7, documents=False), raw_dir, tmp_path / "worlds")

    assert not (world / "left_over.parquet").exists()
    assert (world / "transactions.parquet").exists()
    assert sorted(path.name for path in (tmp_path / "worlds").iterdir()) == ["seed-7"]


def test_a_failed_build_keeps_the_previous_world(tmp_path: Path, raw_dir: Path) -> None:
    world = build_world(WorldConfig(seed=7, documents=False), raw_dir, tmp_path / "worlds")

    with pytest.raises(FileNotFoundError):
        build_world(WorldConfig(seed=7, documents=False), tmp_path / "missing", tmp_path / "worlds")

    assert (world / "transactions.parquet").exists()


def test_needs_the_pattern_file(tmp_path: Path, raw_dir: Path) -> None:
    (raw_dir / "amlworld" / "HI-Small_Patterns.txt").unlink()

    with pytest.raises(FileNotFoundError, match="Patterns"):
        build_world(WorldConfig(seed=7, documents=False), raw_dir, tmp_path / "worlds")


def test_command_builds_a_world(
    tmp_path: Path, raw_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    worlds = tmp_path / "worlds"

    arguments = ["build", "--seed", "7", "--no-documents", "--raw-dir", str(raw_dir)]
    main([*arguments, "--worlds-dir", str(worlds)])

    assert (worlds / "seed-7" / "manifest.json").exists()
    assert "transactions 2" in capsys.readouterr().out


def test_command_exits_with_the_reason_when_input_is_missing(tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match="not found"):
        main(["build", "--seed", "7", "--raw-dir", str(tmp_path), "--worlds-dir", str(tmp_path)])


def test_a_world_is_byte_identical_whatever_the_hash_seed(tmp_path: Path, raw_dir: Path) -> None:
    program = (
        "import sys\n"
        "from pathlib import Path\n"
        "from datagen.world import WorldConfig, build_world\n"
        "build_world(WorldConfig(seed=7, document_sample=2), Path(sys.argv[1]), Path(sys.argv[2]))"
    )

    def build(hash_seed: str) -> dict[str, bytes]:
        worlds = tmp_path / hash_seed
        environment = {**os.environ, "PYTHONHASHSEED": hash_seed}
        command = [sys.executable, "-c", program, str(raw_dir), str(worlds)]
        subprocess.run(command, env=environment, check=True, capture_output=True)
        world = worlds / "seed-7"
        return {
            str(path.relative_to(world)): path.read_bytes()
            for path in sorted(world.rglob("*"))
            if path.is_file()
        }

    first = build("1")

    assert any(name.endswith(".pdf") for name in first)
    assert build("2") == first
