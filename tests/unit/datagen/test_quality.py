import json
import shutil
from datetime import date
from pathlib import Path

import polars as pl
import pytest
from tests.amlworld_files import OUR_BANK, attempt, csv_row, write_raw_files

from datagen import quality
from datagen.documents import content
from datagen.quality import Check, DataQualityError, quality_report, render_markdown
from datagen.world import WorldConfig, build_world


@pytest.fixture(scope="module")
def world(tmp_path_factory: pytest.TempPathFactory) -> Path:
    directory = tmp_path_factory.mktemp("quality")
    laundering = csv_row(OUR_BANK, "C1", "002", "B1", laundering=True)
    rows = [laundering] + [csv_row(OUR_BANK, f"C{n}", "002", f"B{n}") for n in range(2, 40)]
    write_raw_files(directory / "raw", rows, [attempt("CYCLE", [laundering], "Max 1 hops")])
    return build_world(
        WorldConfig(seed=3, documents=False), directory / "raw", directory / "worlds"
    )


def frames(world: Path) -> dict[str, pl.DataFrame]:
    return {
        str(path.relative_to(world)): pl.read_parquet(path)
        for path in sorted(world.rglob("*.parquet"))
    }


def failures(world: Path, files: dict[str, pl.DataFrame]) -> dict[str, list[str]]:
    report = quality_report(world, files)
    return {check.name: check.examples for check in report.failed_checks}


def test_a_built_world_passes_every_check_and_writes_its_report(world: Path) -> None:
    report = json.loads((world / "quality_report.json").read_text())
    markdown = (world / "quality_report.md").read_text()

    assert report["passed"] is True
    assert report["world"] == "seed-3"
    assert report["tables"]["transactions"]["rows"] == 39
    assert report["tables"]["accounts"]["null_rates"]["customer_id"] > 0
    assert f"**All {len(report['checks'])} checks passed.**" in markdown


def test_finds_broken_keys_references_and_ownership(world: Path) -> None:
    files = frames(world)
    customers = files["customers.parquet"]
    files["customers.parquet"] = pl.concat([customers, customers.head(1)])
    devices = files["customer_devices.parquet"]
    files["customer_devices.parquet"] = devices.with_columns(
        device_id=pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit("dev-missing"))
        .otherwise("device_id")
    )
    shareholdings = files["shareholdings.parquet"]
    owned = shareholdings.get_column("company_id")[0]
    files["shareholdings.parquet"] = shareholdings.filter(pl.col("company_id") != owned)

    failed = failures(world, files)

    first_customer = customers.get_column("customer_id")[0]
    assert failed == {
        "customers: customer_id is unique": [first_customer],
        "customer_devices.device_id is in devices.device_id": ["dev-missing"],
        "every company's shares add up to 100%": [owned],
    }


def test_finds_labels_that_contradict_each_other(world: Path) -> None:
    files = frames(world)
    customer_labels = files["ground_truth/customer_labels.parquet"]
    transaction_labels = files["ground_truth/transaction_labels.parquet"]
    files["ground_truth/customer_labels.parquet"] = customer_labels.with_columns(
        laundering_linked=pl.lit(False)
    )
    files["ground_truth/transaction_labels.parquet"] = transaction_labels.with_columns(
        is_laundering=pl.lit(False)
    )

    failed = failures(world, files)

    linked = customer_labels.filter("laundering_linked").get_column("customer_id").to_list()
    in_attempt = transaction_labels.filter(pl.col("attempt_id").is_not_null())
    assert len(linked) == 1
    assert (
        failed[
            "a customer is laundering-linked exactly when one of its accounts has a laundering "
            "transaction"
        ]
        == linked
    )
    assert (
        failed["every transaction in an attempt is laundering"]
        == in_attempt.get_column("transaction_id").to_list()
    )


def test_a_label_pointing_at_a_missing_subject_fails(world: Path) -> None:
    files = frames(world)
    files["ground_truth/media_labels.parquet"] = pl.DataFrame(
        {
            "article_id": ["art-1"],
            "subject_type": ["person"],
            "subject_id": ["per-nobody"],
            "label": ["relevant_adverse"],
            "story": ["a story"],
        }
    )

    failed = failures(world, files)

    assert failed["ground_truth/media_labels.subject_id of a person is in persons"] == [
        "per-nobody"
    ]
    assert failed["ground_truth/media_labels.article_id is in media/articles.article_id"] == [
        "art-1"
    ]


def test_signal_strength_compares_precision_with_the_base_rate() -> None:
    population = {f"c{n}" for n in range(10)}

    signal = quality._signal(
        "flag", "customers", population, {"c0", "c1"}, {"c0", "c5", "c6", "c7"}
    )

    assert (signal.flagged, signal.flagged_laundering_linked) == (4, 1)
    assert (signal.precision, signal.recall, signal.lift) == (0.25, 0.5, 1.25)
    assert quality._signal("flag", "customers", population, {"c0"}, set()).lift is None


def test_a_failed_check_stops_the_build_and_keeps_the_previous_world(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = tmp_path / "raw"
    write_raw_files(raw, [csv_row(OUR_BANK, "C1", "002", "B1")])
    config = WorldConfig(seed=5, documents=False)
    previous = build_world(config, raw, tmp_path / "worlds")
    monkeypatch.setattr(quality, "DOMAIN_CHECKS", [lambda _: Check("always fails", 1, ["x"])])

    with pytest.raises(DataQualityError, match="1 data-quality checks failed"):
        build_world(config, raw, tmp_path / "worlds")

    assert json.loads((previous / "quality_report.json").read_text())["passed"] is True
    staged = tmp_path / "worlds" / ".seed-5.building" / "quality_report.md"
    assert "| always fails | **fail**: 1 rows, e.g. x |" in staged.read_text()


def test_markdown_lists_failures_with_examples(world: Path) -> None:
    report = quality_report(world, frames(world))
    broken = quality.QualityReport(
        world=report.world,
        tables=report.tables,
        checks=[*report.checks, Check("ids are unique", 2, ["a", "b"])],
        signals=report.signals,
        booking_days=report.booking_days,
    )

    markdown = render_markdown(broken)

    assert f"**1 of {len(broken.checks)} checks failed.**" in markdown
    assert "| ids are unique | **fail**: 2 rows, e.g. a, b |" in markdown
    assert "### Among customers" in markdown


def test_finds_impossible_dates_and_foreign_registered_addresses(world: Path) -> None:
    files = frames(world)
    officers = files["officers.parquet"]
    first = officers.row(0, named=True)
    files["officers.parquet"] = officers.with_columns(
        appointed_on=pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit(date(1900, 1, 1)))
        .otherwise("appointed_on")
    )
    addresses = files["addresses.parquet"]
    registered = (
        files["customer_addresses.parquet"]
        .filter(pl.col("role") == "registered")
        .row(0, named=True)
    )
    files["addresses.parquet"] = addresses.with_columns(
        country=pl.when(pl.col("address_id") == registered["address_id"])
        .then(pl.lit("XX"))
        .otherwise("country")
    )

    failed = failures(world, files)

    officer = f"{first['company_id']}/{first['person_id']}"
    assert failed[
        "every director is appointed after incorporation and before the company joined us"
    ] == [officer]
    assert failed[
        "every director is an adult when appointed, and every sole trader when onboarded"
    ] == [first["person_id"]]
    assert (
        registered["customer_id"]
        in failed["every customer's registered address is in its own country"]
    )


def test_finds_a_shared_device_nobody_explained(world: Path) -> None:
    files = frames(world)
    devices = files["customer_devices.parquet"]
    customers = devices.get_column("customer_id").unique().sort()
    shared = pl.DataFrame(
        {
            "customer_id": customers.head(2),
            "device_id": ["dev-unexplained"] * 2,
            "first_seen_at": [date(2022, 1, 1)] * 2,
        },
        schema=devices.schema,
    )
    files["customer_devices.parquet"] = pl.concat([devices, shared])

    failed = failures(world, files)

    assert failed["every address or device several customers use has a labelled reason"] == [
        "dev-unexplained"
    ]


@pytest.fixture(scope="module")
def documented_world(tmp_path_factory: pytest.TempPathFactory) -> Path:
    directory = tmp_path_factory.mktemp("documented")
    rows = [csv_row(OUR_BANK, f"C{n}", "002", f"B{n}") for n in range(4)]
    write_raw_files(directory / "raw", rows)
    # Every customer gets an injection, so each technique is likely to turn up.
    rate = content.INJECTION_RATE
    content.INJECTION_RATE = 1.0
    try:
        config = WorldConfig(seed=11, document_sample=4)
        return build_world(config, directory / "raw", directory / "worlds")
    finally:
        content.INJECTION_RATE = rate


def test_a_document_world_passes_and_its_injections_are_in_the_pdfs(documented_world: Path) -> None:
    report = json.loads((documented_world / "quality_report.json").read_text())

    assert report["passed"] is True
    labels = pl.read_parquet(documented_world / "ground_truth" / "document_labels.parquet")
    assert labels.get_column("injection").drop_nulls().len() == 4


def test_finds_an_injection_missing_from_its_pdf(documented_world: Path, tmp_path: Path) -> None:
    files = frames(documented_world)
    labels = files["ground_truth/document_labels.parquet"]
    injected = labels.filter(pl.col("injection").is_not_null() & ~pl.col("scanned")).row(
        0, named=True
    )
    clean = labels.filter(pl.col("injection").is_null() & ~pl.col("scanned")).row(0, named=True)
    copy = tmp_path / "world"
    shutil.copytree(documented_world, copy)
    shutil.copy(
        copy / "documents" / f"{clean['document_id']}.pdf",
        copy / "documents" / f"{injected['document_id']}.pdf",
    )

    failed = failures(copy, files)

    assert failed["every labelled injection is in its PDF, and only visible ones are scanned"] == [
        injected["document_id"]
    ]
