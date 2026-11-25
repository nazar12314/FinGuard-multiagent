import json
from pathlib import Path

import polars as pl
import pypdfium2 as pdfium
import pytest
from tests.amlworld_files import OUR_BANK, csv_row, write_raw_files

from datagen.documents.content import DocumentSpec, document_specs, sample_customers
from datagen.documents.render import render_html, scan_pdf
from datagen.world import WorldConfig, build_world

PARQUET_FILES = [
    "customers",
    "kyc_profiles",
    "persons",
    "addresses",
    "customer_addresses",
    "companies",
    "officers",
    "shareholdings",
    "ground_truth/customer_labels",
    "ground_truth/company_labels",
]


@pytest.fixture(scope="module")
def world(tmp_path_factory: pytest.TempPathFactory) -> Path:
    directory = tmp_path_factory.mktemp("world")
    rows = [
        csv_row(OUR_BANK, f"C{n}", "002", f"B{n}", laundering=n < 3, amount="5000.00")
        for n in range(12)
    ]
    write_raw_files(directory / "raw", rows)
    return build_world(WorldConfig(seed=3, document_sample=4), directory / "raw", directory)


def frames(world: Path) -> dict[str, pl.DataFrame]:
    return {f"{name}.parquet": pl.read_parquet(world / f"{name}.parquet") for name in PARQUET_FILES}


def specs_for(world: Path, seed: int = 3) -> list[DocumentSpec]:
    loaded = frames(world)
    customers = loaded["customers.parquet"].get_column("customer_id").to_list()
    return document_specs(loaded, customers, seed)


def test_samples_every_laundering_linked_customer_and_some_others(world: Path) -> None:
    loaded = frames(world)
    labels = loaded["ground_truth/customer_labels.parquet"]

    sample = sample_customers(labels, loaded["ground_truth/company_labels.parquet"], 4, seed=3)

    linked = set(labels.filter("laundering_linked").get_column("customer_id"))
    assert linked <= set(sample)
    assert len(sample) == len(linked) + 4


def test_each_customer_gets_a_pack_and_companies_a_registry_extract(world: Path) -> None:
    specs = specs_for(world)
    customer_types = dict(
        pl.read_parquet(world / "customers.parquet").select("customer_id", "customer_type").rows()
    )

    for customer_id, customer_type in customer_types.items():
        doc_types = {s.doc_type for s in specs if s.customer_id == customer_id}
        expected = {"bank_statement", "financial_statement", "proof_of_address", "source_of_funds"}
        if customer_type == "company":
            expected.add("registry_extract")
        assert doc_types == expected


def test_a_statement_adds_up_unless_its_totals_are_the_anomaly(world: Path) -> None:
    for spec in [s for s in specs_for(world) if s.doc_type == "bank_statement"]:
        rows = spec.tables["transactions"]["rows"]
        total_in = round(sum(row[2] or 0 for row in rows), 2)
        anomalies = {a["type"] for a in spec.anomalies}
        assert (spec.fields["total_in"] != total_in) == ("totals_dont_add_up" in anomalies)


def test_an_edited_row_breaks_the_running_balance_where_the_truth_says(world: Path) -> None:
    edited = [
        (spec, anomaly)
        for spec in specs_for(world) + specs_for(world, seed=4) + specs_for(world, seed=5)
        for anomaly in spec.anomalies
        if anomaly["type"] == "edited_row"
    ]
    assert edited, "no edited row was drawn; widen the seeds"

    for spec, anomaly in edited:
        rows = spec.tables["transactions"]["rows"]
        index = anomaly["location"]["row"]
        previous = rows[index - 1][4] if index else spec.fields["opening_balance"]
        money_in, money_out, balance = rows[index][2] or 0, rows[index][3] or 0, rows[index][4]
        assert round(previous + money_in - money_out, 2) != balance


def test_a_profit_and_loss_adds_up_unless_its_arithmetic_is_the_anomaly(world: Path) -> None:
    for spec in [s for s in specs_for(world) if s.doc_type == "financial_statement"]:
        fields = spec.fields
        anomalies = {a["type"] for a in spec.anomalies}
        consistent = fields["gross_profit"] == fields["revenue"] - fields["cost_of_sales"]
        assert consistent == ("arithmetic_error" not in anomalies)


def test_the_rendered_html_carries_the_watermark_and_the_fields(world: Path) -> None:
    spec = next(s for s in specs_for(world) if s.doc_type == "proof_of_address")

    html = render_html(spec)

    assert "SPECIMEN" in html
    assert spec.fields["account_number"] in html


def test_the_world_holds_a_pdf_and_its_ground_truth_per_document(world: Path) -> None:
    index = pl.read_parquet(world / "documents.parquet")

    assert index.height > 0
    for document_id, path in index.select("document_id", "path").rows():
        assert (world / path).read_bytes().startswith(b"%PDF")
        truth = json.loads(
            (world / "ground_truth" / "documents" / f"{document_id}.json").read_text()
        )
        assert truth["document_id"] == document_id
        assert truth["watermark"] == "SPECIMEN"


def all_specs(world: Path) -> list[DocumentSpec]:
    return [spec for seed in range(3, 13) for spec in specs_for(world, seed)]


def test_injections_are_labelled_and_land_in_the_html(world: Path) -> None:
    injected = [spec for spec in all_specs(world) if spec.injection]
    assert {spec.injection["technique"] for spec in injected if spec.injection} == {
        "hidden_text",
        "visible_note",
        "metadata",
    }

    for spec in injected:
        assert spec.injection is not None
        html = render_html(spec)
        assert spec.injection["payload"] in html
        if spec.injection["technique"] == "metadata":
            assert f'<meta name="description" content="{spec.injection["payload"]}">' in html


def test_white_text_injections_are_never_scanned(world: Path) -> None:
    specs = all_specs(world)

    assert any(spec.scan for spec in specs)
    assert not any(
        spec.scan and spec.injection and spec.injection["technique"] == "hidden_text"
        for spec in specs
    )


def test_a_scanned_pdf_keeps_its_pages_but_loses_its_text(world: Path, tmp_path: Path) -> None:
    index = pl.read_parquet(world / "documents.parquet")
    digital = world / index.get_column("path")[0]
    copy = tmp_path / digital.name
    copy.write_bytes(digital.read_bytes())

    scan_pdf(
        copy, {"rotation_degrees": 1.5, "blur_radius": 0.6, "noise_sigma": 8, "jpeg_quality": 50}
    )
    first_scan = copy.read_bytes()
    copy.write_bytes(digital.read_bytes())
    scan_pdf(
        copy, {"rotation_degrees": 1.5, "blur_radius": 0.6, "noise_sigma": 8, "jpeg_quality": 50}
    )

    original, scanned = pdfium.PdfDocument(digital), pdfium.PdfDocument(copy)
    assert len(scanned) == len(original)
    assert original[0].get_textpage().get_text_range().strip()
    assert not scanned[0].get_textpage().get_text_range().strip()
    assert copy.read_bytes() == first_scan
