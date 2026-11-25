import json
import os
import sys
from collections.abc import Sequence
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import jinja2
import polars as pl

from datagen.documents.content import DocumentSpec

TEMPLATES = jinja2.Environment(
    loader=jinja2.PackageLoader("datagen.documents", "templates"),
    autoescape=jinja2.select_autoescape(["html", "j2"]),
    undefined=jinja2.StrictUndefined,
)
TEMPLATES.globals["money"] = lambda amount: "" if amount is None else f"{amount:,.2f}"


def render_html(spec: DocumentSpec) -> str:
    return TEMPLATES.get_template(spec.template).render(spec.context)


def write_documents(
    specs: Sequence[DocumentSpec], documents_dir: Path, truth_dir: Path
) -> pl.DataFrame:
    """Render each spec to documents_dir/<id>.pdf with its ground truth in truth_dir/<id>.json,
    and return the index of what was written."""
    if sys.platform == "darwin":
        # uv's Python doesn't search Homebrew's lib directory for Pango. The render workers are
        # fresh processes, so they pick this up at launch.
        os.environ.setdefault("DYLD_FALLBACK_LIBRARY_PATH", "/opt/homebrew/lib")
    documents_dir.mkdir(parents=True, exist_ok=True)
    truth_dir.mkdir(parents=True, exist_ok=True)
    jobs = [(render_html(spec), documents_dir / f"{spec.document_id}.pdf") for spec in specs]
    for spec in specs:
        truth = json.dumps(spec.truth(), indent=2, ensure_ascii=False, default=str)
        (truth_dir / f"{spec.document_id}.json").write_text(truth + "\n")
    if jobs:
        with ProcessPoolExecutor(max_workers=os.cpu_count()) as pool:
            list(pool.map(_write_pdf, jobs, chunksize=16))
    return pl.DataFrame(
        {
            "document_id": [spec.document_id for spec in specs],
            "customer_id": [spec.customer_id for spec in specs],
            "doc_type": [spec.doc_type for spec in specs],
            "path": [f"documents/{spec.document_id}.pdf" for spec in specs],
        },
        schema=dict.fromkeys(["document_id", "customer_id", "doc_type", "path"], pl.String),
    ).sort("document_id")


def _write_pdf(job: tuple[str, Path]) -> None:
    # Imported here so that only rendering needs Pango, not every datagen command.
    import weasyprint

    html, path = job
    weasyprint.HTML(string=html).write_pdf(path)
