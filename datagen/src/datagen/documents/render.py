import io
import json
import os
import sys
import zlib
from collections.abc import Sequence
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

import jinja2
import numpy as np
import polars as pl
import pypdfium2 as pdfium
from PIL import Image, ImageFilter

from datagen.documents.content import DocumentSpec

SCAN_DPI = 150
SCANNED_ON = datetime(2022, 9, 1, tzinfo=UTC).timetuple()

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
    jobs = [
        (render_html(spec), documents_dir / f"{spec.document_id}.pdf", spec.scan) for spec in specs
    ]
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


def _write_pdf(job: tuple[str, Path, dict[str, float] | None]) -> None:
    # Imported here so that only rendering needs Pango, not every datagen command.
    import weasyprint

    html, path, scan = job
    weasyprint.HTML(string=html).write_pdf(path)
    if scan is not None:
        scan_pdf(path, scan)


def scan_pdf(path: Path, scan: dict[str, float]) -> None:
    """Replace a digital PDF with what a scanner would make of it: greyscale page images,
    slightly skewed, blurred, noisy and JPEG-compressed, with no text layer."""
    seed = zlib.crc32(path.name.encode())
    noise = np.random.default_rng(seed)
    pages = []
    for page in pdfium.PdfDocument(path):
        image = page.render(scale=SCAN_DPI / 72).to_pil().convert("L")
        image = image.rotate(scan["rotation_degrees"], expand=True, fillcolor=255)
        image = image.filter(ImageFilter.GaussianBlur(scan["blur_radius"]))
        pixels = np.asarray(image, dtype=np.float32)
        pixels += noise.normal(0, scan["noise_sigma"], pixels.shape)
        image = Image.fromarray(np.clip(pixels, 0, 255).astype(np.uint8))
        compressed = io.BytesIO()
        image.save(compressed, "JPEG", quality=int(scan["jpeg_quality"]))
        pages.append(Image.open(compressed))
    pages[0].save(
        path,
        "PDF",
        save_all=True,
        append_images=pages[1:],
        resolution=SCAN_DPI,
        # Fixed, so the same world always produces the same bytes.
        creationDate=SCANNED_ON,
        modDate=SCANNED_ON,
    )
