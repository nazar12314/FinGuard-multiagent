import json
import logging
from collections.abc import Callable, Iterator
from io import StringIO
from typing import Any

import pytest
import structlog
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from finguard.core.logging import configure_logging
from finguard.core.settings import Settings
from finguard.core.telemetry import WithoutClientDetails


@pytest.fixture
def log_lines() -> Iterator[Callable[[], list[dict[str, Any]]]]:
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    output = StringIO()
    configure_logging(Settings(env="test", log_format="json"), stream=output)
    yield lambda: [json.loads(line) for line in output.getvalue().splitlines()]
    root.handlers[:], root.level = handlers, level
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()


@pytest.fixture(scope="session")
def span_exporter() -> InMemorySpanExporter:
    # OpenTelemetry allows one global tracer provider per process, so it is set once.
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(WithoutClientDetails(SimpleSpanProcessor(exporter)))
    trace.set_tracer_provider(provider)
    return exporter


@pytest.fixture
def spans(span_exporter: InMemorySpanExporter) -> Iterator[InMemorySpanExporter]:
    """Records the spans finished during the test."""
    span_exporter.clear()
    yield span_exporter
    span_exporter.clear()
