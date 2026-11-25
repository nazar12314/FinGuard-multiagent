import json
import logging
from collections.abc import Callable
from typing import Any, NamedTuple

import pytest
import structlog
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from finguard.core.logging import get_logger, redact
from finguard.infrastructure.tracing import tracer

LogLines = Callable[[], list[dict[str, Any]]]


class Point(NamedTuple):
    x: int
    y: int


@pytest.mark.parametrize(
    ("key", "value", "expected"),
    [
        ("api_key", "sk-123", "***"),
        ("x-api-key", "sk-123", "***"),
        ("Authorization", "Bearer abc", "***"),
        ("access_token", "t", "***"),
        ("private_key", "k", "***"),
        ("database_dsn", "postgresql://u:p@db/x", "***"),
        ("url", "postgresql://user:pw@db:5432/finguard", "postgresql://***@db:5432/finguard"),
        ("headers", {"cookie": "c", "accept": "json"}, {"cookie": "***", "accept": "json"}),
        ("hosts", ("postgresql://u:p@db/x",), ["postgresql://***@db/x"]),
        ("case_id", "c-1", "c-1"),
        ("prompt_tokens", 812, 812),
        ("token_count", 3, 3),
        ("checks_passed", True, True),
    ],
)
def test_redact_masks_secrets(key: str, value: object, expected: object) -> None:
    assert redact(value, key) == expected


def test_log_lines_are_json_with_context_and_without_secrets(log_lines: LogLines) -> None:
    structlog.contextvars.bind_contextvars(request_id="r-1")

    get_logger("finguard.test").info("cases.case.opened", case_id="c-1", token="t-secret")

    line = log_lines()[-1]
    assert line["event"] == "cases.case.opened"
    assert line["request_id"] == "r-1"
    assert line["case_id"] == "c-1"
    assert line["token"] == "***"
    assert line["level"] == "info"
    assert line["timestamp"].endswith("Z")


def test_stdlib_loggers_are_rendered_the_same_way(log_lines: LogLines) -> None:
    logging.getLogger("uvicorn.error").warning("Started server process")

    line = log_lines()[-1]
    assert line["event"] == "Started server process"
    assert line["logger"] == "uvicorn.error"


def test_credentials_formatted_into_a_message_are_scrubbed(log_lines: LogLines) -> None:
    logging.getLogger("some.library").info("connecting to %s", "postgresql://u:pw@db/x")

    assert log_lines()[-1]["event"] == "connecting to postgresql://***@db/x"


def test_credentials_in_exception_text_are_scrubbed(log_lines: LogLines) -> None:
    try:
        raise ConnectionError("cannot reach postgresql://u:pw@db/x")
    except ConnectionError:
        get_logger("finguard.test").exception("db.connect_failed")

    rendered = json.dumps(log_lines()[-1]["exception"])
    assert "postgresql://***@db/x" in rendered
    assert "pw@" not in rendered


def test_logging_a_named_tuple_does_not_raise(log_lines: LogLines) -> None:
    get_logger("finguard.test").info("graph.hop", point=Point(1, 2))

    assert log_lines()[-1]["point"] == [1, 2]


def test_log_lines_inside_a_span_carry_its_trace_and_span_ids(
    log_lines: LogLines, spans: InMemorySpanExporter
) -> None:
    with tracer.start_as_current_span("handle"):
        get_logger("finguard.test").info("cases.case.opened")
    get_logger("finguard.test").info("cases.case.closed")

    [span] = spans.get_finished_spans()
    assert span.context is not None
    inside, outside = log_lines()[-2:]
    assert inside["trace_id"] == format(span.context.trace_id, "032x")
    assert inside["span_id"] == format(span.context.span_id, "016x")
    assert "trace_id" not in outside
