import json
import logging
from collections.abc import Iterator
from io import StringIO
from typing import Any

import pytest
import structlog

from finguard.core.logging import configure_logging, get_logger, redact
from finguard.core.settings import Settings


@pytest.fixture
def log_output() -> Iterator[StringIO]:
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    output = StringIO()
    configure_logging(Settings(log_format="json"), stream=output)
    yield output
    root.handlers[:], root.level = handlers, level
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()


def last_line(output: StringIO) -> dict[str, Any]:
    line: dict[str, Any] = json.loads(output.getvalue().strip().splitlines()[-1])
    return line


@pytest.mark.parametrize(
    ("key", "value", "expected"),
    [
        ("api_key", "sk-123", "***"),
        ("Authorization", "Bearer abc", "***"),
        ("database_dsn", "postgresql://u:p@db/x", "***"),
        ("url", "postgresql://user:pw@db:5432/finguard", "postgresql://***@db:5432/finguard"),
        ("headers", {"cookie": "c", "accept": "json"}, {"cookie": "***", "accept": "json"}),
        ("case_id", "c-1", "c-1"),
    ],
)
def test_redact_masks_secrets(key: str, value: object, expected: object) -> None:
    assert redact(value, key) == expected


def test_log_lines_are_json_with_context_and_without_secrets(log_output: StringIO) -> None:
    structlog.contextvars.bind_contextvars(request_id="r-1")

    get_logger("finguard.test").info("cases.case.opened", case_id="c-1", token="t-secret")

    line = last_line(log_output)
    assert line["event"] == "cases.case.opened"
    assert line["request_id"] == "r-1"
    assert line["case_id"] == "c-1"
    assert line["token"] == "***"
    assert line["level"] == "info"
    assert line["timestamp"].endswith("Z")


def test_stdlib_loggers_are_rendered_the_same_way(log_output: StringIO) -> None:
    logging.getLogger("uvicorn.error").warning("Started server process")

    line = last_line(log_output)
    assert line["event"] == "Started server process"
    assert line["logger"] == "uvicorn.error"
