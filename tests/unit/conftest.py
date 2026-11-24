import json
import logging
from collections.abc import Callable, Iterator
from io import StringIO
from typing import Any

import pytest
import structlog

from finguard.core.logging import configure_logging
from finguard.core.settings import Settings


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
