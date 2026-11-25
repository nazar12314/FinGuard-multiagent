import logging
import re
import sys
from collections.abc import Mapping, MutableMapping
from enum import StrEnum
from typing import Any, TextIO

import structlog
from opentelemetry import trace

from finguard.core.settings import Settings


class LogEvent(StrEnum):
    """Stable event names. Log business and lifecycle events with these, never free text."""

    APP_STARTING = "app.starting"
    APP_STOPPED = "app.stopped"
    HTTP_REQUEST_COMPLETED = "http.request.completed"
    HTTP_UNHANDLED_ERROR = "http.unhandled_error"
    HTTP_SERVER_ERROR = "http.server_error"
    KAFKA_TOPICS_ENSURED = "kafka.topics_ensured"
    KAFKA_TOPIC_PARTITIONS_DIFFER = "kafka.topic_partitions_differ"
    OUTBOX_BATCH_PUBLISHED = "outbox.batch_published"
    OUTBOX_PUBLISH_FAILED = "outbox.publish_failed"
    OUTBOX_PUBLISH_TIMED_OUT = "outbox.publish_timed_out"
    OUTBOX_MESSAGE_PARKED = "outbox.message_parked"
    OUTBOX_RELAY_GAVE_UP = "outbox.relay_gave_up"
    OUTBOX_PRUNED = "outbox.pruned"
    OUTBOX_PRUNE_FAILED = "outbox.prune_failed"
    OUTBOX_BACKLOG_UNMEASURED = "outbox.backlog_unmeasured"
    WATCHLIST_IMPORTED = "screening.watchlist_imported"


# Matches whole trailing name segments, so access_token is masked but token_count is not.
_SENSITIVE_KEY = re.compile(
    r"(^|[_-])(password|passwd|secret([_-]?key)?|token|authorization|cookie"
    r"|api[_-]?key|private[_-]?key|dsn|credentials?)$",
    re.IGNORECASE,
)
_URL_CREDENTIALS = re.compile(r"(?P<scheme>[a-z][a-z0-9+.-]*://)[^/@\s]+@", re.IGNORECASE)
_MASK = "***"


def redact(value: Any, key: str | None = None) -> Any:
    if key is not None and _SENSITIVE_KEY.search(key):
        return _MASK
    if isinstance(value, Mapping):
        return {k: redact(v, str(k)) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [redact(v) for v in value]
    if isinstance(value, str):
        return _URL_CREDENTIALS.sub(rf"\g<scheme>{_MASK}@", value)
    return value


def _redact_secrets(
    _: Any, __: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    # stdlib messages carry formatted arguments, so the event text is scrubbed too.
    return {k: redact(v) if k == "event" else redact(v, k) for k, v in event_dict.items()}


def _add_trace_ids(
    _: Any, __: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    context = trace.get_current_span().get_span_context()
    if context.is_valid:
        event_dict["trace_id"] = format(context.trace_id, "032x")
        event_dict["span_id"] = format(context.span_id, "016x")
    return event_dict


def configure_logging(settings: Settings, stream: TextIO | None = None) -> None:
    """Send structlog and stdlib logging (uvicorn, libraries) through one renderer."""
    # No frame locals (they hold DSNs and keys), and rendered before redaction runs.
    exception_renderer: structlog.types.Processor
    renderer: structlog.types.Processor
    if settings.log_format == "json":
        exception_renderer = structlog.processors.ExceptionRenderer(
            structlog.tracebacks.ExceptionDictTransformer(show_locals=False)
        )
        renderer = structlog.processors.JSONRenderer()
    else:
        exception_renderer = structlog.processors.format_exc_info
        renderer = structlog.dev.ConsoleRenderer()

    shared: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        _add_trace_ids,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        exception_renderer,
        _redact_secrets,
    ]
    structlog.configure(
        processors=[*shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=shared,
            processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, renderer],
        )
    )
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(settings.log_level)

    for name in ("uvicorn", "uvicorn.error"):
        logging.getLogger(name).handlers.clear()
        logging.getLogger(name).propagate = True
    # The request middleware logs one structured line per request instead.
    logging.getLogger("uvicorn.access").disabled = True


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.stdlib.get_logger(name)
