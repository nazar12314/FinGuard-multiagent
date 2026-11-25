from opentelemetry import propagate, trace
from opentelemetry.context import Context
from opentelemetry.trace import Span, Status, StatusCode

from finguard.platform.errors import describe_error

tracer = trace.get_tracer("finguard")


def current_traceparent() -> str | None:
    """The W3C traceparent of the active span, or None when nothing is being traced."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier.get("traceparent")


def context_from(traceparent: str | None) -> Context:
    # A malformed value yields an empty context, so the span starts a new trace.
    return propagate.extract({"traceparent": traceparent} if traceparent else {})


def record_failure(span: Span, error: Exception) -> None:
    # Instead of span.record_exception, whose message and stack text can quote message data.
    span.set_status(Status(StatusCode.ERROR, describe_error(error)))
    span.add_event("exception", {"exception.type": type(error).__name__})
