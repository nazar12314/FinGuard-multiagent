from opentelemetry import propagate, trace
from opentelemetry.context import Context
from opentelemetry.trace import Span, Status, StatusCode

from finguard.platform.errors import describe_error

tracer = trace.get_tracer("finguard")


def current_traceparent() -> str | None:
    return traceparent_of(trace.get_current_span())


def traceparent_of(span: Span) -> str | None:
    """The span's W3C traceparent, or None when it isn't being traced."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier, context=trace.set_span_in_context(span))
    return carrier.get("traceparent")


def context_from(traceparent: str | None) -> Context:
    # A malformed value yields an empty context, so the span starts a new trace.
    return propagate.extract({"traceparent": traceparent} if traceparent else {})


def record_failure(span: Span, error: BaseException) -> None:
    # Instead of span.record_exception, whose message and stack text can quote message data.
    span.set_status(Status(StatusCode.ERROR, describe_error(error)))
    span.add_event("exception", {"exception.type": type(error).__name__})
