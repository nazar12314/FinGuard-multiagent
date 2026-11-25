from opentelemetry import trace
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from finguard.infrastructure.tracing import (
    context_from,
    current_traceparent,
    record_failure,
    tracer,
)
from finguard.platform.errors import ValidationFailed


def test_nothing_is_propagated_outside_a_span(spans: InMemorySpanExporter) -> None:
    assert current_traceparent() is None


def test_a_traceparent_carries_a_span_into_another_process(spans: InMemorySpanExporter) -> None:
    with tracer.start_as_current_span("api"):
        traceparent = current_traceparent()

    with tracer.start_as_current_span("consumer", context=context_from(traceparent)):
        pass

    api, consumer = spans.get_finished_spans()
    assert consumer.context is not None
    assert api.context is not None
    assert consumer.context.trace_id == api.context.trace_id
    assert consumer.parent is not None
    assert consumer.parent.span_id == api.context.span_id


def test_a_malformed_traceparent_starts_a_new_trace(spans: InMemorySpanExporter) -> None:
    with tracer.start_as_current_span("consumer", context=context_from("not-a-traceparent")):
        pass

    [consumer] = spans.get_finished_spans()
    assert consumer.parent is None


def test_failures_are_recorded_without_the_error_text(spans: InMemorySpanExporter) -> None:
    with tracer.start_as_current_span("handle") as span:
        record_failure(span, ConnectionError("Key (iban)=(DE89 3704 0044) already exists"))
    with tracer.start_as_current_span("handle") as span:
        record_failure(span, ValidationFailed("case already closed"))

    driver_error, application_error = spans.get_finished_spans()
    assert driver_error.status.status_code is StatusCode.ERROR
    assert driver_error.status.description == "ConnectionError"
    assert [dict(event.attributes or {}) for event in driver_error.events] == [
        {"exception.type": "ConnectionError"}
    ]
    assert application_error.status.description == "ValidationFailed: case already closed"
    assert trace.get_current_span() is trace.INVALID_SPAN
