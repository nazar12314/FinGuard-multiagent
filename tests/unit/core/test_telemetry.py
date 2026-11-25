import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from finguard.core.settings import Settings, TelemetrySettings
from finguard.core.telemetry import configure_telemetry
from finguard.infrastructure.tracing import tracer


def test_nothing_is_exported_unless_telemetry_is_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    installed: list[object] = []
    monkeypatch.setattr(trace, "set_tracer_provider", installed.append)

    flush = configure_telemetry(Settings(env="test"), process="api")
    flush()

    assert installed == []


def test_each_process_reports_as_its_own_service(monkeypatch: pytest.MonkeyPatch) -> None:
    installed: list[TracerProvider] = []
    monkeypatch.setattr(trace, "set_tracer_provider", installed.append)
    settings = Settings(env="test", telemetry=TelemetrySettings(enabled=True))

    flush = configure_telemetry(settings, process="outbox-relay")
    flush()

    [provider] = installed
    assert provider.resource.attributes["service.name"] == "finguard-outbox-relay"
    assert provider.resource.attributes["deployment.environment.name"] == "test"


def test_exported_spans_keep_other_attributes_and_their_links(spans: InMemorySpanExporter) -> None:
    with (
        tracer.start_as_current_span(
            "GET /cases",
            attributes={
                "url.full": "https://api.example/cases?name=Jane#top",
                "url.query": "name=Jane",
                "client.address": "203.0.113.7",
                "http.route": "/cases",
            },
        ) as parent,
        tracer.start_as_current_span("child"),
    ):
        pass

    child, request = spans.get_finished_spans()
    assert dict(request.attributes or {}) == {
        "url.full": "https://api.example/cases",
        "http.route": "/cases",
    }
    assert request.context == parent.get_span_context()
    assert child.parent is not None
    assert child.parent.span_id == parent.get_span_context().span_id
