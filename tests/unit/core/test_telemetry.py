import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from finguard.core.settings import Settings, TelemetrySettings
from finguard.core.telemetry import configure_telemetry


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
