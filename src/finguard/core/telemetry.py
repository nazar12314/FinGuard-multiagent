from collections.abc import Callable
from importlib.metadata import version

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from finguard.core.settings import Settings


def configure_telemetry(settings: Settings, *, process: str) -> Callable[[], None]:
    """Export this process's spans to the OpenTelemetry Collector when telemetry is enabled.

    Returns the function that flushes pending spans; call it when the process stops.
    """
    if not settings.telemetry.enabled:
        return lambda: None

    provider = TracerProvider(
        resource=Resource.create(
            {
                "service.name": f"{settings.service_name}-{process}",
                "service.version": version("finguard"),
                "deployment.environment.name": settings.env,
            }
        )
    )
    exporter = OTLPSpanExporter(endpoint=f"{settings.telemetry.otlp_endpoint}/v1/traces")
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return provider.shutdown
