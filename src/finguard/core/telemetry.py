from collections.abc import Callable
from importlib.metadata import version
from urllib.parse import urlsplit, urlunsplit

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from finguard.core.settings import Settings

# Who called and what they searched for: a query string can carry an IBAN or a name.
_CLIENT_DETAILS = frozenset(
    {
        "url.query",
        "client.address",
        "client.port",
        "net.peer.ip",
        "net.peer.port",
        "http.client_ip",
    }
)
_URLS = frozenset({"http.url", "url.full"})
_TARGETS = frozenset({"http.target"})


class WithoutClientDetails(SpanProcessor):
    """Hands each finished span on without the request's query string or client address.

    The HTTP instrumentation sets these attributes itself, and a span can't drop an
    attribute once set, so they are removed from a copy just before export.
    """

    def __init__(self, export: SpanProcessor) -> None:
        self._export = export

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        self._export.on_start(span, parent_context)

    def on_end(self, span: ReadableSpan) -> None:
        attributes = span.attributes or {}
        if not attributes.keys() & (_CLIENT_DETAILS | _URLS | _TARGETS):
            self._export.on_end(span)
            return
        kept = {}
        for key, value in attributes.items():
            if key in _CLIENT_DETAILS:
                continue
            if key in _URLS and isinstance(value, str):
                value = urlunsplit(urlsplit(value)._replace(query="", fragment=""))
            elif key in _TARGETS and isinstance(value, str):
                value = value.partition("?")[0]
            kept[key] = value
        self._export.on_end(
            ReadableSpan(
                name=span.name,
                context=span.context,
                parent=span.parent,
                resource=span.resource,
                attributes=kept,
                events=span.events,
                links=span.links,
                kind=span.kind,
                status=span.status,
                start_time=span.start_time,
                end_time=span.end_time,
                instrumentation_scope=span.instrumentation_scope,
            )
        )

    def shutdown(self) -> None:
        self._export.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self._export.force_flush(timeout_millis)


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
    provider.add_span_processor(WithoutClientDetails(BatchSpanProcessor(exporter)))
    trace.set_tracer_provider(provider)
    return provider.shutdown
