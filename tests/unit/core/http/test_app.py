from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import AsyncExitStack
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind

from finguard.core.container import Container, build_api_container
from finguard.core.http.app import create_app
from finguard.core.http.dependencies import CurrentPrincipal
from finguard.core.ops import Readiness
from finguard.core.settings import Environment, Settings
from finguard.platform.errors import (
    ApplicationError,
    Conflict,
    DependencyUnavailable,
    NotFound,
)

USER_ID = "0196d5a0-0000-7000-8000-000000000001"

LogLines = Callable[[], list[dict[str, Any]]]


class DuplicateDecision(Conflict):
    code = "duplicate_decision"


def build_app(settings: Settings) -> FastAPI:
    app = create_app(settings)

    @app.get("/ok")
    async def ok() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/broken-stream")
    async def broken_stream() -> StreamingResponse:
        async def chunks() -> AsyncIterator[bytes]:
            yield b"partial"
            raise RuntimeError("stream broke")

        return StreamingResponse(chunks())

    @app.get("/missing")
    async def missing() -> None:
        raise NotFound("Case", 42)

    @app.get("/duplicate")
    async def duplicate() -> None:
        raise DuplicateDecision("decision already recorded")

    @app.get("/database-down")
    async def database_down() -> None:
        raise DependencyUnavailable("database unavailable")

    @app.get("/unmapped")
    async def unmapped() -> None:
        raise ApplicationError("no status for this error")

    @app.get("/crash")
    async def crash() -> None:
        raise RuntimeError("boom")

    @app.get("/limit")
    async def limit(size: int) -> int:
        return size

    @app.get("/me")
    async def me(principal: CurrentPrincipal) -> dict[str, object]:
        return {"user_id": str(principal.user_id), "roles": sorted(principal.roles)}

    return app


@pytest.fixture
def client() -> Iterator[TestClient]:
    with TestClient(build_app(Settings(env="test")), raise_server_exceptions=False) as client:
        yield client


@pytest.mark.parametrize(
    ("path", "status", "code", "message"),
    [
        ("/missing", 404, "not_found", "Case 42 not found"),
        ("/duplicate", 409, "duplicate_decision", "decision already recorded"),
        ("/database-down", 503, "dependency_unavailable", "database unavailable"),
        ("/unmapped", 500, "application_error", "no status for this error"),
        ("/crash", 500, "internal_error", "Internal server error"),
        ("/no-such-route", 404, "not_found", "Not Found"),
        ("/me", 401, "unauthenticated", "missing X-Dev-User-Id header"),
    ],
)
def test_errors_share_one_json_shape_with_the_request_id(
    client: TestClient, path: str, status: int, code: str, message: str
) -> None:
    response = client.get(path)

    assert response.status_code == status
    assert response.json() == {
        "error": {
            "code": code,
            "message": message,
            "request_id": response.headers["X-Request-ID"],
        }
    }


def test_request_validation_errors_name_the_failing_field(client: TestClient) -> None:
    response = client.get("/limit", params={"size": "many"})

    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == "validation_failed"
    assert "query.size" in error["message"]


def test_a_valid_incoming_request_id_is_kept(client: TestClient) -> None:
    response = client.get("/missing", headers={"X-Request-ID": "trace-me.1"})

    assert response.headers["X-Request-ID"] == "trace-me.1"
    assert response.json()["error"]["request_id"] == "trace-me.1"


def test_a_malformed_incoming_request_id_is_replaced(client: TestClient) -> None:
    response = client.get("/missing", headers={"X-Request-ID": "bad id with spaces"})

    assert response.headers["X-Request-ID"] != "bad id with spaces"
    assert len(response.headers["X-Request-ID"]) == 32


def test_routes_receive_the_authenticated_principal(client: TestClient) -> None:
    response = client.get("/me", headers={"X-Dev-User-Id": USER_ID, "X-Dev-Roles": "admin"})

    assert response.status_code == 200
    assert response.json() == {"user_id": USER_ID, "roles": ["admin"]}


@pytest.mark.parametrize(("env", "status"), [("dev", 401), ("prod", 404)])
def test_dev_endpoints_are_left_out_in_prod(env: Environment, status: int) -> None:
    # model_construct skips validation: prod has no real authenticator yet.
    with TestClient(create_app(Settings.model_construct(env=env))) as client:
        # Without credentials the route answers 401 where it exists and 404 where it doesn't.
        assert client.post("/api/v1/dev/pings", json={}).status_code == status


@pytest.mark.parametrize(("env", "docs_status"), [("dev", 200), ("prod", 404)])
def test_openapi_docs_are_hidden_in_prod(env: Environment, docs_status: int) -> None:
    # model_construct skips validation: prod has no real authenticator yet.
    with TestClient(create_app(Settings.model_construct(env=env))) as client:
        assert client.get("/docs").status_code == docs_status
        assert client.get("/openapi.json").status_code == docs_status


def test_successful_responses_carry_a_request_id(client: TestClient) -> None:
    response = client.get("/ok")

    assert response.status_code == 200
    assert len(response.headers["X-Request-ID"]) == 32


def test_each_request_is_logged_once_with_its_outcome(
    log_lines: LogLines, client: TestClient
) -> None:
    response = client.get("/missing")

    completed = [line for line in log_lines() if line["event"] == "http.request.completed"]
    assert len(completed) == 1
    assert completed[0]["request_id"] == response.headers["X-Request-ID"]
    assert completed[0]["method"] == "GET"
    assert completed[0]["path"] == "/missing"
    assert completed[0]["status"] == 404
    assert completed[0]["duration_ms"] >= 0


def test_a_response_that_fails_mid_stream_is_logged_as_failed(
    log_lines: LogLines, client: TestClient
) -> None:
    client.get("/broken-stream")

    completed = next(line for line in log_lines() if line["event"] == "http.request.completed")
    assert completed["status"] == 500


@pytest.mark.parametrize(
    ("path", "level", "has_traceback"),
    [("/unmapped", "error", True), ("/database-down", "warning", False)],
)
def test_server_side_application_errors_are_logged(
    log_lines: LogLines, client: TestClient, path: str, level: str, has_traceback: bool
) -> None:
    client.get(path)

    logged = next(line for line in log_lines() if line["event"] == "http.server_error")
    assert logged["level"] == level
    assert ("exception" in logged) is has_traceback


class FakeClient:
    def __init__(self) -> None:
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


def test_clients_opened_at_startup_are_closed_on_shutdown() -> None:
    kafka = FakeClient()

    async def factory(settings: Settings, resources: AsyncExitStack) -> Container:
        resources.push_async_callback(kafka.aclose)
        return await build_api_container(settings, resources)

    with TestClient(create_app(Settings(env="test"), container_factory=factory)):
        assert not kafka.closed
    assert kafka.closed


def test_clients_opened_before_a_startup_failure_are_closed() -> None:
    kafka = FakeClient()

    async def factory(settings: Settings, resources: AsyncExitStack) -> Container:
        resources.push_async_callback(kafka.aclose)
        raise DependencyUnavailable("database unavailable")

    app = create_app(Settings(env="test"), container_factory=factory)
    with pytest.raises(DependencyUnavailable), TestClient(app):
        pass
    assert kafka.closed


def test_each_request_is_traced_and_its_log_line_names_the_trace(
    log_lines: LogLines, spans: InMemorySpanExporter
) -> None:
    with TestClient(build_app(Settings(env="test"))) as client:
        client.get("/ok")
        client.get("/docs")

    # Only the server span: /docs is excluded, and so are the ASGI receive and send spans.
    [server] = spans.get_finished_spans()
    assert server.kind is SpanKind.SERVER
    assert server.name == "GET /ok"
    assert server.context is not None
    completed = [line for line in log_lines() if line["event"] == "http.request.completed"]
    ok_line = next(line for line in completed if line["path"] == "/ok")
    assert ok_line["trace_id"] == format(server.context.trace_id, "032x")


def test_the_api_serves_probes_and_metrics_without_logging_them(log_lines: LogLines) -> None:
    async def database_down() -> None:
        raise ConnectionRefusedError("connection refused")

    async def factory(settings: Settings, resources: AsyncExitStack) -> Container:
        container = await build_api_container(settings, resources)
        readiness = Readiness()
        readiness.require("database", database_down)
        return Container(
            settings=container.settings,
            authenticator=container.authenticator,
            session_factory=container.session_factory,
            readiness=readiness,
        )

    with TestClient(build_app(Settings(env="test")), raise_server_exceptions=False) as client:
        client.get("/ok")
        metrics = client.get("/metrics").text
    with TestClient(create_app(Settings(env="test"), container_factory=factory)) as client:
        healthz = client.get("/healthz")
        readyz = client.get("/readyz")

    assert healthz.status_code == 200
    assert readyz.status_code == 503
    assert readyz.json()["checks"] == {"database": "ConnectionRefusedError"}
    # Requests are counted by route template, and probes never reach the request log.
    assert 'finguard_http_requests_total{method="GET",route="/ok",status="200"}' in metrics
    logged_paths = {
        line.get("path") for line in log_lines() if line["event"] == "http.request.completed"
    }
    assert logged_paths == {"/ok"}


def test_request_spans_leave_out_the_query_string_and_the_caller(
    spans: InMemorySpanExporter,
) -> None:
    with TestClient(build_app(Settings(env="test"))) as client:
        client.get("/limit", params={"size": "7", "iban": "DE89370400440532013000"})

    [server] = spans.get_finished_spans()
    attributes = dict(server.attributes or {})
    assert "DE89" not in str(attributes)
    assert attributes["http.url"] == "http://testserver/limit"
    assert attributes["http.target"] == "/limit"
    assert not attributes.keys() & {"net.peer.ip", "net.peer.port", "client.address"}


def test_only_the_docs_paths_themselves_go_untraced(spans: InMemorySpanExporter) -> None:
    app = build_app(Settings(env="test"))

    @app.get("/cases/docs")
    async def case_docs() -> list[str]:
        return []

    with TestClient(app) as client:
        client.get("/docs")
        client.get("/openapi.json")
        client.get("/cases/docs")

    assert [span.name for span in spans.get_finished_spans()] == ["GET /cases/docs"]
