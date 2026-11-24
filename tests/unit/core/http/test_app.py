from collections.abc import Iterator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from finguard.core.http.app import create_app
from finguard.core.http.dependencies import CurrentPrincipal
from finguard.core.settings import Environment, Settings
from finguard.platform.errors import (
    ApplicationError,
    Conflict,
    DependencyUnavailable,
    NotFound,
)

USER_ID = "0196d5a0-0000-7000-8000-000000000001"


class DuplicateDecision(Conflict):
    code = "duplicate_decision"


def build_app(settings: Settings) -> FastAPI:
    app = create_app(settings)

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


@pytest.mark.parametrize(("env", "docs_status"), [("dev", 200), ("prod", 404)])
def test_openapi_docs_are_hidden_in_prod(env: Environment, docs_status: int) -> None:
    # model_construct skips validation: prod has no real authenticator yet.
    with TestClient(create_app(Settings.model_construct(env=env))) as client:
        assert client.get("/docs").status_code == docs_status
        assert client.get("/openapi.json").status_code == docs_status
