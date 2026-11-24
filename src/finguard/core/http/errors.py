from collections.abc import Mapping
from typing import cast

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException

from finguard.core.auth import Unauthenticated
from finguard.core.logging import LogEvent, get_logger
from finguard.platform.errors import (
    ApplicationError,
    Conflict,
    DependencyUnavailable,
    Forbidden,
    NotFound,
    ValidationFailed,
)

# Subclasses inherit the status of their nearest listed ancestor.
STATUS_BY_ERROR: dict[type[ApplicationError], int] = {
    Unauthenticated: 401,
    Forbidden: 403,
    NotFound: 404,
    Conflict: 409,
    ValidationFailed: 422,
    DependencyUnavailable: 503,
}

_CODE_BY_HTTP_STATUS = {
    401: "unauthenticated",
    403: "forbidden",
    404: "not_found",
    405: "method_not_allowed",
    413: "payload_too_large",
}

logger = get_logger(__name__)


def error_response(
    status_code: int,
    code: str,
    message: str,
    request_id: str | None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    return JSONResponse(
        {"error": {"code": code, "message": message, "request_id": request_id}},
        status_code=status_code,
        headers=headers,
    )


def status_for(error: ApplicationError) -> int:
    for cls in type(error).__mro__:
        if cls in STATUS_BY_ERROR:
            return STATUS_BY_ERROR[cls]
    return 500


def _request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None)


async def _application_error(request: Request, exc: Exception) -> JSONResponse:
    error = cast(ApplicationError, exc)
    status_code = status_for(error)
    if status_code == 500:
        # An error with no mapped status is a programming mistake: keep the traceback.
        logger.error(LogEvent.HTTP_SERVER_ERROR, code=error.code, exc_info=error)
    elif status_code >= 500:
        logger.warning(LogEvent.HTTP_SERVER_ERROR, code=error.code, message=error.message)
    return error_response(status_code, error.code, error.message, _request_id(request))


async def _request_validation_error(request: Request, exc: Exception) -> JSONResponse:
    problems = "; ".join(
        f"{'.'.join(str(part) for part in problem['loc'])}: {problem['msg']}"
        for problem in cast(RequestValidationError, exc).errors()
    )
    return error_response(
        422, ValidationFailed.code, f"Request validation failed: {problems}", _request_id(request)
    )


async def _http_error(request: Request, exc: Exception) -> JSONResponse:
    http_error = cast(HTTPException, exc)
    return error_response(
        http_error.status_code,
        _CODE_BY_HTTP_STATUS.get(http_error.status_code, "http_error"),
        str(http_error.detail),
        _request_id(request),
        headers=http_error.headers,
    )


def install_exception_handlers(app: FastAPI) -> None:
    # Unhandled exceptions are rendered by RequestContextMiddleware, which owns the
    # request id and must log them.
    app.add_exception_handler(ApplicationError, _application_error)
    app.add_exception_handler(RequestValidationError, _request_validation_error)
    app.add_exception_handler(HTTPException, _http_error)
