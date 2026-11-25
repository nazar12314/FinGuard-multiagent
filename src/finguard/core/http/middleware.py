import re
import time

import structlog
from prometheus_client import Counter, Histogram
from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from finguard.core.http.errors import error_response
from finguard.core.logging import LogEvent, get_logger
from finguard.core.ops import OPS_PATHS
from finguard.platform.ids import new_id

REQUEST_ID_HEADER = "X-Request-ID"
_VALID_REQUEST_ID = re.compile(r"[A-Za-z0-9._-]{1,128}")

logger = get_logger(__name__)

# Labelled by the route template, not the path, so ids in URLs don't create new series.
HTTP_REQUESTS = Counter(
    "finguard_http_requests", "HTTP requests handled", ["method", "route", "status"]
)
HTTP_REQUEST_DURATION = Histogram(
    "finguard_http_request_duration_seconds", "Time to handle an HTTP request", ["method", "route"]
)
# Any token is a valid method, so anything else is counted as "other" to bound the series.
_KNOWN_METHODS = frozenset({"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"})


class RequestContextMiddleware:
    """Assigns a request id, logs one line per request and renders unhandled errors.

    Pure ASGI rather than BaseHTTPMiddleware, so streaming responses aren't buffered.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Probes and scrapes arrive every few seconds; they would drown the request log.
        if scope["type"] != "http" or scope["path"] in OPS_PATHS:
            await self.app(scope, receive, send)
            return

        incoming = Headers(scope=scope).get(REQUEST_ID_HEADER, "")
        request_id = incoming if _VALID_REQUEST_ID.fullmatch(incoming) else new_id().hex
        scope.setdefault("state", {})["request_id"] = request_id
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id=request_id)

        status_code = 500
        response_started = False
        started_at = time.perf_counter()

        async def send_with_request_id(message: Message) -> None:
            nonlocal status_code, response_started
            if message["type"] == "http.response.start":
                response_started = True
                status_code = message["status"]
                MutableHeaders(scope=message)[REQUEST_ID_HEADER] = request_id
            await send(message)

        try:
            await self.app(scope, receive, send_with_request_id)
        except Exception:
            logger.exception(LogEvent.HTTP_UNHANDLED_ERROR)
            if response_started:
                # The client already got the original status; log the request as failed.
                status_code = 500
                raise
            response = error_response(500, "internal_error", "Internal server error", request_id)
            await response(scope, receive, send_with_request_id)
        finally:
            duration_s = time.perf_counter() - started_at
            route = getattr(scope.get("route"), "path", "unmatched")
            method = scope["method"] if scope["method"] in _KNOWN_METHODS else "other"
            HTTP_REQUESTS.labels(method, route, str(status_code)).inc()
            HTTP_REQUEST_DURATION.labels(method, route).observe(duration_s)
            logger.info(
                LogEvent.HTTP_REQUEST_COMPLETED,
                method=scope["method"],
                path=scope["path"],
                status=status_code,
                duration_ms=round(duration_s * 1000, 2),
            )
            structlog.contextvars.clear_contextvars()
