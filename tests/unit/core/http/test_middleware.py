from collections.abc import Callable
from typing import Any

import structlog
from starlette.responses import PlainTextResponse
from starlette.types import Message, Receive, Scope, Send

from finguard.core.http.middleware import RequestContextMiddleware
from finguard.core.logging import get_logger

LogLines = Callable[[], list[dict[str, Any]]]


async def endpoint(scope: Scope, receive: Receive, send: Send) -> None:
    get_logger("finguard.test").info("handler.ran")
    await PlainTextResponse("ok")(scope, receive, send)


async def call(headers: list[tuple[bytes, bytes]] | None = None) -> list[Message]:
    sent: list[Message] = []

    async def receive() -> Message:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: Message) -> None:
        sent.append(message)

    scope: Scope = {"type": "http", "method": "GET", "path": "/", "headers": headers or []}
    await RequestContextMiddleware(endpoint)(scope, receive, send)
    return sent


def response_request_id(sent: list[Message]) -> str:
    start = next(message for message in sent if message["type"] == "http.response.start")
    headers: dict[str, str] = {name.decode(): value.decode() for name, value in start["headers"]}
    return headers["x-request-id"]


async def test_log_context_does_not_leak_between_requests(log_lines: LogLines) -> None:
    structlog.contextvars.bind_contextvars(case_id="from-an-earlier-request")

    sent = await call()

    handler_line = next(line for line in log_lines() if line["event"] == "handler.ran")
    assert handler_line["request_id"] == response_request_id(sent)
    assert "case_id" not in handler_line
    assert structlog.contextvars.get_contextvars() == {}


async def test_a_request_id_with_a_trailing_newline_is_replaced() -> None:
    sent = await call(headers=[(b"x-request-id", b"abc\n")])

    assert response_request_id(sent) != "abc\n"
