import asyncio
import contextlib
import signal
import time
from collections.abc import Awaitable, Callable, Iterator
from contextlib import AsyncExitStack

import uvicorn
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import BaseRoute, Route

from finguard.core.settings import OpsSettings
from finguard.platform.errors import describe_error

ReadinessCheck = Callable[[], Awaitable[object]]
OPS_PATHS = frozenset({"/healthz", "/readyz", "/metrics"})


class Readiness:
    def __init__(self, *, timeout_s: float = 2.0) -> None:
        self._checks: dict[str, ReadinessCheck] = {}
        self._timeout_s = timeout_s
        # Set on SIGTERM, so no new work arrives while the process drains.
        self.draining = False

    def require(self, name: str, check: ReadinessCheck) -> None:
        self._checks[name] = check

    async def report(self) -> tuple[bool, dict[str, str]]:
        if self.draining:
            return False, {"process": "shutting down"}
        if not self._checks:
            # Fail closed: a process wired without checks would otherwise be ready forever.
            return False, {"process": "no readiness checks registered"}
        results = await asyncio.gather(*(self._run(check) for check in self._checks.values()))
        report = dict(zip(self._checks, results, strict=True))
        return all(result == "ok" for result in report.values()), report

    async def _run(self, check: ReadinessCheck) -> str:
        try:
            async with asyncio.timeout(self._timeout_s):
                await check()
        except Exception as error:
            return describe_error(error)
        return "ok"


class Heartbeat:
    """Liveness for a worker stuck in an await, which still answers HTTP but stops beating."""

    def __init__(self, *, stale_after_s: float) -> None:
        self._stale_after_s = stale_after_s
        self._last_beat = time.monotonic()

    def beat(self) -> None:
        self._last_beat = time.monotonic()

    @property
    def silent_for_s(self) -> float:
        return time.monotonic() - self._last_beat

    @property
    def stale(self) -> bool:
        return self.silent_for_s > self._stale_after_s


def ops_routes(
    readiness_of: Callable[[Request], Readiness], heartbeat: Heartbeat | None = None
) -> list[BaseRoute]:
    async def healthz(request: Request) -> Response:
        # Without a heartbeat (the API), answering at all shows the event loop is alive.
        if heartbeat is not None and heartbeat.stale:
            return JSONResponse(
                {"status": "stalled", "silent_for_s": round(heartbeat.silent_for_s, 1)},
                status_code=503,
            )
        return JSONResponse({"status": "ok"})

    async def readyz(request: Request) -> Response:
        ready, checks = await readiness_of(request).report()
        return JSONResponse(
            {"status": "ready" if ready else "not_ready", "checks": checks},
            status_code=200 if ready else 503,
        )

    async def metrics(request: Request) -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return [
        Route("/healthz", healthz),
        Route("/readyz", readyz),
        Route("/metrics", metrics),
    ]


class _ServerWithoutSignals(uvicorn.Server):
    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        # The process's own SIGTERM handler stops this server.
        yield


class OpsServer:
    """Serves /healthz, /readyz and /metrics for a process that isn't an HTTP server."""

    def __init__(self, readiness: Readiness, heartbeat: Heartbeat, *, host: str, port: int) -> None:
        app = Starlette(routes=ops_routes(lambda _: readiness, heartbeat))
        self._server = _ServerWithoutSignals(
            uvicorn.Config(
                app, host=host, port=port, log_config=None, access_log=False, lifespan="off"
            )
        )
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._server.serve())
        while not self._server.started:
            if self._task.done():
                # Re-raises uvicorn's SystemExit(1) from a failed bind.
                await self._task
            await asyncio.sleep(0.01)

    async def stop(self) -> None:
        self._server.should_exit = True
        if self._task is not None:
            await self._task


def stop_on_signals(stop: asyncio.Event, readiness: Readiness) -> None:
    """On SIGTERM or SIGINT, report not ready at once, then let the process wind down."""

    def request_stop() -> None:
        readiness.draining = True
        stop.set()

    loop = asyncio.get_running_loop()
    for stop_signal in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(stop_signal, request_stop)


async def start_ops_server(
    settings: OpsSettings,
    readiness: Readiness,
    heartbeat: Heartbeat,
    resources: AsyncExitStack,
    *,
    default_port: int,
) -> None:
    server = OpsServer(readiness, heartbeat, host=settings.host, port=settings.port or default_port)
    await server.start()
    resources.push_async_callback(server.stop)
