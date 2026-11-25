import asyncio
import os
import signal
import socket
import urllib.error
import urllib.request

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from finguard.core.ops import OpsServer, Readiness, ops_routes, stop_on_signals


async def ok() -> None:
    return None


async def refused() -> None:
    raise ConnectionRefusedError("connection refused to db:5432")


async def hangs() -> None:
    await asyncio.sleep(60)


async def test_a_process_is_ready_when_every_check_passes() -> None:
    readiness = Readiness()
    readiness.require("database", ok)
    readiness.require("kafka", ok)

    assert await readiness.report() == (True, {"database": "ok", "kafka": "ok"})


async def test_a_failing_or_hanging_check_makes_the_process_not_ready() -> None:
    readiness = Readiness(timeout_s=0.05)
    readiness.require("database", refused)
    readiness.require("kafka", hangs)

    ready, checks = await readiness.report()

    assert not ready
    # Only the error type: driver messages can carry hosts or data.
    assert checks == {"database": "ConnectionRefusedError", "kafka": "TimeoutError"}


async def test_a_draining_process_is_not_ready_whatever_its_checks_say() -> None:
    readiness = Readiness()
    readiness.require("database", ok)
    readiness.draining = True

    assert await readiness.report() == (False, {"process": "shutting down"})


def test_the_ops_routes_report_liveness_readiness_and_metrics() -> None:
    readiness = Readiness()
    readiness.require("database", refused)
    client = TestClient(Starlette(routes=ops_routes(lambda _: readiness)))

    assert client.get("/healthz").json() == {"status": "ok"}
    not_ready = client.get("/readyz")
    assert not_ready.status_code == 503
    assert not_ready.json() == {
        "status": "not_ready",
        "checks": {"database": "ConnectionRefusedError"},
    }
    metrics = client.get("/metrics")
    assert metrics.headers["content-type"].startswith("text/plain")
    assert "python_info" in metrics.text


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
        return port


def get(url: str) -> int:
    try:
        with urllib.request.urlopen(url, timeout=2) as response:
            status: int = response.status
            return status
    except urllib.error.HTTPError as error:
        return error.code


async def test_a_worker_serves_its_probes_on_its_own_port_until_stopped() -> None:
    readiness = Readiness()
    readiness.require("database", ok)
    port = free_port()
    server = OpsServer(readiness, host="127.0.0.1", port=port)

    await server.start()
    statuses = [
        await asyncio.to_thread(get, f"http://127.0.0.1:{port}{path}")
        for path in ("/healthz", "/readyz", "/metrics")
    ]
    await server.stop()

    assert statuses == [200, 200, 200]
    with pytest.raises(ConnectionRefusedError):
        socket.create_connection(("127.0.0.1", port), timeout=0.5)


async def test_sigterm_reports_not_ready_and_asks_the_process_to_stop() -> None:
    stop = asyncio.Event()
    readiness = Readiness()
    stop_on_signals(stop, readiness)

    os.kill(os.getpid(), signal.SIGTERM)
    await asyncio.wait_for(stop.wait(), timeout=2)

    assert readiness.draining
    loop = asyncio.get_running_loop()
    loop.remove_signal_handler(signal.SIGTERM)
    loop.remove_signal_handler(signal.SIGINT)
