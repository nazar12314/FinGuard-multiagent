import asyncio
import os
import signal
import socket
import time
import urllib.error
import urllib.request

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from finguard.core.ops import Heartbeat, OpsServer, Readiness, ops_routes, stop_on_signals


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


async def test_a_process_without_readiness_checks_is_never_ready() -> None:
    assert await Readiness().report() == (False, {"process": "no readiness checks registered"})


def test_a_heartbeat_goes_stale_until_the_loop_beats_again() -> None:
    heartbeat = Heartbeat(stale_after_s=0.05)
    assert not heartbeat.stale

    time.sleep(0.06)
    assert heartbeat.stale

    heartbeat.beat()
    assert not heartbeat.stale


def test_a_worker_whose_loop_stopped_beating_fails_liveness() -> None:
    heartbeat = Heartbeat(stale_after_s=0.05)
    readiness = Readiness()
    client = TestClient(Starlette(routes=ops_routes(lambda _: readiness, heartbeat)))
    assert client.get("/healthz").status_code == 200

    time.sleep(0.06)
    response = client.get("/healthz")

    assert response.status_code == 503
    assert response.json()["status"] == "stalled"


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
    server = OpsServer(readiness, Heartbeat(stale_after_s=60), host="127.0.0.1", port=port)

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
    loop = asyncio.get_running_loop()
    stop_on_signals(stop, readiness)
    try:
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(stop.wait(), timeout=2)
    finally:
        loop.remove_signal_handler(signal.SIGTERM)
        loop.remove_signal_handler(signal.SIGINT)

    assert readiness.draining
