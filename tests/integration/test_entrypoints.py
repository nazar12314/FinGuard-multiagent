import asyncio
import json
import os
import signal
import socket
import urllib.error
import urllib.request
from collections.abc import Callable, Coroutine
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from finguard.core.http.app import create_app
from finguard.core.settings import DatabaseSettings, KafkaSettings, OpsSettings, Settings
from finguard.entrypoints import outbox_relay, ping_consumer
from finguard.infrastructure.kafka.publisher import create_kafka_admin
from finguard.infrastructure.kafka.topics import ensure_topics
from finguard.platform.topics import DIAGNOSTICS_PINGS

UNREACHABLE_DATABASE = "postgresql+asyncpg://finguard:finguard@127.0.0.1:1/finguard"


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
        return port


def get(url: str) -> tuple[int, dict[str, Any]]:
    try:
        with urllib.request.urlopen(url, timeout=3) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())
    except OSError:
        return 0, {}


@pytest.fixture
async def ping_topics(kafka_bootstrap_servers: str) -> None:
    admin = create_kafka_admin(kafka_bootstrap_servers)
    await admin.start()
    try:
        await ensure_topics(admin, [DIAGNOSTICS_PINGS], retry_tiers=2, replication_factor=1)
    finally:
        await admin.close()


@pytest.mark.parametrize("process", [outbox_relay, ping_consumer], ids=["relay", "consumer"])
async def test_a_worker_serves_its_probes_and_stops_cleanly_on_sigterm(
    database_url: str, kafka_bootstrap_servers: str, ping_topics: None, process: Any
) -> None:
    port = free_port()
    settings = Settings(
        env="test",
        database=DatabaseSettings(url=SecretStr(database_url)),
        kafka=KafkaSettings(bootstrap_servers=kafka_bootstrap_servers),
        ops=OpsSettings(host="127.0.0.1", port=port),
    )
    run: Callable[[Settings], Coroutine[Any, Any, None]] = process.run
    loop = asyncio.get_running_loop()
    worker = asyncio.create_task(run(settings))
    try:
        async with asyncio.timeout(30):
            while (readyz := await asyncio.to_thread(get, f"http://127.0.0.1:{port}/readyz"))[
                0
            ] != 200:
                assert not worker.done(), "the worker stopped before it was ready"
                await asyncio.sleep(0.1)
        healthz = await asyncio.to_thread(get, f"http://127.0.0.1:{port}/healthz")

        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(worker, timeout=20)
    finally:
        loop.remove_signal_handler(signal.SIGTERM)
        loop.remove_signal_handler(signal.SIGINT)
        worker.cancel()

    assert readyz[1] == {"status": "ready", "checks": {"database": "ok", "kafka": "ok"}}
    assert healthz == (200, {"status": "ok"})
    with pytest.raises(ConnectionRefusedError):
        socket.create_connection(("127.0.0.1", port), timeout=0.5)


@pytest.mark.parametrize(
    ("url", "status", "database"),
    [("database_url", 200, "ok"), (UNREACHABLE_DATABASE, 503, "not ok")],
    ids=["reachable", "unreachable"],
)
def test_the_api_is_ready_only_while_its_database_answers(
    request: pytest.FixtureRequest, url: str, status: int, database: str
) -> None:
    if url == "database_url":
        url = request.getfixturevalue("database_url")
    settings = Settings(env="test", database=DatabaseSettings(url=SecretStr(url)))

    with TestClient(create_app(settings)) as client:
        response = client.get("/readyz")

    assert response.status_code == status
    assert (response.json()["checks"]["database"] == "ok") is (database == "ok")
