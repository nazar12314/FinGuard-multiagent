import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import AsyncExitStack
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from finguard.core.container import build_message_consumer, open_session_factory
from finguard.core.diagnostics import PING_CONSUMER, PING_REQUESTED, ping_handler
from finguard.core.http.app import create_app
from finguard.core.settings import ConsumerSettings, DatabaseSettings, KafkaSettings, Settings
from finguard.infrastructure.kafka.publisher import (
    KafkaPublisher,
    create_kafka_admin,
    create_kafka_producer,
)
from finguard.infrastructure.kafka.topics import ensure_topics
from finguard.infrastructure.outbox.relay import OutboxRelay
from finguard.platform.topics import DIAGNOSTICS_PINGS

SessionFactory = async_sessionmaker[AsyncSession]
ReadTopic = Callable[..., Awaitable[list[Any]]]
CALLER = {"X-Dev-User-Id": "0196d5a0-0000-7000-8000-000000000001"}


@pytest.fixture
async def settings(database_url: str, kafka_bootstrap_servers: str) -> Settings:
    admin = create_kafka_admin(kafka_bootstrap_servers)
    await admin.start()
    try:
        await ensure_topics(admin, [DIAGNOSTICS_PINGS], retry_tiers=2, replication_factor=1)
    finally:
        await admin.close()
    return Settings(
        env="test",
        database=DatabaseSettings(url=SecretStr(database_url)),
        kafka=KafkaSettings(bootstrap_servers=kafka_bootstrap_servers, retry_delays_s=(0.3, 0.3)),
        consumer=ConsumerSettings(attempts=1, attempt_backoff_s=0.01),
    )


@pytest.fixture
def api(settings: Settings, session_factory: SessionFactory) -> Iterator[TestClient]:
    with TestClient(create_app(settings), headers=CALLER) as client:
        yield client


@pytest.fixture
async def relay(settings: Settings) -> AsyncIterator[OutboxRelay]:
    producer = create_kafka_producer(settings.kafka.bootstrap_servers)
    await producer.start()
    async with AsyncExitStack() as resources:
        yield OutboxRelay(
            open_session_factory(settings, resources),
            KafkaPublisher(producer),
            batch_size=10,
            publish_timeout_s=10,
        )
    await producer.stop()


def status_of(api: TestClient, ping_id: str) -> dict[str, Any]:
    response = api.get(f"/api/v1/dev/pings/{ping_id}")
    assert response.status_code == 200
    body: dict[str, Any] = response.json()
    return body


async def consume_until(settings: Settings, done: Callable[[], bool]) -> None:
    stop = asyncio.Event()
    async with AsyncExitStack() as resources:
        consumer = await build_message_consumer(
            settings,
            resources,
            name=PING_CONSUMER,
            topics=[DIAGNOSTICS_PINGS],
            handlers={PING_REQUESTED: ping_handler(open_session_factory(settings, resources))},
        )
        task = asyncio.create_task(consumer.run(stop))
        try:
            async with asyncio.timeout(30):
                while not done():
                    if task.done():
                        await task
                    await asyncio.sleep(0.1)
        finally:
            stop.set()
            await task


async def send_and_follow(
    api: TestClient,
    relay: OutboxRelay,
    settings: Settings,
    body: dict[str, Any],
    done: Callable[[dict[str, Any]], bool],
) -> tuple[dict[str, Any], list[str]]:
    """Send a ping, publish it and consume until `done(status)`. Also return the states seen."""
    accepted = api.post("/api/v1/dev/pings", json=body)
    assert accepted.status_code == 202
    ping_id = accepted.json()["ping_id"]
    assert status_of(api, ping_id)["status"] == "queued"

    assert await relay.publish_pending() == 1
    assert status_of(api, ping_id)["status"] == "published"

    seen: list[str] = []

    def reached() -> bool:
        status = status_of(api, ping_id)
        if not seen or seen[-1] != status["status"]:
            seen.append(status["status"])
        return done(status)

    await consume_until(settings, reached)
    return status_of(api, ping_id), seen


def has_status(name: str) -> Callable[[dict[str, Any]], bool]:
    return lambda status: bool(status["status"] == name)


async def test_a_ping_travels_from_the_api_through_kafka_to_its_consumer(
    api: TestClient, relay: OutboxRelay, settings: Settings
) -> None:
    status, _ = await send_and_follow(api, relay, settings, {}, has_status("handled"))

    assert [attempt["error"] for attempt in status["attempts"]] == [None]


async def test_a_ping_that_fails_transiently_recovers_on_a_retry_topic(
    api: TestClient, relay: OutboxRelay, settings: Settings
) -> None:
    status, seen = await send_and_follow(
        api, relay, settings, {"fail": "transient", "times": 2}, has_status("handled")
    )

    errors = [attempt["error"] for attempt in status["attempts"]]
    assert errors == ["asked to fail 2 time(s)", "asked to fail 2 time(s)", None]
    assert "failing" in seen


async def test_in_place_retries_and_retry_topics_both_count_towards_the_failures(
    api: TestClient, relay: OutboxRelay, settings: Settings
) -> None:
    # Two attempts per topic: two failures in place, then one failure and a success on the
    # first retry topic.
    two_attempts = settings.model_copy(
        update={"consumer": ConsumerSettings(attempts=2, attempt_backoff_s=0.01)}
    )

    status, seen = await send_and_follow(
        api, relay, two_attempts, {"fail": "transient", "times": 3}, has_status("handled")
    )

    errors = [attempt["error"] for attempt in status["attempts"]]
    assert errors == ["asked to fail 3 time(s)"] * 3 + [None]
    assert seen[0] in ("published", "failing")
    assert seen[-2:] == ["failing", "handled"]


async def test_a_ping_that_fails_permanently_is_dead_lettered(
    api: TestClient, relay: OutboxRelay, settings: Settings
) -> None:
    status, _ = await send_and_follow(
        api, relay, settings, {"fail": "permanent"}, has_status("dead_lettered")
    )

    assert len(status["attempts"]) == 1


async def test_a_ping_that_outlasts_every_retry_topic_reaches_the_dead_letter_topic(
    api: TestClient, relay: OutboxRelay, settings: Settings, read_topic: ReadTopic
) -> None:
    # One attempt on the source topic and one on each of the two retry topics.
    status, _ = await send_and_follow(
        api,
        relay,
        settings,
        {"fail": "transient", "times": 5},
        lambda status: len(status["attempts"]) == 3,
    )

    # The status only reports what was recorded; the dead-letter topic holds the copy.
    assert status["status"] == "failing"
    dead = await read_topic("diagnostics.pings.dlq", 50, timeout_s=3)
    assert status["ping_id"] in {json.loads(record.value)["id"] for record in dead}


def test_an_unknown_ping_is_not_found(api: TestClient) -> None:
    response = api.get("/api/v1/dev/pings/0196d5a0-0000-7000-8000-000000000001")

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"


@pytest.mark.parametrize(
    "body",
    [{"fail": "transient", "times": 99}, {"fail": "sometimes"}, {"fail": "transient", "tims": 4}],
    ids=["too-many-failures", "unknown-mode", "misspelt-field"],
)
def test_an_invalid_ping_is_rejected(api: TestClient, body: dict[str, Any]) -> None:
    response = api.post("/api/v1/dev/pings", json=body)

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_failed"


def test_pings_need_an_authenticated_caller(api: TestClient) -> None:
    response = api.post("/api/v1/dev/pings", json={}, headers={"X-Dev-User-Id": ""})

    assert response.status_code == 401
