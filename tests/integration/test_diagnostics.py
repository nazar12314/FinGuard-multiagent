import asyncio
from collections.abc import AsyncIterator, Callable, Iterator
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
    with TestClient(create_app(settings)) as client:
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
    api: TestClient, relay: OutboxRelay, settings: Settings, body: dict[str, Any], final: str
) -> dict[str, Any]:
    accepted = api.post("/api/v1/dev/pings", json=body)
    assert accepted.status_code == 202
    ping_id = accepted.json()["ping_id"]
    assert status_of(api, ping_id)["status"] == "queued"

    assert await relay.publish_pending() == 1
    assert status_of(api, ping_id)["status"] == "published"

    await consume_until(settings, lambda: status_of(api, ping_id)["status"] == final)
    return status_of(api, ping_id)


async def test_a_ping_travels_from_the_api_through_kafka_to_its_consumer(
    api: TestClient, relay: OutboxRelay, settings: Settings
) -> None:
    status = await send_and_follow(api, relay, settings, {}, "handled")

    assert status["topic"] is None
    assert [attempt["error"] for attempt in status["attempts"]] == [None]


async def test_a_ping_that_fails_transiently_recovers_on_a_retry_topic(
    api: TestClient, relay: OutboxRelay, settings: Settings
) -> None:
    status = await send_and_follow(
        api, relay, settings, {"fail": "transient", "times": 2}, "handled"
    )

    errors = [attempt["error"] for attempt in status["attempts"]]
    assert errors == ["asked to fail 2 time(s)", "asked to fail 2 time(s)", None]


async def test_a_ping_that_fails_permanently_is_dead_lettered(
    api: TestClient, relay: OutboxRelay, settings: Settings
) -> None:
    status = await send_and_follow(api, relay, settings, {"fail": "permanent"}, "dead_lettered")

    assert status["topic"] == "diagnostics.pings.dlq"
    assert len(status["attempts"]) == 1


async def test_a_ping_that_outlasts_every_retry_tier_is_dead_lettered(
    api: TestClient, relay: OutboxRelay, settings: Settings
) -> None:
    # One attempt on the source topic and one on each of the two retry tiers.
    status = await send_and_follow(
        api, relay, settings, {"fail": "transient", "times": 5}, "dead_lettered"
    )

    assert len(status["attempts"]) == 3


def test_an_unknown_ping_is_not_found(api: TestClient) -> None:
    response = api.get("/api/v1/dev/pings/0196d5a0-0000-7000-8000-000000000001")

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"


def test_a_ping_asking_for_too_many_failures_is_rejected(api: TestClient) -> None:
    response = api.post("/api/v1/dev/pings", json={"fail": "transient", "times": 99})

    assert response.status_code == 422
