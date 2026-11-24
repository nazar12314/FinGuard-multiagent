import asyncio
from contextlib import AsyncExitStack, suppress
from dataclasses import dataclass

from finguard.core.auth import Authenticator, DevHeaderAuthenticator
from finguard.core.settings import AuthSettings, Settings
from finguard.infrastructure.db.engine import create_database_engine, create_session_factory
from finguard.infrastructure.kafka.publisher import KafkaPublisher, create_kafka_producer
from finguard.infrastructure.outbox.relay import OutboxRelay


@dataclass(frozen=True, slots=True)
class Container:
    settings: Settings
    authenticator: Authenticator


def build_authenticator(settings: AuthSettings) -> Authenticator:
    # Exhaustive over the Literal: mypy reports a missing return once a new mode is added.
    match settings.mode:
        case "dev_header":
            return DevHeaderAuthenticator()


async def build_api_container(settings: Settings, resources: AsyncExitStack) -> Container:
    """Wire the API process. Every client opened here is registered on `resources`,
    which the caller owns, so it is closed even if a later step fails."""
    return Container(settings=settings, authenticator=build_authenticator(settings.auth))


async def build_outbox_relay(settings: Settings, resources: AsyncExitStack) -> OutboxRelay:
    engine = create_database_engine(
        settings.database.url.get_secret_value(),
        pool_size=settings.database.pool_size,
        idle_in_transaction_timeout_s=settings.database.idle_in_transaction_timeout_s,
    )
    resources.push_async_callback(engine.dispose)

    relay_settings = settings.outbox_relay
    producer = create_kafka_producer(settings.kafka.bootstrap_servers)

    async def stop_producer() -> None:
        # Stopping flushes queued messages, which never finishes while Kafka is unreachable.
        with suppress(TimeoutError):
            await asyncio.wait_for(producer.stop(), timeout=relay_settings.publish_timeout_s)

    resources.push_async_callback(stop_producer)
    await producer.start()

    return OutboxRelay(
        create_session_factory(engine),
        KafkaPublisher(producer),
        batch_size=relay_settings.batch_size,
        publish_timeout_s=relay_settings.publish_timeout_s,
    )
