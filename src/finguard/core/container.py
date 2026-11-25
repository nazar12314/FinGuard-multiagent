import asyncio
from collections.abc import Mapping, Sequence
from contextlib import AsyncExitStack, suppress
from dataclasses import dataclass
from functools import partial

from aiokafka import AIOKafkaProducer
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from finguard.core.auth import Authenticator, DevHeaderAuthenticator
from finguard.core.ops import Readiness
from finguard.core.settings import AuthSettings, Settings
from finguard.infrastructure.db.engine import (
    check_database,
    create_database_engine,
    create_session_factory,
)
from finguard.infrastructure.kafka.consumer import (
    KafkaMessageConsumer,
    MessageHandler,
    RetryPolicy,
    create_kafka_consumer,
)
from finguard.infrastructure.kafka.publisher import (
    KafkaPublisher,
    check_kafka,
    create_kafka_producer,
)
from finguard.infrastructure.kafka.topics import subscribed_topics
from finguard.infrastructure.outbox.relay import OutboxRelay
from finguard.platform.topics import Topic

# How long a consumer's producer may spend flushing retry and dead-letter copies on shutdown.
_CONSUMER_PRODUCER_STOP_TIMEOUT_S = 10.0


@dataclass(frozen=True, slots=True)
class Container:
    settings: Settings
    authenticator: Authenticator
    session_factory: async_sessionmaker[AsyncSession]
    readiness: Readiness


def build_authenticator(settings: AuthSettings) -> Authenticator:
    match settings.mode:
        case "dev_header":
            return DevHeaderAuthenticator()


async def build_api_container(settings: Settings, resources: AsyncExitStack) -> Container:
    """
    Wire the API process. Every client opened here is registered on `resources`,
    which the caller owns, so it is closed even if a later step fails.
    """
    session_factory = open_session_factory(settings, resources)
    readiness = Readiness()
    readiness.require("database", partial(check_database, session_factory))
    return Container(
        settings=settings,
        authenticator=build_authenticator(settings.auth),
        session_factory=session_factory,
        readiness=readiness,
    )


async def build_outbox_relay(
    settings: Settings, resources: AsyncExitStack, readiness: Readiness | None = None
) -> OutboxRelay:
    relay_settings = settings.outbox_relay
    session_factory = open_session_factory(settings, resources)
    producer = await _start_producer(settings, resources, relay_settings.publish_timeout_s)
    if readiness is not None:
        readiness.require("database", partial(check_database, session_factory))
        readiness.require("kafka", partial(check_kafka, producer))

    return OutboxRelay(
        session_factory,
        KafkaPublisher(producer),
        batch_size=relay_settings.batch_size,
        publish_timeout_s=relay_settings.publish_timeout_s,
    )


async def build_message_consumer(
    settings: Settings,
    resources: AsyncExitStack,
    *,
    name: str,
    topics: Sequence[Topic],
    handlers: Mapping[str, MessageHandler],
    readiness: Readiness | None = None,
) -> KafkaMessageConsumer:
    """
    Wire a consumer group named `name` for `topics`, including their retry topics.
    """
    policy = RetryPolicy(
        attempts=settings.consumer.attempts,
        attempt_backoff_s=settings.consumer.attempt_backoff_s,
        handler_timeout_s=settings.consumer.handler_timeout_s,
        retry_delays_s=settings.kafka.retry_delays_s,
    )
    session_factory = open_session_factory(settings, resources)
    producer = await _start_producer(settings, resources, _CONSUMER_PRODUCER_STOP_TIMEOUT_S)
    consumer = create_kafka_consumer(
        settings.kafka.bootstrap_servers,
        group_id=name,
        topics=subscribed_topics(topics, retry_tiers=len(policy.retry_delays_s)),
        policy=policy,
    )
    resources.push_async_callback(consumer.stop)

    await consumer.start()
    if readiness is not None:
        readiness.require("database", partial(check_database, session_factory))
        readiness.require("kafka", partial(check_kafka, producer))

    return KafkaMessageConsumer(
        consumer, producer, session_factory, handlers, name=name, policy=policy
    )


def open_session_factory(
    settings: Settings, resources: AsyncExitStack
) -> async_sessionmaker[AsyncSession]:
    engine = create_database_engine(
        settings.database.url.get_secret_value(),
        pool_size=settings.database.pool_size,
        idle_in_transaction_timeout_s=settings.database.idle_in_transaction_timeout_s,
    )
    resources.push_async_callback(engine.dispose)

    return create_session_factory(engine)


async def _start_producer(
    settings: Settings, resources: AsyncExitStack, stop_timeout_s: float
) -> AIOKafkaProducer:
    producer = create_kafka_producer(settings.kafka.bootstrap_servers)

    async def stop_producer() -> None:
        # Stopping flushes queued messages, which never finishes while Kafka is unreachable.
        with suppress(TimeoutError):
            await asyncio.wait_for(producer.stop(), timeout=stop_timeout_s)

    resources.push_async_callback(stop_producer)
    await producer.start()
    return producer
