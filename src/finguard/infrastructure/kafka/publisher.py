from dataclasses import dataclass

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.errors import (
    InvalidTopicError,
    MessageSizeTooLargeError,
    RecordTooLargeError,
    TopicAuthorizationFailedError,
)

# Errors caused by the message itself, which no retry or fresh producer can fix.
_REJECTIONS = (
    InvalidTopicError,
    MessageSizeTooLargeError,
    RecordTooLargeError,
    TopicAuthorizationFailedError,
)


class MessageRejected(Exception):
    """Kafka refused this message for good."""


@dataclass(frozen=True, slots=True)
class OutgoingMessage:
    topic: str
    key: str
    value: bytes
    headers: tuple[tuple[str, bytes], ...] = ()


def create_kafka_producer(bootstrap_servers: str) -> AIOKafkaProducer:
    # Idempotence stops broker-side duplicates on producer retries and keeps per-partition order.
    return AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        enable_idempotence=True,
    )


def create_kafka_admin(bootstrap_servers: str) -> AIOKafkaAdminClient:
    return AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)


class KafkaPublisher:
    def __init__(self, producer: AIOKafkaProducer) -> None:
        self._producer = producer

    async def publish(self, message: OutgoingMessage) -> None:
        """Return once Kafka acknowledged the message. Raise MessageRejected if it never will."""
        try:
            await self._producer.send_and_wait(
                message.topic,
                message.value,
                key=message.key.encode(),
                headers=list(message.headers),
            )
        except _REJECTIONS as error:
            raise MessageRejected(f"{type(error).__name__}: {error}") from error


async def check_kafka(producer: AIOKafkaProducer) -> None:
    """Fetch cluster metadata through the producer's connection, which fails while the
    brokers are unreachable."""
    await producer.client.fetch_all_metadata()
