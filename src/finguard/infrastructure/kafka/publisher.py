import asyncio
from collections.abc import Sequence
from dataclasses import dataclass

from aiokafka import AIOKafkaProducer


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


class KafkaPublisher:
    def __init__(self, producer: AIOKafkaProducer) -> None:
        self._producer = producer

    async def publish(self, messages: Sequence[OutgoingMessage]) -> None:
        """Return once every message is acknowledged; raise if any of them failed."""
        # Queue the whole batch before waiting, so the sends are pipelined.
        deliveries = [
            await self._producer.send(
                message.topic,
                message.value,
                key=message.key.encode(),
                headers=list(message.headers),
            )
            for message in messages
        ]
        await asyncio.gather(*deliveries)
