import asyncio
from dataclasses import dataclass
from typing import Any, cast
from uuid import uuid4

import pytest
from prometheus_client import REGISTRY
from pydantic import BaseModel, ValidationError

from finguard.infrastructure.kafka import consumer as consumer_runtime
from finguard.infrastructure.kafka.consumer import KafkaMessageConsumer, RetryPolicy, is_permanent
from finguard.platform.errors import (
    Conflict,
    DependencyUnavailable,
    NotFound,
    ValidationFailed,
    describe_error,
)


class Amount(BaseModel):
    value: int


def validation_error() -> ValidationError:
    try:
        Amount.model_validate({"value": "many"})
    except ValidationError as error:
        return error
    raise AssertionError("expected a validation error")


@pytest.mark.parametrize(
    ("error", "permanent"),
    [
        (ValidationFailed("closing needs a rationale"), True),
        (Conflict("decision already recorded"), True),
        (NotFound("Case", 42), True),
        (validation_error(), True),
        (DependencyUnavailable("database unavailable"), False),
        (ConnectionError("connection reset"), False),
        (TimeoutError(), False),
    ],
)
def test_only_errors_a_retry_cannot_change_are_permanent(error: Exception, permanent: bool) -> None:
    assert is_permanent(error) is permanent


def test_the_longest_handling_time_covers_every_attempt_and_backoff() -> None:
    policy = RetryPolicy(
        attempts=3, attempt_backoff_s=0.5, handler_timeout_s=60, retry_delays_s=(30.0,)
    )

    assert policy.longest_handling_s == 3 * 60 + 0.5 + 1.0


def test_error_descriptions_never_quote_message_data() -> None:
    assert describe_error(ValidationFailed("closing needs a rationale")) == (
        "ValidationFailed: closing needs a rationale"
    )
    assert describe_error(validation_error()) == "ValidationError: value: int_parsing"
    assert describe_error(ConnectionError("Key (iban)=(DE89 3704 0044) already exists")) == (
        "ConnectionError"
    )


@dataclass(frozen=True)
class TopicPartition:
    topic: str
    partition: int


class PartitionsTakenAway:
    """A Kafka consumer that loses one of its two partitions after the first poll."""

    def __init__(self, topic: str, stop: asyncio.Event) -> None:
        self.kept, self.lost = TopicPartition(topic, 0), TopicPartition(topic, 1)
        self._assigned = {self.kept, self.lost}
        self._stop = stop
        self.polls = 0

    def assignment(self) -> set[TopicPartition]:
        return self._assigned

    def highwater(self, partition: TopicPartition) -> int:
        return 10

    async def position(self, partition: TopicPartition) -> int:
        return 4

    async def getmany(self, **options: Any) -> dict[TopicPartition, list[Any]]:
        self.polls += 1
        self._assigned = {self.kept}
        if self.polls == 2:
            self._stop.set()
        return {}


async def test_lag_is_reported_only_for_partitions_still_assigned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(consumer_runtime, "_LAG_INTERVAL_S", 0.0)
    stop = asyncio.Event()
    kafka = PartitionsTakenAway(f"test.lag.{uuid4().hex}", stop)
    beats: list[None] = []
    consumer = KafkaMessageConsumer(
        cast(Any, kafka),
        cast(Any, None),
        cast(Any, None),
        {},
        name="lag-test",
        policy=RetryPolicy(attempts=1, attempt_backoff_s=0, handler_timeout_s=1, retry_delays_s=()),
        heartbeat=lambda: beats.append(None),
    )

    await consumer.run(stop)

    def lag(partition: TopicPartition) -> float | None:
        return REGISTRY.get_sample_value(
            "finguard_consumer_lag",
            {
                "consumer": "lag-test",
                "topic": partition.topic,
                "partition": str(partition.partition),
            },
        )

    assert lag(kafka.kept) == 6
    assert lag(kafka.lost) is None
    assert len(beats) == 2
