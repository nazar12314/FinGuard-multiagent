from uuid import uuid4

import pytest

from finguard.infrastructure.kafka.publisher import create_kafka_admin
from finguard.infrastructure.kafka.topics import TopicCreationFailed, ensure_topics
from finguard.platform.topics import Topic


async def test_each_topic_gets_retry_and_dead_letter_topics_once(
    kafka_bootstrap_servers: str,
) -> None:
    topic = Topic(f"test.topics.{uuid4().hex}", partitions=2)
    admin = create_kafka_admin(kafka_bootstrap_servers)
    await admin.start()
    try:
        first = await ensure_topics(admin, [topic], retry_tiers=2, replication_factor=1)
        again = await ensure_topics(admin, [topic], retry_tiers=2, replication_factor=1)
    finally:
        await admin.close()

    assert first.created == [
        topic.name,
        f"{topic.name}.retry.1",
        f"{topic.name}.retry.2",
        f"{topic.name}.dlq",
    ]
    assert again.created == []
    assert again.partition_mismatches == {}


async def test_a_topic_the_broker_refuses_is_reported_not_counted_as_created(
    kafka_bootstrap_servers: str,
) -> None:
    topic = Topic(f"test.topics.{uuid4().hex}", partitions=1)
    admin = create_kafka_admin(kafka_bootstrap_servers)
    await admin.start()
    try:
        # One broker can't hold three replicas.
        with pytest.raises(TopicCreationFailed, match="InvalidReplicationFactor"):
            await ensure_topics(admin, [topic], retry_tiers=0, replication_factor=3)
    finally:
        await admin.close()


async def test_existing_topics_with_other_partition_counts_are_reported(
    kafka_bootstrap_servers: str,
) -> None:
    name = f"test.topics.{uuid4().hex}"
    admin = create_kafka_admin(kafka_bootstrap_servers)
    await admin.start()
    try:
        await ensure_topics(admin, [Topic(name, 1)], retry_tiers=0, replication_factor=1)
        result = await ensure_topics(admin, [Topic(name, 3)], retry_tiers=0, replication_factor=1)
    finally:
        await admin.close()

    assert result.created == []
    assert result.partition_mismatches == {name: (1, 3), f"{name}.dlq": (1, 3)}
