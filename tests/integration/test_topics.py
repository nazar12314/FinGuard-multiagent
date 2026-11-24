from uuid import uuid4

from finguard.infrastructure.kafka.publisher import create_kafka_admin
from finguard.infrastructure.kafka.topics import ensure_topics
from finguard.platform.topics import Topic


async def test_each_topic_gets_retry_and_dead_letter_topics_once(
    kafka_bootstrap_servers: str,
) -> None:
    topic = Topic(f"test.topics.{uuid4().hex}", partitions=2)
    admin = create_kafka_admin(kafka_bootstrap_servers)
    await admin.start()
    try:
        created = await ensure_topics(admin, [topic], retry_tiers=2, replication_factor=1)
        created_again = await ensure_topics(admin, [topic], retry_tiers=2, replication_factor=1)
    finally:
        await admin.close()

    assert created == [
        topic.name,
        f"{topic.name}.retry.1",
        f"{topic.name}.retry.2",
        f"{topic.name}.dlq",
    ]
    assert created_again == []
