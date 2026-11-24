from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import timedelta

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError, for_code

from finguard.platform.topics import Topic

# Dead letters wait for a person, so they outlive the broker's default seven days.
DEAD_LETTER_RETENTION = timedelta(days=30)

_RETRY_INFIX = ".retry."


class TopicCreationFailed(Exception):
    pass


@dataclass(frozen=True, slots=True)
class EnsuredTopics:
    created: list[str]
    # Existing topics whose partition count differs from the catalog, as (actual, declared).
    # Not changed automatically: adding partitions moves keys and breaks per-key order.
    partition_mismatches: dict[str, tuple[int, int]] = field(default_factory=dict)


def retry_topic(base: str, tier: int) -> str:
    return f"{base}{_RETRY_INFIX}{tier}"


def dead_letter_topic(base: str) -> str:
    return f"{base}.dlq"


def split_retry_topic(topic: str) -> tuple[str, int]:
    """Return the source topic and the retry tier, where tier 0 is the source topic itself."""
    base, infix, tier = topic.rpartition(_RETRY_INFIX)
    return (base, int(tier)) if infix and tier.isdigit() else (topic, 0)


def subscribed_topics(topics: Iterable[Topic], *, retry_tiers: int) -> list[str]:
    return [
        name
        for topic in topics
        for name in (topic.name, *(retry_topic(topic.name, tier) for tier in tiers(retry_tiers)))
    ]


def tiers(retry_tiers: int) -> range:
    return range(1, retry_tiers + 1)


async def ensure_topics(
    admin: AIOKafkaAdminClient,
    topics: Iterable[Topic],
    *,
    retry_tiers: int,
    replication_factor: int,
) -> EnsuredTopics:
    """Create each topic with its retry tiers and dead-letter topic.

    Existing topics are left as they are, so running this again, even concurrently, is safe.
    Raises TopicCreationFailed when the broker refuses a topic.
    """
    wanted: list[NewTopic] = []
    for topic in topics:
        names = [topic.name, *(retry_topic(topic.name, tier) for tier in tiers(retry_tiers))]
        wanted += [NewTopic(name, topic.partitions, replication_factor) for name in names]
        wanted.append(
            NewTopic(
                dead_letter_topic(topic.name),
                topic.partitions,
                replication_factor,
                topic_configs={
                    "retention.ms": str(int(DEAD_LETTER_RETENTION.total_seconds() * 1000))
                },
            )
        )

    existing = set(await admin.list_topics())
    missing = [topic for topic in wanted if topic.name not in existing]
    created: list[str] = []
    if missing:
        response = await admin.create_topics(missing)
        failures = []
        for name, error_code, *_ in response.topic_errors:
            if error_code == 0:
                created.append(name)
            elif error_code != TopicAlreadyExistsError.errno:  # created by a concurrent run
                failures.append(f"{name}: {for_code(error_code).__name__}")
        if failures:
            raise TopicCreationFailed("; ".join(failures))

    declared = {topic.name: topic.num_partitions for topic in wanted if topic.name in existing}
    mismatches = {}
    if declared:
        for description in await admin.describe_topics(list(declared)):
            actual = len(description["partitions"])
            if actual != declared[description["topic"]]:
                mismatches[description["topic"]] = (actual, declared[description["topic"]])
    return EnsuredTopics(created=created, partition_mismatches=mismatches)
