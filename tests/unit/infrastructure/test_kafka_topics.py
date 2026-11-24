import pytest

from finguard.infrastructure.kafka.topics import split_retry_topic, subscribed_topics
from finguard.platform.topics import Topic


@pytest.mark.parametrize(
    ("topic", "expected"),
    [
        ("cases.events", ("cases.events", 0)),
        ("cases.events.retry.1", ("cases.events", 1)),
        ("cases.events.retry.2", ("cases.events", 2)),
        ("cases.events.dlq", ("cases.events.dlq", 0)),
        ("cases.retry.later", ("cases.retry.later", 0)),
    ],
)
def test_retry_topics_name_their_source_and_tier(topic: str, expected: tuple[str, int]) -> None:
    assert split_retry_topic(topic) == expected


def test_a_consumer_subscribes_to_each_topic_and_its_retry_tiers() -> None:
    topics = [Topic("cases.events"), Topic("agent.results")]

    assert subscribed_topics(topics, retry_tiers=2) == [
        "cases.events",
        "cases.events.retry.1",
        "cases.events.retry.2",
        "agent.results",
        "agent.results.retry.1",
        "agent.results.retry.2",
    ]
