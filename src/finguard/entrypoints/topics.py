import asyncio

from finguard.core.logging import LogEvent, configure_logging, get_logger
from finguard.core.settings import Settings, load_settings_or_exit
from finguard.infrastructure.kafka.publisher import create_kafka_admin
from finguard.infrastructure.kafka.topics import EnsuredTopics, ensure_topics
from finguard.platform.topics import TOPICS

logger = get_logger(__name__)


async def create_missing_topics(settings: Settings) -> EnsuredTopics:
    admin = create_kafka_admin(settings.kafka.bootstrap_servers)
    await admin.start()
    try:
        return await ensure_topics(
            admin,
            TOPICS,
            retry_tiers=len(settings.kafka.retry_delays_s),
            replication_factor=settings.kafka.replication_factor,
        )
    finally:
        await admin.close()


def main() -> None:
    """Create every topic FinGuard uses, with its retry and dead-letter topics."""
    settings = load_settings_or_exit()
    configure_logging(settings)
    result = asyncio.run(create_missing_topics(settings))
    logger.info(LogEvent.KAFKA_TOPICS_ENSURED, created=result.created)
    for topic, (actual, declared) in result.partition_mismatches.items():
        logger.warning(
            LogEvent.KAFKA_TOPIC_PARTITIONS_DIFFER, topic=topic, actual=actual, declared=declared
        )


if __name__ == "__main__":
    main()
