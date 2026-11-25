from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from aiokafka.admin import NewTopic
from tests.amlworld_files import OUR_BANK, build_small_world, csv_row

from datagen.replay import Pace, bookings, replay
from finguard.infrastructure.kafka.publisher import KafkaPublisher, create_kafka_producer
from finguard.modules.ingestion.messages import TransactionBooked
from finguard.platform.envelope import Envelope
from finguard.platform.topics import TX_RAW

CreateTopics = Callable[..., Awaitable[None]]
ReadTopic = Callable[..., Awaitable[list[Any]]]


async def test_replays_a_world_into_tx_raw(
    tmp_path: Path,
    kafka_bootstrap_servers: str,
    create_topics: CreateTopics,
    read_topic: ReadTopic,
) -> None:
    # The admin reports a topic another test already created instead of raising.
    await create_topics(NewTopic(TX_RAW.name, num_partitions=1, replication_factor=1))
    world = build_small_world(
        tmp_path,
        [
            csv_row(OUR_BANK, "C1", "002", "B1", timestamp="2022/09/01 10:00"),
            csv_row("002", "B1", OUR_BANK, "C2", timestamp="2022/09/01 10:05"),
        ],
    )
    producer = create_kafka_producer(kafka_bootstrap_servers)
    await producer.start()
    try:
        sent = await replay(bookings(world), KafkaPublisher(producer), Pace(speedup=float("inf")))
    finally:
        await producer.stop()

    records = await read_topic(TX_RAW.name, 2)
    assert sent == 2
    received = sorted(
        (
            TransactionBooked.model_validate(
                Envelope.model_validate_json(record.value).data
            ).transaction_id,
            record.key,
        )
        for record in records
    )
    assert received == [
        ("tx-000000000", b"012:C1"),
        ("tx-000000001", b"012:C2"),
    ]
