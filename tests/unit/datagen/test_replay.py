from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from tests.amlworld_files import OUR_BANK, build_small_world, csv_row

from datagen.replay import Booking, Pace, bookings, replay
from finguard.infrastructure.kafka.publisher import OutgoingMessage
from finguard.modules.ingestion.messages import TransactionBooked
from finguard.platform.envelope import Envelope

T0 = datetime(2022, 9, 1, tzinfo=UTC)


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class RecordingPublisher:
    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.sent: list[tuple[float, str]] = []

    async def publish(self, message: OutgoingMessage) -> None:
        self.sent.append((self.clock.now, message.key))


def booking(at: timedelta, key: str = "k") -> Booking:
    return Booking(booked_at=T0 + at, message=OutgoingMessage(topic="tx.raw", key=key, value=b"{}"))


async def run(pace: Pace, schedule: list[Booking]) -> list[tuple[float, str]]:
    clock = FakeClock()
    publisher = RecordingPublisher(clock)
    sent = await replay(schedule, publisher, pace, clock=clock, sleep=clock.sleep)
    assert sent == len(schedule)
    return [(at - 100.0, key) for at, key in publisher.sent]


def test_turns_transactions_into_booked_messages(tmp_path: Path) -> None:
    world = build_small_world(
        tmp_path,
        [
            csv_row(
                OUR_BANK, "C1", "002", "B1", amount="12.50", currency="Yen", payment_format="Wire"
            )
        ],
    )

    [message] = [b.message for b in bookings(world)]

    envelope = Envelope.model_validate_json(message.value)
    assert message.topic == "tx.raw"
    assert envelope.type == "transaction.booked"
    booked = TransactionBooked.model_validate(envelope.data)
    assert booked.transaction_id == "tx-000000000"
    assert booked.booked_at == datetime(2022, 9, 1, 10, 0, tzinfo=UTC)
    assert (booked.from_account.bank_id, booked.from_account.account_number) == (OUR_BANK, "C1")
    assert (booked.to_account.bank_id, booked.to_account.account_number) == ("002", "B1")
    assert (str(booked.amount_paid.amount), booked.amount_paid.currency) == ("12.500000", "JPY")
    assert booked.payment_format == "wire"


def test_keys_each_message_by_our_customers_account(tmp_path: Path) -> None:
    world = build_small_world(
        tmp_path,
        [
            csv_row(OUR_BANK, "OUT", "002", "B1", timestamp="2022/09/01 10:00"),
            csv_row("002", "B1", OUR_BANK, "IN", timestamp="2022/09/01 10:01"),
            csv_row(OUR_BANK, "FROM", OUR_BANK, "TO", timestamp="2022/09/01 10:02"),
        ],
    )

    keys = [b.message.key for b in bookings(world)]

    assert keys == ["012:OUT", "012:IN", "012:FROM"]


def test_streams_in_booking_order_and_honours_the_limit(tmp_path: Path) -> None:
    world = build_small_world(
        tmp_path,
        [
            csv_row(OUR_BANK, "LATE", "002", "B1", timestamp="2022/09/02 10:00"),
            csv_row(OUR_BANK, "EARLY", "002", "B1", timestamp="2022/09/01 10:00"),
        ],
    )

    assert [b.message.key for b in bookings(world)] == ["012:EARLY", "012:LATE"]
    assert [b.message.key for b in bookings(world, limit=1)] == ["012:EARLY"]


def test_needs_a_built_world(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="finguard-datagen build"):
        bookings(tmp_path / "seed-1")


async def test_compresses_simulated_time_by_the_speedup() -> None:
    schedule = [
        booking(timedelta(0), "a"),
        booking(timedelta(days=1), "b"),
        booking(timedelta(days=2), "c"),
    ]

    sent = await run(Pace(speedup=1440), schedule)

    assert sent == [(0.0, "a"), (60.0, "b"), (120.0, "c")]


async def test_infinite_speedup_sends_without_waiting() -> None:
    schedule = [booking(timedelta(0)), booking(timedelta(days=30))]

    assert await run(Pace(speedup=float("inf")), schedule) == [(0.0, "k"), (0.0, "k")]


async def test_caps_the_rate_of_messages_booked_close_together() -> None:
    schedule = [booking(timedelta(0), "a"), booking(timedelta(0), "b"), booking(timedelta(0), "c")]

    sent = await run(Pace(speedup=float("inf"), max_messages_per_s=2), schedule)

    assert sent == [(0.0, "a"), (0.5, "b"), (1.0, "c")]


async def test_the_cap_doesnt_delay_messages_already_spread_out() -> None:
    schedule = [booking(timedelta(0), "a"), booking(timedelta(minutes=10), "b")]

    sent = await run(Pace(speedup=60, max_messages_per_s=1), schedule)

    assert sent == [(0.0, "a"), (10.0, "b")]
