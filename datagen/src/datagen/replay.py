import asyncio
import time
from collections.abc import Awaitable, Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

import polars as pl

from finguard.infrastructure.kafka.publisher import OutgoingMessage
from finguard.modules.ingestion.messages import (
    TRANSACTION_BOOKED,
    AccountReference,
    Money,
    TransactionBooked,
)
from finguard.platform.envelope import Envelope
from finguard.platform.topics import TX_RAW

ONE_DAY_PER_MINUTE = 1440.0


class Publisher(Protocol):
    async def publish(self, message: OutgoingMessage) -> None: ...


@dataclass(frozen=True)
class Pace:
    # Simulated seconds per real second; infinity sends without waiting.
    speedup: float = ONE_DAY_PER_MINUTE
    max_messages_per_s: float | None = None


@dataclass(frozen=True)
class Booking:
    booked_at: datetime
    message: OutgoingMessage


def bookings(world: Path, limit: int | None = None) -> Iterator[Booking]:
    """The world's transactions as tx.raw messages, in the order they were booked."""
    if not (world / "transactions.parquet").is_file():
        raise FileNotFoundError(f"{world} has no transactions; run `finguard-datagen build` first")
    transactions = pl.scan_parquet(world / "transactions.parquet")
    if limit is not None:
        transactions = transactions.head(limit)
    accounts = pl.scan_parquet(world / "accounts.parquet")
    rows = (
        transactions.join(
            accounts.select(
                from_account_id="account_id",
                from_bank_id="bank_id",
                from_account_number="account_number",
                from_ours="held_at_our_bank",
            ),
            on="from_account_id",
        )
        .join(
            accounts.select(
                to_account_id="account_id",
                to_bank_id="bank_id",
                to_account_number="account_number",
            ),
            on="to_account_id",
        )
        .sort("booked_at", "transaction_id")
        .collect()
    )
    return (_booking(row) for row in rows.iter_rows(named=True))


def _booking(row: dict[str, Any]) -> Booking:
    booked = TransactionBooked(
        transaction_id=row["transaction_id"],
        booked_at=row["booked_at"],
        from_account=AccountReference(
            bank_id=row["from_bank_id"], account_number=row["from_account_number"]
        ),
        to_account=AccountReference(
            bank_id=row["to_bank_id"], account_number=row["to_account_number"]
        ),
        amount_paid=Money(amount=row["amount_paid"], currency=row["payment_currency"]),
        amount_received=Money(amount=row["amount_received"], currency=row["receiving_currency"]),
        payment_format=row["payment_format"],
    )
    envelope = Envelope.new(TRANSACTION_BOOKED, booked.model_dump(mode="json"))
    # Keyed by our customer's account, so each customer's transactions stay in order.
    key = row["from_account_id"] if row["from_ours"] else row["to_account_id"]
    message = OutgoingMessage(topic=TX_RAW.name, key=key, value=envelope.model_dump_json().encode())
    return Booking(booked_at=row["booked_at"], message=message)


async def replay(
    bookings: Iterable[Booking],
    publisher: Publisher,
    pace: Pace,
    *,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> int:
    started = clock()
    first_booked_at: datetime | None = None
    previous_due: float | None = None
    sent = 0
    for booking in bookings:
        first_booked_at = first_booked_at or booking.booked_at
        simulated_s = (booking.booked_at - first_booked_at).total_seconds()
        due = started + simulated_s / pace.speedup
        if pace.max_messages_per_s and previous_due is not None:
            due = max(due, previous_due + 1 / pace.max_messages_per_s)
        if (wait := due - clock()) > 0:
            await sleep(wait)
        await publisher.publish(booking.message)
        previous_due = due
        sent += 1
    return sent
