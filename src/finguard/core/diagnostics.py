from collections.abc import Sequence
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Identity,
    Table,
    Text,
    Uuid,
    false,
    func,
    insert,
    select,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from finguard.infrastructure.db.metadata import metadata
from finguard.infrastructure.kafka.consumer import MessageHandler
from finguard.infrastructure.kafka.topics import dead_letter_topic, retry_topic
from finguard.infrastructure.outbox.tables import inbox, outbox
from finguard.platform.envelope import Envelope
from finguard.platform.errors import DependencyUnavailable, ValidationFailed
from finguard.platform.topics import DIAGNOSTICS_PINGS

PING_REQUESTED = "diagnostics.ping.requested"
PING_CONSUMER = "diagnostics-pings"

ping_attempts = Table(
    "ping_attempts",
    metadata,
    Column("id", BigInteger, Identity(always=True), primary_key=True),
    Column("ping_id", Uuid, nullable=False, index=True),
    Column("attempted_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("error", Text),
    Column("permanent", Boolean, nullable=False, server_default=false()),
    schema="diagnostics",
)

PingState = Literal["queued", "published", "retrying", "handled", "dead_lettered", "parked"]
OutboxState = Literal["queued", "parked", "published"]


class PingRequest(BaseModel):
    fail: Literal["never", "transient", "permanent"] = "never"
    # For transient failures: how many deliveries fail before one succeeds.
    times: int = Field(default=1, ge=1, le=20)


class PingAttempt(BaseModel):
    attempted_at: datetime
    error: str | None


class PingStatus(BaseModel):
    ping_id: UUID
    status: PingState
    # Where the message is now: the source topic, a retry tier or the dead-letter topic.
    topic: str | None
    attempts: list[PingAttempt]


def ping_handler(attempts_db: async_sessionmaker[AsyncSession]) -> MessageHandler:
    async def handle(envelope: Envelope, session: AsyncSession) -> None:
        request = PingRequest.model_validate(envelope.data)
        failures = await session.scalar(
            select(func.count())
            .select_from(ping_attempts)
            .where(ping_attempts.c.ping_id == envelope.id, ping_attempts.c.error.is_not(None))
        )
        if request.fail == "permanent":
            await _record_failure(attempts_db, envelope.id, "asked to fail permanently", True)
            raise ValidationFailed("ping asked to fail permanently")
        if request.fail == "transient" and (failures or 0) < request.times:
            reason = f"asked to fail {request.times} time(s)"
            await _record_failure(attempts_db, envelope.id, reason, False)
            raise DependencyUnavailable(f"ping {reason}")
        await session.execute(insert(ping_attempts).values(ping_id=envelope.id))

    return handle


async def _record_failure(
    attempts_db: async_sessionmaker[AsyncSession], ping_id: UUID, error: str, permanent: bool
) -> None:
    # In a transaction of its own: the handler's transaction is about to roll back.
    async with attempts_db.begin() as session:
        await session.execute(
            insert(ping_attempts).values(ping_id=ping_id, error=error, permanent=permanent)
        )


async def load_ping_status(
    session: AsyncSession, ping_id: UUID, *, attempts_per_topic: int, retry_tiers: int
) -> PingStatus | None:
    queued = (
        await session.execute(
            select(outbox.c.published_at, outbox.c.failed_at).where(outbox.c.id == ping_id)
        )
    ).first()
    handled = await session.scalar(
        select(inbox.c.message_id).where(
            inbox.c.message_id == ping_id, inbox.c.consumer == PING_CONSUMER
        )
    )
    attempts = (
        await session.execute(
            select(ping_attempts.c.attempted_at, ping_attempts.c.error, ping_attempts.c.permanent)
            .where(ping_attempts.c.ping_id == ping_id)
            .order_by(ping_attempts.c.id)
        )
    ).all()
    if queued is None and not attempts and handled is None:
        return None

    outbox_state: OutboxState
    if queued is None:
        outbox_state = "published"  # pruned from the outbox after its retention period
    elif queued.failed_at is not None:
        outbox_state = "parked"
    elif queued.published_at is None:
        outbox_state = "queued"
    else:
        outbox_state = "published"
    status, topic = describe_progress(
        outbox_state=outbox_state,
        handled=handled is not None,
        failures=[attempt.permanent for attempt in attempts if attempt.error is not None],
        attempts_per_topic=attempts_per_topic,
        retry_tiers=retry_tiers,
    )
    return PingStatus(
        ping_id=ping_id,
        status=status,
        topic=topic,
        attempts=[
            PingAttempt(attempted_at=attempt.attempted_at, error=attempt.error)
            for attempt in attempts
        ],
    )


def describe_progress(
    *,
    outbox_state: OutboxState,
    handled: bool,
    failures: Sequence[bool],
    attempts_per_topic: int,
    retry_tiers: int,
) -> tuple[PingState, str | None]:
    """Where a ping is, given its failed attempts in order, each flagged permanent or not.

    Mirrors the consumer's retry policy, so it assumes the API runs with the consumer's
    settings.
    """
    source = DIAGNOSTICS_PINGS.name
    if handled:
        return "handled", None
    if outbox_state != "published":
        return outbox_state, None
    if not failures:
        return "published", source
    if failures[-1] or len(failures) >= attempts_per_topic * (retry_tiers + 1):
        return "dead_lettered", dead_letter_topic(source)
    tier = len(failures) // attempts_per_topic
    return "retrying", retry_topic(source, tier) if tier else source
