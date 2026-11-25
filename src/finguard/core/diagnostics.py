from collections.abc import Sequence
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
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
from finguard.infrastructure.outbox.tables import inbox, outbox
from finguard.platform.envelope import Envelope
from finguard.platform.errors import DependencyUnavailable, ValidationFailed

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

PingState = Literal["queued", "published", "failing", "handled", "dead_lettered", "parked"]
OutboxState = Literal["queued", "parked", "published"]


class PingRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fail: Literal["never", "transient", "permanent"] = "never"
    # For transient failures: how many handling attempts fail before one succeeds.
    times: int = Field(default=1, ge=1, le=20)


class PingAttempt(BaseModel):
    attempted_at: datetime
    error: str | None


class PingStatus(BaseModel):
    ping_id: UUID
    status: PingState
    attempts: list[PingAttempt]


def ping_handler(failure_log: async_sessionmaker[AsyncSession]) -> MessageHandler:
    async def handle(envelope: Envelope, session: AsyncSession) -> None:
        # The runtime marks the message in the inbox before calling this, so two deliveries
        # of one ping run one after the other and the second sees the first's failures.
        request = PingRequest.model_validate(envelope.data)
        failures = await session.scalar(
            select(func.count())
            .select_from(ping_attempts)
            .where(ping_attempts.c.ping_id == envelope.id, ping_attempts.c.error.is_not(None))
        )
        if request.fail == "permanent":
            await _record_failure(failure_log, envelope.id, "asked to fail permanently", True)
            raise ValidationFailed("ping asked to fail permanently")
        if request.fail == "transient" and (failures or 0) < request.times:
            reason = f"asked to fail {request.times} time(s)"
            await _record_failure(failure_log, envelope.id, reason, False)
            raise DependencyUnavailable(f"ping {reason}")
        await session.execute(insert(ping_attempts).values(ping_id=envelope.id))

    return handle


async def _record_failure(
    failure_log: async_sessionmaker[AsyncSession], ping_id: UUID, error: str, permanent: bool
) -> None:
    # In a transaction of its own: the handler's transaction is about to roll back.
    async with failure_log.begin() as session:
        await session.execute(
            insert(ping_attempts).values(ping_id=ping_id, error=error, permanent=permanent)
        )


async def load_ping_status(session: AsyncSession, ping_id: UUID) -> PingStatus | None:
    outbox_row = (
        await session.execute(
            select(outbox.c.published_at, outbox.c.failed_at).where(outbox.c.id == ping_id)
        )
    ).first()
    # Attempts before the inbox: a success committing in between then shows as "handled"
    # with a lagging attempts list, never as unhandled with a successful attempt.
    attempts = (
        await session.execute(
            select(ping_attempts.c.attempted_at, ping_attempts.c.error, ping_attempts.c.permanent)
            .where(ping_attempts.c.ping_id == ping_id)
            .order_by(ping_attempts.c.id)
        )
    ).all()
    handled_id = await session.scalar(
        select(inbox.c.message_id).where(
            inbox.c.message_id == ping_id, inbox.c.consumer == PING_CONSUMER
        )
    )
    if outbox_row is None and not attempts and handled_id is None:
        return None

    outbox_state: OutboxState
    if outbox_row is None:
        outbox_state = "published"  # pruned from the outbox after its retention period
    elif outbox_row.failed_at is not None:
        outbox_state = "parked"
    elif outbox_row.published_at is None:
        outbox_state = "queued"
    else:
        outbox_state = "published"
    return PingStatus(
        ping_id=ping_id,
        status=ping_state(
            outbox_state=outbox_state,
            handled=handled_id is not None,
            failures=[attempt.permanent for attempt in attempts if attempt.error is not None],
        ),
        attempts=[
            PingAttempt(attempted_at=attempt.attempted_at, error=attempt.error)
            for attempt in attempts
        ],
    )


def ping_state(*, outbox_state: OutboxState, handled: bool, failures: Sequence[bool]) -> PingState:
    """The state the records show, given the failed attempts in order, each flagged permanent.

    Only facts: which retry topic a failing ping waits on, or whether it ran out of retries,
    is left to the consumer's log, since it depends on restarts and rebalances.
    """
    if handled:
        return "handled"
    if outbox_state != "published":
        return outbox_state
    if not failures:
        return "published"
    return "dead_lettered" if failures[-1] else "failing"
