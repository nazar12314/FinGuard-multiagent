from uuid import UUID

from fastapi import APIRouter
from pydantic import BaseModel

from finguard.core.diagnostics import (
    PING_REQUESTED,
    PingRequest,
    PingStatus,
    load_ping_status,
)
from finguard.core.http.dependencies import ContainerDep
from finguard.infrastructure.outbox.postgres import PostgresOutbox
from finguard.platform.envelope import Envelope
from finguard.platform.errors import NotFound
from finguard.platform.topics import DIAGNOSTICS_PINGS

router = APIRouter(prefix="/api/v1/dev/pings", tags=["diagnostics"])


class PingAccepted(BaseModel):
    ping_id: UUID
    status_url: str


@router.post("", status_code=202)
async def send_ping(request: PingRequest, container: ContainerDep) -> PingAccepted:
    envelope = Envelope.new(PING_REQUESTED, request.model_dump())
    async with container.session_factory.begin() as session:
        await PostgresOutbox(session).add(
            envelope, topic=DIAGNOSTICS_PINGS.name, key=str(envelope.id)
        )
    return PingAccepted(ping_id=envelope.id, status_url=f"{router.prefix}/{envelope.id}")


@router.get("/{ping_id}")
async def get_ping(ping_id: UUID, container: ContainerDep) -> PingStatus:
    settings = container.settings
    async with container.session_factory() as session:
        status = await load_ping_status(
            session,
            ping_id,
            attempts_per_topic=settings.consumer.attempts,
            retry_tiers=len(settings.kafka.retry_delays_s),
        )
    if status is None:
        raise NotFound("Ping", ping_id)
    return status
