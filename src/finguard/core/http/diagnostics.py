from uuid import UUID

from fastapi import APIRouter
from pydantic import BaseModel

from finguard.core.diagnostics import (
    PING_REQUESTED,
    PingRequest,
    PingStatus,
    load_ping_status,
)
from finguard.core.http.dependencies import ContainerDep, CurrentPrincipal
from finguard.infrastructure.outbox.postgres import PostgresOutbox
from finguard.platform.envelope import Envelope
from finguard.platform.errors import NotFound
from finguard.platform.topics import DIAGNOSTICS_PINGS

router = APIRouter(prefix="/api/v1/dev/pings", tags=["diagnostics"])


class PingAccepted(BaseModel):
    ping_id: UUID
    status_url: str


# Any authenticated caller may ping. Asking for a principal now means the routes are
# protected once real authentication replaces the dev stub.
@router.post("", status_code=202)
async def send_ping(
    request: PingRequest, container: ContainerDep, principal: CurrentPrincipal
) -> PingAccepted:
    envelope = Envelope.new(PING_REQUESTED, request.model_dump())
    async with container.session_factory.begin() as session:
        await PostgresOutbox(session).add(
            envelope, topic=DIAGNOSTICS_PINGS.name, key=str(envelope.id)
        )
    return PingAccepted(ping_id=envelope.id, status_url=f"{router.prefix}/{envelope.id}")


@router.get("/{ping_id}")
async def get_ping(
    ping_id: UUID, container: ContainerDep, principal: CurrentPrincipal
) -> PingStatus:
    async with container.session_factory() as session:
        status = await load_ping_status(session, ping_id)
    if status is None:
        raise NotFound("Ping", ping_id)
    return status
