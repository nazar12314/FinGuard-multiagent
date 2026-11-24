from typing import Protocol
from uuid import UUID

from finguard.platform.envelope import Envelope


class Outbox(Protocol):
    """Queues a message for publishing in the caller's database transaction.

    The message is published only if that transaction commits, and at least once.
    Raises ValidationFailed for a message too large to publish.
    """

    async def add(self, envelope: Envelope, *, topic: str, key: str) -> None: ...


class Inbox(Protocol):
    """Records which messages a consumer has processed, in the handler's transaction."""

    async def try_mark_processed(self, message_id: UUID, consumer: str) -> bool:
        """Return False when this consumer has already processed the message."""
        ...
