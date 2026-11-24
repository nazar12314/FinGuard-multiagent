from datetime import UTC, datetime
from typing import Self
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, field_validator

from finguard.platform.ids import new_id

MESSAGE_TYPE_PATTERN = r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$"
TRACEPARENT_PATTERN = r"^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$"


class Envelope(BaseModel):
    """Wrapper for every Kafka message. Consumers deduplicate on `id`."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: UUID = Field(default_factory=new_id)
    type: str = Field(pattern=MESSAGE_TYPE_PATTERN)
    version: int = Field(default=1, ge=1)
    occurred_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
    correlation_id: UUID
    causation_id: UUID | None = None
    traceparent: str | None = Field(default=None, pattern=TRACEPARENT_PATTERN)
    data: dict[str, JsonValue]

    @field_validator("occurred_at")
    @classmethod
    def _normalise_to_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @classmethod
    def new(
        cls,
        type: str,
        data: dict[str, JsonValue],
        *,
        correlation_id: UUID | None = None,
        version: int = 1,
    ) -> Self:
        # A message that starts a flow and has no case to correlate with
        # becomes the flow's correlation root itself.
        message_id = new_id()
        return cls(
            id=message_id,
            type=type,
            version=version,
            correlation_id=correlation_id or message_id,
            data=data,
        )

    def follow_up(self, type: str, data: dict[str, JsonValue], *, version: int = 1) -> Self:
        return self.__class__(
            type=type,
            version=version,
            correlation_id=self.correlation_id,
            causation_id=self.id,
            data=data,
        )
