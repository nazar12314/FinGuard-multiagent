from datetime import UTC, datetime
from typing import Self
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, field_validator

from finguard.platform.ids import new_id

MESSAGE_TYPE_PATTERN = r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$"


class Envelope(BaseModel):
    """Wrapper for every Kafka message.

    Consumers deduplicate on `id`. Envelopes are keyed on it and never hashed, because
    `data` is a plain dict. Unknown top-level fields are ignored, so the envelope can gain
    fields without breaking consumers that haven't been upgraded yet.
    """

    model_config = ConfigDict(frozen=True, extra="ignore")

    id: UUID = Field(default_factory=new_id)
    type: str = Field(pattern=MESSAGE_TYPE_PATTERN)
    version: int = Field(default=1, ge=1)
    occurred_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
    correlation_id: UUID
    causation_id: UUID | None = None
    # Left unvalidated: the OpenTelemetry propagator parses it and drops invalid values,
    # and a malformed trace header must never send a business message to the DLQ.
    traceparent: str | None = None
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
        traceparent: str | None = None,
    ) -> Self:
        # A message that starts a flow and has no case to correlate with
        # becomes the flow's correlation root itself.
        message_id = new_id()
        return cls(
            id=message_id,
            type=type,
            version=version,
            correlation_id=message_id if correlation_id is None else correlation_id,
            traceparent=traceparent,
            data=data,
        )

    def follow_up(
        self,
        type: str,
        data: dict[str, JsonValue],
        *,
        correlation_id: UUID | None = None,
        version: int = 1,
        traceparent: str | None = None,
    ) -> Self:
        # Pass correlation_id to re-scope the chain, e.g. to the case id once an alert
        # opens a case; the causation link back to the alert is kept either way.
        return self.__class__(
            type=type,
            version=version,
            correlation_id=self.correlation_id if correlation_id is None else correlation_id,
            causation_id=self.id,
            traceparent=traceparent,
            data=data,
        )
