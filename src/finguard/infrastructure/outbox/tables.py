from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Identity,
    Index,
    Table,
    Text,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

from finguard.infrastructure.db.metadata import metadata

PLATFORM_SCHEMA = "platform"

outbox = Table(
    "outbox",
    metadata,
    Column("id", Uuid, primary_key=True),
    # Publishing order. Assigned at insert, so a writer that locks its aggregate before adding
    # the message gets a position after every earlier committed message for that aggregate.
    # created_at can't be used: now() is the transaction's start time, not its insert time.
    Column("position", BigInteger, Identity(always=True), nullable=False),
    Column("destination", Text, nullable=False),
    Column("key", Text, nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("published_at", DateTime(timezone=True)),
    # Set when Kafka refused the message for good; such rows wait for a manual re-drive.
    Column("failed_at", DateTime(timezone=True)),
    Column("last_error", Text),
    Index(
        "ix_outbox_pending",
        "position",
        postgresql_where=text("published_at IS NULL AND failed_at IS NULL"),
    ),
    Index(
        "ix_outbox_published_at",
        "published_at",
        postgresql_where=text("published_at IS NOT NULL"),
    ),
    schema=PLATFORM_SCHEMA,
)

inbox = Table(
    "inbox",
    metadata,
    Column("message_id", Uuid, primary_key=True),
    Column("consumer", Text, primary_key=True),
    Column("processed_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    schema=PLATFORM_SCHEMA,
)
