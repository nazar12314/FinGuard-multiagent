from sqlalchemy import Column, DateTime, Index, Table, Text, Uuid, func, text
from sqlalchemy.dialects.postgresql import JSONB

from finguard.infrastructure.db.metadata import metadata

PLATFORM_SCHEMA = "platform"

outbox = Table(
    "outbox",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column("destination", Text, nullable=False),
    Column("key", Text, nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("published_at", DateTime(timezone=True)),
    Index(
        "ix_outbox_unpublished",
        "created_at",
        "id",
        postgresql_where=text("published_at IS NULL"),
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
