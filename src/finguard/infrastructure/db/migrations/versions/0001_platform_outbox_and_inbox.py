"""Create the platform schema with the outbox and inbox tables.

Revision ID: 0001
Revises:
Create Date: 2026-11-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(sa.schema.CreateSchema("platform"))
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("destination", sa.Text(), nullable=False),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_outbox"),
        schema="platform",
    )
    op.create_index(
        "ix_outbox_unpublished",
        "outbox",
        ["created_at", "id"],
        schema="platform",
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_index(
        "ix_outbox_published_at",
        "outbox",
        ["published_at"],
        schema="platform",
        postgresql_where=sa.text("published_at IS NOT NULL"),
    )
    op.create_table(
        "inbox",
        sa.Column("message_id", sa.Uuid(), nullable=False),
        sa.Column("consumer", sa.Text(), nullable=False),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("message_id", "consumer", name="pk_inbox"),
        schema="platform",
    )


def downgrade() -> None:
    op.drop_table("inbox", schema="platform")
    op.drop_index("ix_outbox_published_at", table_name="outbox", schema="platform")
    op.drop_index("ix_outbox_unpublished", table_name="outbox", schema="platform")
    op.drop_table("outbox", schema="platform")
    op.execute(sa.schema.DropSchema("platform"))
