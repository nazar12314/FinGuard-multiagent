"""Create the diagnostics schema with the ping attempts table.

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(sa.schema.CreateSchema("diagnostics"))
    op.create_table(
        "ping_attempts",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("ping_id", sa.Uuid(), nullable=False),
        sa.Column(
            "attempted_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("permanent", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_ping_attempts"),
        schema="diagnostics",
    )
    op.create_index("ix_ping_attempts_ping_id", "ping_attempts", ["ping_id"], schema="diagnostics")


def downgrade() -> None:
    op.drop_index("ix_ping_attempts_ping_id", table_name="ping_attempts", schema="diagnostics")
    op.drop_table("ping_attempts", schema="diagnostics")
    op.execute(sa.schema.DropSchema("diagnostics"))
