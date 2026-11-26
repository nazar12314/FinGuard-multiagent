"""Create the screening schema with the watchlist tables and their trigram indexes.

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.execute(sa.schema.CreateSchema("screening"))
    op.create_table(
        "watchlist_entities",
        sa.Column("entity_id", sa.Text(), nullable=False),
        sa.Column("entity_schema", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("birth_dates", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column("countries", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column("program_ids", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column("datasets", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column("sanctioned", sa.Boolean(), nullable=False),
        sa.Column("pep", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("entity_id", name="pk_watchlist_entities"),
        schema="screening",
    )
    op.create_table(
        "watchlist_names",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("entity_id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("latin", sa.Text(), nullable=False),
        sa.Column("normalised", sa.Text(), nullable=False),
        sa.Column("token_sorted", sa.Text(), nullable=False),
        sa.Column("phonetic", sa.Text(), nullable=False),
        sa.Column("is_alias", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["entity_id"],
            ["screening.watchlist_entities.entity_id"],
            name="fk_watchlist_names_entity_id_watchlist_entities",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_watchlist_names"),
        schema="screening",
    )
    op.create_index(
        "ix_watchlist_names_entity_id", "watchlist_names", ["entity_id"], schema="screening"
    )
    for column in ("normalised", "token_sorted"):
        op.create_index(
            f"ix_watchlist_names_{column}_trgm",
            "watchlist_names",
            [column],
            schema="screening",
            postgresql_using="gin",
            postgresql_ops={column: "gin_trgm_ops"},
        )


def downgrade() -> None:
    op.drop_table("watchlist_names", schema="screening")
    op.drop_table("watchlist_entities", schema="screening")
    op.execute(sa.schema.DropSchema("screening"))
    # pg_trgm stays: it may have existed before, and other schemas may index with it.
