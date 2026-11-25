from sqlalchemy import BigInteger, Boolean, Column, ForeignKey, Identity, Index, Table, Text
from sqlalchemy.dialects.postgresql import ARRAY

from finguard.infrastructure.db.metadata import metadata

SCREENING_SCHEMA = "screening"

watchlist_entities = Table(
    "watchlist_entities",
    metadata,
    Column("entity_id", Text, primary_key=True),
    Column("entity_schema", Text, nullable=False),
    Column("name", Text, nullable=False),
    Column("birth_dates", ARRAY(Text), nullable=False),
    Column("countries", ARRAY(Text), nullable=False),
    Column("program_ids", ARRAY(Text), nullable=False),
    Column("datasets", ARRAY(Text), nullable=False),
    Column("sanctioned", Boolean, nullable=False),
    Column("pep", Boolean, nullable=False),
    schema=SCREENING_SCHEMA,
)

watchlist_names = Table(
    "watchlist_names",
    metadata,
    Column("id", BigInteger, Identity(always=True), primary_key=True),
    Column(
        "entity_id",
        Text,
        ForeignKey(watchlist_entities.c.entity_id, ondelete="CASCADE"),
        nullable=False,
        index=True,
    ),
    Column("name", Text, nullable=False),
    Column("latin", Text, nullable=False),
    Column("normalised", Text, nullable=False),
    Column("token_sorted", Text, nullable=False),
    Column("phonetic", Text, nullable=False),
    Column("is_alias", Boolean, nullable=False),
    # Trigram indexes find candidates by similarity (pg_trgm's % and similarity()).
    Index(
        "ix_watchlist_names_normalised_trgm",
        "normalised",
        postgresql_using="gin",
        postgresql_ops={"normalised": "gin_trgm_ops"},
    ),
    Index(
        "ix_watchlist_names_token_sorted_trgm",
        "token_sorted",
        postgresql_using="gin",
        postgresql_ops={"token_sorted": "gin_trgm_ops"},
    ),
    schema=SCREENING_SCHEMA,
)
