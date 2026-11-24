# 006. One Postgres, a schema per module, no cross-schema queries

- Status: accepted
- Date: 2026-11-24

## Context and problem

FinGuard is a modular monolith (ADR-001) with eight bounded contexts plus a shared kernel. Each context owns its
data, but a separate database per context would add operational cost (backups, connections, migrations, a
distributed transaction problem for the outbox) that a single team at this scale doesn't need.

## Considered options

1. A database per module.
2. One database with a schema per module.
3. One database with every table in `public`.

## Decision

Option 2. Every module gets its own schema (`cases`, `detection`, ...), and the shared kernel uses `platform`
(the outbox and inbox). Code in one module never queries another module's schema, and no foreign key crosses a
schema boundary: modules exchange ids and events instead.

Migrations form a single linear Alembic history inside the package (`finguard.infrastructure.db:migrations`),
run with `finguard-migrate`. One history keeps the order of cross-module changes obvious and ships with the
image. Table definitions live next to the adapters that use them and share one `MetaData` with a fixed naming
convention. `finguard-migrate` imports every table module and hands that metadata to Alembic, because the
migration environment sits in `infrastructure` and may not import modules. `alembic check` in the integration
tests catches drift between the tables and the migrations.

## Consequences

- A state change and its outbox row commit in one local transaction, with no distributed transaction.
- Splitting a module into its own database later means moving one schema, because nothing joins across it.
- Schema boundaries are enforced by import contracts and review, not by the database. Database roles per
  schema can be added if that proves insufficient.

## Revisit when

Module load or ownership diverges enough that one module needs its own scaling, backup or release cycle.
