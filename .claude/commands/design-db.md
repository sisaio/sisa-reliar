---
description: Engineer designs the provider schema and writes the sqlx migrations
argument-hint: <capability/records, e.g. outbox table + claim indexes>
---
Use the **engineer** subagent to design the PostgreSQL schema and write the sqlx migrations in
`crates/reliar-store-postgres/migrations/` for:

$ARGUMENTS

Follow the `sqlx-postgres` skill and the architect's schema direction: dedicated columns for
queryable metadata, JSONB only for extensible data, no value stored twice, `timestamptz`, indexes
derived from the actual claim/purge queries (show the EXPLAIN), lock-safe forward-only migrations,
and the explicit `migrate(&pool)` API. Regenerate `.sqlx/`. Summarize the schema, indexes, and
migration files.
