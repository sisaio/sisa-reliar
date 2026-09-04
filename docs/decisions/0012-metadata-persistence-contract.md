# ADR 0012 — Promoted columns and the `MetadataRest` JSONB contract

**Status:** Accepted — 2026-09-04
**SRS:** §12, §12.2, §24, §24.1, §24.2, §43.A.4
**Extends:** ADR 0003, ADR 0005

## Context

§24 rules that "the same value SHALL NOT be stored both as a dedicated column and inside `metadata`
JSONB". Combined with §43's requirement that an acquired envelope equal the enqueued one, that
makes reconstruction a **merge** of promoted columns with a *partial* JSONB blob.

That partial blob is a **second serialization contract**, distinct from `serde(Metadata)`. It is
not `Metadata` by construction — several fields have been carved out — so it needs its own
compatibility rules: what happens to an unknown field, how the shape evolves, whether a rolling
deploy can have an older instance publish a row a newer instance wrote. v1.0 specified none of
this and left it to emerge from the implementation, which is how metadata contracts rot.

Two fields also needed promoting on their own merits: `tenant_id` (the likeliest filter, the
natural partition key, the idempotency scope) and `expires_at` (unenforceable inside JSONB).

## Decision

- **Promoted to columns — and therefore carved out of the JSONB remainder:** `id`, `message_type`,
  `message_version`, `correlation_id`, `conversation_id`, `causation_id`, `request_id`,
  `content_type`, `expires_at`, `tenant_id`, `ordering_key`, `payload`, and `headers` (its own
  JSONB column, **never merged** with metadata).
- **Everything else is `MetadataRest`**, a `pub(crate)` struct in the provider:
  `trace` (traceparent, tracestate), `routing` (source, destination, reply_to),
  `delivery` (sent_at, deduplication_id).
- **`#[serde(default)]` on every struct and every field**, so a row written by an older version
  deserializes into the current shape. **Unknown fields are ignored, never rejected** — a row
  written by a newer instance must still be publishable by a running older one during a rolling
  deploy. (Note the deliberate contrast with `*Settings`, which use `deny_unknown_fields`: a typo
  in a config file should fail loudly; a stored row must not.)
- **The shape is versioned by the `metadata_version` column, not by a field inside the blob**, so
  it is filterable in SQL. v0.1 writes `1`. An incompatible future shape bumps it and the provider
  keeps a reader for every version it has ever written; a row carrying an unknown
  `metadata_version` is a **poison row** (ADR 0008) — never a panic, never a corrupted publish.
- **An empty remainder is written as SQL `NULL`, not `'{}'`**, so pending rows stay small.
- **Timestamps inside the blob are epoch milliseconds (`i64`), not RFC 3339.** `time`'s RFC 3339
  formatter is fallible over `OffsetDateTime`'s own range — years outside `0000..=9999` cannot be
  represented — so an application-supplied `sent_at` could make serialization fail on the enqueue
  path, where §19.5 forbids a panic and a fallible encoding would only relocate a baffling error
  onto the host's write path. Epoch milliseconds are **total**, so the failure mode is deleted
  rather than handled; they are also smaller, integer-comparable and parser-free. The one affected
  field is `DeliveryRest.sent_at`, written as `sent_at_ms`. `expires_at` is unaffected because it is
  a promoted `timestamptz` column, where PostgreSQL owns the encoding.
  *(Added 2026-09-04 after RELIAR-16 review 1, blocker 1.)*
- **Field renames are forbidden.** A field is only ever added (with a default) or deprecated in
  place. Renaming `RoutingMetadata.destination` would strand every pending row in production with
  no migration path — the exact failure this contract exists to prevent.
- **Reconstruction is proven by a round-trip property test** over arbitrary `Metadata` values, not
  by a hand-written example (§43.A.4).
- `tenant_id` and `expires_at` are promoted (decision 5). `expires_at` then participates in the
  claim predicate and the expiry sweep (ADR 0009); `tenant_id` gets a column now because adding one
  to a large hot outbox table later is the migration operators refuse.

## Consequences

- Two contracts to keep honest instead of one, and the promoted list must stay in sync between the
  writer, the reader and the migration. That is why the list is enumerated here and property-tested.
- Rolling deploys are safe in both directions: an old instance ignores new fields; a new instance
  defaults absent ones.
- `metadata_version` as a column costs 4 bytes per row and buys a SQL-filterable migration path —
  an operator can count rows of each shape before deploying a reader change.
- Promoting a field later is a migration **plus** a `MetadataRest` field removal, and both must
  tolerate old rows that still carry it in JSONB. The reader must therefore prefer the column and
  fall back to the blob during any such transition. Stated here so it is not rediscovered.
- `deduplication_id` stays in the blob: it is never queried by Reliar and only a transport mapper
  reads it (§12.3).

## Alternatives considered

- **Serialize the whole `Metadata` into JSONB and duplicate the queryable fields into columns.**
  Rejected by §24's no-duplication rule, and it doubles the bytes on the hottest table; the two
  copies also drift the moment anything updates one.
- **No promoted columns at all — JSONB only.** Rejected: `expires_at` and `available_at` filtering,
  correlation lookups and tenant filtering all become unindexable expression scans.
- **`deny_unknown_fields` on `MetadataRest`.** Rejected: it turns a rolling deploy into an outage.
- **A version field inside the blob.** Rejected: not filterable in SQL without parsing every row.
