# ADR 0005 — `Envelope`, `OutboxRecord` and `InboxRecord` are distinct types

**Status:** Accepted — 2026-09-04
**SRS:** §17, §17.1, §24, §24.2, §43.B

## Context

An outbox row carries two unrelated kinds of information: *what the message is* (id, type, body,
metadata) and *how its delivery is going* (attempts, lease owner, next due time, last error, dead
state). It is tempting to model that as one struct — the table has one row, after all — and to
hand that struct straight to the publisher.

Doing so leaks delivery bookkeeping onto the wire, makes `Envelope` un-constructible without
inventing delivery state, and means a change to the retry model is a change to the message
contract. It also blurs the inbox, whose state (`received_at`, `completed_at`) is different again.

## Decision

- Three separate types, and no lossy `From` between them:
  - **`Envelope<T>` / `SerializedEnvelope`** — what the message *is* (§9, ADR 0003).
  - **`OutboxRecord`** — an envelope **plus** outbound delivery state: `sequence`,
    `ordering_key`, `attempts`, `available_at`, `locked_by`, `locked_until`, `published_at`,
    `dead_at`, `last_error` (§17).
  - **`InboxRecord`** — inbound processing state, keyed by `message_id` (§17, Phase 3).
- `OutboxRecord` **contains** a `SerializedEnvelope` by composition (`record.envelope`), so the
  publisher is handed `&SerializedEnvelope` and can see nothing else.
- Delivery state SHALL NOT appear in `Metadata` and SHALL NOT be mapped to transport headers.
  Nothing in §14's reserved list refers to attempts, leases or dead state.
- `OutboxRecord` is `#[non_exhaustive]`; it grows as the delivery model grows without touching
  `Envelope`.
- Boundary types are explicit, not `as` casts: `attempts: u32` and `message_version: u16` are
  stored as PostgreSQL `integer` and converted with `i32::from` / `u32::try_from`; an
  out-of-range value read back is a **poison row** (ADR 0008), never a panic (§17.1).
- `last_error` is truncated to 2 KiB at a char boundary and carries only `Display` output of the
  error chain — never payload bytes, header values, or credentialed URLs (§17.1, §33).

## Consequences

- Changing retry or lease mechanics never changes the message contract, and never requires a
  transport mapper update.
- A publisher physically cannot read `attempts` — it has only the envelope — so a transport can
  never grow a hidden dependency on outbox internals.
- Round-tripping a row costs an explicit merge of promoted columns + JSONB remainder into an
  envelope (ADR 0012), rather than a derived `FromRow`. That merge is the thing worth testing
  (§43.A.4), and `FromRow` is banned anyway (skill: sqlx-postgres).
- Three types mean three rustdoc surfaces to keep honest. Accepted.

## Alternatives considered

- **One `OutboxMessage` struct used everywhere.** Rejected: puts `locked_by` on the wire and
  makes `Envelope` unusable in a pure `reliar-core` unit test.
- **`OutboxRecord` as a trait implemented by providers.** Rejected: it is data, not behaviour;
  a trait here buys nothing and blocks `#[non_exhaustive]` field growth.
- **Flatten the envelope's fields into `OutboxRecord`.** Rejected: the publisher would then need
  a constructor to rebuild an envelope, i.e. the same merge, done at every call site.
