# ADR 0011 — `Headers` is a validating newtype; `Envelope` is built through a builder

**Status:** Accepted — 2026-09-04
**SRS:** §9.1, §11, §13, §13.1, §14, §17.1, §24, §43.A.5
**Extends:** ADR 0003, ADR 0004

## Context

Three defects in v1.0 shared one root cause — types with no invariants:

1. `pub type Headers = HashMap<String, String>` is a **type alias**, so §14's "applications SHOULD
   NOT be able to override reserved keys" is unimplementable and §43's reserved-namespace
   criterion has nothing to fail. An alias also gives no size limits, and `headers` lands in a
   JSONB column on a hot table read by every claim: one unbounded header inflates every row and
   every batch.
2. `Envelope.headers` was private while every other field was `pub`, with no constructor — so the
   type could not be built outside `reliar-core`, breaking `EnvelopeMapper::decode`, the provider's
   row → envelope rehydration, tests and examples.
3. `WorkerId`, `MessageId` and `CorrelationId` were used as if defined. `WorkerId` in particular is
   the lease-ownership guard key, so its generation and stability are correctness properties.

## Decision

**Headers.**

- `Headers` is a **newtype over `HashMap<String, String>`** with `insert` returning
  `Result<Option<String>, HeaderError>`. It SHALL NOT implement `Deref<Target = HashMap<..>>` or
  otherwise expose the map — that would re-open unvalidated insertion.
- `insert` returns **`Err`**, never a silent drop or overwrite, for: the reserved `reliar-` prefix
  (matched **case-insensitively**, so `Reliar-Correlation-Id` is rejected too), an empty key, a key
  over `MAX_KEY_LEN` (128), a value over `MAX_VALUE_LEN` (1024), or exceeding `MAX_COUNT` (32).
- The **whole `reliar-` prefix** is reserved, not just §14's current key list, so reserving a new
  framework key later is not a breaking change.
- Case-insensitivity is load-bearing: transport header names are case-insensitive, so a
  case-sensitive check would leak straight through the mapper and let a user header collide with a
  framework one at the wire.
- Keys are stored as given and looked up exactly; Reliar does not require an `x-` prefix.
- Headers are **lazily allocated** (`Option<Headers>` on the envelope) — most messages have none.

**Envelope construction.**

- `headers` stays private to preserve those invariants, and construction/access are made uniform
  and public instead: `Envelope::builder(body)` → `EnvelopeBuilder<T>` with `id`, `metadata`,
  `correlation`, `header(k, v) -> Result<Self, HeaderError>`, `build()`; plus `headers()`,
  `headers_mut()` (lazily allocating) and `map_body`.
- `message_type` is derived from `T::TYPE`/`T::VERSION` and **cannot be passed in** — identity
  cannot drift from the type (ADR 0010).
- `id` defaults to a fresh UUIDv7; `conversation_id` defaults to the message's **own id**, making
  an un-correlated message the root of its own conversation. The mechanism is a *value*, not a
  builder flag: `CorrelationMetadata::default()` carries the `ConversationId::UNSET` sentinel (nil
  UUID) and `build()` replaces it with the envelope's id **iff it is still `UNSET`**, so a caller
  who passes a whole `Metadata` in to tweak an unrelated field still gets a rooted conversation,
  while a genuinely chosen conversation id (`.conversation(id)`, or one copied from the causing
  message) is never overwritten *(amended 2026-09-04, RELIAR-12 review 1)*.
- `map_body` is how `reliar-core` turns `Envelope<T>` into `SerializedEnvelope` and how a provider
  or mapper rehydrates one — neither crate touches the private field.
- `Envelope<T>` SHALL NOT require `T: Clone`; the dispatcher moves owned records into publish tasks.
  `Debug` on `SerializedEnvelope` elides payload bytes.

**Identity types.**

- **`WorkerId`** — opaque capped string newtype (`MAX_LEN = 128`), default **`pid:uuid7`**,
  generated **once per dispatcher instance** (not per batch, not per claim). It is the lease guard
  key, so it SHALL be unique per running dispatcher and SHALL **NOT** be stable across restarts: a
  restarted worker must not be able to complete rows its predecessor claimed. Applications may
  supply their own.
  **No host segment** *(corrected 2026-09-04, RELIAR-13 review 1)*: the original `host:pid:uuid7`
  would have read `HOSTNAME`, and ADR 0019's "the library never reads the environment implicitly"
  is absolute — a rule with one convenience exception is not a rule. A host that wants its pod name
  in the lease guard sets `DispatcherSettings::worker_id` or `RELIAR_OUTBOX_WORKER_ID` explicitly,
  which is also the only way it can be sure of the value. The uuid7 half already guarantees the
  uniqueness the guard needs; the hostname was only ever operator ergonomics.
- **`MessageId` / `ConversationId` / `RequestId`** — UUIDs; every one Reliar generates is **UUIDv7**,
  produced client-side (ADR 0015). Applications may supply any UUID and Reliar SHALL NOT inspect or
  reject its version.
- **`CorrelationId(String)`** — capped at 256 characters; it lands in a `text` column read on every
  claim.

## Consequences

- The reserved-namespace and size rules become a runtime test rather than a review note (§43.A.5).
- Applications already using `reliar-`-prefixed headers must rename them. Documented on `Headers`.
- Every construction path goes through the builder, so a new envelope field is additive.
- A restart-unstable `WorkerId` means a crashed worker's rows are recovered by lease expiry only —
  which is the design (ADR 0006) — and never by a restarted process claiming ownership it lost.
- Capping headers at 32×(128+1024) bounds the JSONB column that every claim reads.

## Alternatives considered

- **Keep the `HashMap` alias, enforce by documentation.** Rejected: unenforceable, and §43 would
  have a criterion with nothing to assert.
- **Silently drop reserved keys.** Rejected (ADR 0004): a programming error becomes a production
  mystery.
- **Public `headers` field.** Rejected: no way to validate, and it would make the caps a lie.
- **A stable `WorkerId` across restarts** (e.g. hostname only). Rejected: a restarted process could
  then complete rows a second worker had already republished — the exact bug the guard prevents.
