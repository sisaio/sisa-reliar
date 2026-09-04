# ADR 0010 — The `Serializer` contract and stable `MessageType` identity

**Status:** Accepted — 2026-09-04
**SRS:** §9, §10, §10.1, §12.1, §24, §24.2, §31, §43.A.2–3
**Extends:** ADR 0003

## Context

§9's `Envelope<T> → SerializedEnvelope` is impossible to write without a serializer: `T` needs
serde bounds and the result needs a `ContentType` for §24's column. v1.0 listed "Serializer" among
things to abstract and defined nothing — `enqueue` could not be implemented. `ContentType` was used
in §12 and never defined either.

Separately, persistent message identity must survive refactoring. `std::any::type_name::<T>()`
changes when a struct moves module or is renamed, which silently orphans every pending row and
every message already on the wire.

Two sub-questions were genuinely open: **who chooses the content type** (caller or serializer) and
**who owns the serializer instance**.

## Decision

- **`Message` carries the serde bounds**, so an envelope cannot be built for a body that cannot be
  persisted:
  `trait Message: serde::Serialize + serde::de::DeserializeOwned { const TYPE: &'static str;
  const VERSION: u16; }`. `serde` is therefore a non-optional dependency of `reliar-core`; only the
  concrete format (`serde_json`) sits behind a feature.
- **`MessageType` is a struct carrying `name` and `version` separately**, not a pre-rendered
  string, with `MessageType::of::<T>()`. `Display` renders `"{name}.v{version}"` — e.g.
  `orders.created.v1` — and **that rendering is a stable public contract** clients parse. Identity
  is never derived from `type_name::<T>()` or a module path.
- §24 persists `message_type` and `message_version` as **two columns**, so a query can filter a
  name across versions; `Display` gives the single wire identity for transport headers and logs.
- **`Serializer` lives in `reliar-core`** — it touches neither storage nor transport:
  `fn content_type(&self) -> &ContentType`, `fn serialize<T: Message>(&self, body: &T) ->
  Result<Bytes, Self::Error>`, `fn deserialize<T: Message>(&self, bytes: &[u8]) -> Result<T,
  Self::Error>`, with `type Error: std::error::Error + Send + Sync + 'static`.
- **`content_type()` belongs to the serializer, not the caller.** A call site cannot know the
  format the store was configured with. The value populates both `DeliveryMetadata.content_type`
  and §24's `content_type` column — one value, promoted, and carved out of the JSONB remainder
  (ADR 0012).
- **The provider store owns the serializer instance.** `PostgresOutboxStore::new(pool)` uses
  `JsonSerializer`; `with_serializer(s)` overrides it. This keeps `enqueue(&mut tx, envelope)` a
  one-liner and makes the format a **deployment** decision, not a per-call-site one.
- `JsonSerializer` ships behind a **default `json` feature**. Turning it off is for a deployment
  supplying its own format; the trait, not the format, is the contract.
- `Serializer` is stateless and cheap and SHALL NOT sit behind a `dyn` on the enqueue path
  (ADR 0001). `ContentType` is a validated newtype over `Cow<'static, str>` with a `JSON` const.
- **Version negotiation is explicitly out of v0.1 scope.** Choosing a deserializer for an
  `orders.created.v2` arriving at a v1 consumer is a Phase-3 concern — v0.1 has no consumer. v0.1
  guarantees only that `name` and `version` round-trip through persistence unchanged.

## Consequences

- Renaming or moving a Rust struct is safe; changing its `TYPE`/`VERSION` is a contract change, and
  it is visible as one.
- Two Rust types sharing `TYPE`/`VERSION` render identically. That is intended (a type may be
  re-declared in another crate) and is asserted in a test (§43.A.3).
- Because the store owns the serializer, a store configured with a non-JSON serializer writes rows
  a JSON-configured store cannot read. The `content_type` column records which one wrote each row;
  a mismatch surfaces as a poison row (ADR 0008), not a silent misparse.
- `Message`'s serde bounds mean a body type must be `DeserializeOwned` even in v0.1, where nothing
  deserializes it. Accepted: it costs one derive and it is what makes Phase 3 possible without a
  breaking bound change.
- Storing name and version in separate columns means the `Display` rendering is reconstructed on
  read; the round-trip is property-tested (§43.A.4).

## Alternatives considered

- **`type_name::<T>()` as identity.** Rejected: refactoring orphans pending rows and live messages.
- **`MessageType(String)` pre-rendered.** Rejected: cannot filter a name across versions in SQL
  without string parsing, and invites hand-constructed identities that drift from the type.
- **Caller passes the content type.** Rejected: the call site does not know the store's format.
- **Serializer owned by the dispatcher or passed per call.** Rejected: `enqueue` happens on the
  application's write path, where the dispatcher is not present; per-call means every handler must
  hold one.
- **`serde` behind a feature.** Rejected: `Message` needs the bounds unconditionally, so the
  feature would be permanently on and would only add a broken configuration.
