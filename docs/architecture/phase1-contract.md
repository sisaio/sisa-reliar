# Phase-1 public contract (frozen)

**Status: FROZEN for Phase 1 — 2026-09-04.** Every signature below is the contract engineers build
against in parallel. **Changing anything here requires an ADR in `../decisions/` first**, then an
update to this file, then a notification to every engineer building against it (assume breaking
until proven otherwise).

Extracted from `../srs.md` v1.1 §7.2, §9–§13, §16, §17, §19, §22.2, §23.1, §24, §33.1, §35.1 and
resolved by ADRs 0001–0022. Where the SRS left a signature detail open, this file **decides it** and
the decision is listed in [§7 Decided here](#7-decided-here).

Conventions that apply to everything below and are not repeated:

- `#![forbid(unsafe_code)]`, `#![warn(missing_docs)]`; `///` on every public item stating the
  guarantee it upholds.
- Every public `struct`/`enum` that may grow is `#[non_exhaustive]` and is constructed through a
  builder or a `Default` + builder-method config (ADR 0022). **`#[non_exhaustive]` forbids
  struct-literal syntax outside the defining crate — including `..Default::default()` — so every
  type another crate must *build* (not just read) carries a documented `new`, `builder`, or
  `Default` + setters.** The types `reliar-store-postgres` constructs are listed in §3.3.
- Trait async methods declare `-> impl Future<Output = …> + Send` — **never `async fn` in the trait
  definition, never `#[async_trait]`** (ADR 0001). Implementors may write plain `async fn`.
- Errors are hand-rolled `#[non_exhaustive]` enums with manual `Display` + `std::error::Error` and a
  wired `source()`. **No `thiserror`, no `anyhow`.** No error `Display` ever prints payload bytes,
  header values, or a credentialed connection string.
- Time is `time::OffsetDateTime` (UTC) — **not `chrono`**. Durations cross settings boundaries as
  integer milliseconds.
- `Debug` on every public type, and **no Reliar type's `Debug` ever prints payload bytes or custom
  header values** (SRS §33: "message payloads and arbitrary custom headers SHALL NOT be logged by
  default"). Any type that holds either gets a **manual** `Debug` that elides or redacts it —
  `Envelope<T>`/`SerializedEnvelope` (body elided, §2.6), `EnvelopeBuilder<T>` (same, §2.6 — it
  holds the body before `build`, so it leaks exactly as readily as the envelope) and `Headers`
  (keys shown, values redacted, §2.5). A type is safe to *derive* `Debug` only when every field it
  holds is already safe, which is why `OutboxRecord` derives it (§3.2). This rule is the reason a
  derive is not the default here.

---

## 1. Crate map and the dependency rule

```text
reliar-core            → serde, bytes, uuid(v7), time                 (+ serde_json behind `json`)
   ↑
reliar-outbox          → reliar-core, uuid(v7), time, tracing,        (+ serde behind `serde`,
                         tokio, tokio-util                              metrics behind `metrics`)
   ↑
reliar-store-postgres  → reliar-outbox, reliar-core, sqlx, time, tracing
```

- **`reliar-core` is pure.** No sqlx, no postgres, no broker client, no transport routing concept
  (no Kafka partition key, no Rabbit exchange, no NATS subject). CI gates this with
  `cargo tree -p reliar-core -e normal` (ADR 0002).
- **`reliar-outbox` never depends on a provider**, and a provider never depends on another provider.
- Phase 1 creates **exactly these three crates** plus `examples/outbox-basic`,
  `examples/axum-outbox`, and `tests/system`. No `reliar`, `reliar-inbox`, `reliar-idempotency`,
  `reliar-transport-*` (SRS §6: crates are created when implementation begins).

**`reliar-outbox` does not depend on `bytes`** (confirmed 2026-09-04). It handles payloads only as
the opaque `SerializedEnvelope` re-exported from `reliar-core`, and never names `bytes::Bytes` in
its own source — a crate that never touches the payload cannot leak it. `tokio`/`tokio-util` arrive
with the dispatcher (S4); the S2 contract types need neither.

**Features.** `reliar-core`: `json` (default, `JsonSerializer` + `serde_json`), `serde` (derive on
settings/metadata types). `reliar-outbox`: `test-support` (the fakes), `metrics` (the `metrics`-crate
adapter), `serde`. `reliar-store-postgres`: `listen-notify` (opt-in wake-up). All additive;
`cargo hack --feature-powerset` must compile every combination.

---

## 2. `reliar-core`

### 2.1 Identity

```rust
/// UUIDv7, generated client-side by Reliar (ADR 0015).  Applications may supply any UUID;
/// Reliar never inspects or rejects its version.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageId(uuid::Uuid);
impl MessageId {
    #[must_use] pub fn new() -> Self;                 // fresh UUIDv7
    #[must_use] pub const fn from_uuid(id: uuid::Uuid) -> Self;
    /// By value — `Uuid` is 16 bytes and `Copy`.
    #[must_use] pub const fn as_uuid(&self) -> uuid::Uuid;
}
/// `Default` mints a fresh UUIDv7 (same as `new`), so the type does not trip
/// `clippy::new_without_default`.
impl Default for MessageId { fn default() -> Self { Self::new() } }

// identical shape, same derives, same `new`/`from_uuid`/`as_uuid`/`Default`:
pub struct ConversationId(uuid::Uuid);
pub struct RequestId(uuid::Uuid);

impl ConversationId {
    /// The reserved "not yet rooted" sentinel: the **nil** UUID. `CorrelationMetadata::default()`
    /// uses it, and `EnvelopeBuilder::build` replaces it with the envelope's own id (§2.4, §2.6).
    /// `new()`/`default()` mint a fresh UUIDv7 and are therefore never `UNSET`. An application
    /// SHALL NOT use the nil UUID as a real conversation id.
    pub const UNSET: Self = Self::from_uuid(uuid::Uuid::nil());

    /// `true` when this id is [`Self::UNSET`].
    #[must_use] pub const fn is_unset(&self) -> bool;
}

/// Application/business workflow correlation.  Capped at 256 chars — it lands in a `text`
/// column read on every claim.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CorrelationId(String);
impl CorrelationId {
    pub const MAX_LEN: usize = 256;
    pub fn parse(s: impl Into<String>) -> Result<Self, IdError>;
    #[must_use] pub fn as_str(&self) -> &str;
}

#[derive(Debug)] #[non_exhaustive]
pub enum IdError {
    Empty,
    /// Any Unicode `Cc` code point, which includes CR and LF.  These values are written into
    /// transport headers by an `EnvelopeMapper` (§2.7), where a bare CRLF splits the header
    /// block — so validation happens at the boundary that creates the value, not at each wire
    /// format that consumes it.
    ControlCharacter,
    TooLong { len: usize, max: usize },
}
```

`Display` on every id newtype renders the inner value verbatim.

### 2.2 Message contract identity

```rust
/// A type that can be persisted and published.  `TYPE`/`VERSION` are stable **application
/// contracts** and are never derived from `type_name::<T>()` or a module path (ADR 0010).
pub trait Message: serde::Serialize + serde::de::DeserializeOwned {
    const TYPE: &'static str;
    const VERSION: u16;
}

/// Name and version carried separately, so SQL can filter a name across versions.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MessageType { /* name: Cow<'static, str>, version: u16 */ }
impl MessageType {
    #[must_use] pub const fn new(name: &'static str, version: u16) -> Self;
    /// Rehydration path: a provider reads two columns back into a `MessageType`.
    pub fn from_parts(name: impl Into<std::borrow::Cow<'static, str>>, version: u16) -> Self;
    #[must_use] pub fn of<T: Message>() -> Self;      // T::TYPE + T::VERSION
    #[must_use] pub fn name(&self) -> &str;
    #[must_use] pub const fn version(&self) -> u16;
}
/// Renders `"{name}.v{version}"` — e.g. `orders.created.v1`.  **Stable public contract**:
/// clients parse it.  Two distinct Rust types sharing TYPE/VERSION render identically.
impl std::fmt::Display for MessageType { … }
```

### 2.3 Serialization

```rust
/// Validated MIME type.  Owned by the `Serializer`, never chosen at the call site (ADR 0010).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ContentType(std::borrow::Cow<'static, str>);
impl ContentType {
    pub const JSON: Self;                                             // "application/json"
    /// Capped for the same reason as `CorrelationId::MAX_LEN`: the value lands in a `text`
    /// column that every claim reads back, and is rehydrated into `DeliveryMetadata`.
    pub const MAX_LEN: usize = 256;
    /// Rejects an empty value, one over `MAX_LEN`, or one containing a control character
    /// (including CR/LF — the header-injection rule of §2.1 and §2.5).
    pub fn parse(s: impl Into<std::borrow::Cow<'static, str>>) -> Result<Self, ContentTypeError>;
    #[must_use] pub fn as_str(&self) -> &str;
}

#[derive(Debug)] #[non_exhaustive]
pub enum ContentTypeError {
    Empty,
    TooLong { len: usize, max: usize },
    Malformed { value: String },
}
```

**`ContentTypeError::Malformed`'s `Display` echoes at most 64 characters** of the rejected value,
truncated with `…` — a fixed prefix, independent of `MAX_LEN`. A content type is
developer-supplied rather than end-user data, so echoing a prefix is what makes the error
actionable; echoing all 256 characters would put an arbitrary attacker-influenced string into a log
line at full length. The cap is deliberately much shorter than `MAX_LEN` so the two cannot drift
into "the whole value" if `MAX_LEN` is ever raised. `TooLong` reports `len`/`max` only and echoes
nothing.

```rust
/// Body ⇄ bytes.  Lives in core: it touches neither storage nor transport.  Stateless and cheap;
/// never placed behind a `dyn` on the enqueue path.
pub trait Serializer: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// The content type this serializer produces.  Populates both
    /// `DeliveryMetadata.content_type` and the provider's `content_type` column — one value.
    fn content_type(&self) -> &ContentType;
    fn serialize<T: Message>(&self, body: &T) -> Result<bytes::Bytes, Self::Error>;
    fn deserialize<T: Message>(&self, bytes: &[u8]) -> Result<T, Self::Error>;
}

/// Default implementation, behind the default `json` feature.
#[derive(Clone, Debug, Default)]
pub struct JsonSerializer;
impl Serializer for JsonSerializer { type Error = JsonError; … }

#[derive(Debug)] #[non_exhaustive]
pub enum JsonError { Serialize { source: serde_json::Error }, Deserialize { source: serde_json::Error } }
```

`JsonError::Display` names the message type and the serde position — **never the payload**.

### 2.4 Metadata

```rust
/// Canonical, typed framework metadata.  The single source of truth: no value here is ever
/// duplicated into `Headers` (ADR 0004).
/// **Derive rule for this family:** `Eq` wherever every field is `Eq` (`TraceContext`,
/// `RoutingMetadata`, `EndpointAddress`, `Headers`), plain `PartialEq` elsewhere.  Adding `Eq`
/// later is additive, so a type only claims it once it is certain.
#[derive(Clone, Debug, Default, PartialEq)] #[non_exhaustive]
pub struct Metadata {
    pub correlation: CorrelationMetadata,
    pub trace:       TraceContext,
    pub routing:     RoutingMetadata,
    pub delivery:    DeliveryMetadata,
    pub tenant_id:   Option<String>,
}

#[derive(Clone, Debug, PartialEq)] #[non_exhaustive]
pub struct CorrelationMetadata {
    pub correlation_id:  Option<CorrelationId>,
    pub conversation_id: ConversationId,
    pub causation_id:    Option<MessageId>,
    pub request_id:      Option<RequestId>,
}
/// `conversation_id` defaults to the `ConversationId::UNSET` sentinel (the nil UUID), **not** to
/// a fresh mint: it is a placeholder that must stay recognisable.  `EnvelopeBuilder::build`
/// replaces `UNSET` with the envelope's own id, so an un-correlated message is the root of its
/// own conversation, and leaves any non-`UNSET` value alone (§2.6).  A `Metadata` that reaches a
/// store or mapper without passing through the builder carries whatever the caller set —
/// providers persist the value verbatim and never re-root it.
impl Default for CorrelationMetadata { … }

/// W3C Trace Context, carried verbatim.  Reliar never invents or re-derives it (ADR 0020).
#[derive(Clone, Debug, Default, PartialEq, Eq)] #[non_exhaustive]
pub struct TraceContext { pub traceparent: Option<String>, pub tracestate: Option<String> }

/// Transport-independent routing only.  Kafka partition keys, Rabbit exchanges and NATS
/// subject options are transport types and SHALL NOT appear here.
#[derive(Clone, Debug, Default, PartialEq, Eq)] #[non_exhaustive]
pub struct RoutingMetadata {
    pub source:      Option<EndpointAddress>,
    pub destination: Option<EndpointAddress>,
    pub reply_to:    Option<EndpointAddress>,
}

/// Opaque, transport-interpreted address string.  Capped at 256 chars.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EndpointAddress(String);
impl EndpointAddress { pub const MAX_LEN: usize = 256;
    pub fn parse(s: impl Into<String>) -> Result<Self, IdError>;
    #[must_use] pub fn as_str(&self) -> &str; }

#[derive(Clone, Debug, Default, PartialEq)] #[non_exhaustive]
pub struct DeliveryMetadata {
    /// **Authoritatively set by the store at enqueue** from `Serializer::content_type()`, and
    /// read back from the `content_type` column on rehydration.  `Default` is `ContentType::JSON`
    /// as a placeholder; a call site never chooses it (ADR 0010).
    pub content_type:     ContentType,
    pub sent_at:          Option<time::OffsetDateTime>,// app clock (ADR 0009)
    /// Enforced in the claim predicate in DB time; an expired pending row goes dead with
    /// `DeadReason::Expired` and consumes no retry attempt (ADR 0009).
    pub expires_at:       Option<time::OffsetDateTime>,
    /// Emitted by a transport mapper as that broker's dedup key (falling back to `message_id`).
    /// Reliar never deduplicates on it in the database.
    pub deduplication_id: Option<String>,
}
```

### 2.5 Headers

```rust
/// Application-defined metadata Reliar does not understand.  Validating newtype — never a
/// `HashMap` alias, and never `Deref`s to one (ADR 0011).
///
/// **`Debug` is a manual impl, never derived** (RELIAR-12 review 1): a derived `Debug` would print
/// every custom header key *and value*, which SRS §33 forbids.  It prints the keys — they are
/// low-cardinality, framework-adjacent and genuinely useful when debugging a mapper — and redacts
/// every value.  It is written with `f.debug_map()`, so the output is the bare map form, with no
/// `Headers(…)` wrapper:
///
/// ```text
/// {"x-import-batch": "<redacted>", "x-api-key": "<redacted>"}
/// ```
///
/// Values are the half that carries tokens, ids and customer data, and no `Debug` output is worth
/// leaking one.  A test that must assert a value uses `get`, not `format!("{:?}")`.
#[derive(Clone, Default, PartialEq, Eq)]        // `Debug` is manual, above
pub struct Headers(std::collections::HashMap<String, String>);

impl Headers {
    pub const RESERVED_PREFIX: &'static str = "reliar-";
    pub const MAX_KEY_LEN:   usize = 128;
    pub const MAX_VALUE_LEN: usize = 1024;
    pub const MAX_COUNT:     usize = 32;

    /// Returns `Err` — never a silent drop or overwrite — for the reserved `reliar-` prefix
    /// (matched **case-insensitively**), an empty key, any cap breach, or a **control character
    /// in either the key or the value**.
    ///
    /// The control-character rule is a **header-injection defence**, the same one
    /// `CorrelationId`, `EndpointAddress` and `ContentType` carry (§2.1, §2.4): custom headers are
    /// written verbatim onto the wire by an `EnvelopeMapper` (§2.7), and a bare CR or LF in a key
    /// or value terminates the header and lets an application-supplied string forge the ones that
    /// follow.  Validating once here — at the boundary that creates the value — is what keeps
    /// every present and future transport mapper from having to re-derive the rule.  The test is
    /// Unicode `Cc` (which covers CR, LF, NUL and the C0/C1 ranges), applied to keys and values
    /// alike.
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>)
        -> Result<Option<String>, HeaderError>;
    /// Replacing the value of a key that is **already present** always succeeds, even at
    /// `MAX_COUNT` — the count does not grow.  Only a new key can breach the cap.
    #[must_use] pub fn get(&self, key: &str) -> Option<&str>;
    pub fn remove(&mut self, key: &str) -> Option<String>;
    #[must_use] pub fn len(&self) -> usize;
    #[must_use] pub fn is_empty(&self) -> bool;
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)>;
}

#[derive(Debug)] #[non_exhaustive]
pub enum HeaderError {
    Reserved { key: String },
    EmptyKey,
    /// A control character in the key.  The key is safe to carry — keys are low-cardinality and
    /// are already printed by `Headers`' `Debug`.
    ControlCharacterInKey { key: String },
    /// A control character in the value.  Carries only the **key**: the offending value is
    /// exactly the kind of application data no error `Display` may echo (SRS §33).
    ControlCharacterInValue { key: String },
    KeyTooLong { len: usize },
    ValueTooLong { len: usize },
    TooManyHeaders { limit: usize },
}
```

`traceparent`/`tracestate` are **not** reserved: they are W3C names, framework-owned via
`Metadata.trace`, and the mapper's value overrides a user-set header (ADR 0004).

### 2.6 Envelope

```rust
#[derive(Clone, PartialEq)]                  // `Clone` only where `T: Clone`; see below
#[non_exhaustive]
pub struct Envelope<T> {
    pub id:           MessageId,
    pub message_type: MessageType,
    pub body:         T,
    pub metadata:     Metadata,
    pub(crate) headers: Option<Headers>,     // private: preserves the Headers invariants
}

/// The persistence/transport form.
pub type SerializedEnvelope = Envelope<bytes::Bytes>;
```

**Derives are public API and are fixed here (review 1, blocker 4).**

- `PartialEq` is derived (`where T: PartialEq`), so §43.A.4's enqueue → acquire round-trip
  equality test can exist. It compares `id`, `message_type`, `body`, `metadata` and `headers`.
- `Clone` is derived but only applies `where T: Clone`. **Nothing in Reliar requires `T: Clone`** —
  the dispatcher moves owned records into publish tasks (SRS §9.1). The derive exists for tests
  and host code.
- **`Debug` is a manual impl that elides `body` entirely, for every `T`**, printing
  `body: "<elided>"`. Rust has no specialization, so a derived `Debug` would print payload bytes
  for `SerializedEnvelope` and payload fields for `Envelope<T>` — both forbidden by §33/ADR 0020.
  A failing `assert_eq!` therefore shows every field except the body; tests that need to see it
  compare `envelope.body` directly.
- `OutboxRecord` therefore **derives** `Debug` safely, and the claim is now exact: its only
  sensitive content sits inside its `SerializedEnvelope`, whose manual `Debug` elides the body and
  whose `Headers` has its own manual `Debug` redacting every value (§2.5). Both of the forbidden
  categories — payload bytes and header values — are stopped by a manual impl one level down, so the
  derive at this level cannot leak either. `last_error` is already truncated and redacted (§17.1).

```rust

impl<T> Envelope<T> {
    #[must_use] pub fn headers(&self) -> Option<&Headers>;
    pub fn headers_mut(&mut self) -> &mut Headers;                       // lazily allocates
    /// Sets the whole map (rehydration path for providers and mappers).
    pub fn set_headers(&mut self, headers: Option<Headers>);
    /// The only conversion between typed and serialized forms — no field is ever re-declared.
    #[must_use] pub fn map_body<U>(self, f: impl FnOnce(T) -> U) -> Envelope<U>;
    /// Fallible variant, for `SerializedEnvelope → Envelope<T>` via a `Serializer`.
    pub fn try_map_body<U, E>(self, f: impl FnOnce(T) -> Result<U, E>) -> Result<Envelope<U>, E>;
}

impl<T: Message> Envelope<T> {
    #[must_use] pub fn builder(body: T) -> EnvelopeBuilder<T>;
}

/// Rehydration entry point for providers and transport mappers, which have a `MessageType`
/// read from storage/wire rather than from a Rust type.
impl SerializedEnvelope {
    pub fn from_parts(id: MessageId, message_type: MessageType, body: bytes::Bytes,
                      metadata: Metadata, headers: Option<Headers>) -> Self;
}

/// **Manual `Debug`, body elided** — the builder holds the body before `build`, so a derive would
/// leak exactly what `Envelope`'s own `Debug` refuses to print.
#[must_use]
pub struct EnvelopeBuilder<T> { /* … */ }
impl<T: Message> EnvelopeBuilder<T> {
    pub fn id(self, id: MessageId) -> Self;                     // default: a fresh UUIDv7
    pub fn metadata(self, metadata: Metadata) -> Self;
    pub fn correlation(self, correlation: CorrelationMetadata) -> Self;
    pub fn correlation_id(self, id: CorrelationId) -> Self;
    /// Joins an existing conversation (typically the causing message's `conversation_id`).
    pub fn conversation(self, id: ConversationId) -> Self;
    pub fn causation(self, parent: MessageId) -> Self;
    pub fn tenant(self, tenant_id: impl Into<String>) -> Self;
    pub fn expires_at(self, at: time::OffsetDateTime) -> Self;
    pub fn trace(self, traceparent: impl Into<String>, tracestate: Option<String>) -> Self;
    pub fn header(self, k: impl Into<String>, v: impl Into<String>) -> Result<Self, HeaderError>;
    /// `message_type = MessageType::of::<T>()` — never passed in.
    /// `conversation_id`: **iff** it is still `ConversationId::UNSET`, it becomes this
    /// envelope's own id (the message roots its own conversation); any other value is kept.
    pub fn build(self) -> Envelope<T>;
}
```

**Conversation rooting is decided by the *value*, not by which setter was called.** `build`
tests `metadata.correlation.conversation_id.is_unset()` and nothing else — the builder keeps no
"was correlation set explicitly?" flag. Consequences, all intended:

- `.metadata(m)` / `.correlation(c)` built from `Default` — the common "tweak one unrelated
  field" path — still root at the envelope's own id, because the sentinel travels with them.
- `.conversation(id)`, or a `CorrelationMetadata` carrying a real conversation id (e.g. copied
  from the causing message), wins — `build` never overwrites it.
- Setter order is irrelevant: `.conversation(x).metadata(m)` yields `m`'s conversation id, since
  `.metadata` replaces the whole struct. Documented on `.metadata`.

**`Envelope<T>` SHALL NOT require `T: Clone`** — the dispatcher moves owned records into publish
tasks (SRS §9.1).

### 2.7 Transport mapping (defined in Phase 1, implemented in Phase 2)

```rust
/// Wire representation of the canonical envelope.  Transport headers are a *projection* of
/// `Metadata`, not a second source of truth (ADR 0004).
pub trait EnvelopeMapper<M> {
    type Error: std::error::Error + Send + Sync + 'static;
    fn encode(&self, envelope: &SerializedEnvelope) -> Result<M, Self::Error>;
    fn decode(&self, message: M) -> Result<SerializedEnvelope, Self::Error>;
}
```

No implementation ships in Phase 1. The reserved `reliar-*` header names a mapper writes are
listed in SRS §14 and are a public contract.

---

## 3. `reliar-outbox`

### 3.1 Worker identity and ordering

```rust
/// Lease-ownership guard key.  Generated **once per dispatcher instance** — not per batch, not
/// per claim.  Default **`pid:uuid7`** — deliberately **no host segment**: reading `HOSTNAME`
/// would be the library reading the environment implicitly, which ADR 0019 forbids without
/// exception.  A host that wants its pod name in the id sets one explicitly through
/// `DispatcherSettings::worker_id` or `RELIAR_OUTBOX_WORKER_ID`.  Unique per running dispatcher
/// and deliberately **not** stable across restarts (ADR 0011).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct WorkerId(String);
impl WorkerId {
    pub const MAX_LEN: usize = 128;
    #[must_use] pub fn generate() -> Self;
    pub fn parse(s: impl Into<String>) -> Result<Self, IdError>;
    #[must_use] pub fn as_str(&self) -> &str;
}
impl Default for WorkerId { fn default() -> Self { Self::generate() } }

/// `Unordered` guarantees **nothing** about order — not globally, not per conversation, not per
/// aggregate, not approximately (ADR 0013).  `PerKey` is implemented in 0.2; selecting it in
/// v0.1 is a configuration error.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)] #[non_exhaustive]
pub enum Ordering { #[default] Unordered, PerKey }
```

### 3.2 The record

```rust
/// An envelope **plus** its outbound delivery state.  Distinct from `Envelope` (ADR 0005):
/// nothing here reaches the wire.
///
/// `Clone` and `PartialEq` are required API (review 2, blocker 2): the `test-support` fakes hand
/// records out by value and the `unit` acceptance criteria compare them.  **`Debug` is derived and
/// is payload-safe** — it delegates to `Envelope`'s manual `Debug`, which elides the body for every
/// `T`; `last_error` is already truncated and redacted (SRS §17.1).
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct OutboxRecord {
    pub envelope:     SerializedEnvelope,

    pub sequence:     i64,                  // monotonic, store-assigned; not gap-free
    pub created_at:   time::OffsetDateTime, // immutable (ADR 0016)
    pub ordering_key: Option<String>,       // None = unordered

    /// Publish outcomes observed, **not** claims (ADR 0009).  A claimed-then-crashed row
    /// reports 0.
    pub attempts:     u32,
    pub available_at: time::OffsetDateTime,

    pub locked_by:    Option<WorkerId>,
    pub locked_until: Option<time::OffsetDateTime>,

    pub published_at: Option<time::OffsetDateTime>,
    pub dead_at:      Option<time::OffsetDateTime>,
    pub dead_reason:  Option<DeadReason>,

    /// Truncated to 2 KiB at a char boundary with `"…[truncated]"`; `Display` output only —
    /// never payload bytes, header values, or credentials.
    pub last_error:   Option<String>,
}

impl OutboxRecord {
    /// The value the dead-letter API and partition-pruned by-id operations take.
    #[must_use] pub fn message_ref(&self) -> MessageRef;

    /// **Provider entry point.**  `OutboxRecord` is `#[non_exhaustive]`, so a crate other than
    /// `reliar-outbox` cannot build one with struct-literal syntax — every provider rehydrating a
    /// row goes through this builder.  Defaults: `attempts = 0`, `available_at = created_at`, no
    /// lease, not published, not dead, no error, no ordering key.
    #[must_use] pub fn builder(envelope: SerializedEnvelope, sequence: i64,
                               created_at: time::OffsetDateTime) -> OutboxRecordBuilder;
}

#[must_use]
pub struct OutboxRecordBuilder { /* … */ }
impl OutboxRecordBuilder {
    pub fn ordering_key(self, key: Option<String>) -> Self;
    pub fn attempts(self, attempts: u32) -> Self;
    pub fn available_at(self, at: time::OffsetDateTime) -> Self;
    pub fn lease(self, by: Option<WorkerId>, until: Option<time::OffsetDateTime>) -> Self;
    pub fn published_at(self, at: Option<time::OffsetDateTime>) -> Self;
    pub fn dead(self, at: Option<time::OffsetDateTime>, reason: Option<DeadReason>) -> Self;
    /// Truncates to 2 KiB at a char boundary with `"…[truncated]"` (SRS §17.1).
    pub fn last_error(self, error: Option<String>) -> Self;
    pub fn build(self) -> OutboxRecord;
}
```

### 3.3 Contract types

**Construction (review 1, blocker 1).** `OutboxRecord`, `AcquiredBatch`, `DeadLetterPage`,
`PoisonedRow`, `MessageRef`, `PurgeReport` and `OutboxStats` are **built by the provider**, in a
different crate
from where they are declared. Each therefore has an inherent constructor or builder below;
`CompletedMessage`, `FailedMessage`, `AcquireRequest`, `PurgeRequest` and `DeadQuery` get one too,
because tests, operator surfaces and the `test-support` fakes build them from outside as well.

```rust
#[derive(Clone, Debug)] #[non_exhaustive]
pub struct AcquireRequest {
    pub worker:     WorkerId,
    pub batch_size: u32,        // default 100
    pub lease:      Duration,   // default 30 s
    pub ordering:   Ordering,   // default Unordered
}
impl AcquireRequest { pub fn new(worker: WorkerId) -> Self; /* + builder methods */ }

#[derive(Debug, Default)] #[non_exhaustive]
pub struct AcquiredBatch {
    pub records:  Vec<OutboxRecord>,
    /// Rows the store could not decode.  **Already moved to dead** by the same call with
    /// `DeadReason::Undecodable` (ADR 0008) — the caller only reports them.
    pub poisoned: Vec<PoisonedRow>,
}
impl AcquiredBatch {
    #[must_use] pub fn new(records: Vec<OutboxRecord>, poisoned: Vec<PoisonedRow>) -> Self;
    #[must_use] pub fn is_empty(&self) -> bool;      // no records *and* no poisoned rows
}

/// One page of `list_dead`.  Poisoned rows here are **already dead**, so unlike `AcquiredBatch`
/// there is no transition to make — they are reported so an operator can see them.
#[derive(Debug, Default)] #[non_exhaustive]
pub struct DeadLetterPage {
    pub records:  Vec<OutboxRecord>,
    pub poisoned: Vec<PoisonedRow>,
    /// The **largest `sequence` scanned** in this page, poisoned rows included.  Feed it into
    /// the next `DeadQuery::after_sequence`.
    ///
    /// `None` when the page was **not full**, which is the caller's termination condition.
    /// "Full" is `records.len() + poisoned.len() == query.limit` — poisoned rows occupy a slot
    /// in the page because they occupy a row in the scan, so counting only `records` would stop
    /// pagination early on a page whose tail happened to be undecodable.
    pub next_after_sequence: Option<i64>,
}
impl DeadLetterPage {
    #[must_use] pub fn new(records: Vec<OutboxRecord>, poisoned: Vec<PoisonedRow>,
                           next_after_sequence: Option<i64>) -> Self;
    #[must_use] pub fn is_empty(&self) -> bool;
}

#[derive(Clone, Debug)] #[non_exhaustive]
pub struct PoisonedRow { pub id: MessageId, pub sequence: i64, pub error: String }
impl PoisonedRow {
    /// `error` is truncated to 2 KiB at a char boundary.
    #[must_use] pub fn new(id: MessageId, sequence: i64, error: impl Into<String>) -> Self;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)] #[non_exhaustive]
pub struct MessageRef { pub id: MessageId, pub created_at: time::OffsetDateTime }
impl MessageRef {
    #[must_use] pub const fn new(id: MessageId, created_at: time::OffsetDateTime) -> Self;
}

#[derive(Clone, Debug)] #[non_exhaustive]
pub struct CompletedMessage { pub message: MessageRef }
impl CompletedMessage { #[must_use] pub const fn new(message: MessageRef) -> Self; }

#[derive(Clone, Debug)] #[non_exhaustive]
pub struct FailedMessage {
    pub message: MessageRef,
    pub error:   String,          // already truncated and redacted
    pub outcome: FailureOutcome,  // the store never re-derives policy
}
impl FailedMessage {
    #[must_use] pub fn new(message: MessageRef, error: impl Into<String>,
                           outcome: FailureOutcome) -> Self;
}

#[derive(Clone, Debug, PartialEq, Eq)] #[non_exhaustive]
pub enum FailureOutcome {
    /// The store applies it as `available_at = now() + delay` **in SQL** (ADR 0009).
    Retry { delay: Duration },
    /// Terminal.  The store sets `dead_at = now()` and records `reason`.
    Dead { reason: DeadReason },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)] #[non_exhaustive]
pub enum DeadReason { PermanentError, AttemptsExhausted, Expired, Undecodable }

#[derive(Clone, Debug)] #[non_exhaustive]
pub struct PurgeRequest {
    pub published_retention: Option<Duration>,  // default 7 days
    pub dead_retention:      Option<Duration>,  // default None = keep until explicit purge
    /// Bounds **all three** statements in a pass — the published delete, the dead delete, and
    /// the expired→dead sweep.  Default 1000.
    pub batch_size:          u32,
}
/// **Hand-written, never derived** (review 2, blocker 1): a derived `Default` would give
/// `None / None / 0` — a `purge` that deletes nothing and reports success.
impl Default for PurgeRequest {
    fn default() -> Self {
        Self { published_retention: Some(Duration::from_secs(7 * 24 * 60 * 60)),
               dead_retention: None,
               batch_size: 1_000 }
    }
}
impl PurgeRequest { /* builder methods; `#[non_exhaustive]`, so no struct literal */ }

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)] #[non_exhaustive]
pub struct PurgeReport { pub published_deleted: u64, pub dead_deleted: u64, pub expired_to_dead: u64 }
impl PurgeReport {
    #[must_use] pub const fn new(published_deleted: u64, dead_deleted: u64,
                                 expired_to_dead: u64) -> Self;
    /// `true` only when **all three** counts came in under `batch_size`.  Any one of them
    /// reaching the bound means that statement was cut short and the caller should call `purge`
    /// again — so the host's drain loop drains expiry as well as the two deletes
    /// (one call = **one bounded pass**; see §3.4).
    #[must_use] pub const fn is_complete(&self, batch_size: u32) -> bool;
}

#[derive(Clone, Copy, Debug)] #[non_exhaustive]
pub struct OutboxStats {
    /// **Claimable** rows only — same predicate as the claim, so an expired row is excluded
    /// (review 2, major 5).
    pub pending: u64,
    pub dead:    u64,
    /// Expired-but-not-yet-swept pending rows: unclaimable, awaiting the next `purge`.  Counted
    /// separately so they can be alerted on without pinning lag.  Served by `ix_outbox_expires`,
    /// the same partial index as the expiry sweep (§4).
    pub expired_pending: u64,
    /// Over claimable rows only, so it cannot be pinned by an expired row.
    pub oldest_pending_available_at: Option<time::OffsetDateTime>,
    /// DB `now()` at the moment of the query, so lag is computed without comparing clocks.
    pub as_of:   time::OffsetDateTime,
}
impl OutboxStats {
    #[must_use] pub const fn new(pending: u64, dead: u64, expired_pending: u64,
                                 oldest_pending_available_at: Option<time::OffsetDateTime>,
                                 as_of: time::OffsetDateTime) -> Self;
    /// `as_of - oldest_pending_available_at`, clamped at zero.  The "outbox lag" gauge.
    #[must_use] pub fn lag(&self) -> Option<Duration>;
}

/// Builder methods only (`#[non_exhaustive]`, so no struct literal outside the crate).
#[derive(Clone, Debug)] #[non_exhaustive]
pub struct DeadQuery {
    pub message_type:   Option<String>,
    pub tenant_id:      Option<String>,
    pub dead_before:    Option<time::OffsetDateTime>,
    pub limit:          u32,          // provider-capped; default 100
    /// Keyset cursor over `sequence`, the column `list_dead` orders by.  Exclusive lower bound.
    pub after_sequence: Option<i64>,
}
impl DeadQuery {
    /// Builder methods; `#[non_exhaustive]`, so no struct literal outside the crate.
    #[must_use] pub fn message_type(self, ty: impl Into<String>) -> Self;
    #[must_use] pub fn tenant_id(self, tenant: impl Into<String>) -> Self;
    #[must_use] pub fn dead_before(self, at: time::OffsetDateTime) -> Self;
    #[must_use] pub fn limit(self, limit: u32) -> Self;
    #[must_use] pub fn after_sequence(self, sequence: i64) -> Self;
}
/// **Hand-written, never derived** (review 2, blocker 1): a derived `Default` would set
/// `limit = 0` and return nothing.
impl Default for DeadQuery {
    fn default() -> Self {
        Self { message_type: None, tenant_id: None, dead_before: None,
               limit: 100, after_sequence: None }
    }
}
```

### 3.4 `OutboxStore`

```rust
/// The dispatcher's portable side of the outbox.  **`enqueue` is deliberately not here** — it
/// must join the application's own transaction and stays provider-inherent (ADR 0008).
///
/// Every state-changing method takes `worker` and **SHALL** match it (`AND locked_by = $worker`).
/// A store that ignores it does not implement this trait.  Each returns rows affected; a
/// shortfall means the lease was lost and is **benign, never an error**.
pub trait OutboxStore: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static + Classify;

    /// Claims up to `batch_size` due, unlocked, unexpired rows.  **Must have committed before
    /// the future resolves** — the caller publishes outside any transaction (ADR 0006).
    /// `Self::Error` is for failures of the *call*, never the content of a row.
    fn acquire(&self, request: AcquireRequest)
        -> impl Future<Output = Result<AcquiredBatch, Self::Error>> + Send;

    /// Marks rows published and increments `attempts`.  Idempotent under the worker guard.
    fn complete(&self, worker: &WorkerId, items: &[CompletedMessage])
        -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Applies each item's `FailureOutcome` and increments `attempts`.
    fn fail(&self, worker: &WorkerId, items: &[FailedMessage])
        -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Hands rows back at once: clears the lease, `available_at` unchanged,
    /// **`attempts` unchanged**.  Used on graceful shutdown.
    fn release(&self, worker: &WorkerId, items: &[MessageRef])
        -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Renews `locked_until = now() + lease` for rows this worker still owns.  Best-effort.
    fn extend_lease(&self, worker: &WorkerId, items: &[MessageRef], lease: Duration)
        -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// **One bounded pass** (review 1, major 9).  All **three** statements are bounded by
    /// `request.batch_size`: at most `batch_size` published rows deleted, at most `batch_size`
    /// dead rows deleted, and at most `batch_size` expired pending rows moved to dead
    /// (`DeadReason::Expired`) — the sweep is an `UPDATE … WHERE id IN (SELECT … LIMIT n)`, never
    /// an unbounded `UPDATE`.  An unbounded `UPDATE` on `outbox` is the same hazard as an
    /// unbounded `DELETE`: it holds row locks across the whole matched set, blocks concurrent
    /// claims, and writes one WAL record per row with no cancellation point.
    ///
    /// It **does not loop internally** — unbounded work inside a trait method has no cancellation
    /// point and no progress reporting.  The **caller** repeats while
    /// `!report.is_complete(batch_size)`, which is `true` only when all three counts are under the
    /// bound, so the host loop drains expiry too.  Reliar starts no maintenance timer; the host
    /// calls this from its own periodic task.
    ///
    /// **The sweep never touches a row with a live lease.**  Its predicate carries the claim's
    /// own lease clause — `published_at IS NULL AND dead_at IS NULL AND expires_at <= now() AND
    /// (locked_until IS NULL OR locked_until < now())` — so it moves only rows that are *expired
    /// and unowned*.  A leased row that expired **after** it was claimed belongs to its worker:
    /// that worker's `complete` still wins if the publish succeeded, and the row becomes sweepable
    /// once the lease lapses, at most one lease later.  Dead-lettering it under the owner would be
    /// a write racing an unguarded maintenance statement against a worker-guarded one, and
    /// `ck_outbox_terminal` (`published_at IS NULL OR dead_at IS NULL`) turns that race into a
    /// **constraint violation on a healthy path** — a successful publish reported by its owner,
    /// rejected because maintenance had already marked the row dead.
    ///
    /// The `test-support` `InMemoryOutboxStore` mirrors the same bound and the same predicate, so a
    /// host's drain loop behaves identically against the fake and against Postgres.
    fn purge(&self, request: PurgeRequest)
        -> impl Future<Output = Result<PurgeReport, Self::Error>> + Send;

    /// Feeds the outbox-lag and dead-count gauges.  **The dispatcher's `run` loop ticks this
    /// every `stats_interval`** and forwards the result to `OutboxMetrics::{pending,
    /// expired_pending, oldest_pending_age}`; `Duration::ZERO` disables the tick.  Never called
    /// per batch.  It is also `pub`, so a host may call it directly for an admin endpoint.
    fn stats(&self) -> impl Future<Output = Result<OutboxStats, Self::Error>> + Send;
}

/// Operator surface.  A separate small capability — the dispatcher never calls it (SRS §34).
pub trait OutboxDeadLetters: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// **`ORDER BY sequence ASC` — normative, not an implementation detail** (review 3,
    /// blocker 1).  `after_sequence` is a keyset cursor, and a keyset cursor is only correct over
    /// the column it orders by: paginating by `sequence` while ordering by `(dead_at, sequence)`
    /// silently skips every row whose `dead_at` sorts before the previous page's tail.  `sequence`
    /// is the store-assigned monotonic identity (§22.2) and is unique, so it needs no tiebreak.
    /// `dead_before` and the other `DeadQuery` fields are **filters**, never part of the order.
    ///
    /// Returns a **batch**, not a bare `Vec` (review 2, major 4): a dead row can itself be
    /// undecodable, and ADR 0023 says such a row is reported, never silently skipped.  The page
    /// also carries the keyset cursor, computed over **every row scanned including the poisoned
    /// ones** — deriving it from the last decoded record would loop forever on a poisoned tail.
    fn list_dead(&self, query: DeadQuery)
        -> impl Future<Output = Result<DeadLetterPage, Self::Error>> + Send;

    /// Returns dead rows to pending: clears `dead_at`/`dead_reason`, sets `available_at = now()`,
    /// resets `attempts` to 0, keeps `last_error` for audit.  Affects only `dead_at IS NOT NULL`.
    /// The **only** operation in the system that resets `attempts`, and always an explicit
    /// operator action.  Not worker-guarded — a dead row holds no lease.
    fn retry_dead(&self, refs: &[MessageRef])
        -> impl Future<Output = Result<u64, Self::Error>> + Send;

    fn purge_dead(&self, refs: &[MessageRef])
        -> impl Future<Output = Result<u64, Self::Error>> + Send;
}
```

### 3.5 `Publisher` and classification

```rust
pub trait Publisher: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static + Classify;

    fn publish(&self, envelope: &SerializedEnvelope)
        -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Results are **positional** — one per envelope, so a partial batch failure never loses a
    /// per-message verdict.  The default loops; transports with a native batch API override it.
    fn publish_batch(&self, envelopes: &[SerializedEnvelope])
        -> impl Future<Output = Vec<Result<(), Self::Error>>> + Send
    { async move { let mut out = Vec::with_capacity(envelopes.len());
                   for e in envelopes { out.push(self.publish(e).await); } out } }
}

/// Carried **by the error type**, not by the publisher: the error value is what crosses the
/// `JoinSet` boundary into the dispatcher, so it must carry its own verdict (ADR 0008).
pub trait Classify { fn kind(&self) -> FailureKind; }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureKind { Transient, Permanent }
```

**v0.1's dispatcher calls `publish`, not `publish_batch`** — it needs a per-message outcome and a
per-message timeout, and the default `publish_batch` is a loop over `publish` anyway. The method is
in the trait now because the shape is semver-visible (ADR 0008); it is covered by a direct test of
the default implementation, not by a dispatcher test, and a transport that overrides it owns
proving its positional results.

A publish **timeout classifies as `Transient`**. A payload the broker rejects as too large
classifies as `Permanent` — retrying forever cannot help (SRS §24.1).

### 3.6 Retry policy

```rust
/// Pure: I/O-free and **clock-free**.  Returns a `Duration`, never a timestamp — the store
/// applies `now() + delay` in SQL (ADR 0009).
pub trait RetryPolicy: Send + Sync {
    /// `attempts` is the count **before** this outcome.
    fn next(&self, attempts: u32, kind: FailureKind) -> FailureOutcome;
}

#[derive(Clone, Copy, Debug, PartialEq)] #[non_exhaustive]
pub struct ExponentialBackoff {
    pub base:         Duration,  // 1 s
    pub max_delay:    Duration,  // 5 min
    pub max_attempts: u32,       // 10
    pub jitter:       f64,       // 0.2 → delay × U(0.8, 1.2)
}
impl Default for ExponentialBackoff { /* the values above */ }
impl ExponentialBackoff { /* `Default` + builder methods */ }
```

Invariants (proptest-able without a database): `Permanent` → `Dead { PermanentError }` whatever
`attempts`; `attempts + 1 >= max_attempts` → `Dead { AttemptsExhausted }`; otherwise
`Retry { delay = min(max_delay, base × 2^attempts) × jitter_factor }`, monotonic in `attempts`,
capped at `max_delay × (1 + jitter)`, never zero.

**The exponent is computed with saturating arithmetic** — `base.saturating_mul(2u32.saturating_pow(attempts))`
then `min(max_delay, …)` — so a large `attempts` (a resurrected dead row, a hand-edited column)
saturates at the cap instead of overflowing. Fields are `pub` for readability, but the values are
validated in `OutboxDispatcherBuilder::build()` (`jitter ∈ [0.0, 1.0)`, `max_attempts >= 1`,
`base > 0`), not in a constructor — a struct a host mutates directly is checked where it is used.

**`DispatcherSettings::retry` *is* the default policy (S4 review, blocker 3).** It was previously
validated by `build()` and then never applied — `run` used the builder's `R`, so
`RELIAR_OUTBOX_RETRY_*` was accepted, range-checked, and silently ignored. That is the exact failure
ADR 0019 exists to prevent: a configuration value that does nothing is worse than one that is
rejected, because the operator sets `RETRY_MAX_ATTEMPTS=3` and watches messages die at 10.

The two paths are now distinct in the type system:

- **Default path.** The builder starts as `OutboxDispatcherBuilder<S, P, M, DefaultRetry>`, where
  `DefaultRetry` is a public marker that deliberately **does not implement `RetryPolicy`**.
  Its `build()` constructs the policy **from `settings.retry`** and yields
  `OutboxDispatcher<S, P, M, ExponentialBackoff>`.
- **Custom path.** `.retry_policy(p)` moves the builder to `R = R2: RetryPolicy`, and that
  `build()` uses `p`. Because `DefaultRetry` is not a `RetryPolicy`, the two `build()` impls do not
  overlap and the compiler picks the right one — no specialization, no second method name.

**A custom policy takes over retry entirely**, so `settings.retry` would then be dead configuration.
Rather than ignore it a second time, `build()` returns `ConfigError::RetryPolicyConflict` when a
custom policy is supplied **and** `settings.retry != ExponentialBackoff::default()`. A host that
wants both leaves `settings.retry` at its default (or reads it itself and feeds its own policy);
`RELIAR_OUTBOX_RETRY_*` set against a custom policy is a mistake worth a startup error rather than a
shrug. This is why `ExponentialBackoff` derives `PartialEq`.

### 3.7 Settings

```rust
/// The one settings struct for the outbox feature.  Env prefix `RELIAR_OUTBOX_`.
#[derive(Clone, Debug, Default)] #[non_exhaustive]
pub struct OutboxSettings {
    pub dispatcher: DispatcherSettings,
    pub retention:  RetentionSettings,
}

/// Worker-loop tunables — the struct SRS §23.1's defaults table and §26.1's drain rule refer to.
#[derive(Clone, Debug)] #[non_exhaustive]
pub struct DispatcherSettings {
    pub batch_size:         u32,               // 100        BATCH_SIZE
    pub lease:              Duration,          // 30 s       LEASE_MS
    pub max_in_flight:      usize,             // 16         MAX_IN_FLIGHT
    pub publish_timeout:    Duration,          // 10 s       PUBLISH_TIMEOUT_MS
    pub poll_interval:      Duration,          // 500 ms     POLL_INTERVAL_MS
    pub idle_poll_interval: Duration,          // 5 s        IDLE_POLL_INTERVAL_MS
    pub drain_timeout:      Duration,          // 30 s       DRAIN_TIMEOUT_MS
    /// Client-side bound on **every** `OutboxStore` call `run` makes.  Without it a hung
    /// statement makes `drain_timeout` unenforceable (S4 review).
    pub store_timeout:      Duration,          // 30 s       STORE_TIMEOUT_MS
    /// How often `run` calls `OutboxStore::stats` and feeds the gauges.  `Duration::ZERO`
    /// disables the tick entirely (the host then polls `stats()` itself, or does without).
    pub stats_interval:     Duration,          // 15 s       STATS_INTERVAL_MS
    pub ordering:           Ordering,          // Unordered  ORDERING
    pub retry:              ExponentialBackoff,//            RETRY_BASE_MS, RETRY_MAX_DELAY_MS,
                                               //            RETRY_MAX_ATTEMPTS, RETRY_JITTER
    pub worker_id:          Option<WorkerId>,  // generated  WORKER_ID
}

#[derive(Clone, Debug)] #[non_exhaustive]
pub struct RetentionSettings {
    pub published_retention: Duration,          // 7 days     PUBLISHED_RETENTION_MS
    pub dead_retention:      Option<Duration>,  // None       DEAD_RETENTION_MS
    pub purge_batch_size:    u32,               // 1000       PURGE_BATCH_SIZE
}
```

All three: `Default` + **builder methods** (`fn lease(mut self, d: Duration) -> Self`);
`serde` derive behind the `serde` feature with `#[serde(default, deny_unknown_fields)]` and
durations as **integer milliseconds** (`lease_ms`).

```rust
impl OutboxSettings {
    /// Opt-in.  Starts from `Default`, overrides **only** present variables, and returns `Err`
    /// for an unparseable or out-of-range value — never a silent fallback.
    pub fn from_env(prefix: &str) -> Result<Self, SettingsError>;
}
#[derive(Clone, Debug, PartialEq)] #[non_exhaustive]
pub enum SettingsError {
    Parse { key: String, value_kind: &'static str },   // never echoes the value
    OutOfRange { key: String, message: &'static str },
}
/// **Public constructors, because every provider's `from_env` returns this type.**  The enum is
/// `#[non_exhaustive]`, so `reliar-store-postgres` (a different crate) cannot build a variant with
/// struct-literal syntax — without these it is forced into a parallel error type, and a host
/// wiring two `from_env` calls then handles two unrelated errors for the same failure.
impl SettingsError {
    /// The variable was present but did not parse.  `value_kind` names the expected shape
    /// (`"u32"`, `"milliseconds"`); the offending **value is never carried**.
    #[must_use] pub fn parse(key: impl Into<String>, value_kind: &'static str) -> Self;
    /// The variable parsed but is outside the range the setting accepts.
    #[must_use] pub fn out_of_range(key: impl Into<String>, message: &'static str) -> Self;
    /// The full environment-variable name, prefix included — what an operator has to go fix.
    #[must_use] pub fn key(&self) -> &str;
}
```

**The library never reads the environment implicitly.** No constructor, `Default` or `build()`
touches `std::env` (ADR 0019).

### 3.8 Metrics hook

```rust
/// Static-dispatch hook with no-op defaults, so it costs nothing unused and adding an
/// instrument later is not a breaking change.  No library crate depends on an exporter.
pub trait OutboxMetrics: Send + Sync {
    fn claimed(&self, _n: usize) {}
    fn published(&self, _n: usize, _message_type: &MessageType) {}
    fn retried(&self, _n: usize, _kind: FailureKind) {}
    fn dead(&self, _n: usize, _reason: DeadReason) {}
    fn publish_duration(&self, _d: Duration, _message_type: &MessageType) {}
    fn pending(&self, _n: u64) {}
    fn expired_pending(&self, _n: u64) {}
    /// **Not called at all when there is nothing pending** — see the tick rule in §3.4.
    fn oldest_pending_age(&self, _age: Duration) {}   // "outbox lag" — the alerting signal
    fn purged(&self, _published: u64, _dead: u64) {}
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopMetrics;
impl OutboxMetrics for NoopMetrics {}
```

Labels are bounded to `message_type`, `kind`, `reason`, `state`. `message_id`, `correlation_id`,
`tenant_id`, `worker_id` and `last_error` SHALL **NEVER** be metric labels (ADR 0020).

**Expired rows never pin the lag gauge (review 2, major 5).** A pending row past its `expires_at`
is excluded by the claim predicate, so it can never be published and would otherwise make
`oldest_pending_age` grow without bound — paging an operator about a backlog that does not exist.
`pending` and `oldest_pending_available_at` therefore use the **claim predicate**, and expired rows
are surfaced as their own `expired_pending` count and gauge. The expired → dead transition stays in
`purge` (ADR 0009): moving it into the claim statement would put a write on the hottest path in the
system to fix a bookkeeping problem. A host that never calls `purge` accumulates `expired_pending`
and sees it on a gauge — the correct signal, since it is also failing to run retention.

**The dispatcher polls `stats()`, not the host (review 3, major 3).** `run` ticks it every
`stats_interval` and is the sole caller of `OutboxMetrics::{pending, expired_pending,
oldest_pending_age}` — which is what makes §43.A.25 (`RecordingMetrics` observes oldest-pending-age)
testable without a second moving part, and what keeps the lag gauge alive in a host that has no
maintenance task at all. `stats_interval = Duration::ZERO` disables the tick for hosts that would
rather own it.

**When `OutboxStats::lag()` is `None` — no claimable rows — the dispatcher skips the
`oldest_pending_age` call entirely** (review 4). It does **not** report `Duration::ZERO`: zero means
"there is a pending row and it just became due", and an empty outbox is a different fact. Reporting
zero would make a drained outbox indistinguishable from a perfectly-keeping-up one and would let a
`max_over_time` alert reset on an outbox that is merely idle. `pending` and `expired_pending` are
still reported (as `0`), so a dashboard can tell "empty" from "not scraped". §43.A.25 asserts the
skip: with no pending rows, `RecordingMetrics::oldest_pending_age()` is `None`. `stats()` stays `pub` on the store, so an admin endpoint can call it directly; that
call simply does not feed the hook. **`purge` is the opposite case** — it writes, so it stays the
host's to schedule (ADR 0009); the dispatcher never calls it.

`stats()` runs three counting queries plus a `min(available_at)` every `stats_interval` (15 s). All
three are served by `ix_outbox_pending`, `ix_outbox_dead_at` and `ix_outbox_expires`, but on a very
large table the counts
are still index scans — operators with a big backlog raise the interval rather than lose the
gauges. Rustdoc on `stats_interval` says so.

### 3.9 The dispatcher

```rust
pub struct OutboxDispatcher<S, P, M = NoopMetrics, R = ExponentialBackoff> { /* … */ }
```

**No `Clone` bound on `P` or `M`** (review 1, major 8). The dispatcher wraps the publisher and the
metrics hook in an internal `Arc` so each spawned publish task gets a cheap handle; `Arc` is not
dynamic dispatch (ADR 0001) and the bound is the dispatcher's business, not the host's. A host may
still pass a `Clone` type — it simply is not required to.

```rust

impl<S, P> OutboxDispatcher<S, P>
where S: OutboxStore, P: Publisher
{
    #[must_use] pub fn builder(store: S, publisher: P) -> OutboxDispatcherBuilder<S, P>;
}

/// Marker for "the retry policy comes from `DispatcherSettings::retry`".  It intentionally does
/// **not** implement `RetryPolicy`, which is what keeps the two `build()` impls from overlapping.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultRetry;

#[must_use]
pub struct OutboxDispatcherBuilder<S, P, M = NoopMetrics, R = DefaultRetry> { /* … */ }

/// Default path: the policy is built from `settings.retry`.
impl<S, P, M> OutboxDispatcherBuilder<S, P, M, DefaultRetry> {
    pub fn build(self) -> Result<OutboxDispatcher<S, P, M, ExponentialBackoff>, ConfigError>;
}

impl<S, P, M, R> OutboxDispatcherBuilder<S, P, M, R> {
    /// Replaces the whole settings struct.  **Last call wins**: individual setters below apply
    /// to whatever `settings` is current, so `.ordering(x).settings(s)` discards `x` while
    /// `.settings(s).ordering(x)` keeps it.  Documented because both orders read naturally.
    pub fn settings(self, settings: DispatcherSettings) -> Self;
    pub fn ordering(self, ordering: Ordering) -> Self;
    pub fn worker_id(self, worker: WorkerId) -> Self;
    pub fn metrics<M2: OutboxMetrics>(self, metrics: M2) -> OutboxDispatcherBuilder<S, P, M2, R>;
    /// Replaces retry **entirely**; `settings.retry` is then unused, and `build()` rejects the
    /// combination rather than ignore it — see §3.6.
    pub fn retry_policy<R2: RetryPolicy>(self, policy: R2) -> OutboxDispatcherBuilder<S, P, M, R2>;
    /// Validates and returns a configuration error — **never panics**.  The complete rejection
    /// list: `max_in_flight == 0` (`ZeroInFlight`); a `lease` not longer than `publish_timeout`
    /// (`LeaseTooShort`); `Ordering::PerKey` before 0.2 (`UnsupportedOrdering`, naming the
    /// version); `retry.jitter` outside `[0.0, 1.0)` (`InvalidJitter`); `retry.max_attempts == 0`
    /// (`ZeroMaxAttempts`); `retry.base == Duration::ZERO` (`ZeroRetryBase`); a custom policy
    /// together with non-default `settings.retry` (`RetryPolicyConflict`, §3.6).
    /// Schema disagreements are **not** a `ConfigError` at all — see §4.
    /// **Warns** — does not fail — when `lease > batch_size × publish_timeout ÷ max_in_flight`
    /// does not hold (true publish latency is unknown at construction), and when
    /// `store_timeout > drain_timeout`.
    pub fn build(self) -> Result<OutboxDispatcher<S, P, M, R>, ConfigError>
    where R: RetryPolicy;
}

impl<S, P, M, R> OutboxDispatcher<S, P, M, R>
where S: OutboxStore + Send + Sync + 'static,
      P: Publisher + Send + Sync + 'static,
      M: OutboxMetrics + Send + Sync + 'static,
      R: RetryPolicy + Send + Sync + 'static,
{
    /// Runs until cancelled.  **At-least-once**: a crash between publish and `complete`, a lease
    /// that expires mid-batch, and a drain timeout each republish the message (ADR 0007).
    /// **`Ordering::Unordered` guarantees no order of any kind** (ADR 0013).
    ///
    /// A store error is transient by assumption: logged at `error`, backed off, and the loop
    /// continues.  Returns `Err` only for invalid configuration or a store error the provider
    /// classifies as `FailureKind::Permanent`.  Returns `Ok(())` on cancellation, after draining
    /// (ADR 0014).  Idempotent under repeated cancellation; safe to run many instances against
    /// one table.
    ///
    /// **Drain finishes what started; it never starts anything new** (S4 review).  On
    /// cancellation, a spawned publish task that has **not yet acquired its concurrency permit**
    /// is dropped and its row goes straight into the `release` set — waiting to begin a publish
    /// that will be released anyway only burns the drain budget and widens the duplicate window
    /// for no delivery.  Only publishes already in flight are awaited, which is exactly §26.1's
    /// "finish in-flight".
    ///
    /// The honest form of that claim is **"has not completed a publish"**, not "has not touched
    /// the broker": on a multi-thread runtime a spawned task may be polled once before the drop
    /// lands, so it can have begun the broker call.  Such a row is released and may already have
    /// been delivered — the same at-least-once window as everything else here, not a new one.
    ///
    /// **The claim loop is bounded by `max_in_flight`** (S4 review 2).  `run` claims only while
    /// `outstanding < max_in_flight`, and asks for `min(batch_size, max_in_flight - outstanding)`.
    /// Without that gate the loop re-claims `batch_size` rows every poll regardless of how many it
    /// is still holding, so a publisher slower than the poll interval makes one dispatcher hoard
    /// leases without bound — and the rows at the tail sit leased and unpublished until their
    /// lease expires under a healthy worker, which is precisely the §22.1 slow-batch duplicate
    /// window.  Backpressure here does not merely bound memory: it **shrinks that window**.
    ///
    /// Two consequences, stated because the defaults make them non-obvious.  **`max_in_flight` is
    /// the real ceiling on outstanding rows; `batch_size` only caps a single claim statement.**
    /// With the accepted defaults (`batch_size` 100, `max_in_flight` 16) the claim never asks for
    /// more than 16, so `batch_size` does not bind — it is not a bug, and it is not warned about,
    /// because a warning on the default configuration is noise.  Pipelining survives: the loop
    /// tops up as permits are released, so a permit is not left idle waiting for a claim.  And the
    /// set of claimed-but-not-yet-started rows is now at most `max_in_flight`, which is what keeps
    /// the release-not-start rule above cheap.
    ///
    /// **`max_in_flight` is bounded twice, on purpose** (S4 review 3).  The claim gate above bounds
    /// the **rows this worker holds leased**; the publish semaphore bounds the **concurrent
    /// `Publisher::publish` calls**, which is what §43.A.23 actually promises.  With a store that
    /// honours `AcquireRequest::batch_size` the two coincide and the semaphore is never contended
    /// — that is the gate working, not dead weight.  `OutboxStore` is a public extension point:
    /// the semaphore is what keeps the promise when a third-party store over-delivers a batch, so
    /// the failure mode is "more leases held than intended", never "thousands of concurrent broker
    /// calls".  It is the last gate before the one resource Reliar does not own.
    ///
    /// **Every store call is bounded by `store_timeout`.**  `drain_timeout` is otherwise
    /// unenforceable: a `complete` that hangs — a lost connection with no server-side
    /// `statement_timeout`, a saturated pool — would hold shutdown open indefinitely.  A timeout
    /// is treated as a transient store error.  This is a *client-side* bound on the future, which
    /// is what cancellation needs; the provider's `statement_timeout` (§4) bounds the *server*
    /// statement and defaults to inherit, so neither substitutes for the other.
    ///
    /// **An outcome write that fails keeps its rows outstanding** (S4 review 2).  When
    /// `complete`/`fail` errors or hits `store_timeout`, the rows are **not** dropped: they stay
    /// in `outstanding` in a pending-outcome state and the write is retried on the next loop
    /// iteration.  Dropping them would leave the rows leased with `attempts` unadvanced until the
    /// lease expired — the worker still owns them and would simply have forgotten.  Retrying is
    /// safe because the `locked_by` guard makes a repeated `complete` idempotent, and if the lease
    /// has since been lost the retry affects zero rows, which is benign (ADR 0008).  Because they
    /// remain outstanding they also apply backpressure: a store that cannot accept outcomes stops
    /// the loop claiming more work, which is the correct behaviour.
    ///
    /// This is **not a fourth duplicate window** — it is SRS §23.2's "publish succeeded,
    /// completion failed", and it is documented there.
    ///
    /// **The retry is bounded, in both directions** (S4 review 3).  L2 as first written retried
    /// forever and ignored `FailureKind`, so a *permanently* failing outcome write — a schema
    /// drift, a `23514` on a row Reliar itself wrote — wedged the worker: `outstanding` filled to
    /// `max_in_flight`, claiming stopped, the leases were renewed indefinitely, and `run` never
    /// returned.  A silently stalled worker is the worst of the available outcomes, because
    /// nothing about it looks like a failure.
    ///
    /// - **A `Permanent` outcome-write error ends the loop.**  `run` performs its best-effort
    ///   drain — persisting what it still can for *other* rows — and returns
    ///   `Err(DispatchError::Store(..))` carrying the original error.  Rows whose publish
    ///   succeeded but whose outcome never landed are **left to their lease**, never released:
    ///   they are already delivered, so releasing them buys an immediate certain duplicate and
    ///   recovers nothing (L3).  Another worker picks them up when the lease lapses.
    /// - **Transient outcome-write failures are bounded by `lease`.**  Once a row's unwritten
    ///   outcome has been retried for longer than the lease duration, the row is dropped from
    ///   `outstanding` **and excluded from lease renewal**, so its lease lapses and another worker
    ///   reclaims it.  Both halves are required: dropping it while still renewing would leave a
    ///   row nobody owns and nobody can claim.  The bound is the lease because that is already how
    ///   long an unreachable worker's rows stay dark — spending longer buys nothing a reclaim does
    ///   not, and the duplicate this admits is the one §22.1 already documents.
    ///
    /// The gate therefore always frees: either the write lands, or the row leaves `outstanding`
    /// within a lease, or `run` returns.
    ///
    /// At drain, rows still holding an unresolved outcome are handled by what actually happened:
    /// a row whose publish **failed or never resolved** is released, but a row whose publish
    /// **succeeded** and whose `complete` never landed is **left to its lease** rather than
    /// released.  Releasing it would turn a possible duplicate into an immediate certain one for a
    /// message that is already delivered; there is nothing to recover sooner, so the lease is
    /// allowed to lapse instead.
    ///
    /// **The permanent-store-error exit drains too.**  Before returning `Err`, `run` performs the
    /// same drain — persist resolved outcomes, `release` the rest — on a **best-effort** basis:
    /// its errors are logged and discarded, and the original error is what surfaces.  A store
    /// broken badly enough to be permanent will often fail the release as well, and losing the
    /// real diagnosis behind a secondary failure would be the worse outcome; but a dispatcher that
    /// exits leaving a full batch leased is a batch dark for a whole lease, which is worth one
    /// attempt to avoid.
    pub async fn run(self, cancel: tokio_util::sync::CancellationToken)
        -> Result<(), DispatchError<S::Error>>;
}

#[derive(Debug)] #[non_exhaustive]
pub enum DispatchError<E> { Configuration(ConfigError), Store(E) }

/// `Clone + PartialEq` so a test can assert the exact rejection without matching on a shape.
#[derive(Clone, Debug, PartialEq)] #[non_exhaustive]
pub enum ConfigError {
    ZeroInFlight,
    LeaseTooShort { lease: Duration, publish_timeout: Duration },
    UnsupportedOrdering { ordering: Ordering, available_in: &'static str },  // "0.2"
    InvalidJitter { value: f64 },
    ZeroMaxAttempts,
    ZeroRetryBase,
    /// A custom `RetryPolicy` was supplied *and* `settings.retry` is non-default — one of them
    /// would be silently ignored.
    RetryPolicyConflict,
}
```

### 3.10 `test-support` fakes (feature `test-support`)

Shipped from `reliar-outbox` so provider crates, examples and `tests/system` reuse one set.

```rust
/// Full in-memory `OutboxStore` + `OutboxDeadLetters`.  Keeps its own instant so lease expiry and
/// `available_at` can be driven forward without a database — the fake's substitute for SQL
/// time-travel.  Its `Send` future must never hold a `std::sync::MutexGuard` across an `.await`:
/// lock, take what is needed, drop the guard, *then* await.
///
/// **Documented divergence from Postgres:** the fake applies its mutations **eagerly, at call
/// time**, not when the returned future is first polled.  Dropping an un-awaited
/// `complete`/`fail`/`release` future therefore still mutates the fake, where against Postgres it
/// would send nothing.  This falls straight out of "drop the guard before awaiting" — the work
/// happens before the `async` block exists — and it is stated rather than engineered around,
/// because no dispatcher path drops a store future un-awaited and pretending otherwise would cost
/// an internal channel for no test value.
#[derive(Clone, Debug, Default)]
pub struct InMemoryOutboxStore { /* … */ }

impl InMemoryOutboxStore {
    /// Seeds a pending row and returns its `MessageRef`.  The fake assigns `sequence` and
    /// `created_at` exactly as a provider would.
    pub fn insert(&self, envelope: SerializedEnvelope) -> MessageRef;
    pub fn insert_with(&self, envelope: SerializedEnvelope, available_at: time::OffsetDateTime,
                       ordering_key: Option<String>) -> MessageRef;
    /// Moves the fake's notion of "now" forward — expires leases and makes retried rows due.
    /// The store-side counterpart of `tokio::time::advance`.
    pub fn advance(&self, by: Duration);
    /// Full row inspection for assertions.
    pub fn records(&self) -> Vec<OutboxRecord>;
    pub fn record(&self, id: MessageId) -> Option<OutboxRecord>;
    /// Makes the next `n` calls to any `OutboxStore` method fail transiently — drives the
    /// "`run` survives a store error" test (§43.A.18).
    pub fn fail_next(&self, n: usize);
}
#[derive(Debug)] #[non_exhaustive]
pub enum InMemoryStoreError { Injected }        // `LeaseLost` removed: never constructible —
impl Classify for InMemoryStoreError { /* Transient */ }   // a lost lease is 0 rows, not an error

/// Records every publish, **in order, with duplicates** — duplicates are the assertion, not a bug.
///
/// `publish` is **timer-free by default**: it resolves immediately, so a paused-time test never has
/// to advance the clock just to drain a batch.  A concurrency assertion needs overlap to observe,
/// which is what `with_concurrency_probe` is for.
#[derive(Clone, Debug, Default)]
pub struct RecordingPublisher { /* … */ }
impl RecordingPublisher {
    /// Each publish holds for `hold` before resolving, so in-flight publishes actually overlap and
    /// `in_flight_peak` becomes meaningful.  Use with `#[tokio::test(start_paused = true)]`.
    #[must_use] pub fn with_concurrency_probe(hold: Duration) -> Self;

    pub fn published(&self) -> Vec<MessageId>;
    pub fn count(&self, id: MessageId) -> usize;      // 2 proves the duplicate window
    pub fn envelopes(&self) -> Vec<SerializedEnvelope>;
    pub fn in_flight_peak(&self) -> usize;            // asserts `max_in_flight` (§43.A.23)
}

/// Replays a script of outcomes, one per publish call, cycling the last entry when exhausted.
#[derive(Clone, Debug)]
pub struct ScriptedPublisher { /* … */ }
#[derive(Clone, Copy, Debug)] #[non_exhaustive]
pub enum PublishStep { Ok, Transient, Permanent, Hang(Duration) }   // Hang drives publish_timeout
impl ScriptedPublisher {
    /// Positional script, consumed in call order.  **Deterministic only with
    /// `max_in_flight = 1`** — with concurrent publishes the call order is a race, so a test that
    /// needs a specific outcome for a specific message uses `keyed` instead.
    #[must_use] pub fn new(script: impl IntoIterator<Item = PublishStep>) -> Self;
    /// Per-message outcomes, order-independent and safe at any `max_in_flight`.
    #[must_use] pub fn keyed(steps: impl IntoIterator<Item = (MessageId, PublishStep)>) -> Self;
    #[must_use] pub fn always(step: PublishStep) -> Self;
    pub fn published(&self) -> Vec<MessageId>;
}
#[derive(Debug)] #[non_exhaustive]
pub enum FakePublishError { Transient { detail: &'static str }, Permanent { detail: &'static str } }
impl Classify for FakePublishError { … }
```

**Both publishers record into `published()` at the first poll of the returned future**, not when
`publish` is called. That is what a real transport does — nothing reaches a broker until the future
runs — so a test that builds a publish future and drops it un-awaited sees no recorded publish, and
`in_flight_peak` counts futures that are actually running rather than merely created.

```rust

/// Recording `OutboxMetrics`, for §43.A.25.
#[derive(Clone, Debug, Default)]
pub struct RecordingMetrics { /* … */ }
impl RecordingMetrics {
    pub fn claimed(&self) -> usize;
    pub fn published(&self) -> Vec<MessageType>;
    pub fn retried(&self) -> Vec<FailureKind>;
    pub fn dead(&self) -> Vec<DeadReason>;
    pub fn pending(&self) -> Option<u64>;
    pub fn expired_pending(&self) -> Option<u64>;
    pub fn oldest_pending_age(&self) -> Option<Duration>;
    pub fn publish_duration(&self) -> Vec<(Duration, MessageType)>;
    /// `None` until `purged` is called at least once — `(0, 0)` is a real report from a pass that
    /// deleted nothing, and a test asserting "purge never ran" must be able to tell them apart.
    pub fn purged(&self) -> Option<(u64, u64)>;
}
impl OutboxMetrics for RecordingMetrics { … }
```

---

## 4. `reliar-store-postgres`

```rust
/// Cheap to clone into an `AppState` (it wraps a `PgPool`); **no outer `Arc` required**.
/// The connection pool stays the host's — Reliar never owns or reads a `DATABASE_URL`.
/// **`Clone` is a manual impl, never derived** (review 2, major 3): a derived `Clone` would
/// condition on `Ser: Clone`, so a host with a non-`Clone` serializer could not put the store in
/// an `AppState`.  The serializer is held as `Arc<Ser>` — it is stateless and cheap, and `Arc` is
/// not dynamic dispatch (ADR 0001).  Cloning is a `PgPool` clone plus two refcount bumps.
pub struct PostgresOutboxStore<
    #[cfg(feature = "json")] Ser = JsonSerializer,
    #[cfg(not(feature = "json"))] Ser,
> { /* pool: PgPool, settings: PostgresOutboxSettings, serializer: Arc<Ser> */ }
impl<Ser> Clone for PostgresOutboxStore<Ser> { /* no `Ser: Clone` bound */ }

/// **Always available**, whatever the feature set.  Verifies **once at construction** that the
/// unqualified name `outbox` resolves to the configured schema; fails fast with an error naming
/// the configured schema, the observed `search_path`, and the `ALTER ROLE` remedy, and warns when
/// a same-named table also exists in another schema on the path (ADR 0017).
impl<Ser: Serializer + Send + Sync + 'static> PostgresOutboxStore<Ser> {
    pub async fn connect(pool: sqlx::PgPool, settings: PostgresOutboxSettings, serializer: Ser)
        -> Result<Self, PostgresStoreError>;
}

/// Convenience over `connect`, behind the crate's **default `json` feature**
/// (`json = ["reliar-core/json"]`).
#[cfg(feature = "json")]
impl PostgresOutboxStore<JsonSerializer> {
    pub async fn new(pool: sqlx::PgPool) -> Result<Self, PostgresStoreError>;
    pub async fn with_settings(pool: sqlx::PgPool, settings: PostgresOutboxSettings)
        -> Result<Self, PostgresStoreError>;
}

impl<Ser: Serializer + Send + Sync + 'static> PostgresOutboxStore<Ser> {
    /// The `ContentType` this store writes to every row — `Serializer::content_type()`.
    /// Exposed because it is the only way a caller can predict the `content_type` of an
    /// envelope it will later acquire (see the round-trip note below).
    pub fn content_type(&self) -> &ContentType;

    /// Stages a message in the **application's own transaction** — atomicity is visible in the
    /// signature.  Plain `INSERT`, **no `ON CONFLICT`**: a reused `MessageId` aborts the
    /// caller's transaction rather than silently losing a message.  Returns the id it wrote, so
    /// the caller can use it as the next message's `causation_id` in the same transaction.
    pub async fn enqueue<T: Message>(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        envelope: &Envelope<T>,
    ) -> Result<MessageId, EnqueueError<Ser::Error>>;

    /// Same, with provider-side options (currently the `ordering_key`, which is
    /// application-supplied and deliberately not part of `Metadata`).
    pub async fn enqueue_with<T: Message>(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        envelope: &Envelope<T>,
        options: EnqueueOptions<'_>,
    ) -> Result<MessageId, EnqueueError<Ser::Error>>;
}

#[derive(Clone, Debug, Default)] #[non_exhaustive]
pub struct EnqueueOptions<'a> { pub ordering_key: Option<&'a str> }

impl<Ser: Serializer + Send + Sync + 'static> OutboxStore       for PostgresOutboxStore<Ser> { type Error = PostgresStoreError; … }
impl<Ser: Serializer + Send + Sync + 'static> OutboxDeadLetters for PostgresOutboxStore<Ser> { type Error = PostgresStoreError; … }
```

**`content_type` is the store's, not the call site's (review 2, major 6).** `enqueue` takes
`&Envelope<T>` and does **not** mutate it. It writes `self.content_type()` — the serializer's value
— into the `content_type` column, **ignoring** whatever `envelope.metadata.delivery.content_type`
held; a call site cannot know the store's format, and `DeliveryMetadata::default()`'s
`ContentType::JSON` is a placeholder (ADR 0010). The value is ignored, not validated: rejecting a
mismatch would make the default envelope unusable with any non-JSON serializer.

The consequence for **§43.A.4's round-trip equality** is therefore stated exactly, rather than
holding by coincidence for JSON:

```text
acquired.envelope == { the enqueued envelope, serialized, with
                       metadata.delivery.content_type = store.content_type() }
```

The test builds that expected value with `store.content_type()`; it SHALL NOT assume `JSON`, and it
SHALL run against a non-JSON serializer as well, which is what makes the rule observable.

**Feature policy (review 1, blocker 2).** The `Ser = JsonSerializer` **default type parameter** and
the `new`/`with_settings` conveniences require `reliar-core/json`, so both are gated on the
provider's own default `json` feature, which forwards it. `connect(pool, settings, serializer)` is
always available and is the only constructor under `--no-default-features`, so
`cargo hack --feature-powerset` compiles every combination. The provider does **not**
unconditionally hard-enable `json`: a deployment supplying its own format should not pull in
`serde_json`. `Ser: 'static` is required because the store is cloned into spawned publish tasks and
into an `AppState`.

```rust
#[derive(Clone, Debug)] #[non_exhaustive]
pub struct PostgresOutboxSettings {
    pub schema:                   String,    // "reliar"   SCHEMA
    pub enqueue_sets_search_path: bool,      // false      ENQUEUE_SETS_SEARCH_PATH
    pub statement_timeout:        Duration,  // ZERO       STATEMENT_TIMEOUT_MS
    // `listen_notify` is deliberately absent — see below.

}
impl PostgresOutboxSettings {
    pub fn from_env(prefix: &str) -> Result<Self, SettingsError>;   // "RELIAR_STORE_POSTGRES_"
}
```

`schema` and `MigrateOptions::schema` default to the same `"reliar"`. **`ConfigError::SchemaMismatch`
is removed** (review 1, S5): no point in the type system ever holds both values. `migrate` is a free
function taking `MigrateOptions`, `connect` takes `PostgresOutboxSettings`, and under ADR 0018 the
migration may have been applied by a DBA's pipeline in a different process entirely — so a
"compare the two structs" check is unreachable in exactly the deployments where it would matter.

The real defence already exists and is strictly stronger: **`connect` verifies that unqualified
`outbox` actually resolves to the configured schema** (ADR 0017). That catches *every* cause of a
mismatch — wrong `MigrateOptions`, wrong `search_path`, a pooler dropping startup options, the
migration never run — instead of the single case where both structs happen to be in scope, and it
checks the database rather than two strings that were only ever hoped to agree.

**Schema identifiers are validated before any interpolation.** `migrate` reaches
`dangerous_set_table_name`, which is string interpolation into DDL, so the name is checked once —
against `[A-Za-z_][A-Za-z0-9_$]*` and a 63-byte cap (PostgreSQL's `NAMEDATALEN - 1`) — and the same
check runs on `PostgresOutboxSettings.schema`. A schema name arrives from configuration, which is
not automatically trusted input.

**`enqueue_sets_search_path` (review 1, major 6).** When `true`, `enqueue` reads
`current_setting('search_path')`, issues `set_config('search_path', '<schema>,public', true)`,
performs the `INSERT`, then restores the previous value with a second `set_config(…, true)`. The
**`true` third argument means transaction-local** (`SET LOCAL` semantics): the change dies with the
caller's `COMMIT`/`ROLLBACK`, so **the host's session state is never mutated and a pooled connection
returned to the pool carries nothing**. The explicit restore exists so the rest of the caller's
transaction sees the path it had before Reliar touched it. Cost: **three extra statements per
enqueue** (`current_setting`, `set_config`, `set_config`), which is why it is `false` by default.
If the `INSERT` fails, the restore is
skipped and the transaction is already doomed — the transaction-local scope makes that safe.

**`statement_timeout` (review 1, major 6; scope settled in review 3).** Applies to **every**
statement Reliar issues on a connection it checks out — `acquire`, `complete`, `fail`, `release`,
`extend_lease`, `stats`, `purge`, and all three `OutboxDeadLetters` operations — issued as
`SET LOCAL statement_timeout` inside the transaction Reliar opens for that operation.

There is **no exception list**, and that is the point: an operator sets one number to bound how
long Reliar may hold a backend, and a carve-out is exactly where the runaway statement then lives.
`purge` and `list_dead` are in fact the two most likely to run long — a bounded `DELETE` over a
large backlog, a keyset page over millions of dead rows — so exempting the "operational" calls
would exempt the ones that need it. The bound is per **statement**, not per pass, so a `purge` that
needs many passes is not penalised: each pass gets the full budget and the caller loops (§3.4).

It is **never** applied to the caller's `enqueue` transaction. It is **never** applied to the caller's
`enqueue` transaction — Reliar does not impose a timeout on a transaction it does not own.
**The default is `Duration::ZERO`, meaning "issue nothing and inherit the server/role setting"**
(review 2, major 7). SRS §7.2's 5 s default is **withdrawn**: a non-zero value forces every store
call — `acquire`, `complete`, `fail`, `release`, `extend_lease`, `stats` — from one statement into a
four-round-trip `BEGIN`/`SET LOCAL`/statement/`COMMIT`, which is a large, permanent cost paid by
every deployment to fix a problem most already solve with a server-side `statement_timeout` on the
role. Setting it non-zero is an opt-in for hosts that cannot. *(SRS §7.2 amendment: PO, RELIAR-23.)*
Wrapping the claim in such a transaction does not weaken ADR 0006 — it still contains no network
I/O to a broker, and the claim is still released before `acquire` returns.

**`ensure_partitions_ahead` and `listen_notify` are deliberately absent** from v0.1's
`PostgresOutboxSettings`, though SRS §7.2 lists both. The partitioned variant and
`ensure_partitions()` ship in 0.2 (ADR 0016), and `LISTEN/NOTIFY` is a wake-up optimisation with no
implementation in v0.1 (SRS §26).

**A settings field that does nothing is removed, not documented as doing nothing** (review 3). A
`listen_notify: bool` that a host can set to `true` and observe no change from is a worse artefact
than an absent field: it reads as a supported feature, it will be set in production configs, and
the "no behaviour in 0.1" rustdoc is the first thing nobody reads. `#[non_exhaustive]` makes adding
it in 0.2 — together with the code that honours it — a non-breaking change, so nothing is lost by
waiting. The same reasoning as `Ordering::PerKey`, which ships as a *variant* only because its
schema support had to land early (ADR 0013); `listen_notify` has no such constraint, so it simply
waits.

```rust
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Clone, Copy, Debug)] #[non_exhaustive]
pub struct MigrateOptions<'a> { pub schema: &'a str }
impl Default for MigrateOptions<'_> { fn default() -> Self { Self { schema: "reliar" } } }
impl<'a> MigrateOptions<'a> {
    /// Builder setter — `#[non_exhaustive]` forbids struct-literal syntax outside this crate, so
    /// `MigrateOptions::default().schema("tenant_a")` is the only way a caller can name one.
    #[must_use] pub const fn schema(self, schema: &'a str) -> Self;
}

/// Applies Reliar's migrations.  **Never invoked implicitly** (SRS §35).  Creates the schema,
/// keeps bookkeeping in `<schema>._migrations` — never `_sqlx_migrations` — and serializes
/// concurrent callers via `set_locking(true)`.  Idempotent: every caller after the first
/// observes `Ok(())`.  Self-contained: does not depend on the caller's `search_path` (ADR 0018).
pub async fn migrate(pool: &sqlx::PgPool, options: MigrateOptions<'_>)
    -> Result<(), MigrateError>;

/// Provider-owned, because a rejected schema identifier has no `sqlx` variant to report it as.
#[derive(Debug)] #[non_exhaustive]
pub enum MigrateError {
    /// `options.schema` is not a valid PostgreSQL identifier.  Checked **before** the name
    /// reaches `dangerous_set_table_name`, which is string interpolation into DDL.
    InvalidSchema { schema: String },
    Sqlx { source: sqlx::migrate::MigrateError },
}
```

```rust
#[derive(Debug)] #[non_exhaustive]
pub enum PostgresStoreError {
    /// `outbox` does not resolve, or resolves to a different schema.  Carries the configured
    /// schema, the observed `search_path`, and the `ALTER ROLE` remedy.  **Permanent.**
    SchemaResolution { configured: String, observed: String },
    /// The relation is missing — migrations have not been run.  **Permanent.**
    NotMigrated { schema: String },
    /// Connection lost, statement timeout, pool exhausted, deadlock.  **Transient.**
    Database { source: sqlx::Error },
    /// A row could not be turned into an `OutboxRecord`.  Surfaces as a poisoned row, never as
    /// an `acquire` failure.
    Decode { id: MessageId, detail: String },
    /// `metadata_version` the reader does not know.
    UnknownMetadataVersion { id: MessageId, version: i32 },
    /// Duplicate `MessageId`, mapped from the `pk_outbox` violation.  **Permanent.**
    DuplicateMessage { id: MessageId },
    /// The configured schema name is not a valid PostgreSQL identifier.  **Permanent.**
    InvalidSchema { schema: String },
}
```

**Classification is per variant, and there is no blanket "everything else is transient"**
(review 1, S5).  A wrong verdict here is not cosmetic: `Transient` means the dispatcher retries
until `max_attempts`, so misclassifying a permanent failure burns the retry budget and delays the
dead transition, while misclassifying a transient one kills a healthy message.

| Variant | Kind | Why |
|---|---|---|
| `SchemaResolution` | **Permanent** | No retry resolves a name; the operator must fix `search_path` |
| `NotMigrated` | **Permanent** | The relation will not appear on its own |
| `InvalidSchema` | **Permanent** | Configuration, not weather |
| `DuplicateMessage` | **Permanent** | A reused `MessageId` **never** succeeds on retry — the row is already there. Retrying is pure waste and hides an application bug |
| `Decode` | **Permanent** | The bytes on disk do not change between attempts |
| `UnknownMetadataVersion` | **Permanent** | Same — it needs a newer reader, not another try |
| `Database { source }` | **depends on the SQLSTATE** — see below | |

`Database` wraps everything the driver reports, and those are **not** uniformly transient. It
classifies by the wrapped error's SQLSTATE **class**, never by message text:

- **Transient** — `08*` (connection exception), `40*` (transaction rollback: deadlock,
  serialization failure), `53*` (insufficient resources), `55*` (object in use), `57014`
  (query_canceled, i.e. the `statement_timeout` above), and any pool/IO error with no SQLSTATE.
- **Permanent** — `22*` (data exception), `23*` (integrity constraint violation), `42*` (syntax
  error or access rule violation). A `ck_outbox_*` violation retried ten times is ten identical
  failures; it is a bug in Reliar or a hand-edited row, and it must reach `dead` with its
  constraint named rather than spin.
- Anything unrecognised classifies **Transient**, because an unknown fault is more often weather
  than logic — but it is logged at `warn` with its SQLSTATE so the table above can be extended.

**Variant mapping is by SQLSTATE and constraint name, never by message text**, and it applies on
**every** path, not just at startup: `42P01` (undefined_table) → `NotMigrated` wherever it is seen
— mapping it only inside `connect`'s verification leaves the operational path returning a transient
`Database` that retries forever against a table that does not exist. `23505` on `pk_outbox` →
`DuplicateMessage`; `23514` on a `ck_outbox_*` → `Database` carrying the constraint name, which the
SQLSTATE rule above classifies **permanent**.

`Decode` and `UnknownMetadataVersion` are reachable only through `acquire`/`list_dead`, where they
become `PoisonedRow`s rather than a returned `Err` (ADR 0008) — they exist as variants because the
provider's own row decoder returns them internally and `list_dead` surfaces them.

```rust

#[derive(Debug)] #[non_exhaustive]
pub enum EnqueueError<E> {
    Serialize { source: E },      // Permanent — the same body serializes the same way
    Duplicate { id: MessageId },  // Permanent — the id is already taken
    Database  { source: sqlx::Error },   // classified by SQLSTATE, exactly as above
}
impl<E: std::error::Error + Send + Sync + 'static> Classify for EnqueueError<E> { … }
```

`EnqueueError` implements `Classify` on the same rules even though the dispatcher never sees it:
`enqueue` runs on the **host's** write path, where the host is the one deciding whether to retry
its own transaction, and it should not have to re-derive which SQLSTATEs are worth retrying.

**Schema direction** (the engineer writes the migration): SRS §24 + §24.1 verbatim — one
`0001_outbox.sql` creating table `outbox` in the configured schema with the promoted columns,
`sequence bigint GENERATED ALWAYS AS IDENTITY`, `ordering_key`, `updated_at`, `metadata_version`,
**plus `dead_reason text` (ADR 0023)**; constraints `pk_outbox`, `ck_outbox_attempts`,
`ck_outbox_message_version`, `ck_outbox_metadata_version`, `ck_outbox_lease`,
`ck_outbox_terminal`, **`ck_outbox_dead_reason CHECK ((dead_at IS NULL) = (dead_reason IS NULL))`**;
and indexes
`ix_outbox_sequence` (unique), `ix_outbox_pending`, `ix_outbox_published`,
**`ix_outbox_dead ON outbox (sequence) INCLUDE (dead_at) WHERE dead_at IS NOT NULL`**,
**`ix_outbox_dead_at ON outbox (dead_at) WHERE dead_at IS NOT NULL`**,
`ix_outbox_ordering_key`, `ix_outbox_expires` — every one derived from the canonical claim, purge,
dead-listing or expiry query in §24.1. **Every index is `ix_`, unique or not; there is no `uq_`.**
The canonical claim query in §24.1 is fixed for v0.1.

**`MetadataRest` timestamps are persisted as epoch milliseconds (`i64`), not RFC 3339.** `time`'s
RFC 3339 formatter is **fallible over the type's own range**: `OffsetDateTime` accepts years outside
`0000..=9999`, which the format cannot represent, so a `sent_at` an application set from arithmetic
on an untrusted value makes serialization fail — and the tempting `.expect()` there is a panic on
the enqueue path, which §19.5 forbids outright. Making it fallible instead would only move a
guaranteed-to-be-confusing error onto the host's write path.

Epoch milliseconds are **total** over the whole range, so the failure mode disappears rather than
being handled: they are smaller in JSONB, they sort and compare as integers, and they need no
parser. The cost is that the blob is less readable in `psql` — worth it to delete a panic.

**The codec, exactly:** the one affected field is `DeliveryRest.sent_at`, serialized as
**`sent_at_ms: Option<i64>`** — milliseconds since the Unix epoch, UTC, negative before 1970.
**`expires_at` is not in the blob at all**: it is a promoted `timestamptz` column (ADR 0012), where
PostgreSQL owns the encoding and the claim predicate compares it in DB time — so it neither has
nor needs a JSONB representation. `metadata_version` **stays `1`**: no row has ever been written
with the other encoding, so there is no shape to be compatible with. Had any row shipped as
RFC 3339, this would have required a bump and a reader for both.

**The dead rows carry two indexes, and both are needed (review 4, major 1).** They serve opposite
access patterns and neither substitutes for the other:

| Index | Query it exists for |
|---|---|
| `ix_outbox_dead (sequence) INCLUDE (dead_at) WHERE dead_at IS NOT NULL` | `list_dead`'s `ORDER BY sequence` keyset page (ADR 0008) |
| `ix_outbox_dead_at (dead_at) WHERE dead_at IS NOT NULL` | `purge`'s dead-retention delete, `dead_at < now() - dead_retention LIMIT n` (ADR 0009) |

Review 3 replaced the original `(dead_at, sequence)` index to make the cursor correct, which left
the retention delete with **no** supporting index — a bounded `DELETE` degrading to a scan of every
dead row on each pass. `ix_outbox_dead_at` restores it. Both are partial on `dead_at IS NOT NULL`,
so on a healthy table they index almost nothing; the cost is two nearly-empty indexes, and the
alternative is a retention pass that gets slower the longer dead rows are kept — which is exactly
the workload `dead_retention` exists for.

**The `INCLUDE (dead_at)` claim, stated precisely:** it lets `dead_before` be evaluated from the
index tuple, avoiding a heap fetch **only when `message_type` and `tenant_id` are unset**. Those
two are not in the index — promoting them would widen it for an operator-browse query — so a page
filtered by either still visits the heap. That is acceptable: `list_dead` is an operator surface
polled by a human or a dashboard, not a hot path.

---

## 5. Composition (what a host writes)

```rust
reliar_store_postgres::migrate(&pool, MigrateOptions::default()).await?;

let store = PostgresOutboxStore::new(pool.clone()).await?;   // fails fast on search_path

// write path — the application owns the transaction
let mut tx = pool.begin().await?;
let order = orders::insert(&mut tx, &req).await?;
store.enqueue(&mut tx, &Envelope::builder(OrderCreated { order_id: order.id }).build()).await?;
tx.commit().await?;                       // business row and outbox row commit together, or neither

// worker — one CancellationToken drives both Axum's shutdown and the dispatcher
let dispatcher = OutboxDispatcher::builder(store, publisher)
    .settings(DispatcherSettings::default())
    .build()?;
let worker = tokio::spawn(dispatcher.run(cancel.clone()));
// … cancel.cancel() on shutdown …
worker.await??;                           // a real drain barrier, not an abort
```

`OutboxDispatcher<PostgresOutboxStore, NatsPublisher>` is the Phase-2 shape. No `Box<dyn>` appears
on any hot path.

---

## 6. What is **not** in the Phase-1 contract

`Ordering::PerKey`'s implementation (0.2) · `ensure_partitions` and the partitioned DDL (0.2,
ADR 0016) · the `reliar` facade crate (0.2) · any `EnvelopeMapper` implementation, `NatsPublisher`,
or `SubjectResolver` (Phase 2) · `InboxStore`, `IdempotencyStore`, `Handler`,
`reliar-messaging` (Phases 3–5) · a `Clock` trait (never — ADR 0009) · message-version negotiation
on read (Phase 3) · `enqueue_ignore_duplicates` · per-`message_type` retry policies.

---

## 7. Decided here

Signature details the SRS left open, closed by this contract. None requires a new ADR; each is
noted against the ADR that governs it.

| # | Open detail | Decision |
|---|---|---|
| 1 | `OutboxSettings` vs `DispatcherSettings` — SRS §7.2 names both for one field list | **`OutboxSettings` is the feature struct** (env `RELIAR_OUTBOX_`), composing `dispatcher: DispatcherSettings` (loop tunables, §23.1's defaults) and `retention: RetentionSettings` (purge tunables). Env variable names stay **flat** under the prefix. ADR 0019 |
| 2 | `stats` polling interval had no setting | `DispatcherSettings::stats_interval`, default 15 s, `STATS_INTERVAL_MS`. ADR 0020 |
| 3 | Whether `RetryPolicy` is generic on the dispatcher | Yes: `OutboxDispatcher<S, P, M = NoopMetrics, R = ExponentialBackoff>`, set via `builder().retry_policy(..)`. Consistent with ADR 0001; `RetryPolicy` is a trait in §23.1 and must be substitutable without `dyn` |
| 4 | How `run()` recognises a permanent store error (ADR 0014 requires it) | `OutboxStore::Error` gains the bound `+ Classify`, symmetric with `Publisher::Error`. `Permanent` means "no retry can fix it" — unresolvable schema, missing relation. ADR 0014 |
| 5 | `run()`'s error type | `DispatchError<E>` generic over `S::Error`, with variants `Configuration(ConfigError)` and `Store(E)`, so the concrete provider error is preserved rather than boxed |
| 6 | `release`/`extend_lease` took `&[MessageId]` in §19 but by-id ops must carry `created_at` (§24.3) | Both take `&[MessageRef]`, matching `complete`/`fail`. ADR 0016 |
| 7 | `OutboxRecord` carried no `created_at`, though §24.3 makes it mandatory for by-id ops | Added `created_at` (immutable) and `message_ref()`. ADR 0016 |
| 8 | `DeadReason` was persisted but absent from `OutboxRecord`, while §43.A.20 asserts `list_dead` returns it | Added `dead_reason: Option<DeadReason>` to `OutboxRecord` and a `dead_reason text` column with its codec and `ck_outbox_dead_reason`. **Recorded as ADR 0023**; the SRS §24.1/§43.A.32 amendment is the PO's (RELIAR-23) |
| 9 | Where the application supplies `ordering_key` — §24.2 says it is not part of `Metadata` | `PostgresOutboxStore::enqueue_with(tx, envelope, EnqueueOptions { ordering_key })`; plain `enqueue` writes `NULL`. ADR 0013 |
| 10 | Who owns the serializer type parameter | `PostgresOutboxStore<Ser = JsonSerializer>`; `with_serializer` changes the type. Static dispatch, no `dyn` on the enqueue path. ADR 0010 |
| 11 | `PostgresOutboxStore::new` must query to verify `search_path`, so it cannot be a plain `fn` | `new` and `with_settings` are **`async` and fallible**. ADR 0017 |
| 12 | `SerializedEnvelope` had no public constructor for rehydration (`MessageType` comes from storage, not a Rust type) | `SerializedEnvelope::from_parts(..)` + `Envelope::set_headers` + `MessageType::from_parts`. ADR 0011 |
| 13 | `OutboxStats` gave no clock-safe way to compute lag | Added `as_of: OffsetDateTime` (DB `now()` at query time), so lag is `as_of - oldest_pending_available_at` with no app/DB clock comparison. ADR 0009 |
| 14 | `EndpointAddress` was used in §12 and never defined | Capped (256) validated string newtype, transport-interpreted, in `reliar-core`; `Option<EndpointAddress>` everywhere, so `RoutingMetadata: Default` holds |
| 15 | `Metadata: Default` needs `CorrelationMetadata`/`DeliveryMetadata` defaults the SRS does not give | `CorrelationMetadata::default()` sets `conversation_id = ConversationId::UNSET` (the nil sentinel the builder replaces with the message's own id — superseded by C3 below); `DeliveryMetadata::default()` uses `ContentType::JSON` as a **placeholder the store overwrites at enqueue** from `Serializer::content_type()`. ADR 0010 |
| 16 | Where `purge` is driven from | The **host** calls `OutboxStore::purge` (it is public and the host owns the store) from its own periodic task. There is no dispatcher method — `run(self)` consumes the dispatcher, so a `&self` maintenance call would be unreachable. Reliar starts no maintenance thread |

### Resolved in review 1 (2026-09-04)

| # | Finding | Resolution |
|---|---|---|
| B1 | `#[non_exhaustive]` return types unbuildable from `reliar-store-postgres` | `OutboxRecord::builder(..)` + `OutboxRecordBuilder`; `new` on `AcquiredBatch`, `PoisonedRow`, `MessageRef`, `CompletedMessage`, `FailedMessage`, `PurgeReport`, `OutboxStats`; `Default` + setters on `AcquireRequest`, `PurgeRequest`, `DeadQuery`. Stated as a standing convention in the header (§ conventions) |
| B2 | `Ser = JsonSerializer` and `new()` break `--no-default-features` | `connect(pool, settings, serializer)` is always available; the default type parameter and `new`/`with_settings` are `#[cfg(feature = "json")]` behind the provider's own default `json = ["reliar-core/json"]`. The provider does **not** hard-enable `json` |
| B3 | `purge_once(&self)` unreachable after `run(self)` | Removed. Host calls `OutboxStore::purge` (see #16); `overview.md` corrected |
| B4 | No `PartialEq`/explicit `Debug` → §43.A.4 untestable | `PartialEq` derived on `Envelope<T>` and the whole `Metadata` family; `Debug` is a **manual impl that elides the body for every `T`** (no specialization, so a derive would print payload bytes). Same rule for `OutboxRecord` |
| M5 | Fakes lacked a seeding/clock/script API | `InMemoryOutboxStore::{insert, insert_with, advance, records, record, fail_next}`, `RecordingPublisher::{published, count, envelopes, in_flight_peak}`, `ScriptedPublisher::{new, always}` + `PublishStep`, and a new `RecordingMetrics` with per-instrument accessors |

### Ruling — Postgres test-harness hygiene (RELIAR-27, 2026-09-04)

Not a contract change; recorded here for traceability. Full design in
`../analysis/architect-review.md` §9.

| # | Finding | Resolution |
|---|---|---|
| P1 | 167 leaked containers / 31 GB of volumes from the provider suite | Two causes. **A `static` is never dropped**: the harness parked `ContainerAsync` in a `static OnceLock`, and Rust runs no destructors for `static`s at exit — so `Drop`, which is the removal path, never ran. And **26 `[[test]]` targets are 26 binaries**, so a single `cargo test -p` started 26 containers. Fix: one `harness = false` binary (`tests/postgres/main.rs` + a `mod` per scenario) whose `main` owns the container as a **local** and drops it before exiting. `Conclusion::exit()` is forbidden — it calls `process::exit`, which skips destructors and reproduces the leak exactly |
| P2 | The card assumed a Ryuk reaper had been disabled | **There is no Ryuk.** Verified against the pinned `testcontainers` 0.27.3 source: `grep -rli 'ryuk\|reaper'` over the crate returns nothing, and removal happens *only* in `ContainerAsync::Drop`. Nothing was disabled on the machine; there is nothing to enable. The equivalent safety net that *does* exist is the **`watchdog` feature** (off by default), which removes containers on SIGTERM/SIGINT/SIGQUIT — enabled, as braces to `Drop`'s belt |
| P3 | Guaranteed removal, and where sweeping belongs | Three layers, in order: `Drop` (normal exit and panics), `watchdog` (signals), and a documented manual label-scoped sweep (`CONTRIBUTING.md`) as the **third** line of defence. Filter verified against the actual leak — `label=org.testcontainers.managed-by=testcontainers` matches exactly the 161 leaked containers. A sweep that is *relied on* hides a harness bug, so it is never the first line. The pooler scenario owns its `PgDog` handles as **locals in the trial function**; `run_scenario_in_child` passes the parent's `DATABASE_URL` through, or every re-executed child starts its own Postgres |
| P4 | Are `TESTCONTAINERS_COMMAND=keep` / container reuse allowed in CI? | **Forbidden in CI**, local debugging aid only. Both defeat `Drop`-based removal, which in 0.27.3 is the entire removal mechanism. CI asserts `TESTCONTAINERS_COMMAND` is unset or `remove`, and the `reusable-containers` feature stays off |
| M6 | `enqueue_sets_search_path` / `statement_timeout` unspecified | Both specified in §4: `set_config(…, true)` is transaction-local, so host session state is never mutated; `statement_timeout` applies only to Reliar's own operations, never to the caller's transaction, and `Duration::ZERO` means "inherit" |
| M7 | `dead_reason` column added with no ADR | **ADR 0023** — `text` column, snake_case codec, `ck_outbox_dead_reason`, unknown value → poison row |
| M8 | `P: Clone`, `M: Clone` avoidable | Dropped; the dispatcher wraps both in an internal `Arc` |
| M9 | `purge`: one pass vs internal loop | **One bounded pass per call**; the host repeats while `!report.is_complete(batch_size)`. ADR 0009 clarified; AC §43.A.19's wording is the PO's to align |

### Resolved in review 2 (2026-09-04)

Findings introduced by the review-1 fixes, plus five majors. **Three of these change a
`reliar-outbox` signature** and must reach the engineer building S2: `list_dead`'s return type,
`OutboxStats::new`'s arity, and the additive `OutboxMetrics::expired_pending`.

| # | Finding | Resolution |
|---|---|---|
| B1 | `PurgeRequest`/`DeadQuery` derived `Default` → `None/0/0`, so `purge` deletes nothing and `list_dead` returns nothing | **Hand-written `Default`** on both, matching the documented values (7 d retention, batch 1000, limit 100). Derived `Default` is banned on any settings-like type whose zero value is not its documented default |
| B2 | `OutboxRecord` had no derives, but fakes hand records out and `unit` ACs compare them | `#[derive(Clone, Debug, PartialEq)]`. `Debug` is safe to **derive** here: the only payload is inside its `SerializedEnvelope`, whose manual `Debug` already elides the body |
| M3 | Derived `Clone` on `PostgresOutboxStore<Ser>` conditions on `Ser: Clone` | Serializer held as `Arc<Ser>`; **manual `impl<Ser> Clone`** with no `Ser: Clone` bound, so the store still drops into an `AppState` unchanged |
| M4 | `list_dead` had no channel for a dead row that is itself undecodable (ADR 0023 says reported, never silent) | Returns **`DeadLetterPage { records, poisoned, next_after_sequence }`**. The cursor is computed over every row scanned, including poisoned ones — otherwise a poisoned tail loops forever |
| M5 | Expired pending rows go dead only via `purge`, so `stats.pending`/lag pin forever if the host never purges | `pending` and `oldest_pending_available_at` use the **claim predicate** (expired excluded); new `expired_pending` count + gauge. The transition stays in `purge` — the claim path takes no write for bookkeeping. ADRs 0009 and 0020 updated |
| M6 | `enqueue` overwrites `content_type`, so §43.A.4 equality held only for JSON by coincidence | The store's value is **authoritative and the call site's is ignored, not validated**; `PostgresOutboxStore::content_type()` added, and §43.A.4's equality is now stated exactly against the enqueued-and-filled envelope, with a non-JSON serializer in the matrix |
| M7 | `statement_timeout` default 5 s forces a 4-round-trip `SET LOCAL` wrap on every store call | **Default `Duration::ZERO`** = inherit the server/role setting. SRS §7.2's 5 s default is withdrawn *(amendment: PO, RELIAR-23)* |
| m8 | `ConfigError` lacked retry-validation variants | Added `ZeroMaxAttempts`, `ZeroRetryBase` |
| m9 | `ScriptedPublisher`'s positional script races with `max_in_flight > 1` | `new` documented as deterministic only at `max_in_flight = 1`; added `ScriptedPublisher::keyed(..)` for per-message outcomes at any concurrency |
| m10 | "two round trips" for `enqueue_sets_search_path` | Corrected to **three** (`current_setting` + two `set_config`) |

### Amended after the `reliar-core` review (RELIAR-12 review 1, 2026-09-04)

Two corrections found while building `reliar-core` against this file. Flagged here so the
concurrent re-check of the contract sees them.

| # | Finding | Resolution |
|---|---|---|
| C1 | §2.5 pinned `#[derive(… Debug …)]` on `Headers`, but a derived `Debug` prints every custom header **value** — contradicting SRS §33 and this contract's own no-payload rule | `Headers` gets a **manual `Debug`**: keys printed, values `"<redacted>"`. The general rule — **no Reliar type's `Debug` prints payload bytes or header values** — is now stated once in the conventions at the top, with `Envelope<T>`, `Headers` and `OutboxRecord` named as its three cases |
| C2 | §1 listed `tracing` among `reliar-core`'s dependencies; the crate correctly has none (it emits no spans — the outbox and provider crates do) | Removed from the §1 dependency line. `tracing` stays a dependency of `reliar-outbox` and `reliar-store-postgres` only |

### Resolved in review 3 (2026-09-04)

| # | Finding | Resolution |
|---|---|---|
| B1 | `list_dead`'s `after_sequence` cursor had no stated `ORDER BY`; the index implied `(dead_at, sequence)`, which silently **skips rows** | **`ORDER BY sequence ASC` is normative.** `sequence` is unique and monotonic, so the keyset cursor is correct with no tiebreak, and `dead_before`/`message_type`/`tenant_id` are filters only. The index becomes `ix_outbox_dead ON outbox (sequence) INCLUDE (dead_at) WHERE dead_at IS NOT NULL` — the `INCLUDE` evaluates `dead_before` without a heap fetch. `next_after_sequence` is the largest `sequence` **scanned**, poisoned rows included. Recorded in ADR 0008; §24.1's index and §19.3 need the SRS amendment *(PO, RELIAR-23)* |
| M2 | "derived `Debug` on `OutboxRecord` is safe" was asserted before `Headers` had a redacting `Debug` | The claim now holds **and says why**: both forbidden categories are stopped one level down — `Envelope`'s manual `Debug` elides the body, `Headers`' manual `Debug` redacts every value (§2.5 amendment) — so the derive at this level cannot leak either |
| M3 | Who polls `stats()` — `overview.md` said the host, the contract put `stats_interval` in `DispatcherSettings`, §43.A.25 needs the dispatcher to drive `RecordingMetrics` | **The dispatcher polls it**, on `stats_interval`, and is the sole caller of `OutboxMetrics::{pending, expired_pending, oldest_pending_age}`; `Duration::ZERO` disables the tick. `stats()` stays `pub` for admin endpoints. The split is *reads on the worker's timer, writes on the host's* — `purge` writes, so it stays the host's. `overview.md` and ADR 0008 aligned |
| m4 | "two extra round trips … three extra statements" self-contradiction | Now says **three extra statements** once |
| m5 | `build()`'s documented rejection list was incomplete | Complete: `ZeroInFlight`, `LeaseTooShort`, `UnsupportedOrdering`, `InvalidJitter`, `ZeroMaxAttempts`, `ZeroRetryBase`. *(`SchemaMismatch` was listed here as the provider constructor's; it was removed outright in J3 — see below.)* |
| m6 | `DeadLetterPage` missing from the construction preamble; `DeadQuery` had "builder methods only" and no `impl` | Both added |

### Resolved in review 4 (2026-09-04)

| # | Finding | Resolution |
|---|---|---|
| M1 | Review 3's cursor fix removed the only `dead_at`-leading index, leaving `purge`'s dead-retention `DELETE` with nothing to use | **Two indexes on dead rows, for two opposite access patterns**: `ix_outbox_dead (sequence) INCLUDE (dead_at)` for `list_dead`'s keyset page, `ix_outbox_dead_at (dead_at)` for the retention delete. Both partial on `dead_at IS NOT NULL`, so they index almost nothing on a healthy table. The `INCLUDE` claim is corrected: it avoids the heap **only when `message_type`/`tenant_id` are unset**. ADRs 0008 and 0009 updated; §24.1 needs the SRS amendment *(PO, RELIAR-23)* |
| M2 | `JsonError`'s `Display` interpolates `serde_json::Error`, which carries payload fragments, and flows into `PoisonedRow.error` / `PostgresStoreError::Decode.detail` | **Engineer's fix in `reliar-core` (RELIAR-12)** — print the error *kind* and line/column only, never the serde message, with a test. The contract already forbids it (conventions: no error `Display` prints payload bytes); no signature changes |
| m3 | `DeadLetterPage` fullness undefined | "Full" is `records.len() + poisoned.len() == query.limit` — a poisoned row occupies a slot because it occupies a row in the scan, so counting only `records` would stop pagination early on an undecodable tail |
| m4 | `expired_pending`'s supporting index unnamed | `ix_outbox_expires`, the same partial index as the expiry sweep |
| m5 | Undefined what the dispatcher passes to `oldest_pending_age` when `lag()` is `None` | **It skips the call**, rather than reporting `Duration::ZERO`: zero means "a row just became due", and an empty outbox is a different fact — reporting zero would make a drained outbox look like one that is keeping up. `pending`/`expired_pending` still report `0`. §43.A.25 asserts the skip |
| m6 | Documented `Headers` `Debug` output showed a `Headers(…)` wrapper the `debug_map` impl does not emit | Aligned to the bare map form |
| m7 | `EnvelopeBuilder<T>` is a fourth manual-`Debug` case — it holds the body before `build` | Listed in the conventions and on the type |

### Amended after the `reliar-core` re-check (RELIAR-12 review 2, 2026-09-04)

| # | Finding | Resolution |
|---|---|---|
| D1 | `Headers` keys and values accepted control characters, including CRLF — a header-injection surface. §2.5 listed only the reserved prefix and the size caps | `Headers::insert` now **rejects any Unicode `Cc` code point in the key or the value**, with `HeaderError::{ControlCharacterInKey, ControlCharacterInValue}`. The value-side variant carries **only the key**: the offending value is precisely the data no error `Display` may echo. Rationale recorded on the method — custom headers go onto the wire verbatim via an `EnvelopeMapper`, so validating once at the boundary that *creates* the value keeps every present and future transport from re-deriving the rule. Matches `CorrelationId`/`EndpointAddress`/`ContentType`, whose `IdError::ControlCharacter` variant is now in §2.1 too |
| D2 | Extra derives added while building `reliar-core` were ahead of the contract | Recorded as the contract's own: `Headers` is `Clone, Default, PartialEq, Eq` (with the manual `Debug`); `TraceContext` and `RoutingMetadata` gain `Eq`. Stated as a family rule — **`Eq` wherever every field is `Eq`, `PartialEq` elsewhere** — since adding `Eq` later is additive |
| D3 | Reviewer still reported `tracing` listed for `reliar-core` in §1 | **Re-verified: it is not there.** `grep -rn tracing docs/architecture/` returns only the `reliar-outbox` and `reliar-store-postgres` lines plus the §7 note recording the removal. The C2 fix landed; the finding is against a stale copy of the file |

### Amended after the `reliar-outbox` S2 review (RELIAR-13 review 1, 2026-09-04)

| # | Finding | Resolution |
|---|---|---|
| E1 | `WorkerId::generate()` defaulted to `host:pid:uuid7`, which means reading `HOSTNAME` — the library reading the environment implicitly | **PO ruling: the default is `pid:uuid7`, with no host segment.** ADR 0019's "the library never reads the environment implicitly" is absolute; a rule with one convenience exception is not a rule. The `uuid7` half already gives the guard all the uniqueness it needs — the hostname was only operator ergonomics, and a host that wants its pod name sets `DispatcherSettings::worker_id` / `RELIAR_OUTBOX_WORKER_ID` explicitly, which is also the only way it can be certain of the value. Contract §3.1, ADR 0011 and ADR 0019 updated; **SRS §17.1 still says `host:pid:uuid7`** *(PO, RELIAR-23)* |
| E2 | `ConfigError`'s derives were behind the implementation | Recorded as contract: `#[derive(Clone, Debug, PartialEq)]`, so a test can assert the exact rejection rather than matching on a shape. Additive |
| E3 | Confirm `bytes` is not a normal dependency of `reliar-outbox` | **Confirmed, and §1 now says so explicitly.** The crate handles payloads only as the opaque `SerializedEnvelope` from `reliar-core` and never names `bytes::Bytes`. §1's `reliar-outbox` line is also corrected to the dependencies it actually has (`uuid`, `time` were missing; `tokio`/`tokio-util` arrive with the dispatcher in S4). **Note for the engineer: `crates/reliar-outbox/Cargo.toml` currently declares `bytes.workspace = true` with no use in `src/` — an unused dependency `cargo machete` should fail on; remove it** |

### Amended after the `reliar-core` final re-check (2026-09-04)

| # | Finding | Resolution |
|---|---|---|
| F1 | `ContentType` had no length cap in the contract, though every other string newtype does | **`ContentType::MAX_LEN = 256`** recorded, with `ContentTypeError::TooLong { len, max }`. Same reasoning as `CorrelationId`: the value lands in a `text` column that every claim reads back and is rehydrated into `DeliveryMetadata`. `parse` also rejects control characters, matching §2.1/§2.5's header-injection rule. Additive on a `#[non_exhaustive]` enum |
| F2 | `ContentTypeError::Malformed`'s `Display` echoed the whole rejected value | **Truncated to a 64-character prefix with `…`**, recorded as contract. A content type is developer-supplied, so a prefix is what makes the error actionable — but 256 characters of attacker-influenced string in a log line is not. The 64 is a **fixed** cap, deliberately far below `MAX_LEN`, so raising `MAX_LEN` later cannot silently turn this back into "echo the whole value". `TooLong` echoes nothing at all |


### Ruling before S5 implements `purge` (RELIAR-13 review 2, 2026-09-04)

| # | Finding | Resolution |
|---|---|---|
| G1 | `batch_size` was documented as bounding only the two deletes, leaving the expired→dead sweep an **unbounded `UPDATE`**, and `is_complete` ignored `expired_to_dead` | **All three statements are bounded by `batch_size`**, the sweep as `UPDATE … WHERE id IN (SELECT … LIMIT n)`. An unbounded `UPDATE` on `outbox` is the same hazard as the unbounded `DELETE` this rule already forbids — it holds row locks across the whole matched set, blocks concurrent claims, and writes a WAL record per row with no cancellation point; a table with a large expired backlog would stall every worker on the first maintenance run. **`is_complete` is now `true` only when all three counts are under the bound**, so the host's existing drain loop drains expiry too and needs no second loop. The `InMemoryOutboxStore` mirrors the bound, so the fake and Postgres behave identically under a host's drain loop. ADR 0009 updated |
| G2 | Unstated whether the expired→dead sweep may transition a row **currently leased by a live worker**, possibly mid-publish | **No.** The sweep's predicate carries the claim's lease clause, so it moves only rows that are expired *and* unowned. A row that expired after being claimed belongs to its worker — whose `complete` still wins if the publish succeeded — and becomes sweepable once the lease lapses, at most one lease later. The alternative is an unguarded maintenance write racing a worker-guarded one, and `ck_outbox_terminal` turns that race into a **constraint violation on a healthy path**: a successful publish, reported by its rightful owner, rejected because maintenance had already marked the row dead. Waiting one lease costs nothing; the row is unpublishable either way. ADR 0009 updated *(RELIAR-14 review 1)* |

### Resolved in RELIAR-14 review 1/2 — `test-support` fakes (2026-09-04)

§3.10 catch-up: the fakes were built and reviewed, and five details of their surface are now
contract rather than implementation.

| # | Finding | Resolution |
|---|---|---|
| H1 | `RecordingMetrics::purged()` returned `(u64, u64)`, so "purge never ran" and "a pass deleted nothing" were indistinguishable | Returns **`Option<(u64, u64)>`**. `(0, 0)` is a real report from a pass that found nothing to delete; a test asserting the dispatcher never purged needs to tell that apart from no call at all |
| H2 | No getter for `publish_duration`, though it is an `OutboxMetrics` instrument §43.A.25 covers | Added `RecordingMetrics::publish_duration() -> Vec<(Duration, MessageType)>` |
| H3 | `RecordingPublisher` had no way to make publishes overlap, so `in_flight_peak` could not observe concurrency | `publish` is **timer-free by default** (a paused-time test never advances the clock just to drain a batch), and `RecordingPublisher::with_concurrency_probe(hold)` adds the hold that makes `max_in_flight` observable |
| H4 | `InMemoryStoreError::LeaseLost` was never constructible | **Removed.** A lost lease is `0` rows affected, not an error — that is ADR 0008's rule, and a variant that contradicts it invites an implementor to return it |
| H5 | When the publisher records, and when the fake store mutates, were unstated | Both publishers record into `published()` **at the first poll of the returned future**, matching a real transport: nothing reaches a broker until the future runs, so a dropped un-awaited future records nothing and `in_flight_peak` counts futures that are actually running. `InMemoryOutboxStore`, by contrast, mutates **eagerly at call time** — a **documented divergence** from Postgres that falls out of "drop the guard before awaiting", left as-is because no dispatcher path drops a store future un-awaited and hiding it would cost an internal channel for no test value |

### Resolved for S5 — platform + provider settings (2026-09-04)

| # | Finding | Resolution |
|---|---|---|
| I1 | `cargo deny check bans` failed because `sqlx-core` depends on `thiserror`. The house ban is meant to stop **our** crates using it, not to forbid it transitively | **Mechanism switched, not patched.** `async-trait`/`thiserror`/`anyhow` are removed from `deny.toml`'s graph-wide bans and enforced per-manifest by a new `ci.yaml` gate over `crates/*/Cargo.toml` — the same shape, and the same reason, as the `reliar-core` purity gate: the rule is per-manifest and cargo-deny's bans are graph-wide. `wrappers = ["sqlx-core", …]` was rejected for its maintenance tail (every sqlx bump adding an internal crate reddens an unrelated PR) and because the grep is **stricter** for our code — it catches a direct dependency in one of our crates even when a third party already pulls the same crate transitively. `chrono` stays a graph-wide ban on different grounds: two datetime stacks in one binary is ecosystem hygiene, not style. ADR 0022 updated; `cargo deny check` verified green |
| I2 | Same run surfaced a licence rejection: `webpki-roots` (CDLA-Permissive-2.0), reached through sqlx's TLS support — a **normal** dependency, so it ships | Added to `deny.toml`'s allow-list with its justification: it is a *data* licence covering the Mozilla CA root store, permissive, with no copyleft and no share-alike on anything derived, so it imposes nothing on Reliar's users. Allowed by list rather than by ADR because, unlike MPL-2.0, it raises no vendoring or static-linking question |
| I3 | `reliar_outbox::SettingsError` is `#[non_exhaustive]` with no constructors, so `reliar-store-postgres` could not build one and defined a parallel crate-local error | **Constructors added to the contract** — `SettingsError::{parse, out_of_range, key}`, plus `Clone + PartialEq`. One error type from every `from_env` is the point of the pattern (ADR 0019): a host wiring `OutboxSettings::from_env` and `PostgresOutboxSettings::from_env` should not handle two unrelated errors for the same class of failure. Needs a small S2 follow-up in `reliar-outbox`, then `reliar-store-postgres` drops its local copy |
| I4 | `MigrateOptions` had `#[non_exhaustive]` + `Default` and no way to set the schema | Recorded: `MigrateOptions::schema(&'a str)` builder setter. Without it `MigrateOptions::default()` was the only constructible value, which made the configurable-schema story (ADR 0017) unreachable through the public API |

### Resolved from the S5 review (RELIAR-16 review 1, 2026-09-04)

| # | Finding | Resolution |
|---|---|---|
| J1 | `Classify for PostgresStoreError` was "SchemaResolution \| NotMigrated → Permanent; **rest Transient**", which makes `DuplicateMessage` transient | **Per-variant table, no blanket rule.** `DuplicateMessage` is **Permanent** — a reused `MessageId` never succeeds on retry, the row is already there, so retrying is pure waste that also hides an application bug. `Decode`, `UnknownMetadataVersion` and the new `InvalidSchema` are Permanent for the same shape of reason: the input does not change between attempts. `Database` is **classified by the wrapped SQLSTATE class**, not blanket-transient — `08*`/`40*`/`53*`/`55*`/`57014` transient, `22*`/`23*`/`42*` permanent, unknown → transient with a `warn`. Without that split a `ck_outbox_*` violation retries ten times and fails identically ten times. `EnqueueError` implements `Classify` on the same rules, because `enqueue` runs on the host's write path and the host should not re-derive them |
| J2 | `42P01` was mapped to `NotMigrated` only inside `connect`'s verification, so the operational path saw a transient `Database` and retried forever against a missing table | Mapping is stated as applying on **every** path, not just at startup |
| J3 | `ConfigError::SchemaMismatch` had no home in the type system | **Removed.** No point in the type system ever holds both values: `migrate` is a free function taking `MigrateOptions`, `connect` takes `PostgresOutboxSettings`, and under ADR 0018 the migration may have been applied by a DBA's pipeline in another process — so the check is unreachable in exactly the deployments where it would matter. `connect`'s startup verification is strictly stronger: it catches every cause of a mismatch by asking the database, rather than comparing two strings that were only hoped to agree. **Removes a public variant from `reliar-outbox` — relay to S2** |
| J4 | Schema names reach `dangerous_set_table_name`, which interpolates into DDL, unvalidated | Validation recorded as contract: `[A-Za-z_][A-Za-z0-9_$]*`, ≤ 63 bytes (`NAMEDATALEN - 1`), applied once to **both** `PostgresOutboxSettings.schema` and `MigrateOptions.schema`. A schema name comes from configuration, which is not automatically trusted input. It needed somewhere to be reported, so `migrate` now returns a provider-owned **`MigrateError { InvalidSchema, Sqlx }`** instead of `sqlx::migrate::MigrateError`, and `PostgresStoreError` gains `InvalidSchema` |
| J5 | `MetadataRest` serialization panicked: RFC 3339 cannot format years outside `0000..=9999`, which `OffsetDateTime` accepts | **Persist epoch milliseconds (`i64`).** Architect ruling, since the JSONB shape is ADR 0012's contract: the encoding is **total** over the type's range, so the failure mode is deleted rather than made fallible — and a fallible `sent_at` would only move a baffling error onto the host's write path. Also smaller, integer-comparable, parser-free; less readable in `psql`, which is the trade. `metadata_version` stays `1` — no row has ever been written with the other encoding |

### Rulings for S4 — the dispatcher loop (RELIAR-15 review 1, 2026-09-04)

| # | Finding | Resolution |
|---|---|---|
| K1 | `DispatcherSettings::retry` was validated by `build()` and then **never applied** — `run` used the builder's `R`, so `RELIAR_OUTBOX_RETRY_*` was accepted, range-checked and silently ignored | **`settings.retry` *is* the default policy.** The builder now starts at `R = DefaultRetry`, a public marker that deliberately does **not** implement `RetryPolicy`; its `build()` constructs the policy from `settings.retry`. `.retry_policy(p)` moves to `R: RetryPolicy`, whose `build()` uses `p` — the two impls cannot overlap precisely because `DefaultRetry` is not a policy, so no specialization and no second method name. A configuration value that does nothing is the exact failure ADR 0019 exists to prevent |
| K2 | With a custom policy, `settings.retry` becomes dead configuration — the same silent-ignore one level up | `build()` returns **`ConfigError::RetryPolicyConflict`** when a custom policy is supplied *and* `settings.retry != ExponentialBackoff::default()`. A host wanting both leaves `settings.retry` at its default or feeds it to its own policy. `ExponentialBackoff` gains `PartialEq` for the check |
| K3 | Drain could **start** publishes that had not been attempted when cancellation arrived | It does not. A spawned task that has not yet acquired its concurrency permit has not touched the broker, so it is dropped and its row goes into the `release` set. Waiting to begin a publish that will be released anyway spends the drain budget and widens the duplicate window for no delivery — §26.1's "finish in-flight", read literally. ADR 0014 updated |
| K4 | A hung store call makes `drain_timeout` unenforceable | New **`DispatcherSettings::store_timeout`** (default 30 s, `STORE_TIMEOUT_MS`) bounds **every** `OutboxStore` call in `run`; a timeout is a transient store error. Kept separate from `publish_timeout` because the two bound different systems with different latency profiles — a 10 s broker budget applied to `purge` would fail healthy calls. It is a *client-side* bound on the future (what cancellation needs), while the provider's `statement_timeout` bounds the *server* statement and defaults to inherit; neither substitutes for the other. `build()` warns when `store_timeout > drain_timeout` |
| K5 | The permanent-store-error exit returned `Err` without releasing leases | It drains first, **best-effort**: outcomes persisted, remainder released, and any error from that drain logged and discarded so the original diagnosis surfaces. A store broken badly enough to be permanent will often fail the release too, but exiting with a batch still leased leaves it dark for a full lease. ADR 0014 updated |

### Rulings for S4 — claim backpressure and outcome durability (RELIAR-15 review 2, 2026-09-04)

| # | Finding | Resolution |
|---|---|---|
| L1 | **Blocker.** The claim loop had no backpressure: it re-claimed `batch_size` every poll regardless of outstanding rows, so a publisher slower than the poll interval made one dispatcher hoard leases without bound | `run` claims only while `outstanding < max_in_flight`, asking for `min(batch_size, max_in_flight - outstanding)`. **No new knob** — `max_outstanding` would be a third number meaning almost what `max_in_flight` already means. This is not only a memory bound: the hoarded tail sits leased and unpublished until its lease expires under a perfectly healthy worker, which *is* the §22.1 slow-batch duplicate window, so the gate **shrinks that window**. It also caps the claimed-but-not-yet-started set at `max_in_flight`, which is what keeps K3's release-not-start rule cheap. Documented consequence: **`max_in_flight` is the real ceiling and `batch_size` only caps a single claim statement** — with the accepted defaults (100 / 16) `batch_size` never binds. Deliberately **not** warned about, because a warning on the default configuration is noise; SRS §23.1's `batch_size` default is now largely informational *(PO may wish to revisit — RELIAR-23)* |
| L2 | A failed or timed-out `complete`/`fail` dropped the outcome: the row left `outstanding`, stayed locked for a full lease, and `attempts` never advanced | Such rows **stay in `outstanding`** in a pending-outcome state and the write is retried on the next iteration. The worker still owns them — dropping them means it merely *forgot*. Retry is safe because the `locked_by` guard makes a repeated `complete` idempotent, and a lost lease makes it affect zero rows (benign, ADR 0008). Keeping them outstanding is also correct backpressure: a store that cannot accept outcomes stops the loop claiming more work. **Not a fourth duplicate window** — it is SRS §23.2's "publish succeeded, completion failed" |
| L3 | K3's "has not touched the broker" was too strong, and drain's treatment of a resolved-but-unpersisted success was unstated | Softened to **"has not completed a publish"**: on a multi-thread runtime a dropped task may have been polled once and begun the broker call, so a released row may already have been delivered — the same at-least-once window, not a new one. And at drain the two cases are now split: a row whose publish **failed or never resolved** is released, but one whose publish **succeeded** and whose `complete` never landed is **left to its lease** — releasing it would turn a possible duplicate into an immediate certain one for a message already delivered, with nothing recovered sooner |

### Rulings for S4/S5 — wedged worker, timeout scope, dead settings (RELIAR-15/16 review 3, 2026-09-04)

| # | Finding | Resolution |
|---|---|---|
| M1 | L2's outcome retry ignored `FailureKind`, so a **permanently** failing `complete`/`fail` write wedged the worker: `outstanding` filled to `max_in_flight`, claiming stopped, leases renewed forever, `run` never returned | **A `Permanent` outcome-write error ends the loop.** `run` drains best-effort — still persisting what it can for other rows — and returns `Err(DispatchError::Store(..))` with the original error. The unwritten published rows are **left to their lease, never released** (L3): they are already delivered, so releasing buys an immediate certain duplicate and recovers nothing. A silently stalled worker is the worst available outcome precisely because nothing about it looks like a failure |
| M2 | Transient outcome-write failures could also retry forever, holding the gate shut | **Bounded by `lease`.** Once a row's unwritten outcome has been retried for longer than the lease, the row is dropped from `outstanding` **and excluded from lease renewal**, so the lease lapses and another worker reclaims it. Both halves are required — dropping it while still renewing leaves a row nobody owns and nobody can claim. The lease is the right bound because it is already how long an unreachable worker's rows stay dark, so waiting longer buys nothing a reclaim does not; the duplicate admitted is §22.1's, already documented. The gate now always frees: the write lands, the row leaves `outstanding` within a lease, or `run` returns |
| M3 | Should `purge`, `list_dead`, `retry_dead`, `purge_dead` also be bounded by `statement_timeout`? | **Yes — every statement Reliar issues, with no exception list.** The operator sets one number bounding how long Reliar may hold a backend, and a carve-out is exactly where the runaway statement then lives. `purge` and `list_dead` are the two most likely to run long (a bounded `DELETE` over a large backlog, a keyset page over millions of dead rows), so exempting the "operational" calls would exempt the ones that need it most. The bound is per **statement**, not per pass, so a multi-pass `purge` is not penalised. `enqueue` remains excluded — Reliar does not impose a timeout on a transaction it does not own |
| M4 | `PostgresOutboxSettings.listen_notify` was a silent no-op | **Removed**, not documented as inert. A field a host can set to `true` and observe no change from reads as a supported feature, gets set in production configs, and its "no behaviour in 0.1" rustdoc is the first thing nobody reads — worse than an absent field. `#[non_exhaustive]` makes adding it in 0.2, together with the code that honours it, non-breaking, so waiting costs nothing. `Ordering::PerKey` ships early only because its *schema* had to; `listen_notify` has no such constraint |
| M5 | **Platform blocker.** `test.yaml` probed for a package `reliar-migrate` that had never been created, so the sqlx-cli install, the migration and `cargo sqlx prepare --check` were all skipped — a green job with the DoD gate unenforced | Both probes now key on **`reliar-store-postgres`**. A skip-guard whose condition can never be true is worse than no guard, because it reports success. The missing entry point is now real: **`crates/reliar-store-postgres/examples/migrate.rs`** calls `reliar_store_postgres::migrate(&pool, MigrateOptions::default())` over `DATABASE_URL`, with `RELIAR_SCHEMA` for a non-default schema. It carries no query macros of its own, so it builds from the committed `.sqlx` cache — which makes the migrate step a smoke test of that cache too. *(Amended 2026-09-04, decision #32: this shipped first as a `tools/reliar-migrate` workspace member with its own `rust-version = "1.94"`; `tools/` is gone and the same code is an example of the crate that owns `migrate()`, so it needs no manifest and no second MSRV.)* Verified: `SQLX_OFFLINE=true cargo check -p reliar-store-postgres --all-targets` clean, the probe simulates to `exists=true`, `actionlint` clean |
| C3 | §2.4's `CorrelationMetadata::default()` minted a **fresh** `ConversationId`, so `build` could not tell a caller-chosen conversation from the placeholder; it used a `correlation_explicit` flag instead, which mis-fired on the common `.metadata(tweaked_default)` path (there are no setters for `routing`/`sent_at`/`deduplication_id`) and left a random unrooted id | The placeholder is now the comparable sentinel **`ConversationId::UNSET`** (nil UUID, §2.2). `build` roots the conversation **iff the value is still `UNSET`**; the flag is gone. Additive `EnvelopeBuilder::conversation(id)` added to §2.6 for the join-an-existing-conversation case. Breaking for anyone reading `CorrelationMetadata::default().conversation_id` directly (nothing does) |

### Rulings for S4 — the concurrency bound (RELIAR-15 review 3, 2026-09-04)

| # | Finding | Resolution |
|---|---|---|
| N1 | **Blocker.** §43.A.23 was no longer proven: with the L1 claim gate in place, `Semaphore::new(1_000_000)` keeps `dispatcher_bounds_concurrency_to_max_in_flight` green, because the gate alone caps the peak. Engineer's accounting confirmed: a claim requests at most `max_in_flight - outstanding_count`; a resolved task's move from `outstanding` into `unwritten_*` is net-zero in that count, so the gate never re-opens a slot before the underlying permit is already free — under a **conforming** store `acquire_owned()` never waits | **Keep the semaphore; the guarantee is bounded twice, deliberately.** The gate bounds *leased rows*, the semaphore bounds *concurrent publishes* — §43.A.23's actual wording. Removing it would make a promise about broker concurrency conditional on every third-party `OutboxStore` honouring `batch_size`, and would leave the K3 "not yet started" state reachable only through a scheduling race with no deterministic test. Both are proven with one test-only fake: a delegating `OverDeliveringStore` in `tests/common/` that rewrites the acquired batch size, so `outstanding` (20) genuinely exceeds permits (4). §43.A.23 stands as written — no reframing |

