# Phase-2 public contract — `reliar-transport-nats` (frozen)

**Status: FROZEN for Phase 2 — 2026-09-04.** Every signature below is the contract engineers build
against in parallel. **Changing anything here requires an ADR in `../decisions/` first**, then an
update to this file, then a notification to every engineer building against it (assume breaking
until proven otherwise).

**Rebased 2026-09-05 onto Amendments ADR 0026 A/B/C · 0028 A/B · 0030 A/B · 0031 A/B · ADR 0032.**
Freezing a contract and then amending the ADRs behind it left this file describing an API the
crate no longer had (S0 review B1). Every line below now matches the shipped crate; each rebased
statement names the amendment it came from, and the freeze still holds — the amendments *are* the
ADR-first route this preamble demands, and no signature moved without one. The single **new**
decision in this rebase is ADR 0028 Amendment B (`max_in_flight` → `batch_pipeline_depth`, §4.1).

Extracted from `../srs.md` v1.1.4 §12, §12.3, §14–§16, §17.1, §18, §19.4, §22, §23, §32, §33, §42
and resolved by **ADRs 0026–0032**. Where the SRS left a detail open, this file **decides it** and
the decision is listed in [§9 Decided here](#9-decided-here).

The Phase-1 contract (`phase1-contract.md`) still governs everything it covers — this file adds one
crate. **Amended 2026-09-05 by ADR 0032:** `Publisher`, `Classify`, `FailureKind` and
`SettingsError` moved from `reliar-outbox` to `reliar-core` with their signatures unchanged, so
every path below that read `reliar_outbox::` now reads `reliar_core::`, and this crate depends on
`reliar-core` alone. No signature in this file changed. Its "conventions that apply to
everything below" preamble (rustdoc on every public item, `#[non_exhaustive]`, `impl Future + Send`
in traits and never `async fn`, hand-rolled errors with `source()`, `time` not `chrono`, a `Debug`
that never prints payloads or header values) applies here verbatim and is not repeated.

Two extra rules are specific to this crate:

- **No `&str` into an `async-nats` header.** `impl IntoHeaderName for &str` and
  `impl IntoHeaderValue for &str` **panic** on invalid input. Every name goes through
  `async_nats::HeaderName::from_str` and every value through `async_nats::HeaderValue::from_str`,
  and the error becomes a permanent `NatsMapError` (ADR 0026 §3). A panicking conversion anywhere
  in this crate is a blocker finding.
- **No `Display` in this crate ever prints a server address**, a header value, or payload bytes
  (§17.1, ADR 0030). Subjects and numeric facts are allowed and wanted.

---

## 1. The crate

```
reliar-transport-nats ──▶ reliar-core   (EnvelopeMapper, SerializedEnvelope, Metadata,
                     └──▶ async-nats     Publisher, Classify, FailureKind, SettingsError)
```

It depends on **no other provider**, on **no abstraction crate**, and **nothing depends on it**. It
implements only traits `reliar-core` owns, so ADR 0032's dependency rule gives it no
`reliar-outbox` edge at all — in `[dependencies]` or `[dev-dependencies]`.

Adding this crate introduces no NATS symbol into `reliar-core` or `reliar-outbox` and no
infrastructure crate into their `cargo tree` — that is §43.B's constraint and it holds. (ADR 0032
separately relocates four Phase-1 items *out* of `reliar-outbox`, which does touch files under both
`src/` trees; §43.C's draft C4 is reworded accordingly — the binding clause is "no NATS symbol",
not "no file changed".)

`crates/reliar-transport-nats/Cargo.toml`:

```toml
[package]
name = "reliar-transport-nats"
version = "0.1.0"
description = "NATS JetStream transport for Reliar: envelope↔header mapping, subject resolution, and an at-least-once Publisher."
edition.workspace = true
rust-version.workspace = true          # 1.88 — async-nats 0.50 needs exactly the workspace floor,
license.workspace = true               # so NO per-crate override (ADR 0025 does not apply here)
repository.workspace = true
authors.workspace = true
homepage.workspace = true
categories.workspace = true
keywords.workspace = true

[lints]
workspace = true

[features]
default = []                           # no feature flags in Phase 2 (see §8)

[dependencies]
reliar-core = { workspace = true }     # the crate's ONLY Reliar dependency (ADR 0032): EnvelopeMapper,
                                       # SerializedEnvelope, Metadata, Publisher, Classify,
                                       # FailureKind, SettingsError. default-features = false at
                                       # the workspace pin: no `json` needed
async-nats = { workspace = true }      # default-features = false, features = ["jetstream"] (ADR 0031)
bytes.workspace = true
uuid.workspace = true
time = { workspace = true, features = ["formatting", "parsing"] }   # RFC 3339 on the wire
tokio.workspace = true                 # RUNTIME dep, not a dev-dep: `tokio::time::{timeout,
                                       # timeout_at, Instant}` bound §4.2's publish and each
                                       # publish_batch window. The workspace pin already carries
                                       # `time`; no new feature and no new third-party crate
                                       # enters the graph (ADR 0031 Amendment A)
tracing.workspace = true               # RUNTIME dep since S2: §4.4's spans and the `warn`

[dev-dependencies]
proptest.workspace = true
serde.workspace = true                 # `Message`'s supertraits: test fixtures derive Serialize/Deserialize
time = { workspace = true, features = ["macros"] }   # `datetime!` for fixed instants in generators
tracing-subscriber.workspace = true    # the recording subscriber behind U13
# `tokio` and `tracing` are runtime dependencies above; this entry only adds the test-only
# features on top of `tokio`:
tokio = { workspace = true, features = ["rt-multi-thread", "macros"] }
# `watchdog` for the same reason reliar-store-postgres enables it: testcontainers 0.27 has no
# reaper, so a killed process needs the signal handler to remove the container.
testcontainers = { workspace = true, features = ["watchdog"] }
libtest-mimic = "0.8"                  # one NATS-touching binary, as in reliar-store-postgres
criterion = { workspace = true, features = ["async_tokio"] }   # benches/nats_encode, benches/nats_publish
```

`[[test]] name = "nats", path = "tests/nats/main.rs", harness = false` and the two `[[bench]]`
entries (`nats_encode`, `nats_publish`) complete the manifest; they are mechanics, not contract.

`lib.rs` starts with `#![forbid(unsafe_code)]` + `#![warn(missing_docs)]` and re-exports exactly
the items in §2–§5. Nothing else is public.

**MSRV:** 1.88, the workspace floor. `async-nats` 0.50.0 declares `rust-version = "1.88.0"`, so this
crate is *not* excluded from the `msrv` job and declares no `rust-version` of its own (ADR 0031 §1).

---

## 2. Mapping — `NatsEnvelopeMapper`

### 2.1 The wire message

```rust
/// The NATS wire form of an envelope: the header block and the payload, and nothing else.
///
/// It deliberately carries **no subject and no reply subject**. Those are routing, owned by
/// [`SubjectResolver`] (ADR 0027) and by the subscription on the receiving side — keeping them out
/// is what stops a NATS routing concept from becoming part of the envelope mapping (SRS §12).
#[non_exhaustive]
pub struct NatsWireMessage {
    /// The projected headers (ADR 0026 §1). Never contains a payload byte.
    pub headers: async_nats::HeaderMap,
    /// The serialized envelope body, byte for byte — never re-wrapped (SRS §16).
    pub payload: bytes::Bytes,
}

impl NatsWireMessage {
    /// Builds a wire message from an already-projected header block and payload.
    pub fn new(headers: async_nats::HeaderMap, payload: bytes::Bytes) -> Self;

    /// Consumes it into `(headers, payload)` — what `Context::publish_with_headers` wants.
    pub fn into_parts(self) -> (async_nats::HeaderMap, bytes::Bytes);

    /// The number of bytes the server counts against `max_payload`, mirroring async-nats'
    /// own `Client::check_payload_size` exactly:
    ///
    /// - with **at least one** header: the NATS/1.0 header block (`"NATS/1.0\r\n"`, then
    ///   `"{name}: {value}\r\n"` once per header **value** — a multi-value header contributes one
    ///   line per value — then the trailing `"\r\n"`) **plus** `payload.len()`;
    /// - with an **empty** header map: `payload.len()` alone. async-nats adds the header block's
    ///   byte count only when the map is non-empty (`headers: Some(h) if !h.is_empty()`), so
    ///   counting an empty block's 12 bytes here would over-report against the very limit this
    ///   number is compared to.
    ///
    /// Used by the publisher's pre-flight guard (§4.2); U15 asserts it byte-for-byte in both
    /// shapes, because an off-by-`n` silently dead-letters publishable messages.
    #[must_use]
    pub fn wire_len(&self) -> usize;
}

/// Drops `subject`, `reply` and `status`: broker routing and protocol state, not envelope data.
/// Phase 3's consumer converts here and reads routing from its subscription (ADR 0026 §4).
impl From<async_nats::Message> for NatsWireMessage { … }
```

`Debug` is **manual**: header names and `payload.len()` only — never a header value, never a byte
of payload (Phase-1 preamble; §17.1; SRS §33).

### 2.2 The mapper

```rust
/// Projects the canonical envelope onto NATS headers + a raw payload, and back (SRS §15–§16).
///
/// Stateless, `Copy`, and cheap: one per publisher, or one per call — it makes no difference.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct NatsEnvelopeMapper;

impl reliar_core::EnvelopeMapper<NatsWireMessage> for NatsEnvelopeMapper {
    type Error = NatsMapError;

    fn encode(&self, envelope: &reliar_core::SerializedEnvelope)
        -> Result<NatsWireMessage, NatsMapError>;

    fn decode(&self, message: NatsWireMessage)
        -> Result<reliar_core::SerializedEnvelope, NatsMapError>;
}
```

### 2.3 The header projection (normative — ADR 0026 §1)

Names are written **lowercase** and recognised **case-insensitively**. The `reliar-*` set is exactly
SRS §14's closed list; nothing outside this table is written.

| # | Canonical source | Header | Value encoding | Written when |
|---|---|---|---|---|
| 1 | `envelope.id` | `reliar-message-id` | `MessageId: Display` — hyphenated lowercase UUID | always |
| 2 | `envelope.message_type.name()` | `reliar-message-type` | verbatim (`"orders.created"`) | always |
| 3 | `envelope.message_type.version()` | `reliar-message-version` | decimal `u16` (`"1"`) | always |
| 4 | `metadata.delivery.content_type` | `reliar-content-type` | `ContentType::as_str()` | always |
| 5 | `metadata.correlation.correlation_id` | `reliar-correlation-id` | `CorrelationId::as_str()` | `Some` |
| 6 | `metadata.correlation.conversation_id` | `reliar-conversation-id` | UUID | `!is_unset()` |
| 7 | `metadata.correlation.causation_id` | `reliar-causation-id` | UUID | `Some` |
| 8 | `metadata.correlation.request_id` | `reliar-request-id` | UUID | `Some` |
| 9 | `metadata.tenant_id` | `reliar-tenant-id` | verbatim | `Some` |
| 10 | `metadata.delivery.sent_at` | `reliar-sent-at` | RFC 3339, `to_offset(UTC)` → `…Z` | `Some` |
| 11 | `metadata.delivery.expires_at` | `reliar-expires-at` | RFC 3339, `to_offset(UTC)` → `…Z` | `Some` |
| 12 | `metadata.routing.source` | `reliar-source` | `EndpointAddress::as_str()` | `Some` |
| 13 | `metadata.routing.destination` | `reliar-destination` | `EndpointAddress::as_str()` | `Some` |
| 14 | `metadata.routing.reply_to` | `reliar-reply-to` | `EndpointAddress::as_str()` | `Some` |
| 15 | `metadata.trace.traceparent` | `traceparent` — **unprefixed** (W3C, §14) | verbatim | `Some` |
| 16 | `metadata.trace.tracestate` | `tracestate` — **unprefixed** (W3C, §14) | verbatim | `Some` |
| 17 | `metadata.delivery.deduplication_id` else `envelope.id` | `Nats-Msg-Id` | verbatim / UUID | always |
| 18 | `envelope.headers()` entries | the key, verbatim, case preserved — **except** a key that case-insensitively equals row 15/16, which is dropped or canonicalised (§2.4) | the value, verbatim | per entry |
| 19 | `envelope.body` | *the payload* | raw `Bytes` (cloned — refcount, no copy) | always |

Public constants for every framework name, so tests and Phase 3 never spell one by hand:

```rust
/// The `reliar-*` header names this transport writes — SRS §14's closed list, in table order.
pub mod headers {
    pub const MESSAGE_ID: &str = "reliar-message-id";
    pub const MESSAGE_TYPE: &str = "reliar-message-type";
    pub const MESSAGE_VERSION: &str = "reliar-message-version";
    pub const CONTENT_TYPE: &str = "reliar-content-type";
    pub const CORRELATION_ID: &str = "reliar-correlation-id";
    pub const CONVERSATION_ID: &str = "reliar-conversation-id";
    pub const CAUSATION_ID: &str = "reliar-causation-id";
    pub const REQUEST_ID: &str = "reliar-request-id";
    pub const TENANT_ID: &str = "reliar-tenant-id";
    pub const SENT_AT: &str = "reliar-sent-at";
    pub const EXPIRES_AT: &str = "reliar-expires-at";
    pub const SOURCE: &str = "reliar-source";
    pub const DESTINATION: &str = "reliar-destination";
    pub const REPLY_TO: &str = "reliar-reply-to";
    /// W3C Trace Context — deliberately **not** `reliar-` prefixed (SRS §14).
    pub const TRACEPARENT: &str = "traceparent";
    /// W3C Trace Context — deliberately **not** `reliar-` prefixed (SRS §14).
    pub const TRACESTATE: &str = "tracestate";
    /// JetStream's duplicate-suppression key (SRS §12.3).
    pub const NATS_MSG_ID: &str = "Nats-Msg-Id";
    /// The prefix `reliar_core::Headers` reserves, and the one decode skips (§2.5).
    pub const RELIAR_PREFIX: &str = "reliar-";
    /// NATS's own reserved prefix: rejected on encode, skipped on decode (§2.4, §2.5).
    pub const NATS_PREFIX: &str = "Nats-";
}
```

### 2.4 `encode` semantics

1. **Custom headers first**, framework headers second, and the override is **case-insensitive**
   (ADR 0026 §2 + Amendment A). `async_nats::HeaderName` is case-sensitive, so ordering alone would
   let `TraceParent` and `traceparent` both reach the wire — a message this crate's own `decode`
   then rejects as `DuplicateHeader`. The invariant instead is:

   > `encode` writes **at most one header per framework name, always in the canonical lowercase
   > spelling**, and never two names differing only in case. `decode(encode(e))` therefore never
   > returns `DuplicateHeader`.

2. A custom key is handled in this order — **permanently, never a panic**:
   1. `Nats-` prefix (case-insensitive) → `ReservedHeaderName { key }`.
   2. equal ignoring ASCII case to a framework name core does not reserve (today exactly
      `traceparent`, `tracestate`): **dropped** when the matching `metadata.trace` field is `Some`
      (the framework value overrides — SRS §14's rule); written under the **canonical lowercase
      name**, not the caller's spelling, when it is `None` (SRS §14's escape hatch); and
      `DuplicateHeader { header }` — naming the canonical spelling — when a second custom key
      normalises to the same framework name with no framework value to arbitrate (`Headers` is a
      map, so choosing one would make the wire depend on hash order).
   3. otherwise not ASCII-graphic-without-colon → `UnsupportedHeaderName { key }`.
   `reliar-*` cannot occur: core rejects the prefix case-insensitively at `insert`.
3. Every value is written through `HeaderValue::from_str`; a `\r`/`\n` in an unvalidated core
   `String` (`MessageType::name`, `tenant_id`, `traceparent`, `tracestate`, `deduplication_id`) or
   in a custom value is `InvalidHeaderValue { header }` — the header **name** as a runtime `String`,
   never the value (ADR 0026 Amendment B). A rejected custom *value* is **not**
   `UnsupportedHeaderName`: that variant means the *key* cannot be a NATS header name.
4. The payload is `envelope.body.clone()` — a `Bytes` refcount bump, no copy (§32).
5. `encode` performs no size check: the pre-flight guard is the publisher's, because the limit is a
   server property the mapper knows nothing about (§4.2).

### 2.5 `decode` semantics

Required: `reliar-message-id`, `reliar-message-type`, `reliar-message-version`,
`reliar-content-type`. Missing → `MissingHeader { header }`; unparseable → `MalformedHeader
{ header }`. Both **permanent**; neither ever panics.

An **empty value** on a required header is `MalformedHeader`, never an accepted value (ADR 0026
Amendment C). `reliar-message-id` (UUID), `reliar-message-version` (`u16`) and
`reliar-content-type` (`ContentType::parse` returns `ContentTypeError::Empty`) already reject it
through their own parse; `reliar-message-type` does **not**, because
`MessageType::from_parts` is core's deliberately unvalidated rehydration path (ADR 0011) and would
accept `""`, producing a `MessageType` that renders as `".v1"`. The mapper therefore rejects an
empty `reliar-message-type` itself: `MalformedHeader { header: headers::MESSAGE_TYPE }`, permanent,
no panic. **Emptiness is the only name rule decode enforces** — a non-empty foreign name is
accepted verbatim, since a producer's naming convention is not this crate's to police and the
mapper never validates beyond what core's own constructors do. This needs no change under
`crates/reliar-core/src/` (§43.B holds).

Everything else, in one pass over `headers.iter()`:

| Header shape | Handling |
|---|---|
| a known framework name (case-insensitive), **including a case variant from a foreign producer** | recognised — parsed into `Metadata` exactly as the lowercase spelling; malformed → `MalformedHeader`. Never ignored, never "malformed" merely for its casing |
| `traceparent` / `tracestate` | into `metadata.trace` — **never** into `Headers` |
| `Nats-Msg-Id` | `deduplication_id = Some(v)` iff `v != reliar-message-id`, else `None` (ADR 0026 §5) |
| any other `Nats-*` (case-insensitive) | **ignored** — broker bookkeeping |
| any other `reliar-*` (case-insensitive) | **ignored** — forward compatibility with a newer producer |
| anything else | a custom header, via `Headers::insert`; rejection → `RejectedHeader { key, source }` |
| a framework header appearing twice, or under two casings | `DuplicateHeader { header }`, naming the canonical lowercase spelling — permanent. Reachable only from a **non-Reliar** producer (§2.4's invariant) |
| a custom header with several values | the **first** value wins (encode never writes one) |

`message_type` is rebuilt with `MessageType::from_parts(name, version)` (ADR 0011's rehydration
path); the envelope with `SerializedEnvelope::from_parts(...)`. `metadata.correlation.conversation_id`
is `ConversationId::UNSET` when header 6 is absent.

**The canonical key class** (the term §7's U1 and U14 use) is the set of custom header keys this
crate round-trips unchanged: non-empty, every byte ASCII-graphic (`0x21..=0x7E`), no `:`, not
inside the `Nats-` prefix (case-insensitive), and not case-insensitively equal to `traceparent` or
`tracestate` — the two framework names core does not reserve, which §2.4 arbitrates into
`metadata.trace` instead. `reliar-*` is excluded by construction: core rejects that prefix at
`Headers::insert`, so such a key cannot reach the mapper. A key outside the class is not a bug —
it is one of §2.4's named permanent errors, or one of the three normalisations below.

**Round-trip (story C1):** `decode(encode(e)) == e` for every envelope whose custom header keys are
in the canonical key class. `OffsetDateTime` equality is by instant, so the UTC
conversion in rows 10–11 is not a difference. **Three** normalisations exist, each documented and
each individually tested:

1. a `deduplication_id` equal to the message id decodes as `None` (ADR 0026 §5);
2. a custom `traceparent`/`tracestate`, **in any casing**, returns in `metadata.trace` and is absent
   from the decoded `Headers` — its original spelling is not preserved (ADR 0026 Amendment A);
3. an envelope carrying `Some(<empty Headers>)` decodes as `None`, because absence of a custom
   header is the only thing the wire can express and `None` is its canonical form. This one is a
   **core** artefact, not a mapper one: `Envelope::headers_mut()` allocates `Some(empty)`, which is
   tracked as **RELIAR-36**. The **interim mapper rule is: no mapper change** — `encode` writes no
   header for an empty map and `decode` maps absence to `None`, both of which are already right.
   The obligation is on the tests: the round-trip generator must not produce `Some(empty)`, or the
   assertion must compare an empty `Headers` as equal to `None`. When RELIAR-36 makes `Some(empty)`
   unrepresentable in core, this normalisation disappears with no change to this crate and no change
   to the wire — so it is not a semver-visible property of the mapper.

### 2.6 `NatsMapError`

```rust
/// Why an envelope could not be expressed as a NATS message, or a NATS message as an envelope.
/// Every variant is **permanent** (ADR 0030): the same bytes fail identically on every retry.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NatsMapError {
    /// A required framework header was absent (§2.5).
    MissingHeader { header: &'static str },
    /// A framework header was present but could not be parsed (bad UUID, bad RFC 3339, an
    /// over-length `CorrelationId`/`EndpointAddress`, a `content_type` `ContentType::parse`
    /// rejects). The **value is never included** (§17.1).
    MalformedHeader { header: &'static str },
    /// A framework header appeared more than once, or under two casings (§2.5).
    DuplicateHeader { header: &'static str },
    /// A custom header key NATS cannot express: not ASCII-graphic, or containing `:` (§2.4).
    UnsupportedHeaderName { key: String },
    /// A custom header key inside NATS's reserved `Nats-` namespace (§2.4).
    ReservedHeaderName { key: String },
    /// A value carried `\r` or `\n` — the header-injection surface core's unvalidated `String`
    /// fields leave open (§2.4). Names the header, **never** the value: a `headers::*` constant
    /// for a framework field, the caller's key for a custom one (ADR 0026 Amendment B).
    InvalidHeaderValue { header: String },
    /// `Headers::insert` refused a decoded custom header (over-length, over-count).
    RejectedHeader { key: String, source: reliar_core::HeaderError },
}
```

`Display` names the failure and the header/key; `source()` returns the `HeaderError` for
`RejectedHeader` and `None` elsewhere. A key is user-chosen configuration, not payload, so printing
it is intended; a *value* is data and is never printed.

---

## 3. Subject resolution (ADR 0027)

```rust
/// Chooses the NATS subject an envelope is published to. **Pure and synchronous** — resolution is
/// a function of the envelope, which is what makes a failure permanently classifiable (ADR 0027).
/// A resolver that needs a lookup table builds it at construction; it must never perform I/O.
pub trait SubjectResolver: Send + Sync {
    /// Why this resolver rejected an envelope.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Resolves the subject.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` when the envelope cannot be routed — the publisher turns it into a
    /// **permanent** `NatsPublishError::Subject`, so the outbox row dead-letters instead of
    /// retrying (ADR 0030).
    fn subject(&self, envelope: &reliar_core::SerializedEnvelope)
        -> Result<async_nats::Subject, Self::Error>;
}

/// The default: `<prefix>.<message_type>` — `reliar.orders.created.v1` for prefix `"reliar"`,
/// using `MessageType`'s `Display` (a documented public contract, ADR 0010). Ignores
/// `RoutingMetadata.destination` (ADR 0027 §5).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct PrefixSubjects { /* private: prefix */ }

impl PrefixSubjects {
    /// The prefix used when `NatsSettings::subject_prefix` is left at its default.
    pub const DEFAULT_PREFIX: &'static str = "reliar";

    /// Validates the prefix as one or more subject tokens (§3.1).
    ///
    /// # Errors
    /// [`SubjectError`] for an empty, wildcard-bearing or otherwise illegal prefix.
    pub fn new(prefix: impl Into<String>) -> Result<Self, SubjectError>;

    /// The configured prefix.
    #[must_use] pub fn prefix(&self) -> &str;
}

impl Default for PrefixSubjects { /* DEFAULT_PREFIX — cannot fail */ }
impl SubjectResolver for PrefixSubjects { type Error = SubjectError; … }

/// Opt-in: `RoutingMetadata.destination` verbatim when set, else the wrapped `PrefixSubjects`
/// (ADR 0027 §5). For applications that already decided routing per message.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct DestinationSubjects { /* private: fallback: PrefixSubjects */ }

impl DestinationSubjects {
    #[must_use] pub fn new(fallback: PrefixSubjects) -> Self;
}
impl SubjectResolver for DestinationSubjects { type Error = SubjectError; … }
```

### 3.1 Subject validation (both resolvers, on the resolved subject)

Rejected: empty; any empty token (a leading, trailing or doubled `.`); whitespace or a control
character; a `*` or `>` token or character (a wildcard publish is a silent mis-route, not an error
the server reports); any byte outside ASCII `0x21..=0x7E`; a total length over
`SubjectError::MAX_LEN = 255`.

```rust
/// Why an envelope could not be turned into a legal NATS subject.
/// Always **permanent** — the same envelope resolves the same way every time.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubjectError {
    /// The resolved subject, or the configured prefix, was empty.
    Empty,
    /// A token between two `.`s was empty (`a..b`, `.a`, `a.`).
    EmptyToken { subject: String },
    /// A wildcard (`*` or `>`) would publish to a subject the caller did not choose.
    Wildcard { subject: String },
    /// Whitespace, a control character, or a non-printable-ASCII byte.
    IllegalCharacter { subject: String },
    /// Over [`SubjectError::MAX_LEN`] bytes.
    TooLong { len: usize, limit: usize },
}
```

A subject is routing configuration, not user data, so including it in `Display` is intended and is
what makes a dead row actionable (ADR 0030).

---

## 4. `NatsPublisher`

### 4.1 Construction and settings

```rust
/// An at-least-once `Publisher` over JetStream: encodes with [`NatsEnvelopeMapper`], resolves the
/// subject with `R`, publishes, and **awaits the server ack** before returning `Ok` (ADR 0028).
///
/// # Guarantees
///
/// - `Ok` means the stream holds the message — never merely "written to a socket".
/// - `Nats-Msg-Id` lets JetStream suppress a duplicate republished inside the stream's
///   `duplicate_window`; outside it the duplicate is stored. This narrows SRS §22's duplicate
///   window; it does not close it, and this type makes no exactly-once claim.
/// - It never creates a stream, never connects, and never reads the environment (ADR 0029).
#[derive(Clone)]                  // Debug is MANUAL — see below
pub struct NatsPublisher<R = PrefixSubjects> { /* private: context, mapper, resolver, settings */ }

impl NatsPublisher<PrefixSubjects> {
    /// Wraps an application-owned JetStream context, resolving subjects with a
    /// [`PrefixSubjects`] built from `settings.subject_prefix`.
    ///
    /// # Errors
    /// [`NatsConfigError`] for a zero `batch_pipeline_depth`, a zero `publish_timeout`, a
    /// `max_payload` of `Some(0)`, or a `subject_prefix` that is not a legal subject prefix.
    pub fn new(context: async_nats::jetstream::Context, settings: NatsSettings)
        -> Result<Self, NatsConfigError>;
}

impl<R: SubjectResolver> NatsPublisher<R> {
    /// Same, with an explicit resolver. `settings.subject_prefix` is then **unused** — `R` owns
    /// subject selection entirely.
    ///
    /// # Errors
    /// [`NatsConfigError`] for a zero `batch_pipeline_depth`, a zero `publish_timeout`, or a
    /// `max_payload` of `Some(0)`.
    pub fn with_resolver(context: async_nats::jetstream::Context, settings: NatsSettings, resolver: R)
        -> Result<Self, NatsConfigError>;

    /// The settings in force.
    #[must_use] pub fn settings(&self) -> &NatsSettings;
}

/// A publisher configuration that cannot be started — **never a panic**, mirroring
/// `reliar-outbox`'s `ConfigError` role for the dispatcher (a code span, not a link: this crate
/// no longer depends on that crate — ADR 0032).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NatsConfigError {
    /// `batch_pipeline_depth` was zero: `publish_batch` could never send anything.
    ZeroBatchPipelineDepth,
    /// `publish_timeout` was zero: every publish would time out immediately.
    ZeroPublishTimeout,
    /// `max_payload` was `Some(0)`: **every** message — including one with an empty body — exceeds
    /// a zero limit, so §4.2's step-3 guard would turn the whole outbox into dead rows without a
    /// single byte leaving the process. The one payload limit that is unusable for every possible
    /// envelope is therefore a construction error, not a runtime verdict (ADR 0030 Amendment A).
    ZeroMaxPayload,
    /// `subject_prefix` is not a legal subject prefix.
    Subject(SubjectError),
}
```

```rust
/// Publisher settings. `Default` + `const` builder methods + an **opt-in** `from_env`
/// (ADR 0019) — the library never reads the environment on its own (SRS §7.2).
///
/// There is no server URL and no credentials here **by design**: the application builds the
/// connection and the JetStream context and keeps ownership of both (ADR 0029).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct NatsSettings {
    /// Prefix for the default [`PrefixSubjects`]. Default `"reliar"`. Ignored when a resolver is
    /// supplied through [`NatsPublisher::with_resolver`].
    pub subject_prefix: String,
    /// **Upper** bound on one publish — send **and** ack together. Default 10 s. Exceeded ⇒ a
    /// transient `Timeout`. In `publish_batch` it bounds one window (ADR 0028 §3).
    ///
    /// The effective ack deadline is `min(publish_timeout, Context::timeout)`. `Context::timeout`
    /// (async-nats default **5 s**) is applied by async-nats to every JetStream ack await and
    /// belongs to the application, not to Reliar — so with default settings on both sides the
    /// deadline that fires is the host's, and raising this setting above it has no effect. A host
    /// that wants the full window builds its context with
    /// `async_nats::jetstream::ContextBuilder::timeout`. Whichever bound fires, the resulting
    /// `Timeout { after_ms }` reports the **measured** elapsed time, not this setting
    /// (ADR 0028 Amendment A; RELIAR-38 owns whether Reliar should validate or derive the pair).
    pub publish_timeout: Duration,
    /// How many publishes `publish_batch` pipelines before awaiting their acks. Default 64.
    ///
    /// **Reached only through [`Publisher::publish_batch`].** v0.1's `OutboxDispatcher` calls
    /// `publish` once per claimed row, so on the only wiring Reliar ships today this setting is
    /// inert: it takes effect for a third-party caller that batches, and for the messaging layer
    /// of SRS §36 when it lands. RELIAR-39 tracks giving the dispatcher a batch path (§4.2).
    ///
    /// Keep it **at or below the host context's `max_ack_inflight`** (async-nats default 5000),
    /// which caps the acks one `Context` may have outstanding. Above that cap the host's
    /// `backpressure_on_inflight` decides the failure mode: `true` — **async-nats' default**, for
    /// both `jetstream::new(client)` and `ContextBuilder::default()` (verified in 0.50
    /// `jetstream/context.rs:186`, `Context::new` builds from that default) — makes each excess
    /// send **wait** for a permit that only an awaited or dropped ack releases, and §4.2's window
    /// issues every send before awaiting any ack, so the window then stalls until
    /// `publish_timeout` and its whole remainder fails `Timeout`. `false` — which a host opts into
    /// with `ContextBuilder::backpressure_on_inflight(false)` — instead fails each excess send
    /// immediately with a transient `MaxAckPending`, which the dispatcher retries. **The stalling
    /// mode is the one a host gets by default**, which is why this setting is documented as a cap
    /// to stay under rather than a knob to raise. Reliar cannot validate it: `Context` exposes no
    /// getter for the cap (verified against async-nats 0.50), so the constraint is documented and
    /// the default 64 sits far below the default cap (ADR 0028 Amendment A).
    pub batch_pipeline_depth: usize,
    /// The server's `max_payload`, when the host chooses to declare it: an oversized message is
    /// rejected locally as a permanent `PayloadTooLarge` against **this** limit, which is only
    /// useful when it is **below** the server's own. async-nats already rejects an oversized
    /// payload locally, before any I/O, against the `max_payload` the server advertised at
    /// connect (`Client::check_payload_size`, called at the top of `Context::send_publish` in
    /// 0.50) — so this guard buys a *lower* ceiling and an error type Reliar owns, never "a
    /// round-trip saved". Default `None` — Reliar does not guess a server limit (ADR 0030;
    /// RELIAR-37 derives the real limit from the connected server instead).
    ///
    /// `Some(0)` is rejected at construction as [`NatsConfigError::ZeroMaxPayload`]. A merely
    /// *small* limit is **documented, not validated**: any value below the framework header block
    /// (the `NATS/1.0` line, the four required `reliar-*` headers and the terminator — on the order
    /// of 150 bytes before a single payload byte) also dead-letters everything, but the exact floor
    /// depends on the message-type name and the metadata present, and pinning a numeric floor into
    /// this field would pin `encode`'s byte formatting into its semver contract (ADR 0030
    /// Amendment A).
    pub max_payload: Option<usize>,
}

impl Default for NatsSettings { /* "reliar", 10 s, 64, None */ }

impl NatsSettings {
    pub fn subject_prefix(self, prefix: impl Into<String>) -> Self;
    pub const fn publish_timeout(self, timeout: Duration) -> Self;
    pub const fn batch_pipeline_depth(self, batch_pipeline_depth: usize) -> Self;
    pub const fn max_payload(self, max_payload: Option<usize>) -> Self;

    /// Opt-in. Starts from [`Self::default`], overrides **only** the variables present under
    /// `prefix`, and returns `Err` for a present-but-unparseable or out-of-range value — never a
    /// silent fallback. Keys: `{prefix}SUBJECT_PREFIX`, `{prefix}PUBLISH_TIMEOUT_MS`,
    /// `{prefix}BATCH_PIPELINE_DEPTH`, `{prefix}MAX_PAYLOAD_BYTES`. Conventional prefix:
    /// `"RELIAR_NATS_"`. **No `URL` key exists** (ADR 0029 §1).
    ///
    /// # Errors
    /// [`reliar_core::SettingsError`] — `Parse` for an unparseable value, `OutOfRange` for a
    /// zero `BATCH_PIPELINE_DEPTH`/`PUBLISH_TIMEOUT_MS`/`MAX_PAYLOAD_BYTES` or an unusable
    /// `SUBJECT_PREFIX`.
    pub fn from_env(prefix: &str) -> Result<Self, reliar_core::SettingsError>;
}
```

**Renamed 2026-09-05 (ADR 0028 Amendment B): `max_in_flight` → `batch_pipeline_depth`.** The old
name collided in *meaning* with `DispatcherSettings::max_in_flight`, which bounds how many
publishes the outbox dispatcher runs concurrently. This one bounds how many sends
`publish_batch` issues before it starts awaiting acks — a pipeline depth inside a single call,
not a concurrency budget across calls. Two settings that a host sets in the same file, that read
identically and mean different things, are a configuration hazard; the crate is unreleased and
pre-1.0, so the rename lands now. It renames, in one change:

| Was | Is |
|---|---|
| `NatsSettings::max_in_flight` (field) | `NatsSettings::batch_pipeline_depth` |
| `NatsSettings::max_in_flight(…)` (builder) | `NatsSettings::batch_pipeline_depth(…)` |
| `{prefix}MAX_IN_FLIGHT` (env key) | `{prefix}BATCH_PIPELINE_DEPTH` — conventionally `RELIAR_NATS_BATCH_PIPELINE_DEPTH` |
| `NatsConfigError::ZeroInFlight` | `NatsConfigError::ZeroBatchPipelineDepth` (§4.1's enum) |

`batch_window` was considered and rejected: "window" is already this contract's word for the unit
of work (`publish_timeout` bounds *one window*), and a name ending in `_window` next to a
`Duration`-typed neighbour reads as a duration. `acks_in_flight` keeps the very phrase that
collides. `batch_pipeline_depth` is unambiguously a count, names the mechanism, and shares no
substring with the dispatcher setting.

`Debug` on `NatsPublisher` is **manual**: it prints the `settings` and the resolver and
**never the `async_nats::jetstream::Context`**, whose own `Debug` belongs to `async-nats` and may
render the server address — a credentialed `nats://user:pass@host` is the exact thing §17.1 keeps
out of logs. `Clone` is derived, so `NatsPublisher<R>` is `Clone` when `R` is; the dispatcher
requires `Publisher + Clone + 'static`, so a custom resolver is `Clone` in practice — which is why
`SubjectResolver` does **not** add `Clone`/`Debug` supertraits it does not need
(`async_nats::jetstream::Context` is itself `Clone + Send + Sync + 'static`, verified against 0.50).

`SettingsError` is **reused from `reliar-core`** and re-exported (`pub use
reliar_core::SettingsError;`) rather than duplicated, so a host configuring Reliar from the
environment matches one error type, not two. It lived in `reliar-outbox` until ADR 0032 moved it to
core — being the second of only two things this crate imported from the outbox is what made that
dependency worth removing.

### 4.2 `Publisher` impl

```rust
impl<R: SubjectResolver> reliar_core::Publisher for NatsPublisher<R> {
    type Error = NatsPublishError;

    fn publish(&self, envelope: &SerializedEnvelope)
        -> impl Future<Output = Result<(), NatsPublishError>> + Send;

    fn publish_batch(&self, envelopes: &[SerializedEnvelope])
        -> impl Future<Output = Vec<Result<(), NatsPublishError>>> + Send;   // overridden
}
```

`publish`, in order:

1. `subject = self.resolver.subject(envelope)?` → `Subject { source }` on failure (permanent).
2. `wire = self.mapper.encode(envelope)?` → `Map(..)` on failure (permanent).
3. if `settings.max_payload == Some(limit)` and `wire.wire_len() > limit` →
   `PayloadTooLarge { len, limit }` (permanent), **before** any I/O.
4. `tokio::time::timeout(settings.publish_timeout, async { let ack = ctx.publish_with_headers(subject, headers, payload).await?; ack.await })`
   — both stages inside one timeout. Elapsed → `Timeout { after_ms }` (transient), where
   `after_ms` is the **measured** elapsed time from an `Instant` taken before step 4 and never the
   configured `publish_timeout`: async-nats applies its own `Context::timeout` (default 5 s) to
   every ack await, so with default settings the deadline that actually fires is the host's and
   arrives as a mapped `PublishErrorKind::TimedOut`. Both routes produce the same variant, and both
   report what really elapsed (ADR 0028 Amendment A, §4.1).
5. `Ok(())` only after the ack. The ack's `duplicate` and `sequence` are recorded on the span
   (§4.4) and then dropped: JetStream's dedup verdict is diagnostics, not a guarantee.

`publish_batch` (ADR 0028 §3) — **not reached by v0.1's dispatcher.** `OutboxDispatcher` calls
`publish` once per claimed row (`reliar-outbox`'s publisher call site), so this override and
`NatsSettings::batch_pipeline_depth` take effect only for a third-party caller that batches, and
for SRS §36's messaging layer when it lands. It is specified, implemented and tested now because
the trait shape is semver-visible and SRS §19.4 requires a transport with a native batch API to
override the default loop; RELIAR-39 tracks whether the dispatcher grows a batch path. The
reachability caveat is repeated in the rustdoc on `publish_batch` and on
`batch_pipeline_depth` (§4.1) so a host cannot tune a setting that does nothing for it.

The result vector is always `envelopes.len()` long and positional.
Steps 1–3 run for every envelope first; failures are recorded in place and those envelopes are not
sent. The rest are processed in windows of `settings.batch_pipeline_depth`: every send in the window is
issued before any ack in that window is awaited, then each ack is awaited and recorded at its own
index. One `publish_timeout` bounds one window; on elapse, every not-yet-acked envelope **in that
window** gets `Timeout` and later windows still run. A failing ack never affects its neighbours'
verdicts. No ordering is promised beyond "sends are issued in slice order on one connection"
(SRS §22.2, ADR 0013).

Cancellation: both futures are cancel-safe in the sense the dispatcher needs — dropping one stops
awaiting the ack, releases async-nats' ack permit with the dropped `PublishAckFuture`, and leaks no
task (nothing is `spawn`ed). A dropped `publish` whose bytes already reached the server is exactly
SRS §22's duplicate window. Dropping `publish_batch` mid-window yields **no** results at all — the
future's output is the whole vector — so the caller learns nothing about the sends in flight and
every message in that window is in the same duplicate window.

The statement below is **normative and copied verbatim** onto `NatsPublisher` (review M3): a
paraphrase of a duplicate-window guarantee is how an exactly-once claim gets born.

```rust
/// # Cancellation
///
/// Dropping a [`publish`](reliar_core::Publisher::publish) or
/// [`publish_batch`](reliar_core::Publisher::publish_batch) future — a cancelled dispatcher, a
/// drain deadline — stops this process awaiting the ack. It does **not** unsend bytes already on
/// the wire: the stream may store the message while Reliar records no outcome, so the outbox row
/// stays claimable and the message is published again. That is SRS §22's duplicate window;
/// `Nats-Msg-Id` lets JetStream collapse the repeat *inside* the stream's `duplicate_window`, and
/// nothing collapses it outside. This type is **at-least-once** and makes no exactly-once claim.
```

Test ids **N8** (drop `publish` mid-ack) and **N9** (drop `publish_batch` mid-window) prove the
behaviour rather than the wording — §7.

### 4.3 `NatsPublishError` and `Classify` (ADR 0030 — normative table)

```rust
/// Why a publish failed. Every variant's `Classify` verdict is fixed and asserted by test.
/// No `Display` here prints payload bytes, a header value, or a server address (§17.1).
#[derive(Debug)]
#[non_exhaustive]
pub enum NatsPublishError {
    Map(NatsMapError),                                                    // Permanent
    Subject { source: Box<dyn std::error::Error + Send + Sync> },         // Permanent
    PayloadTooLarge { len: usize, limit: usize },                         // Permanent
    MaxPayloadExceeded { subject: async_nats::Subject },                  // Permanent
    WrongLastMessage { subject: async_nats::Subject },                    // Permanent
    Timeout { subject: async_nats::Subject, after_ms: u64 },              // Transient, measured (§4.2)
    Connection { subject: async_nats::Subject },                          // Transient
    StreamNotFound { subject: async_nats::Subject },                      // Transient
    MaxAckPending { subject: async_nats::Subject },                       // Transient
    Broker { subject: async_nats::Subject },                              // Transient + `warn`
}

impl reliar_core::Classify for NatsPublishError {
    fn kind(&self) -> reliar_core::FailureKind { /* exactly the table above */ }
}
```

Mapping from `async_nats::jetstream::context::PublishErrorKind`:
`MaxPayloadExceeded → MaxPayloadExceeded` · `WrongLastMessageId | WrongLastSequence →
WrongLastMessage` · `TimedOut → Timeout` · `BrokenPipe → Connection` · `StreamNotFound →
StreamNotFound` · `MaxAckPending → MaxAckPending` · `Other` **and anything unrecognised** `→ Broker`,
logged once at `warn` with the **kind name only** — a `&'static str` from this crate's total match
over `PublishErrorKind` (`"other"`, or `"unrecognised"` for a kind this crate does not know) — plus
the subject. The `async-nats` error's own `Display` is **never** logged, never persisted and never
rendered anywhere in this crate: it can contain `nats://user:pass@host`, and an invariant that
holds "except in one `warn`" is not an invariant U13 can prove. This **replaces** the earlier
"logged with the `async-nats` `Display`" rule and resolves the contradiction the round-2 review
raised as m7 (ADR 0030 Amendment B).

`source()` is wired for `Map` and `Subject` — Reliar's own errors, safe to persist. `Connection`,
`Broker` and the other broker-side variants deliberately expose **no** `source()`: an `async-nats`
error's `Display` can carry `nats://user:pass@host`, and `last_error` is persisted forever (§17.1).
The `async-nats` error is **dropped**, not stored and not logged; what survives is the variant, the
subject, and the kind name in the `warn`. Diagnosing a `Broker` failure therefore needs the
*server's* log, which is the cost §17.1 is worth (ADR 0030 Amendment B).

### 4.4 Observability (SRS §33, skill `observability`)

| Span / event | Level | Fields |
|---|---|---|
| `reliar.transport_nats.publish` | `debug` | `message.id`, `message.type`, `subject`; on success `jetstream.sequence`, `jetstream.duplicate` |
| `reliar.transport_nats.publish_batch` | `info` | `batch.size`, `windows` |
| `PublishErrorKind::Other` or an unrecognised kind | `warn` | `subject`, `error.kind` (a bounded `&'static str`) — **never** the `async-nats` `Display` (ADR 0030 Amendment B) |

`prepare()` (§4.2 — resolve the subject, encode the wire message, check `max_payload`) runs
**before** the `reliar.transport_nats.publish` span is opened, so a permanent pre-flight failure
emits no span from this crate at all. That is deliberate, not an omission: the failure is returned
to the dispatcher, whose own span already carries `message.id`, `message.type` and the outcome, and
a span whose whole body is "we never touched the broker" would add a second record of one event.

`publish` is `debug` deliberately: it is a child of the dispatcher's `info` `reliar.outbox.publish`
span (one per message already), so an `info` span here would double span volume and add no id the
parent lacks. No payload, no header value, and no metric labels are emitted by this crate — it
exposes no metrics hook of its own; the dispatcher's `OutboxMetrics` already counts publishes.

---

## 5. Composition (what a host writes)

```rust
let client = async_nats::connect(&std::env::var("NATS_URL")?).await?;   // the APP reads the env
let js = async_nats::jetstream::new(client);
// The stream is created by the application or the operator — never by Reliar (ADR 0029).

let publisher = NatsPublisher::new(js, NatsSettings::default().subject_prefix("app"))?;
let store = PostgresOutboxStore::new(pool.clone());
let dispatcher = OutboxDispatcher::builder(store, publisher)
    .settings(OutboxSettings::default())
    .build()?;
dispatcher.run(cancel).await?;
```

---

## 6. Slices — the concrete work list

| Slice | Card | Deliverable |
|---|---|---|
| S0 | RELIAR-31 | **done by the architect**: `async-nats` pin, compose `nats`, CI NATS + `NATS_URL`, pin-equality gate (ADR 0031) |
| S1 | RELIAR-32 | the crate skeleton + §2 mapper (+ `headers` consts, `NatsMapError`) + §3 resolvers + §4.1 settings, with §7's `unit` matrix |
| S2 | RELIAR-33 | §4.2–§4.4 publisher, `NatsPublishError`/`Classify`, the one-binary NATS harness, §7's `nats` matrix |
| S3 | RELIAR-34 | `tests/system` package + C9 e2e, `examples/nats-pub-sub`, `docs/guides/nats.md`, `CHANGELOG` |

**All four slices have shipped as of this rebase** (2026-09-05); the table stays as the record of
how the work was cut. Both root-`Cargo.toml` changes the slices needed are in: the
`reliar-transport-nats` `[workspace.dependencies]` entry (S1) and `"tests/*"` in
`[workspace] members` (S3, ADR 0031 §6).

Follow-on work is tracked outside this contract: **RELIAR-36** (core's `Some(<empty Headers>)`),
**RELIAR-37** (derive `max_payload` from the connected server), **RELIAR-38** (`publish_timeout`
versus `Context::timeout`), **RELIAR-39** (a dispatcher batch path so `publish_batch` is reachable),
**RELIAR-40** (ADR 0032's relocation in `crates/**`). None of them changes a signature in this
file; each is named at the point it applies.

---

## 7. Test matrix (the reviewer audits against this)

House rules: no inline `#[cfg(test)]`; tests exercise the **public API** from `tests/`; shared
helpers in `tests/common/`; deterministic, no wall-clock sleeps.

**`unit` — no server needed.** Shipped files: `tests/mapper_roundtrip.rs`, `tests/mapper_decode_errors.rs`, `tests/mapper_encode_errors.rs`, `tests/mapper_framework_collision.rs`, `tests/mapper_wire_len.rs`, `tests/subjects.rs`, `tests/publish_error_classification.rs`, `tests/errors_never_print_payload_or_header_values.rs`, `tests/settings_defaults_and_builder.rs`, `tests/settings_from_env_overrides_present_only.rs`, `tests/settings_from_env_rejects_bad_values.rs`, and `tests/common/`.

| Id | Scenario | AC |
|---|---|---|
| U1 | proptest: `decode(encode(e)) == e` over ids/metadata/custom headers/body, generator restricted to §2.5's **canonical key class** (defined there) so the identity is exact rather than special-cased | C1 |
| U2 | every canonical field appears **exactly once** as a header, and never inside decoded `Headers`; `traceparent`/`tracestate` unprefixed | C1 |
| U3 | `Nats-Msg-Id == reliar-message-id` with no `deduplication_id`; equals the dedup id when set; payload bytes are the body unchanged (no outer JSON) | C2 |
| U4 | §2.5's documented normalisations: dedup-id-equals-message-id, and a custom `traceparent`/`tracestate` (any casing) returning in `metadata.trace`. The third — `Some(<empty Headers>)` decoding as `None` — is a **core** artefact (RELIAR-36) and is absorbed by the generator never emitting `Some(empty)`, not by a mapper special case | C1 |
| U5 | decode: missing/malformed `reliar-message-id`, `-message-type`, `-message-version`, `-content-type` → the named permanent error; **no panic** | C3 |
| U6 | decode: unknown `reliar-*` ignored · other `Nats-*` ignored · duplicate/two-cased framework header → `DuplicateHeader` · custom multi-value takes the first | C3 |
| U7 | encode: a custom key with a space/`:`/non-ASCII → `UnsupportedHeaderName`; a `Nats-`-prefixed one → `ReservedHeaderName`; `\r\n` in `tenant_id` or a `MessageType` name → `InvalidHeaderValue` — all permanent, none panicking | C3, C8 |
| U8 | RFC 3339 round-trip for `sent_at`/`expires_at`, including a non-UTC offset and sub-second digits | C1 |
| U9 | `PrefixSubjects` yields `<prefix>.<message_type>`; each §3.1 rejection has a case; `DestinationSubjects` prefers `destination` and falls back | C4 |
| U11 | `NatsSettings::from_env` — each key, an unparseable value, a zero `BATCH_PIPELINE_DEPTH`; **absent variables change nothing** (`tests/settings_from_env_overrides_present_only.rs`, `tests/settings_from_env_rejects_bad_values.rs`) | non-functional |
| U13 | recording subscriber: no span field, event, `Debug` or `Display` this crate produces contains payload bytes, a header value, or `nats://user:pass@…` — including the `Broker` variant's own `Display` and every `NatsMapError`. The invariant is **absolute**: ADR 0030 Amendment B removed the single exception (the `warn` that logged the `async-nats` `Display`), which is what makes this row provable instead of aspirational. The parts needing a live `Context` — `NatsPublisher`'s `Debug` and the `warn` event itself — are **N10** | C8, §43.A.26 |
| U14 | the case-insensitive framework collision (§2.4, ADR 0026 Amendment A): a custom `TraceParent` with `metadata.trace.traceparent` set → **one** header, the metadata value, and `decode(encode(e))` succeeds (no `DuplicateHeader`); with it unset → one header under the canonical lowercase name, decoding into `metadata.trace`; two custom keys differing only in case with `metadata.trace` unset → `DuplicateHeader`; a case-variant framework header from a foreign producer decodes into `Metadata`, not `Headers` | C1, C3 |
| U15 | `NatsWireMessage::wire_len` is byte-exact against what the server counts: the `"NATS/1.0\r\n"` line, `"{name}: {value}\r\n"` per header (multi-value headers counted once per value) and the trailing `"\r\n"`, plus `payload.len()`; asserted for zero headers, one header, several headers and an empty payload. It decides S2's permanent `PayloadTooLarge` verdict, so an off-by-`n` here silently dead-letters publishable messages | C8, §4.2 |
| U16 | `Classify` for **all ten** `NatsPublishError` variants, each constructed directly in a pure test (`tests/publish_error_classification.rs`) and asserted against §4.3's table; flipping any one verdict must fail this test (review B1) | C8 |
| U17 | decode rejects an **empty** `reliar-message-type` with `MalformedHeader` — permanent, no panic — and accepts a non-empty foreign name verbatim; empty `reliar-message-id`/`-message-version`/`-content-type` are `MalformedHeader` through their own parse (ADR 0026 Amendment C) | C3 |
| U18 | `NatsSettings::default` matches §4.1's documented values (`"reliar"`, 10 s, 64, `None`), each builder method sets exactly the field it names, and **no** constructor, `Default` or builder reads the environment — only `from_env` does (SRS §7.2, ADR 0019). `tests/settings_defaults_and_builder.rs` | non-functional |

**Retired ids.** **U10** (a resolver honoured end-to-end) and **U12** (`NatsConfigError` from the constructors) are gone from this table: both need a live `Context`, so neither was ever a unit test. U10's claim is subsumed by **N6**, which asserts the stronger fact — the subject the resolver produced is the subject the stream received; U12 moved wholesale to **N7**. The ids are kept resolvable here because the shipped files still cite them (`tests/subjects.rs`, `tests/nats/n6_custom_resolver_subject.rs`, `tests/nats/n7_config_validation.rs`).

**`nats` — one binary, one server (`tests/nats/main.rs`, `harness = false` + `libtest-mimic`)**

Harness rules (RELIAR-27's lesson, ADR 0031 §4): `NATS_URL` when set, otherwise **one**
`GenericImage` held as a **local** in `main` and dropped before `main` returns an `ExitCode` (never
a `static`, never `Conclusion::exit()`). Consts the CI pin gate reads:

```rust
const NATS_IMAGE: &str = "nats";
const NATS_TAG: &str = "2.14-alpine";
```

started with `-js -m 8222`. Readiness is a **functional** probe, per ADR 0031 Amendment A: retry
`connect()` **and** a JetStream API call (a stream create/lookup) a bounded number of times with a
short delay, treating a failed first `connect()` as one more retryable attempt rather than a fatal
error. No log line and no `/jsz` poll is the harness's readiness signal — `WaitFor` on "Server is
ready" races nats-server, which prints it before the JetStream API accepts requests. (The
`/jsz?config=1` → `store_dir` probe stays correct where it is used from *outside* the harness:
compose's healthcheck and `test.yaml`'s `docker run` step.) **Every scenario
creates its own stream** (`RELIAR_TEST_<uuid>` over `reliar.test.<uuid>.>`) and deletes it at the
end — the CI server is shared across binaries and nothing may assume an empty server.

| Id | Scenario | AC |
|---|---|---|
| N1 | `publish` → exactly one stored message with the expected headers and raw body | C5 |
| N2 | `publish` genuinely **awaits** the ack rather than writing to a socket — proven against a `no_ack: true` stream the server never acks (an implementation that awaits times out; one that does not returns `Ok`). A same-connection message-count check cannot distinguish the two: the server's own ordering makes the write visible to a later query either way (`tests/nats/n1_publish_awaits_the_ack.rs`, with N1) | C5 |
| N3 | the same envelope twice inside a stream `duplicate_window` → one stored message | C6 |
| N4 | `publish_batch`: positional results, one bad envelope (unroutable or unencodable) leaves its neighbours `Ok`, `batch_pipeline_depth` smaller than the batch exercises >1 window | C7 |
| N5 | classification: no stream for the subject → `StreamNotFound`/Transient; server stopped mid-run → `Connection`/Transient; an oversized payload → permanent (`PayloadTooLarge` with `max_payload` set, `MaxPayloadExceeded` without). **Not every arm of the `PublishErrorKind` map is reachable from a test**: `PublishError` is not constructible outside async-nats, and `MaxAckPending` cannot occur at all with a default `Context` (`backpressure_on_inflight` defaults to `true`, so the excess send waits instead — ADR 0028 Amendment A as corrected). U16 asserts the verdicts of all ten variants directly for exactly this reason; the gap belongs in this file's test module docs, not in a silent absence (review M5) | C8 |
| N6 | a stream that captures the subject via a wildcard still receives the exact subject the resolver produced — this also covers withdrawn U10 (a custom resolver honoured end-to-end) | C4 |
| N7 | (was U12) `NatsConfigError` from `new`/`with_resolver` over a live `Context`: zero `batch_pipeline_depth` → `ZeroBatchPipelineDepth`, zero `publish_timeout` → `ZeroPublishTimeout`, `max_payload(Some(0))` → **`ZeroMaxPayload`**, a bad `subject_prefix` → `Subject`. Plus the reason that variant exists: with `max_payload(Some(1))` accepted at construction, every envelope fails `PayloadTooLarge` and nothing is ever stored | C8, non-functional |
| N8 | drop the `publish` future mid-ack (a `select!`/`timeout` that loses the race, or a paused ack): no panic, no leaked task, and the stream is asserted to hold **either zero or one** copy. Accepting both outcomes is the point — the test encodes SRS §22's duplicate window instead of a delivery guarantee (contract §4.2, review gap 1) | C5, §22 |
| N9 | drop `publish_batch` mid-window: no panic, no leaked task, and no result vector at all; the stream then holds some prefix of the batch, and republishing the same envelopes inside `duplicate_window` still yields one copy each | C7, §22 |
| N10 | credential hygiene with a **credentialed** connection: `format!("{publisher:?}")` contains neither the address nor the credentials (review M2), and the `Broker`/`Other` `warn` path — provoked by a server-side publish rejection, e.g. a stream with `max_msgs = 1, discard = new` published to twice — emits only `subject` + `error.kind`, with no `async-nats` `Display` in the event and none in the error's `Display` (ADR 0030 Amendment B) | C8, §43.A.26 |
| N11 | `publish_batch`'s per-window `Timeout` covers **every** envelope in **every** window, and a later window still runs after an earlier one's deadline has already elapsed — "one window timing out never stops the next window from starting" (ADR 0028 §3). A `no_ack` stream makes each window genuinely run out its own `publish_timeout` instead of racing a fast local round trip | C7, §4.2 |

**`e2e` — `tests/system/` (one Postgres **and** one NATS container per binary, both dropped at exit)**

| Id | Scenario | AC |
|---|---|---|
| E1 | enqueue N rows in a host transaction that commits → `OutboxDispatcher<PostgresOutboxStore, NatsPublisher>` runs against a real Postgres and a real stream → **all N rows** end `published_at` and in the stream with a matching `reliar-message-id` header and the identical raw body, and the cancel is clean: every lease released, no row dead-lettered. Cancelling only after the last row has published is what makes "all N" deterministic — an earlier cancel proves liveness but not completeness (`e1_outbox_drains_into_jetstream.rs`) | C9 |
| E2 | a publish failure (stream deleted mid-run) leaves the row retryable with `attempts` incremented, and it publishes after the stream returns | C9, C8 |
| E3 | §22's headline, end to end: a worker acquires a row, its `publish` is **acked**, and it never writes `complete` — the crash window itself. The lease is then expired by SQL time-travel (not a wall-clock wait), a real `OutboxDispatcher` reclaims the row and republishes it, and the stream still holds **one** copy because `Nats-Msg-Id` = the message id (ADR 0026 §5) makes the second publish a JetStream duplicate inside `duplicate_window`. Worker A is driven directly against `OutboxStore`/`Publisher` rather than through a dispatcher, so there is no race to win. Mutations that must fail it: dropping `Nats-Msg-Id` from the mapper, or deriving it from anything per-attempt, turns the count into 2; a `complete` guard that ignored `locked_by` would let the count stay 1 for the wrong reason — hence the pre-reclaim assertions that the stream already holds 1 and the row is *not* published. Distinct from `reliar-store-postgres`'s own crash-after-publish test, which has no broker and therefore asserts two publishes (`e3_crash_after_publish_dedupes_on_the_stream.rs`) | C6, C9, §22 |
| E4 | an envelope legal in `reliar-core` but permanently unrepresentable on this transport — a custom header key containing a space, which `Headers::insert` accepts and `async_nats::HeaderName` does not — fails as `NatsMapError::UnsupportedHeaderName` → `NatsPublishError::Map` → **Permanent**. The row dead-letters on attempt **1** with `dead_reason = PermanentError`, nothing reaches the stream, `last_error` names the mapping failure and never contains the header's *value*, and a later `acquire` excludes the row with `attempts` frozen at 1 — calling `acquire` directly is what makes "no future poll retries it" deterministic without waiting. Mutations that must fail it: classifying `Map` as Transient (attempts climbs, no dead row), formatting the offending header's value into `NatsMapError`'s `Display` (the secret-value assertion), or letting `acquire` return dead rows (§23, ADR 0026 §3) (`e4_unrepresentable_envelope_dead_letters.rs`) | C8, C9, §23 |

All four run from the `tests/system` package (ADR 0031 §6) — the only place a scenario may span
`reliar-store-postgres` and `reliar-transport-nats` together, since no provider crate may depend on
another. E3 and E4 were added by the PO's 2026-09-05 addendum to RELIAR-34.

**`doc`** — `examples/nats-pub-sub` compiles as a workspace member and creates its own stream;
`docs/guides/nats.md` covers stream ownership, the `duplicate_window` versus retry backoff, the
subject strategy, settings, and the fact that TLS/auth features come from the host's own
`async-nats` (C11).

---

## 8. Not in this contract

Phase 3 and later, and **not** to be built now (SRS §6: no crate or API for a future phase):
a subscriber/consumer API, `InboxStore` integration, ack/nak/term handling, request/reply (the NATS
`reply` subject is not written by this publisher), stream/consumer provisioning helpers, a
`metrics` feature for this crate, ordered or per-key publishing, and any RabbitMQ/Kafka mapping.
`decode` is specified and tested now precisely so Phase 3 inherits a frozen wire format.

No feature flags ship in Phase 2: there is nothing optional to gate, and an empty `[features]`
keeps `cargo hack --feature-powerset` trivial. A later `metrics` or `interop-decode` feature is
additive.

---

## 9. Decided here

Where the SRS was silent, this contract (and the ADR behind it) decides:

1. `reliar-message-type` carries the **name** and `reliar-message-version` the **version**, rather
   than one header holding `orders.created.v1`. §14 lists both names, and splitting is what makes
   decode unambiguous when a name itself ends in `.vN` (ADR 0026 §1).
2. Timestamps are **RFC 3339 in UTC** on the wire, unlike the store's epoch millis (ADR 0026).
3. Required on decode: id, type, version, content type. Everything else optional; unknown
   `reliar-*` ignored; malformed-but-present is permanent (ADR 0026 §4).
4. `Nats-Msg-Id` = `deduplication_id` else message id, with decode's difference rule and the
   documented normalisation (ADR 0026 §5).
5. Custom keys in the `Nats-` namespace are rejected at **encode**; encode order is custom-then-
   framework so framework values override, and that override is **case-insensitive** — encode emits
   at most one entry per framework name, in the canonical lowercase spelling
   (ADR 0026 §2–§3 + Amendment A).
6. `SubjectResolver` is **fallible** and **synchronous**, returns `async_nats::Subject`, and lives
   in this crate; `destination` participates only via `DestinationSubjects` (ADR 0027).
7. The mapper's transport type is a crate-local `NatsWireMessage` (headers + payload), **not**
   `async_nats::Message` — the subject is routing and must not be part of the mapping (ADR 0027).
8. `publish_batch` pipelines in `batch_pipeline_depth` windows and awaits acks positionally; the timeout
   bounds a window (ADR 0028 §3).
9. `NatsSettings` has **no URL/credentials**; `NatsPublisher::new` takes a `Context` and is
   fallible (`NatsConfigError`) (ADR 0029, §4.1).
10. `reliar_core::SettingsError` is reused and re-exported rather than duplicated (§4.1). It was
    `reliar_outbox::SettingsError` when this contract froze; ADR 0032 moved the type, not the rule.
11. Broker-side error variants expose **no `source()`**; the chain is logged, not persisted
    (ADR 0030).
12. `StreamNotFound` and unrecognised broker kinds are **transient**; the size/precondition/mapping
    failures are permanent (ADR 0030).
13. The e2e test lives in a new `tests/system` package, not in either provider crate (ADR 0031 §6).
14. `async-nats` is pinned `default-features = false, features = ["jetstream"]` — no crypto
    provider is forced on the host (ADR 0031 §1).
15. `InvalidHeaderValue` carries the header **name** as a runtime `String` so it can name a custom
    header too; a rejected custom *value* is not reported as an unsupported *name*
    (ADR 0026 Amendment B).
16. `Some(<empty Headers>)` decoding as `None` is a third documented normalisation, owned by core
    (RELIAR-36) and absorbed by the tests, not by a mapper special case (§2.5).
17. Decode rejects an **empty** `reliar-message-type` as `MalformedHeader`, and emptiness is the
    only name rule it enforces. The check lives in the mapper because `MessageType::from_parts` is
    core's deliberately unvalidated rehydration path (ADR 0026 Amendment C). No `reliar-core`
    change (§43.B).
18. `max_payload = Some(0)` is a **construction** error (`NatsConfigError::ZeroMaxPayload`, an
    additive variant on a `#[non_exhaustive]` enum). A merely small limit is documented, not
    validated; RELIAR-37 replaces the setting's guesswork with a server-derived limit
    (ADR 0030 Amendment A).
19. `publish_timeout` is an **upper** bound — the effective ack deadline is
    `min(publish_timeout, Context::timeout)` (async-nats default 5 s) — the default stays **10 s**,
    and `Timeout { after_ms }` reports **measured** elapsed time. RELIAR-38 owns whether to
    validate or derive the pair (ADR 0028 Amendment A).
20. `batch_pipeline_depth` must stay **≤** the host context's `max_ack_inflight`; Reliar documents the
    constraint because `Context` exposes no getter to validate against, and both failure modes are
    spelled out — the **stalled window** under async-nats' default `backpressure_on_inflight: true`,
    and the fast transient `MaxAckPending` only when a host opts out with `false`
    (ADR 0028 Amendment A, as corrected by Amendment B).
21. The `Broker`/unrecognised-kind `warn` carries the **kind name and the subject only**. The
    `async-nats` `Display` is never logged, so §17.1's "no credential anywhere" invariant has no
    exception left and U13 can prove it (ADR 0030 Amendment B).
22. `tokio` is a **runtime** dependency of this crate (`timeout` + `Instant` in §4.2), not a
    dev-dependency. It entered the graph via `reliar-outbox` when this was written; since ADR 0032
    removed that edge, `tokio` is declared directly in this crate's `[dependencies]` (it already
    was). No new third-party crate enters either way (ADR 0031 Amendment A). `tracing` is a runtime
    dependency for the same reason: §4.4's spans are production code.
23. The pipeline-depth setting is named **`batch_pipeline_depth`**, not `max_in_flight`, so it
    cannot be confused with `DispatcherSettings::max_in_flight`, which means something else
    (§4.1, ADR 0028 Amendment B). Env key `RELIAR_NATS_BATCH_PIPELINE_DEPTH`; the config error is
    `NatsConfigError::ZeroBatchPipelineDepth`.
24. `publish_batch` — and therefore `batch_pipeline_depth` — is **unreachable through v0.1's
    dispatcher**, which calls `publish` per row. Both are shipped because SRS §19.4 mandates the
    override and the trait shape is semver-visible; the rustdoc says so rather than leaving a host
    to tune a setting that does nothing (§4.1, §4.2; RELIAR-39).
25. `max_payload`'s pre-flight guard does **not** save a round-trip — async-nats already rejects an
    oversized payload locally against the server-advertised limit before any I/O. It buys a limit
    *below* the server's and a Reliar-owned error type; RELIAR-37 replaces it with a server-derived
    limit (§4.1, ADR 0030).
