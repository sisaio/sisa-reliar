# ADR 0026 — The NATS header projection and the decode policy

**Status:** Accepted — 2026-09-04 · **amended 2026-09-04** (§2, §3 and §4 — see [Amendments](#amendments))
**SRS:** §12, §12.3, §13.1, §14, §15, §16, §17.1, §45 ("metadata/header ownership", "transport mapping")
**Builds on:** [ADR 0004](0004-no-metadata-header-duplication.md) (metadata has one source of truth),
[ADR 0003](0003-canonical-envelope.md), [ADR 0011](0011-headers-and-envelope-construction.md)
**Related:** RELIAR-2 (Phase 2), RELIAR-30, contract `../architecture/phase2-contract.md` §2

## Context

Phase 2 turns SRS §15–§16 from a diagram into bytes on a wire. `NatsEnvelopeMapper` must project
the canonical `Envelope` onto NATS headers + a raw payload, and (for Phase 3) read it back. The SRS
fixes the *names* (§14's reserved list, and the deliberate exception that `traceparent`/`tracestate`
are unprefixed) and the *shape* (native headers + raw body, never an outer JSON wrapper). It fixes
neither the **value encoding** nor the **decode policy**, and both are semver-visible and
security-relevant:

- `async_nats::HeaderName`'s `IntoHeaderName for &str` **panics** on a name that is not
  ASCII-graphic-without-colon, and `IntoHeaderValue for &str` **panics** on `\r`/`\n`. A library
  that must never take a host process down cannot use those impls.
- `reliar_core::Headers` validates less than NATS does: it forbids control characters, the
  `reliar-` prefix, keys > 128 B, values > 1 KiB and more than 32 entries — but it *permits* a key
  with a space, a colon, or non-ASCII text, all of which NATS rejects. Some legal envelopes are
  therefore unrepresentable on this transport.
- `MessageType::name()` and `Metadata.tenant_id` are unvalidated `String`s in core; either can hold
  a `\r\n` and become a header-injection vector the moment it is written verbatim.
- Timestamps, ids and `ContentType` need one documented spelling, or an independent consumer cannot
  read a Reliar message and Phase 3 cannot round-trip it.
- §12.3 obliges the mapper to emit `deduplication_id`, falling back to `message_id`, as
  `Nats-Msg-Id` — which is *not* in §14's `reliar-*` list, so the dedup id has no header of its own
  and its round-trip has to be derived.

## Decision

**One header per canonical field, lowercase `reliar-*` names on encode, case-insensitive
recognition on decode, RFC 3339 timestamps, and a strict-but-forward-compatible decode.**

### 1. The projection (complete and exhaustive)

| Canonical source | Header | Value encoding | Present when |
|---|---|---|---|
| `envelope.id` | `reliar-message-id` | hyphenated lowercase UUID (`Display`) | always |
| `envelope.message_type.name()` | `reliar-message-type` | verbatim | always |
| `envelope.message_type.version()` | `reliar-message-version` | decimal `u16` | always |
| `metadata.delivery.content_type` | `reliar-content-type` | `ContentType::as_str()` | always |
| `metadata.correlation.correlation_id` | `reliar-correlation-id` | `CorrelationId::as_str()` | `Some` |
| `metadata.correlation.conversation_id` | `reliar-conversation-id` | UUID | not `is_unset()` |
| `metadata.correlation.causation_id` | `reliar-causation-id` | UUID | `Some` |
| `metadata.correlation.request_id` | `reliar-request-id` | UUID | `Some` |
| `metadata.tenant_id` | `reliar-tenant-id` | verbatim | `Some` |
| `metadata.delivery.sent_at` | `reliar-sent-at` | RFC 3339, converted to UTC (`Z`) | `Some` |
| `metadata.delivery.expires_at` | `reliar-expires-at` | RFC 3339, converted to UTC (`Z`) | `Some` |
| `metadata.routing.source` | `reliar-source` | `EndpointAddress::as_str()` | `Some` |
| `metadata.routing.destination` | `reliar-destination` | `EndpointAddress::as_str()` | `Some` |
| `metadata.routing.reply_to` | `reliar-reply-to` | `EndpointAddress::as_str()` | `Some` |
| `metadata.trace.traceparent` | `traceparent` (**unprefixed**, W3C) | verbatim | `Some` |
| `metadata.trace.tracestate` | `tracestate` (**unprefixed**, W3C) | verbatim | `Some` |
| `metadata.delivery.deduplication_id` **or** `envelope.id` | `Nats-Msg-Id` | verbatim / UUID | always |
| `envelope.headers()` | each key verbatim | value verbatim | per entry |
| `envelope.body` | the NATS **payload** | raw `Bytes`, cloned (zero-copy) | always |

That is §14's list in full, plus the two W3C names and JetStream's `Nats-Msg-Id`. **No other header
is written**, and no framework value is ever also written into a custom header (ADR 0004). Nothing
is wrapped: the payload is the serialized body, byte for byte.

### 2. Encode order: custom headers first, framework headers second

> **Refined by [Amendment A](#amendment-a--2026-09-04--2-override-is-case-insensitive-and-encode-never-emits-two-casings):**
> the override is **case-insensitive**, and encode writes at most one entry per framework name.

Custom headers are written first and framework headers second, so a framework value **overrides** a
colliding custom one. Core already makes a `reliar-*` collision impossible; the case this settles is
§14's deliberate escape hatch, `traceparent`/`tracestate`, which core does *not* reserve. When
`metadata.trace` carries a value it wins; when it is `None` the caller's own `traceparent` header
reaches the wire unchanged. This is the SRS's stated rule ("the mapper's own value overrides"),
implemented rather than re-litigated.

### 3. Values are written through the fallible constructors, never the panicking ones

> **Refined by [Amendment B](#amendment-b--2026-09-04--invalidheadervalue-names-the-header-at-runtime):**
> `InvalidHeaderValue` carries the header **name** as a runtime `String`, so it can also name a
> custom header whose value NATS rejects.

Every name goes through `HeaderName::from_str`, every value through `HeaderValue::from_str`, and
both errors become a **permanent** `NatsMapError` naming the *header* (never printing the value).
Consequences that follow, all of them permanent errors rather than panics or silent drops:

- a custom key NATS cannot express (space, `:`, non-ASCII) → `UnsupportedHeaderName { key }`;
- a custom key in NATS's own reserved `Nats-` namespace (case-insensitive) → `ReservedHeaderName
  { key }`, which is what keeps §4's decode rule lossless;
- a `MessageType` name, `tenant_id`, `traceparent`/`tracestate` or custom value containing `\r`/`\n`
  → `InvalidHeaderValue { header }`. (Core's own newtypes — `CorrelationId`, `EndpointAddress`,
  `ContentType`, `Headers` values — already reject control characters, so only the unvalidated
  `String` fields can reach this.)

### 4. Decode

- **Required:** `reliar-message-id`, `reliar-message-type`, `reliar-message-version`,
  `reliar-content-type`. Absent → `MissingHeader { header }`; unparseable → `MalformedHeader
  { header }`. Both are **permanent**, and neither ever panics. Identity and content type cannot be
  guessed: defaulting a version silently forks the message contract, and defaulting a content type
  hands a protobuf body to a JSON deserializer.
- **Optional:** every other framework header. Present-but-malformed is a **permanent**
  `MalformedHeader`, never a silent drop — losing a correlation id quietly is worse than failing
  loudly.
- **Unknown `reliar-*` headers are ignored**, not rejected. §14's list is additive by design, and a
  0.3 producer's new header must not make its messages undecodable by a 0.2 consumer. They can
  never land in `Headers` either — core rejects the prefix.
- **`Nats-*` headers are ignored** except `Nats-Msg-Id`: they are broker bookkeeping
  (`Nats-Stream`, `Nats-Sequence`, `Nats-Time-Stamp`, …), not user data. §3 rejects a custom key in
  that namespace at encode time, so this rule discards nothing a Reliar producer wrote.
- **`traceparent`/`tracestate` always decode into `metadata.trace`**, never into `Headers`.
- **Everything else** is a custom header, inserted through `Headers::insert` with its case
  preserved; a rejection (over-length, over-count) is a permanent `RejectedHeader { key, source }`.
- **Framework names are matched case-insensitively** (core reserves the prefix case-insensitively,
  so `Reliar-Message-Id` must not read as a custom header). Two spellings of one framework header,
  or a repeated one, is a permanent `DuplicateHeader { header }`. For a custom header carrying
  multiple values, the **first** wins — encode never produces one, so no round-trip is affected.

### 5. `Nats-Msg-Id` and the dedup id

Encode writes `deduplication_id` when set, otherwise the message id (§12.3). Decode sets
`deduplication_id = Some(v)` when `Nats-Msg-Id` differs from `reliar-message-id`, and `None` when
they are equal. An envelope whose `deduplication_id` was explicitly `Some(<its own id>)` therefore
decodes as `None` — the same dedup key, the same broker behaviour, so this is **normalisation, not
loss**. `Nats-Msg-Id` is used rather than a new `reliar-dedup-id` because §14's list is closed
without an SRS amendment and because JetStream only honours its own name.

### 6. Round-trip property (what C1 may assert)

`decode(encode(e)) == e` for every envelope whose custom header keys are ASCII-graphic, colon-free
and outside the `Nats-` namespace. Exactly two documented normalisations exist: §5's
dedup-id-equals-message-id case, and a custom `traceparent`/`tracestate` (in **any** casing)
returning in `metadata.trace` (§2, Amendment A). Every other input either round-trips exactly or fails permanently with a
named error.

## Consequences

- An independent consumer reads a plain NATS message: real headers, a real body, RFC 3339 times.
  Nothing has to know about Reliar to consume it.
- The mapper is total: no input panics, and every rejection is a permanent classification the
  dispatcher can dead-letter immediately instead of retrying 10 times.
- Some legal `Headers` keys cannot be published over NATS. This is a real, documented reduction in
  what this transport carries, discovered at the wire boundary and not at `insert` time. Tightening
  core to NATS's charset was rejected: core is transport-neutral (§12) and a future transport with
  a different charset would make it wrong again.
- Wire timestamps are RFC 3339 while the Postgres store persists epoch millis (ADR 0012). Two
  encodings, deliberately: storage optimises for a compact ordered column, the wire optimises for a
  human and a foreign consumer. Both convert through `time`, and neither is derived from the other.
- Sub-millisecond precision survives the wire (RFC 3339 keeps `time`'s digits) but not a Postgres
  round-trip. A message published from a stored record therefore carries millisecond times — a
  property of the store, stated so it is not read as a mapper bug.

## Alternatives considered

- **Wrap the envelope in an outer JSON document.** Rejected by SRS §16 and by the whole point of
  Phase 2: a non-Reliar consumer would have to know our envelope schema to read one field.
- **Epoch millis on the wire** for symmetry with the store. Rejected: the wire is read by humans
  and by foreign services; `1789…` is not a timestamp anyone can read, and the symmetry buys
  nothing because neither side parses the other's format.
- **Lenient decode** (default the version, tolerate a missing content type). Rejected for now, and
  additive later: a second constructor can relax the required set without breaking anyone, whereas
  tightening a lenient default would be breaking.
- **Reject a custom `traceparent` at encode.** Cleaner round-trip, but it contradicts §14's stated
  override semantics and removes an escape hatch the SRS explicitly grants.
- **`reliar-dedup-id` in addition to `Nats-Msg-Id`.** Would make §5's normalisation unnecessary at
  the cost of a header outside §14's closed list — an SRS amendment for a degenerate input.
- **Sanitising bad names/values** (percent-encoding, replacement). Rejected: silent mutation of
  application data, and an un-invertible one.

---

## Amendments

Amendments A and B were prompted by the S1 review
(`../../../sisa-reliar-backlog/docs/analysis/reviews/phase2-s1-mapper-review-1.md`, blocker 1 and
finding 9); Amendment C by the round-2 review of the crate
(`../../../sisa-reliar-backlog/docs/analysis/reviews/phase2-nats-crate-review-2.md`, gap 7). A and B
do not change the wire format; C **narrows what decode accepts** and is taken now, deliberately,
because Phase 3 inherits this wire rule. None of the three needs a change under
`crates/reliar-core/src/` or `crates/reliar-outbox/src/` (§43.B still holds).

### Amendment A — 2026-09-04 — §2 override is case-insensitive, and encode never emits two casings

**Problem.** `async_nats::HeaderName` is case-sensitive for names it does not know, so a custom
header `TraceParent` and the framework `traceparent` are two distinct entries in one `HeaderMap`.
§2's "framework overrides custom" was written as an *ordering* rule and ordering only overrides an
exactly-equal name. Encode therefore emitted both, and §4's case-insensitive decode then rejected
the result with `DuplicateHeader` — a message Reliar produced and Reliar cannot read.

**Decision.** The override in §2 is **case-insensitive**, and encode maintains this invariant:

> `encode` writes **at most one header per framework name, always in the canonical lowercase
> spelling**, and never two names that differ only in case. `decode(encode(e))` can therefore never
> return `DuplicateHeader`.

Concretely, for each custom entry, before any name conversion:

1. `Nats-` prefix (case-insensitive) → permanent `ReservedHeaderName { key }` — unchanged (§3).
2. Key equal, **ignoring ASCII case**, to a framework name that core does not reserve — today
   exactly `traceparent` and `tracestate`:
   - the corresponding `metadata.trace` field is `Some` → the custom entry is **dropped**. This is
     §2's override, now actually reachable: the framework value wins and is the only entry.
   - it is `None` → the custom value is written **under the canonical lowercase name**, not under
     the caller's spelling. §14's escape hatch survives (the caller's trace context reaches the
     wire) and a foreign consumer finds it under the W3C name.
   - a second custom key normalising to the same framework name (`traceparent` *and* `TRACEPARENT`,
     both with `metadata.trace` unset) → permanent `DuplicateHeader { header }` naming the canonical
     spelling. Two spellings of one header with no framework value to arbitrate has no
     order-independent answer — `Headers` is a map, so picking one would make the wire depend on
     hash iteration order.
3. Any other key → the existing path (`HeaderName::from_str`, `HeaderValue::from_str`).
   `reliar-*` remains unreachable: core rejects the prefix case-insensitively at `Headers::insert`,
   and `Nats-*` was rejected in step 1.

**Decode is unchanged and is restated here because the review asked:** a case-variant framework
header arriving from a **foreign** producer is **recognised**, not ignored and not malformed —
framework names are matched case-insensitively (§4) and the value is parsed into `Metadata` exactly
as the lowercase spelling would be. Two entries whose names differ only in case, or one name
repeated, remain a permanent `DuplicateHeader { header }` naming the canonical lowercase spelling.
That state is now reachable only from a non-Reliar producer.

**Consequences.**

- §6's normalisation 2 is stated more precisely: a custom `traceparent`/`tracestate` returns in
  `metadata.trace` **and loses its original casing** — the key is absent from the decoded `Headers`
  either way, so the round-trip property is unaffected.
- A caller who sets both `metadata.trace.traceparent` and a custom `TraceParent` silently loses the
  custom one. That is what "the mapper's own value overrides" means; it is documented on `encode`
  and covered by a test (contract §7, U14).
- No new error variant: `DuplicateHeader` already means "one framework header, two spellings" and
  its `&'static str` field is exactly the canonical name.

**Alternatives.** *Reject any custom key colliding with a framework name* — rejected again for the
reason in "Alternatives considered": it removes §14's stated escape hatch, and it would turn a
today-legal envelope into a dead row. *Preserve the caller's casing on the wire when
`metadata.trace` is unset* — rejected: it publishes a header a W3C-aware, case-sensitive consumer
will not find, and it makes the wire form depend on the caller's spelling for no gain.

### Amendment B — 2026-09-04 — `InvalidHeaderValue` names the header at runtime

**Problem.** §3 assigns a value rejected by `HeaderValue::from_str` to `InvalidHeaderValue`, but the
contract typed that variant `InvalidHeaderValue { header: &'static str }`, which cannot name a
**custom** header — the key is the caller's runtime `String`. The implementation resolved the
mismatch by reporting a rejected custom *value* as `UnsupportedHeaderName`, which tells a host its
key is unspellable when the key is fine.

**Decision.** The variant becomes `InvalidHeaderValue { header: String }` and covers both
provenances: a framework header passes its `headers::*` constant (`.to_owned()` — an allocation on
an error path only), a custom header passes the caller's key. The field is a header **name** in both
cases; the value is still never carried and never printed (§17.1). `UnsupportedHeaderName` and
`ReservedHeaderName` keep `key: String` and keep their meaning — *the name* cannot be expressed.

**Consequences.**

- A dead row's `last_error` now distinguishes "this key cannot be a NATS header name" from "this
  header's value cannot be a NATS header value", which are two different fixes for the host.
- For a value arriving through `Headers`, the case stays unreachable in practice: core rejects every
  control character in a custom value, a superset of NATS's `\r`/`\n` check. The variant exists for
  the unvalidated core `String` fields (`MessageType::name`, `tenant_id`, `traceparent`,
  `tracestate`, `deduplication_id`) and is asserted through those (contract §7, U7).
- `NatsMapError` keeps `Clone + PartialEq + Eq`.

**Alternative.** *Footnote §3 to bless `UnsupportedHeaderName` for custom values.* Rejected: it
merges two distinct failures behind one name to avoid one `String`, and it would have to explain
that the variant sometimes does not mean what it says.

### Amendment C — 2026-09-04 — an empty required header value is `MalformedHeader`

**Problem.** §4 requires four headers and rejects an unparseable value, but "parseable" is decided
by whatever constructor rehydrates the field — and `MessageType::from_parts` is core's *deliberately
unvalidated* rehydration path (ADR 0011): it accepts `""`. So a message carrying
`reliar-message-type: ` (empty) decoded successfully into a `MessageType` that renders as `".v1"`.
The other three required headers happen to be safe by accident — `MessageId` parses a UUID,
`reliar-message-version` parses a `u16`, and `ContentType::parse` returns `ContentTypeError::Empty`
— which is exactly why the gap was invisible. Phase 3's consumer will inherit whatever this decides.

**Decision.** **An empty value on a required header is `MalformedHeader { header }` — permanent,
never a panic, never an accepted value.** In practice this is one explicit check, on
`reliar-message-type`; the other three are already rejected by their own parse and the rule is
stated over all four so a future required header cannot reintroduce the hole.

**Emptiness is the only name rule decode enforces.** A non-empty name from a foreign producer is
accepted verbatim — no charset, length or dot-shape validation. Two reasons: a producer's naming
convention is not this crate's to police (SRS §14 makes the wire interoperable on purpose), and the
mapper must not be stricter than core's own constructors, or `decode` would reject envelopes
`encode` can legally produce.

**The check lives in the mapper, not in core.** Tightening `MessageType::from_parts` would be a
breaking change to a Phase-1 published API, would make a *store* rehydration path fallible, and
would be a `reliar-core` change this phase is required not to need (§43.B). The wire is the right
boundary to validate at: the mapper is where untrusted bytes enter.

**Consequences.**

- A producer that emits an empty message-type name gets a permanent failure with a named header
  instead of a silently malformed `MessageType` propagating into a consumer, a subject, or a log.
- Permanent means the message dead-letters rather than retrying — correct: the same bytes fail
  identically forever (ADR 0030).
- Contract §2.5 carries the rule and §7 test id **U17** proves it, including that a non-empty
  foreign name still round-trips.
- If core later gains a validating `MessageType::parse`, the mapper can delegate to it with no wire
  change; this amendment is the interim owner of the rule, not a permanent home claim.

**Alternatives.** *Document acceptance* — rejected: an empty type name is not a message any
consumer can route or deserialize, so accepting it only moves the failure somewhere with less
context. *Validate the full name shape* — rejected as over-reach, per the paragraph above.
