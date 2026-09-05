# ADR 0030 — `NatsPublishError` classifies per variant, and never prints payloads or credentials

**Status:** Accepted — 2026-09-04 · **amended 2026-09-04** (A: the size guard · B: the `Broker`
log) · **corrected 2026-09-05** (the size guard saves no round-trip — S0 review M6) — see
[Amendments](#amendments)
**SRS:** §17.1, §19.4, §23, §33, §45 ("retry/dead-state semantics", "delivery guarantees")
**Builds on:** [ADR 0009](0009-retry-and-dead-state.md) (retry policy and dead state),
[ADR 0008](0008-outbox-store-contract.md) (per-variant classification, never a blanket rule)
**Related:** RELIAR-2 (Phase 2), RELIAR-30, contract `../architecture/phase2-contract.md` §4.3

## Context

`Publisher::Error: Classify` is what the dispatcher uses to choose retry-with-backoff versus dead
(§19.4, §23). `reliar-store-postgres` set the house precedent for how a provider does this: classify
**per variant**, never with a blanket rule, and classify an unrecognised third-party failure as
transient with a `warn` (§19.4). The NATS side has to do the same over `async-nats`'s
`PublishErrorKind` — `StreamNotFound`, `WrongLastMessageId`, `WrongLastSequence`, `TimedOut`,
`BrokenPipe`, `MaxAckPending`, `MaxPayloadExceeded`, `Other` — plus the failures the publisher
raises itself before anything reaches the wire.

There is a second obligation in the same place. §17.1 requires `last_error` — the `Display` of the
error chain, persisted into the outbox row — to contain no payload bytes, no custom header values
and no credentials. Broker errors are a classic leak: an `async-nats` connection error can carry the
server URL, and a NATS URL routinely embeds `user:password@` or a token.

## Decision

**A hand-rolled `#[non_exhaustive] NatsPublishError` whose every variant has a fixed `Classify`
verdict, a wired `source()`, and a `Display` that names the failure and the subject but never a
payload, a header value, or a server address.**

### Classification

| Variant | Origin | Verdict |
|---|---|---|
| `Map(NatsMapError)` | the envelope cannot be expressed as a NATS message (ADR 0026 §3) | **Permanent** |
| `Subject { source }` | `SubjectResolver` rejected the envelope (ADR 0027) | **Permanent** |
| `PayloadTooLarge { len, limit }` | the publisher's pre-flight guard against `NatsSettings::max_payload` | **Permanent** |
| `MaxPayloadExceeded` | the server's `max_payload` (`PublishErrorKind::MaxPayloadExceeded`) | **Permanent** |
| `WrongLastMessage` | `WrongLastMessageId` / `WrongLastSequence` — a publish precondition | **Permanent** |
| `Timeout { after_ms }` | the publisher's own `publish_timeout`, or `PublishErrorKind::TimedOut` | **Transient** |
| `Connection` | `BrokenPipe` and any I/O-shaped failure of the send | **Transient** |
| `StreamNotFound` | no stream captures the subject (ADR 0029 §3) | **Transient** |
| `MaxAckPending` | the server is applying back-pressure | **Transient** |
| `Broker` | `PublishErrorKind::Other` and anything unrecognised | **Transient**, logged at `warn` |

Three of these deserve their reasoning stated, because they are the ones a reader will question:

- **`StreamNotFound` is transient.** It is a provisioning gap an operator fixes in seconds (ADR
  0029), and after the fix a backed-off row publishes successfully. Dead-lettering a whole table
  because a stream was created five minutes late is the worse error. It still ends in dead state if
  nobody fixes it — `max_attempts` guarantees termination (§23).
- **`Other`/unrecognised is transient with a `warn`.** Identical to the Postgres store's rule for an
  unrecognised SQLSTATE (§19.4). A misclassified-as-permanent unknown destroys deliverable messages;
  a misclassified-as-transient one costs retries and leaves a `warn` in the log to fix the mapping.
- **`WrongLastMessage` is permanent but unreachable by construction.** Reliar never sets
  `expected_last_msg_id`/`expected_last_sequence`, so this cannot occur from our own publish path.
  It is mapped rather than swallowed because `PublishErrorKind` is non-exhaustive-in-spirit and a
  silent fallthrough into `Broker` would hide a real precondition failure if the variant ever
  becomes reachable.

### The pre-flight size guard

`NatsSettings::max_payload` (default `None`) lets a host declare a payload ceiling Reliar enforces
itself, as a permanent `PayloadTooLarge` carrying lengths only. `Some(0)` is rejected at
construction — [Amendment A](#amendment-a--2026-09-04--max_payload--some0-is-a-construction-error).
Both this guard and the server's own `MaxPayloadExceeded` are counted against **headers +
payload**, which is what the server measures.

**What it does not buy is a saved round-trip** (corrected 2026-09-05, S0 review M6). The original
wording here — "so an oversized message is rejected *before* a round-trip" — was wrong about
async-nats. `Context::send_publish` calls `Client::check_payload_size` at the top, before it takes
an ack permit and before anything is written, and compares against the `max_payload` the **server
advertised in its INFO at connect** (async-nats 0.50, `src/jetstream/context.rs:493-502`,
`src/client.rs:368-383`). An oversized message therefore never reaches the wire with or without
this setting; unset, it fails as `MaxPayloadExceeded` locally, not one round-trip later.

What the setting actually buys is narrow and still worth having: (a) a ceiling **below** the
server's — a host that wants to dead-letter at 256 KiB on a server that accepts 1 MiB has no other
way to say so; and (b) a Reliar-owned error variant with our own lengths, rather than an
`async-nats` error whose `Display` we deliberately do not persist. It is off by default because the
real limit is a server property Reliar refuses to guess, and **RELIAR-37** removes the guesswork
entirely by reading the connected server's advertised limit — at which point this setting means
only (a).

### Display and Debug

`Display` prints the variant, the **subject**, and numeric facts (lengths, limits, elapsed ms). It
never prints payload bytes, custom header values, a header value of any kind, or a server URL —
and the `source()` chain is only rendered by the dispatcher's own error formatting, so an
`async-nats` error string never enters `last_error` unless it is the source Reliar chose to expose.
For that reason `Connection` and `Broker` **do not carry the `async-nats` error as a public
`source()` when its `Display` can contain a server address**: they carry the kind and a
Reliar-authored message. (The clause that followed here — "and the underlying error is logged, not
persisted, at `warn`" — is **superseded by [Amendment B](#amendment-b--2026-09-04--the-broker-warn-logs-the-kind-name-never-the-async-nats-display)**:
the `async-nats` error is not logged either. Only the kind name and the subject are.) `Debug` is
manual where a variant holds anything that could be data, and derived only where every field is a
number or a subject.

The subject is deliberately *in*: it is routing configuration, not user data, and without it a dead
row's `last_error` cannot be acted on. `NatsMapError`'s variants name a **header key**, never a
value, for the same reason.

## Consequences

- The dispatcher's retry/dead decision on this transport is a table a reader can check, and each row
  is asserted by a test (story C8).
- A dead outbox row's `last_error` is safe to display in a future dashboard, log aggregator or
  support ticket. That is the property §17.1 exists to protect.
- Diagnosing a connection failure needs the log, not the row. Accepted: the row carries the verdict
  and the subject, the log carries the chain, and only one of the two is persisted forever.
- `#[non_exhaustive]` means a future `async-nats` kind can be mapped to a new variant in a minor
  release; adding one is additive as long as the existing verdicts do not move.

## Alternatives considered

- **`Transient { source } | Permanent { reason }`** — the two-variant shape the design skill sketches
  for a generic publisher. Rejected here: the dispatcher would be fine, but an operator reading
  `last_error` learns nothing, and C8 could not assert a per-cause verdict.
- **Blanket-classify every `async-nats` error as transient.** Rejected by §19.4's "never with a
  blanket rule": an oversized payload would then retry until `max_attempts` on every single
  attempt, ten times, identically.
- **Carry the `async_nats` error as `source()` on every variant.** Rejected: the shortest path from
  a broker error to a credentialed URL in a persisted column. Kept for the variants whose sources
  are Reliar's own (`Map`, `Subject`).
- **Default `max_payload` to 1 MiB** (NATS's own default). Rejected: it silently becomes wrong on a
  server configured otherwise, in the direction that rejects deliverable messages.

## Amendments

Both prompted by the round-2 crate review
(`../../../sisa-reliar-backlog/docs/analysis/reviews/phase2-nats-crate-review-2.md`, M1 and m7).

### Amendment A — 2026-09-04 — `max_payload = Some(0)` is a construction error

**Problem.** "The pre-flight size guard" made `NatsSettings::max_payload` an opt-in `Option<usize>`
and said nothing about which values are legal. `NatsSettings::from_env` rejects `0`; the infallible
builder does not. `NatsPublisher::new(ctx, NatsSettings::default().max_payload(Some(0)))` therefore
built a publisher that fails **every** message with a permanent `PayloadTooLarge` before any I/O —
a silently dead-lettering outbox, configured by one plausible typo, with the same setting rejected
on the environment path. Two doors, two answers.

**Decision.** A new variant **`NatsConfigError::ZeroMaxPayload`**, raised by `NatsPublisher::new`
and `NatsPublisher::with_resolver` from the validation that already guards `max_in_flight` and
`publish_timeout`. Adding a variant to a `#[non_exhaustive]` enum is semver-additive, so this is a
minor-release change even after publication.

**Only `Some(0)` is rejected. A merely small limit is documented, not validated.** `0` is the one
value provably unusable for *every* possible envelope; below that, the floor is envelope-dependent
(the framework header block runs to roughly 150 bytes before a payload byte, and grows with the
message-type name and the metadata present). Encoding a numeric floor into the setting would pin
`encode`'s exact byte formatting into that field's semver contract, and would still be a guess.
The rustdoc states the floor qualitatively so the failure mode is discoverable, and **RELIAR-37**
removes the guesswork properly by deriving the limit from the connected server (`Context::client()`
is the route — it is the only getter async-nats 0.50 exposes, and it carries the server's own
`max_payload`).

**`Timeout { after_ms }` reports measured elapsed time**, not the configured `publish_timeout`
(review m1). The reasoning is ADR 0028 Amendment A's; recorded here too because `after_ms` is this
ADR's field.

**Consequences.** One unusable configuration becomes unrepresentable at construction; the rest of
the range stays the host's responsibility with a documented hazard. Contract §4.1, §9 item 18, and
test id **N7** (which also asserts that `max_payload(Some(1))` is *accepted* and dead-letters
everything — the behaviour `ZeroMaxPayload` exists to keep out of reach by accident).

**Alternatives.** *Reject anything below a `MIN_WIRE_LEN` constant* — rejected: a public constant
that must move whenever the header block changes, i.e. a semver trap in exchange for a partial
guard. *Make the builder fallible* — rejected: `NatsSettings` is a plain `Default` + `const`
builder by ADR 0019, and construction-time validation already has a home. *Auto-populate from the
server at `new`* — that is RELIAR-37, and it needs `new` to be `async` or to accept a `Client`;
a separate decision, not a fix.

### Amendment B — 2026-09-04 — the `Broker` `warn` logs the kind name, never the `async-nats` `Display`

**Problem.** An internal contradiction the review caught as m7. "Display and Debug" and contract
§7/U13 assert that **nothing** this crate emits — `Display`, `Debug`, span field or event — can
contain `nats://user:pass@host`. Yet the same ADR and contract §4.3/§4.4 required the unrecognised-
kind path to log the `async-nats` error's `Display` at `warn`, which is precisely a string this
crate does not bound. U13 could not both pass and mean anything.

**Decision.** **The `warn` carries the subject and a kind name only.** The kind name is a
`&'static str` from this crate's total match over `PublishErrorKind` — `"other"` for `Other`,
`"unrecognised"` for a kind this crate does not know — so the event's value space is a finite set
of literals this crate owns. The `async-nats` error is **dropped**: not persisted (it never was),
and now not logged either.

The credential invariant wins over the diagnostic string because it is the one that must be
*provable*. An invariant with one exception is a review argument every time it is touched, and the
exception was in the one path that fires on unknown input. What the `warn` exists for — "a kind
appeared that our table does not map, go extend it" — is fully served by the kind name.

**Consequences.**

- Diagnosing a `Broker` failure needs the **server's** log, not Reliar's. Accepted and documented:
  Reliar's row carries the verdict and the subject, the server carries the cause, and only one of
  the two is persisted forever (§17.1).
- U13 becomes an absolute, testable statement; **N10** covers the live-`Context` half (the `warn`
  event itself, and `NatsPublisher`'s manual `Debug` under a credentialed connection — review M2).
- Contract §4.3, §4.4, §7 U13/N10 and §9 item 21 are updated to match.
- If a host ever needs the underlying chain, the additive route is an explicit, opt-in accessor that
  the host chooses to log — never a default log line. Not built in Phase 2.

**Alternatives.** *Redact the `Display` with a URL-stripping filter* — rejected: a security
invariant resting on a regex over an upstream string we do not control, and unprovable for future
async-nats messages. *Log it at `trace` instead* — rejected: a lower level is not a smaller leak;
`trace` is exactly what an operator turns on while debugging the failure that carries the
credential. *Keep the `Display` and narrow U13 to exclude that event* — rejected: that is the
contradiction, written down.
