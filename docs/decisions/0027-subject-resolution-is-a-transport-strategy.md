# ADR 0027 — Subject resolution is a transport-side strategy, not envelope metadata

**Status:** Accepted — 2026-09-04
**SRS:** §12 (transport-specific fields SHALL NOT be in `reliar-core`), §16, §18, §45 ("transport mapping")
**Related:** RELIAR-2 (Phase 2), RELIAR-30, contract `../architecture/phase2-contract.md` §3

## Context

Something has to decide which NATS subject an envelope is published to. The three candidate homes
are `reliar-core` (a `subject` field on `Metadata`), `reliar-outbox` (a routing hook on the
dispatcher), and the transport crate. SRS §12 names "NATS subject-specific configuration" as an
example of what SHALL NOT be part of `reliar-core`, and §43.B requires that adding a transport
touches no file under `reliar-outbox/src/`. That rules out two of the three, but leaves the shape
open: fallible or infallible, whether `RoutingMetadata.destination` participates, and who validates
that the result is a legal subject.

Validation is not a detail. NATS subjects are dot-separated tokens; a token may not be empty or
contain whitespace, and `*` and `>` are wildcards. A `MessageType` name is an unvalidated string in
core, so `<prefix>.<message_type>` can produce `reliar.orders..v1` or `reliar.a b.v1` or a subject
containing `>`, which is a routing bug (or, with a wildcard, a publish to a subject the app never
intended).

## Decision

**A `SubjectResolver` trait in `reliar-transport-nats`, called by `NatsPublisher`, with an
associated error type and pure synchronous resolution.**

```rust
pub trait SubjectResolver: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    fn subject(&self, envelope: &SerializedEnvelope) -> Result<async_nats::Subject, Self::Error>;
}
```

1. **It lives in the transport crate.** `reliar-core` gains no subject concept; `reliar-outbox`
   gains nothing at all. `NatsPublisher<R: SubjectResolver>` resolves the subject itself, inside
   `publish`, so the dispatcher never sees a routing concept.
2. **It is fallible.** An envelope that cannot be routed is a `NatsPublishError::Subject` classified
   **permanent** (ADR 0030) — a dead-letter, not ten retries and not a panic.
3. **It is synchronous and pure.** No `async`, no I/O: resolution is a function of the envelope.
   That is what makes (2) sound — a resolver that called out to a network service would have
   transient failures a permanent classification would mis-handle. The rustdoc states the
   obligation; a resolver that needs a lookup table loads it at construction.
4. **`PrefixSubjects` is the default:** `<prefix>.<message_type>`, i.e. `reliar.orders.created.v1`
   for prefix `reliar` — `MessageType`'s `Display`, which is already a documented public contract
   (ADR 0010). It validates the whole subject and rejects an empty token, whitespace or a control
   character, `*`, `>`, a non-ASCII-printable byte, and a subject over 255 B.
5. **`RoutingMetadata.destination` participates only through an opt-in resolver.**
   `DestinationSubjects` uses `destination` verbatim as the subject when it is `Some`, falling back
   to a `PrefixSubjects` otherwise, with the same validation. The default resolver **ignores**
   `destination`: routing that changes with a per-message string is a deliberate choice, not
   something an application discovers because one caller populated a metadata field.

## Consequences

- `cargo tree -p reliar-core` and `-p reliar-outbox` stay free of `async-nats`; the §43.B "no file
  under `reliar-outbox/src/` changed" constraint holds by construction.
- Every other transport gets the same seam without a core change: an exchange/routing-key resolver
  for RabbitMQ, a topic/partition-key resolver for Kafka. Nothing about that is promised here — it
  is simply not blocked.
- `NatsPublisher` carries a second type parameter. Static dispatch, monomorphised, no `dyn` on the
  publish path (ADR 0001); `NatsPublisher<PrefixSubjects>` is the default type argument so the
  common case names no resolver.
- Two resolver types ship. Both are ~20 lines and neither is an abstraction over a hypothetical:
  `PrefixSubjects` is the documented default and `DestinationSubjects` is the one behaviour
  applications would otherwise all hand-roll from the metadata field the SRS already gives them.
- Subject validation duplicates a rule the server also enforces. Deliberate: the local check turns
  a server-side rejection (and, for `*`/`>`, a *successful* publish to the wrong subject) into a
  named permanent error before anything leaves the process.

## Alternatives considered

- **`Metadata.subject` in `reliar-core`.** Directly forbidden by §12, and it would put a NATS
  concept in the envelope every other transport has to ignore.
- **A routing hook on `OutboxDispatcher`.** Would touch `reliar-outbox/src/`, breaking §43.B, and
  would make routing a dispatcher concern for stores that publish nowhere.
- **Infallible `fn subject(&self, …) -> Subject`** (the shape sketched in the transport-nats
  skill). Rejected: with an unvalidated `MessageType` name, the only ways to be infallible are to
  panic or to publish to a malformed/wildcard subject.
- **`&'static str`/`String` return instead of `async_nats::Subject`.** `Subject` is
  `Bytes`-backed and is what `publish_with_headers` wants; returning `String` would allocate and
  then convert on every publish.
- **A resolver that may be `async`.** Rejected with (3): it would make every subject failure
  ambiguous between transient and permanent, for no use case Phase 2 has.
