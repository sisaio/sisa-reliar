# ADR 0004 — Framework metadata has exactly one source of truth

**Status:** Accepted — 2026-09-04
**SRS:** §13, §13.1, §14, §15, §16, §43.A.5

## Context

Libraries that expose both typed metadata and a free-form header map invariably end up writing
the same value into both — `metadata.correlation_id` *and* `headers["x-correlation-id"]` — because
different code paths reach for different ones. The two then drift: one is updated, the other is
stale, and a consumer reading the wrong one gets a silently wrong answer. Worse, an application
can overwrite a framework header and corrupt correlation for an entire conversation.

Reliar needs both surfaces: typed metadata for what it understands (§12), and headers for what it
does not (`x-import-batch`, `x-feature`).

## Decision

- **`Envelope.metadata` is canonical for every framework concept.** Reliar SHALL NOT copy a
  metadata value into `Envelope.headers`, and SHALL NOT read a framework value out of headers.
- **`Headers` reserves the entire `reliar-` prefix**, matched case-insensitively, and
  `Headers::insert` returns `Err(HeaderError::Reserved)` — never a silent drop (§13.1, ADR 0011).
  Reserving the whole prefix (not just today's key list) makes adding a framework key later a
  non-breaking change.
- The `reliar-*` names in §14 exist **only at the transport boundary**, written by an
  `EnvelopeMapper` from `Metadata` at encode time and parsed back into `Metadata` at decode time
  (§15, §16). Transport headers are a *projection*, not a second store, so they are not
  duplication.
- **`traceparent`/`tracestate` are deliberately not `reliar-` prefixed** and are deliberately not
  reserved by `Headers::insert`. They are W3C Trace Context names; renaming them would make
  Reliar's messages invisible to every standard tracing tool. The mapper writes them from
  `Metadata.trace` and its value wins over a user-set header. This asymmetry is intentional.
- Reliar carries trace context; it never invents it (§33.1). No `tracing-opentelemetry` dependency
  in any library crate.

## Consequences

- A reader always knows where to look: `envelope.metadata.correlation.correlation_id`. There is
  no "which one is right" question, and no reconciliation code.
- Header-name collisions between an application and the framework become a **compile-adjacent
  error** (`insert` returns `Err`) rather than a production mystery.
- Each transport mapper must map every canonical field it wants on the wire; forgetting one loses
  it. That is a per-transport test obligation (Phase 2), not a core concern.
- The reserved prefix is a public contract: applications that already use `reliar-`-prefixed
  header names must rename them. Documented, and the caps/prefix rules are rustdoc'd on `Headers`.
- Because `traceparent` is not reserved, an application *can* set it — and the mapper overrides
  it. Documented as a deliberate escape hatch rather than an enforcement gap.

## Alternatives considered

- **Reserve only the specific `reliar-*` keys in §14.** Rejected: adding `reliar-tenant-id` later
  would then be a breaking change for anyone who happened to use that key.
- **Silently drop reserved keys.** Rejected: turns a programming error into an unexplained
  runtime behaviour that surfaces days later at the consumer.
- **Also reserve `traceparent`/`tracestate`.** Rejected: breaks W3C interop, which is the whole
  reason those names exist.
- **Deref `Headers` to `HashMap` for ergonomics.** Rejected: re-opens unvalidated `insert`.
