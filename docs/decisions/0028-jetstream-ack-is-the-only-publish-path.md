# ADR 0028 — JetStream with an awaited ack is the only publish path

**Status:** Accepted — 2026-09-04 · **amended 2026-09-04** (A: §2 and §3) and **2026-09-05**
(A corrected: `backpressure_on_inflight` defaults to **`true`** · B: `max_in_flight` is renamed
`batch_pipeline_depth`) — see [Amendments](#amendments)
**SRS:** §19.4, §21, §22, §22.1, §32, §45 ("delivery guarantees", "transport mapping")
**Builds on:** [ADR 0007](0007-at-least-once-publication.md) (at-least-once and its duplicate windows)
**Related:** RELIAR-2 (Phase 2), RELIAR-30, contract `../architecture/phase2-contract.md` §4

## Context

`OutboxDispatcher` marks a row `published_at` when `Publisher::publish` returns `Ok`. Whatever
"published" means to the publisher is therefore what Reliar's durability claim means to the user.

NATS offers two publish paths:

- **Core NATS** (`Client::publish`) — fire-and-forget. It returns as soon as the bytes are handed to
  the connection's write buffer. There is no server acknowledgement, no persistence, and no error
  when nothing is listening. A row marked `published_at` on that basis can vanish with the process,
  and Reliar's "durable at-least-once publication" (§22) would be false: the outbox row is deleted
  by retention while the message never existed anywhere.
- **JetStream** (`Context::publish_with_headers`) — the server stores the message in a stream and
  replies with a `PubAck` carrying the stream name and sequence. `async-nats` splits this into two
  awaits: the call returns `Result<PublishAckFuture, PublishError>` once the publish has been sent,
  and awaiting the `PublishAckFuture` yields the server's ack.

Awaiting only the first of those two is a subtler version of the same bug — it proves the bytes were
written to a socket, not that a stream holds them.

## Decision

**`NatsPublisher` publishes through JetStream and returns `Ok` only after the server's ack has been
received. Reliar ships no Core-NATS publisher.**

1. `publish` awaits both stages: the send, then the `PublishAckFuture`. Any failure of either is
   an `Err` classified per ADR 0030; the dispatcher then retries or dead-letters, and the row is
   **never** marked published on an unacked send.
2. **A publish timeout is the publisher's own**, from `NatsSettings::publish_timeout` (default
   10 s), covering send + ack together and reported as a transient `Timeout`. It is the publisher's
   **ceiling**, not the effective bound, and `after_ms` is measured rather than configured — see
   [Amendment A](#amendments). It exists
   independently of `DispatcherSettings::publish_timeout` because `Publisher` is usable without the
   dispatcher, and because §22.1's slow-batch duplicate window is bounded only if the transport
   itself cannot hang.
3. **`publish_batch` overrides the default loop** (SRS §19.4 requires transports with a native
   batch API to). The strategy is **pipeline, then await positionally**, in windows of
   `NatsSettings::batch_pipeline_depth` (default 64, named `max_in_flight` until
   [Amendment B](#amendments)):
   - envelopes that fail encode or subject resolution get their permanent error recorded **in
     place** and are not sent — a bad envelope never fails its neighbours;
   - within a window, every remaining envelope's send is issued before any ack is awaited, so one
     round-trip covers the window instead of one per message (§32);
   - each ack is then awaited and recorded at its own index, so a single failing ack does not
     discard its window's verdicts;
   - the window, not the individual message, is what `publish_timeout` bounds; a whole call is
     therefore bounded by `ceil(n / batch_pipeline_depth) × publish_timeout`.
   The result vector is always `envelopes.len()` long and positional (§19.4).
4. **No ordering promise is added.** Sends inside a window are issued in order on one connection,
   which is the strongest thing that can honestly be said; nothing about windows, retries, or
   multiple workers preserves order (§22.2, ADR 0013). The rustdoc says exactly this.
5. **The duplicate window is narrowed, not closed.** `Nats-Msg-Id` (ADR 0026 §5) lets JetStream
   suppress a duplicate republished inside the stream's configured `duplicate_window`. Outside that
   window the duplicate is stored, exactly as §22 says. `NatsPublisher` publishes; it does not
   claim exactly-once, and the ack's `duplicate` flag is recorded on the span rather than turned
   into a guarantee.

## Consequences

- `publish` costs one server round-trip. That is the price of the durability claim and is why
  `publish_batch` exists; §32's throughput target is met by batching round-trips, never by dropping
  the ack.
- A stream must already capture the subject, or every publish fails (see ADR 0029). This is the
  intended, loud failure mode.
- Reliar cannot be used as a Core-NATS event emitter through `Publisher`. An application that wants
  fire-and-forget messaging holds an `async_nats::Client` and calls it directly — it just does not
  get to call that an outbox publication.
- `publish_batch` is specified and tested but **unused by v0.1's dispatcher**, which calls `publish`
  for its per-message verdict and timeout (contract §3.5). It is here because the trait shape is
  semver-visible, because SRS §19.4 requires a transport with a native batch API to override the
  default loop, and because the messaging layer (§36) will use it. The consequence is that the
  override **and** `NatsSettings::batch_pipeline_depth` reach only a third-party batch caller today
  — that reachability gap is stated in the rustdoc on both, rather than left for a host to discover
  by tuning a setting that does nothing (S0 review M3). **RELIAR-39** owns whether the dispatcher
  grows a batch path.

## Alternatives considered

- **Core NATS with an optional JetStream mode.** Rejected: it makes the durability of the whole
  outbox a configuration flag, and the wrong setting is silent — everything looks green while
  messages are dropped on any hiccup.
- **Await the send but not the ack.** The cheapest correct-looking option and the most dangerous:
  `Ok` would mean "queued in a socket buffer". A restart between the write and the server's fsync
  loses the message with the row already marked published.
- **Await acks for the whole batch at once (unbounded pipelining).** Faster in a microbenchmark,
  but it lets one batch open thousands of unacked publishes, which trips the server's
  `MaxAckPending`-shaped limits and makes the failure mode a cliff. `batch_pipeline_depth` is the same
  bounded-concurrency rule the dispatcher already applies (§26).
- **One timeout per message inside a pipelined window.** Each message's clock would start at a
  different point for no observable benefit; bounding the window is simpler to reason about and to
  document.

## Amendments

### Amendment A — 2026-09-04 — the ack deadline is `min(publish_timeout, Context::timeout)`, and the pipeline depth lives under the host's `max_ack_inflight`

*(The setting this amendment calls `max_in_flight` throughout was renamed `batch_pipeline_depth` by [Amendment B](#amendment-b--2026-09-05--the-pipeline-depth-setting-is-batch_pipeline_depth-not-max_in_flight); the text below is updated to the new name, and the decision it records is unchanged.)*

Prompted by the round-2 crate review
(`../../../sisa-reliar-backlog/docs/analysis/reviews/phase2-nats-crate-review-2.md`, m1–m3). Both
findings are the same shape: §2 and §3 describe bounds as if Reliar owned them, and it does not —
the `Context` is the application's (ADR 0029 §1) and carries bounds of its own. Verified against
async-nats 0.50 source, because the docs do not state the defaults:

| async-nats knob | Default | What it does to us |
|---|---|---|
| `ContextBuilder::timeout` | **5 s** | `PublishAckFuture` awaits the ack under `tokio::time::timeout(self.timeout, …)` and yields `PublishErrorKind::TimedOut`. It is *not* `ack_timeout` (30 s), which applies only to the backpressure path |
| `ContextBuilder::max_ack_inflight` | **5000** | a semaphore over outstanding publish acks |
| `ContextBuilder::backpressure_on_inflight` | **`true`** — corrected 2026-09-05, see below | at the cap: `true` → each excess send *waits* for a permit released only by awaiting or dropping an ack future; `false` → each excess send fails `PublishErrorKind::MaxAckPending` |
| `Context` getters | none (only `client()`) | Reliar **cannot read** any of the above, so it cannot validate against them |

**Decision — the Phase-2 interim rule (RELIAR-38 owns the final one):**

1. **`publish_timeout` is documented as an *upper* bound**, not the bound. The effective ack
   deadline is `min(publish_timeout, Context::timeout)`. §2's "a publish timeout is the publisher's
   own" is amended accordingly: it is the publisher's *ceiling*.
2. **The default stays 10 s.** Dropping it to 5 s to match today's async-nats default was
   considered and rejected: it would encode an upstream default we do not own into our public
   `Default`, it does not fix the mismatch (a host can set 1 s or 60 s), and it would have to change
   again when RELIAR-38 lands. A host that wants the full 10 s builds its context with
   `ContextBuilder::timeout`, and the rustdoc says so.
3. **`Timeout { after_ms }` reports measured elapsed time**, from an `Instant` taken before the
   send, never the configured value. This is the part of m1 that was a *correctness* bug rather
   than a documentation gap: with defaults, the deadline that fires is the host's 5 s while
   `after_ms` claimed 10 000, and an operator sizing a stream from `last_error` would have been
   reading a number no clock produced. Both routes into the variant — our `tokio::time::timeout` and
   the mapped `TimedOut` — report what really elapsed.
4. **`batch_pipeline_depth` must stay ≤ the host's `max_ack_inflight`, documented and not
   validated**, because no getter exists. The default 64 sits two orders of magnitude below the
   default 5000. Both failure modes are spelled out in the rustdoc, and the `true` one matters most
   **because it is the one a host gets by default**: §3's window issues every send before awaiting
   any ack, so a host over the cap stalls the window until `publish_timeout` and fails its whole
   remainder as `Timeout`. That is a configuration hazard, not a bug in §3 — the alternative
   (awaiting acks eagerly to release permits) would abandon pipelining, which is the point of §3.

**Consequences.**

- Every timeout number an operator sees is real; no setting claims a bound it cannot enforce.
- Reliar takes no dependency on async-nats' defaults staying put: the *relationship* is documented,
  the numbers are quoted as "today's default, verified against 0.50".
- RELIAR-38 may still choose to derive `publish_timeout` from the context or to reject the
  mismatch; both remain additive on top of this.
- Contract §4.1 (`publish_timeout`, `batch_pipeline_depth`), §4.2 step 4, §9 items 19–20 carry
  the rule.

**Alternatives.** *Validate `publish_timeout ≤ Context::timeout` at construction* — impossible today
(no getter) and, if a getter appeared, it would reject a perfectly workable configuration. *Set
`Context::timeout` ourselves from `NatsSettings`* — rejected: the `Context` is the host's and Reliar
mutating it would reach outside the object it was handed (ADR 0029 §1). *Take the `Context` by
`ContextBuilder`* — rejected for the same reason plus the loss of a host-shared context.

**Correction — 2026-09-05 — `backpressure_on_inflight` defaults to `true`, not `false`.** The table
above originally recorded the default as `false`, which inverted the failure mode this amendment
exists to document. Verified in async-nats 0.50 source: `impl Default for ContextBuilder<Yes>` sets
`backpressure_on_inflight: true` (`src/jetstream/context.rs:186`), and `Context::new` — the
constructor behind `async_nats::jetstream::new(client)`, which is what every host in our docs and
tests calls — is `ContextBuilder::default().build(client)` (`:340-342`). So the default a host
actually gets is the **waiting** one: `send_publish` does `max_ack_semaphore.acquire_owned().await`
when the flag is set and `try_acquire_owned()` (fast `MaxAckPending`) only when it is cleared
(`:504-521`). Two consequences follow and are now stated wherever the pair is documented:

- The default over-cap failure mode is a **stalled window ending in `Timeout`**, not a fast
  transient `MaxAckPending`. The fast mode requires the host to opt out with
  `ContextBuilder::backpressure_on_inflight(false)`.
- `NatsPublishError::MaxAckPending` is therefore **unreachable with a default `Context`**. The
  variant stays — the mapping is fixed by ADR 0030's table and a host may opt out — but no
  integration scenario can provoke it without building a non-default context, which is why §7's N5
  does not cover that arm and `tests/publish_error_classification.rs` (U16) asserts its verdict
  directly instead.

Corrected in `crates/reliar-transport-nats/src/settings.rs` (the field rustdoc), contract §4.1 and
§9 item 20. Nothing about the decision changes — only the fact it was documented against.

### Amendment B — 2026-09-05 — the pipeline-depth setting is `batch_pipeline_depth`, not `max_in_flight`

Prompted by the S0/contract review
(`../../../sisa-reliar-backlog/docs/analysis/reviews/phase2-contract-s0-review-1.md`, out-of-scope
item 2) and ruled in scope for Phase 2 by the PO.

**Problem.** Reliar now has two public settings spelled `max_in_flight` that mean different things:

| Setting | Meaning |
|---|---|
| `DispatcherSettings::max_in_flight` | how many claimed rows the outbox dispatcher publishes **concurrently** — a bounded-concurrency budget across tasks (SRS §26) |
| `NatsSettings::max_in_flight` | how many sends a **single** `publish_batch` call issues before it starts awaiting acks — a pipeline depth inside one call (§3 above) |

A host wires both in the same file, from the same environment, often on the same screen. The names
are identical; the units, the scope and the failure modes are not. That is a configuration hazard
of exactly the kind ADR 0019's settings pattern exists to avoid, and the crate is unreleased and
pre-1.0, so it costs one mechanical rename now and a breaking change later.

**Decision.** `NatsSettings::max_in_flight` becomes **`batch_pipeline_depth`**, in one change:

| Was | Is |
|---|---|
| `NatsSettings::max_in_flight` (field) | `NatsSettings::batch_pipeline_depth` |
| `NatsSettings::max_in_flight(…)` (builder) | `NatsSettings::batch_pipeline_depth(…)` |
| `{prefix}MAX_IN_FLIGHT` (env key) | `{prefix}BATCH_PIPELINE_DEPTH`, conventionally `RELIAR_NATS_BATCH_PIPELINE_DEPTH` |
| `NatsConfigError::ZeroInFlight` | `NatsConfigError::ZeroBatchPipelineDepth` |

The default (64), the semantics, the `≤ max_ack_inflight` constraint and every classification are
unchanged — this renames, it does not redesign. `DispatcherSettings::max_in_flight` keeps its name:
it is the older, published-contract one, its meaning is the one "in flight" ordinarily carries, and
renaming it would touch Phase 1's frozen surface for a Phase 2 problem.

**Alternatives.** *`batch_window`* (the reviewer's suggestion) — rejected: "window" is already this
design's word for the unit of work ("one `publish_timeout` bounds one window"), and a name ending
in `_window` sitting next to a `Duration`-typed `publish_timeout` reads as a duration.
*`acks_in_flight`* — rejected: it keeps the very phrase that collides. *Leave it and document the
difference* — rejected: a comment does not survive a host skimming two config structs, and the
window to rename without a semver event closes at 0.1.0.

**Consequences.** Contract §4.1/§4.2/§9 (#8, #20, #23) carry the new name; `settings.rs`,
`publisher.rs`, `tests/nats/n4_*`, `tests/nats/n7_*`, `tests/settings_*`, the crate README and
`docs/guides/nats.md` are mechanical follow-through, owned by the engineer holding the crate.
`NatsSettings::from_env` **ignores** a leftover `{prefix}MAX_IN_FLIGHT` rather than rejecting it —
deliberately, and only because nothing was ever released under that key: `from_env` overrides *only*
the variables it names that are present in the environment (contract §4.1, ADR 0019), so it has no
notion of an unknown or retired key, and gaining one for a single pre-release rename would turn
every `*Settings::from_env` in the workspace into an environment validator. A host that had set the
old key silently gets the default 64 — acceptable precisely because no such host can exist yet, and
it needs no change beyond the key list updated above. No `CHANGELOG` entry beyond *Unreleased*:
nothing was published under the old name.
