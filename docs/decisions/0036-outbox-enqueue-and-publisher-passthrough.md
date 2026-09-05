# 0036 — The call site names the guarantee: `OutboxPublisher::enqueue` + a pass-through `Publisher`

- **Status:** Accepted 2026-09-05, **amended 2026-09-06 (Amendment A — the capability is
  `OutboxEnqueue::enqueue`, not `OutboxStaging::stage`)**. **Supersedes [ADR
  0033](0033-outbox-routing-publisher.md)** in full (0033 shipped in `reliar-outbox` 0.3.0 /
  `reliar-store-postgres` 0.3.0, so this is a successor record, not an amendment — `README.md`'s
  ship test). This record itself has **not** shipped, so it is amended in place.
- **SRS:** §7, §12, §18, §19.4, §19.6, §20, §20.2, §21, §22, §23, §33, §36 (baseline v1.1.9; the
  PO's v1.2.0 amendment applies this decision to the document).
- **Contract:** `../architecture/outbox-publisher-contract.md`
- **Story:** `$BACKLOG_DIR/docs/stories/RELIAR-53-outbox-enqueue-and-publish.md` (D1–D10);
  decision #33 in `$BACKLOG_DIR/docs/analysis/decisions-2026-09-03.md`.
- **Cards:** RELIAR-54 (this record + the contract), RELIAR-55 (`reliar-outbox`), RELIAR-56
  (provider tests, system tests, examples, guides).

---

## Context

ADR 0033 shipped a **routing rule**: `OutboxSettings.enabled` / `allowed_types` /
`disallowed_types` decided, per message type, whether a publish was staged in the caller's
transaction (durable, at-least-once through the dispatcher) or sent straight to the transport (one
attempt, no Reliar guarantee). The call site was one `publish` either way; that was the selling
point — "the routing decision is invisible at the call site" (SRS §20.2, v1.1.9).

On 2026-09-05, after 0.3.0 was on crates.io, the human withdrew the rule. The reasoning, developed
with the PO and recorded as decision #33:

**A message's durability was decided by configuration while the call site looked identical.** That
single property produced every symptom below.

1. **The call site lied.** `published.publish(&e).await?` is either "this is atomic with my
   database write and will be delivered at least once" or "this is on the wire now and gone if I
   roll back" — two guarantees a reader cannot tell apart, in either direction. Neither the
   compiler nor a review can catch a call site that assumed the wrong one.
2. **An operator could weaken a guarantee by editing an environment variable.** `RELIAR_OUTBOX_
   DISALLOWED_TYPES=orders.created` turned a business event into a fire-once send with no code
   change, no deploy, and no signal at the call site. Durability is a design property of the code
   that emits the event; it is not a deployment knob.
3. **The direct path published to the broker inside an open database transaction.** With the
   scoped publisher borrowing `tx`, a disallowed type was sent while that transaction was still
   open. That is: phantom events when the transaction rolls back; consumers reading an event
   before the row it describes is committed; the DB write's latency and availability coupled to
   the broker's; a transaction held open across network I/O (SRS §21's own prohibition, honoured
   by the dispatcher and broken here); order inversion between a staged type and a direct type;
   and a `publish_batch` that could be half-sent and half-staged.
4. **The ceremony.** `outbox.in_transaction(&mut tx)` existed only to give the rule somewhere to
   read a transaction from. A developer had to remember it for a call whose *own* semantics might
   not need a transaction at all, and `publish_direct` existed as the escape hatch for call sites
   that had none — a third method, whose only job was to fail loudly.
5. **The surface cost.** `OutboxPolicy`, `RouteKind`, `MessageTypeNames`, `ScopedOutboxPublisher`,
   `RouteError`, `DirectPublishError`, three settings fields with cross-field validation on four
   construction paths, three environment keys, a metrics label and a span field — all of it
   machinery for a decision the call site can make for free by calling a different method.

The alternatives were assessed and rejected before this one was chosen (see *Alternatives
considered*). The rule the human chose instead is one sentence:

> **The call site names the guarantee.**

---

## Decision

> **Read the names below through [Amendment A](#amendment-a--the-capability-is-outboxenqueueenqueue-2026-09-06)
> (2026-09-06).** Every `OutboxStaging<Tx>` in this section is the trait now called
> `OutboxEnqueue<Tx>`, every `stage` its method now called `enqueue`, and "staging" the operation
> now called *enqueueing*. The original wording is kept as the record of what was accepted on
> 2026-09-05; nothing else in it changed.

### 1. Two operations, two names, two guarantees

`reliar_outbox::OutboxPublisher<S, P, M = NoopMetrics>` keeps its name and its composition (a
staging capability `S`, a transport publisher `P`, a metrics sink `M`) and exposes exactly two
things:

- **`enqueue(&mut tx, &envelope)`** (and `enqueue_batch`) — stages the serialized envelope in the
  **caller's** transaction through `S: OutboxStaging<Tx>`. Durable, atomic with the business
  write, published later by `OutboxDispatcher` with SRS §22's at-least-once guarantee and its
  documented duplicate windows. **It requires the transaction by type**: there is no
  transaction-less staging call, so "stage without atomicity" is unrepresentable rather than
  merely discouraged.
- **`impl reliar_core::Publisher`** — `publish` / `publish_batch` forward to `P`, **byte-identical
  and unconditional**. Sent now, one attempt, no Reliar durability, no retry, no backoff, no dead
  state, no duplicate window. The store is never touched on this path.

Nothing decides between them at runtime. The two operations have different names, different
argument lists and different documented guarantees, and the compiler enforces the difference:
`enqueue` does not typecheck without a transaction.

### 2. `OutboxPublisher` **is** a `reliar_core::Publisher` — and that is now safe

ADR 0033 §4 forbade a `Publisher` impl on the un-scoped publisher because a `'static`, cloneable
`Publisher` can be wired into `OutboxDispatcher` as the dispatcher's own publisher, and the
dispatcher would then drain the outbox back into itself in a loop. That hazard is **gone by
construction**, not by a bound:

`Publisher::publish` forwards to `P` and has no path to `S`. Staging requires a `&mut Tx`
argument, and `publish`'s signature has none — `OutboxPublisher` holds no pool, no connection and
no transaction it could manufacture one from, only `S`, `P` and `M`. A dispatcher wired with an
`OutboxPublisher` is therefore exactly a dispatcher wired with `P` plus a no-op wrapper: legal,
harmless, and pointless. The cycle is unrepresentable because the *code path does not exist*,
which is a stronger guarantee than 0033's lifetime trick and costs no type-level ceremony.

Consequence: `ScopedOutboxPublisher` and `in_transaction` are deleted. Application code written
against `reliar_core::Publisher` accepts an `OutboxPublisher` directly, with the honest meaning —
"this sends to the transport" — and code that wants durability calls `enqueue` by name.

### 3. Both error types are **transparent**

- `Publisher::Error = P::Error`. No wrapper.
- `enqueue` returns `<S as OutboxStaging<Tx>>::Error`. No wrapper.

A wrapper would be a newtype whose only job is to be unwrapped: each path has exactly one
collaborator and exactly one failure mode. Transparency buys three concrete things. `Classify`
forwarding is not written at all, so it cannot be written wrongly — `P::Error: Classify` is
already required by `reliar_core::Publisher` and `S::Error: Classify` by `OutboxStaging`.
`source()` chains stay one link shorter, so a host's error reporter sees the transport's own error
rather than a Reliar variant wrapping it. And a host that swaps `NatsPublisher` for
`OutboxPublisher<_, NatsPublisher>` in generic code sees **the same error type** — the pass-through
is a pass-through in the error channel too, which is what makes "byte-identical forwarding"
believable.

The admitted cost: introducing a variant later is a breaking change. Accepted, because there is no
plausible second failure mode left — the policy is gone and, by the human's "caller serializes
once" rule, no serializer will ever live on this path (§4).

`enqueue_batch` is the one exception and gets **one** small type,
`EnqueueBatchError<E> { index, source }` — see §5.

`RouteError` and `DirectPublishError` are removed. Net: two error types deleted, one added.

### 4. The caller still serializes once

Unchanged from 0033 Amendment D §3, and now the *only* rule: both operations take a
`SerializedEnvelope`. `reliar-outbox` holds no `Serializer` on this path, names no wire format,
and never rewrites `metadata.delivery.content_type`. A message enqueued today and published
directly tomorrow is byte-identical, because the bytes were produced by the caller before either
call.

This is also why `enqueue` takes `&SerializedEnvelope` and **not** a typed `Envelope<T>` + a
serializer. The typed path already exists where it belongs: `PostgresOutboxStore::enqueue<T:
Message>` / `enqueue_with`, an inherent provider method that owns a `Serializer` (SRS §19.6). A
second typed entry point on `OutboxPublisher` would put a serializer back into `reliar-outbox`,
give one crate two ways to spell the same insert, and let the bytes depend on which one the caller
picked.

### 5. `enqueue_batch` fails fast and names the position

```rust
pub async fn enqueue_batch<Tx>(&self, tx: &mut Tx, envelopes: &[SerializedEnvelope])
    -> Result<(), EnqueueBatchError<<S as OutboxStaging<Tx>>::Error>>
```

Not `Vec<Result<…>>`. `Publisher::publish_batch` is positional because each publish is an
independent send with its own verdict; a batch of `stage` calls is **not** independent — every row
lands in one transaction whose fate is decided by the caller's `commit`, and a staging failure
typically aborts that transaction (§6), voiding every earlier `Ok`. A `Vec` of mostly-`Ok` where
nothing is durable is not a positional result, it is a misleading one; 0.3.0's contract needed a
paragraph to explain that a positional `Ok` was not durability. Fail-fast with the failing index
says the true thing in the type: the batch went into the transaction, or it did not and here is
the envelope that stopped it.

Staging is sequential and order-preserving, and the error's `index` is the position in
`envelopes`. `EnqueueBatchError` forwards `Classify` to its source.

> **Dated note — 2026-09-06 (clarification, no behaviour change; ratifying the test matrix).**
> "Voiding every earlier `Ok`" above is a property of the *transaction*, not of `enqueue_batch`:
> the batch itself only fails fast and names the index. It therefore cannot be asserted against an
> in-memory fake, which has no transaction to abort and will honestly still hold the first *k*
> rows — asserting their absence there would assert the fake, not the contract. The split is:
> the **fake** proves fail-fast, the index, and that the tail was never attempted (contract §9 E6);
> **Postgres** proves that the earlier write is discarded when the aborted transaction is rolled
> back (contract §9 E18). Both halves are required; neither is optional.

`OutboxStaging` gains **no** `stage_batch` (post-amendment A: `OutboxEnqueue` gains no
trait-level `enqueue_batch`). A multi-row `INSERT … UNNEST` would be the reason to
add one; nothing measures a need today, and adding it later with a default body that loops `stage`
is additive.

### 6. `OutboxStaging<Tx>` is kept unchanged

> **Amendment A (2026-09-06) renames this section's subject** — the trait is `OutboxEnqueue<Tx>`
> and its method is `enqueue`, in module `enqueue`. Everything this section decides is unchanged;
> read the names through the amendment.

Same trait, same `stage(&self, &mut Tx, &SerializedEnvelope) -> impl Future<Output =
Result<MessageId, Self::Error>> + Send`, same bounds, same documented invariants — including the
one that matters most and carries over verbatim from 0.3.0 (contract §3): **an `Err` MAY leave
`tx` unusable**, so a caller treats any staging error as *abort this transaction*, and with
`reliar-store-postgres` the transaction **is** aborted by PostgreSQL. It is the withdrawal's one
survivor because it never encoded the rule — it is the provider-portable "stage a row in a
transaction I do not name" capability, and it is exactly what `enqueue` needs.

`PostgresOutboxStore`'s inherent typed `enqueue<T: Message>` / `enqueue_with` are likewise
unchanged and stay the typed path (§4).

### 7. Settings: three fields, three env keys and one newtype are removed

`OutboxSettings` loses `enabled`, `allowed_types`, `disallowed_types`, their builder setters, the
`MessageTypeNames` newtype and the `check_disjoint` cross-field rule. `dispatcher` and `retention`
are untouched, as are `Default`, the builder, serde and `from_env` for them.

Two consequential details:

- **serde stays `deny_unknown_fields`.** A config document that still carries `enabled = false`
  now fails to deserialize, naming the field. That is deliberate: silently ignoring a retired
  *durability* key would recreate this ADR's own failure mode in the mirror — an operator who
  believes the outbox is off while every message is durable. A loud error at startup is the
  migration signal. (With the three fields gone the `OutboxSettingsRepr` + `TryFrom` indirection
  has no validation left to perform and is deleted; `OutboxSettings` derives `Deserialize`
  directly with `#[serde(default, deny_unknown_fields)]`, matching `DispatcherSettings` and
  `RetentionSettings`. Its hand-written `Default` — which existed only to keep `enabled: true` —
  becomes a derive.)
- **`from_env` does not read the retired keys and does not reject them.** `RELIAR_OUTBOX_ENABLED`
  set in an environment is inert. The asymmetry with serde is justified, not an oversight: a
  config document is a closed set, so an unknown key is detectable; the environment is an open
  namespace shared with the whole process, so rejecting a retired key requires maintaining a
  permanent deny-list that could also collide with a host's own variable.

### 8. Observability

- **`enqueue`** runs under `debug_span!("reliar.outbox.enqueue", message.id, message.type)`.
- **`enqueue_batch`** runs under `debug_span!("reliar.outbox.enqueue_batch", batch.size)`, and
  each envelope's `reliar.outbox.enqueue` span nests inside it.
- **`publish` / `publish_batch` open no span.** They add no behaviour, so a span there would
  report only that a forward happened; the transport publisher owns its own instrumentation and
  the dispatcher already spans the claim/publish loop. An outbox span on a path that never touches
  the outbox would also be actively misleading in a trace.
- **`OutboxMetrics::routed(RouteKind, &MessageType)` is replaced by `enqueued(usize,
  &MessageType)`**, called once per successfully staged envelope with `n = 1`, mirroring the
  existing `published(1, &message_type)` call shape in the dispatcher. Labels stay bounded;
  `RouteKind` disappears with the rule. `publish` records **no** metric — the same sink is
  routinely wired to the dispatcher, and counting a forward as a publish would double-count.

Payloads, header values and high-cardinality ids other than `message.id` on a span remain
forbidden (SRS §33, ADR 0020).

### 9. Versions (ADR 0034)

| crate | from | to | why |
|---|---|---|---|
| `reliar-outbox` | 0.3.0 | **0.4.0** | public items removed |
| `reliar-store-postgres` | 0.3.0 | **0.4.0** | its `reliar-outbox` requirement leaves `^0.3`; its rustdoc names the rule |
| `reliar-core` | 0.2.0 | 0.2.0 | no item, no doc, no dependency change |
| `reliar-transport-nats` | 0.1.1 | 0.1.1 | untouched |

Root `[workspace.dependencies]` pins for the two bumped crates move with them; `tests/system` and
`examples/*` are `publish = false` and follow through the pins. `CHANGELOG.md` *Unreleased* gains
**Removed** / **Changed** / **Added** sections plus a 0.3 → 0.4 migration note.

---

## Consequences

**Good**

- A reader of a call site knows the guarantee without opening a config file, and a reviewer can
  see a durability mistake in the diff.
- No configuration can weaken a guarantee. Nothing in `OutboxSettings` decides durability any more.
- The direct-publish-inside-an-open-transaction hazard is gone: `publish` needs no transaction and
  a host publishes before opening or after committing one. SRS §21's "no network I/O while holding
  a transaction" is restored on the application path, not just the worker path.
- `OutboxPublisher` is a plain `reliar_core::Publisher`, so generic host code, test doubles and
  the dispatcher accept it with no scoped-borrow dance.
- Net **smaller** public surface than 0.3.0: seven public items and three environment keys removed,
  one error type and one metrics hook added.

**Bad / accepted**

- **A breaking change on a published crate**, one day after 0.3.0. Every 0.3.0 call site changes:
  `outbox.in_transaction(&mut tx).publish(&e)` → `outbox.enqueue(&mut tx, &e)`, and
  `outbox.publish_direct(&e)` → `outbox.publish(&e)`. `MessageTypeNames`, `OutboxPolicy` and the
  three settings fields have no replacement — that is the point.
- A deployment that used `RELIAR_OUTBOX_DISALLOWED_TYPES` to keep a type off the outbox must change
  code, not configuration. That is the intended cost.
- A stale config document fails at startup rather than degrading. Intended (§7).
- Transparent errors mean adding a Reliar-side failure mode to either path later is breaking (§3).
- 0.3.0 remains on crates.io with the routing rule and its documentation. ADR 0033 keeps its text
  as the record of what that version shipped.

**Neutral**

- `reliar-outbox` still names no SQLx or PostgreSQL type: the transaction reaches it only as the
  opaque parameter `Tx`.
- **Nothing moves into `reliar-core`** (SRS §18's kind test): `OutboxPublisher`, `OutboxStaging`,
  `EnqueueBatchError` and `OutboxMetrics::enqueued` all encode *how the outbox capability works* —
  they are outbox mechanics, not vocabulary two capabilities need to talk to each other.
  `reliar-core` gains no item and no doc change, and is not bumped.

---

## Alternatives considered

Each was assessed with the PO on 2026-09-05 and rejected by the human (decision #33).

- **Keep the rule as shipped (ADR 0033).** Rejected: the Context is the argument. No amount of
  documentation makes two guarantees behind one call site readable.
- **Keep the rule, but make the direct path defer until after commit.** Fixes the phantom-event
  and open-transaction symptoms, and introduces an **at-most-once** window in their place — a
  message the caller believes was sent, lost if the process dies between commit and send. Trading
  a documented at-least-once for an undocumented at-most-once is a worse guarantee, not a better
  one.
- **Ambient transaction scope (task-local `tokio::task_local!`).** One `publish` that finds the
  caller's transaction implicitly. Rejected: runtime scoping and hidden data flow — the failure
  mode is a publish that silently takes the wrong path because a caller forgot to enter the scope,
  and it is invisible in the type system and in review.
- **A request-scoped unit-of-work object.** Same hidden data flow, plus a framework-shaped
  abstraction Reliar has no business owning (SRS §3.12 — no DI container).
- **An optional `tx` argument on `publish`** (`publish_in(Option<&mut Tx>, …)`), on
  `OutboxPublisher` or on `reliar_core::Publisher`. On core it taxes every transport with a
  storage concept it cannot use (SRS §18). On the outbox it re-creates the lie: the same method
  with two guarantees, now selected by an argument that is easy to pass as `None`.
- **A pool-owned fallback transaction** — the publisher begins its own transaction when the caller
  has none. Rejected: it hides a pool checkout inside a publish (deadlock under load when the
  caller already holds the pool's last connection) and hints at an atomicity with the caller's
  work that does not exist.
- **Rename `publish` to something scary** (`publish_unsafe`, `publish_now_bypassing_outbox`) and
  keep the rule. Rejected: naming is not a guarantee, and it does not address configuration
  deciding durability.
- **`Vec<Result<…>>` for `enqueue_batch`**, mirroring `publish_batch`. Rejected in §5: positional
  `Ok`s are not independent verdicts inside one transaction.
- **A wrapper error on the publish path**, for future room. Rejected in §3: no plausible variant
  exists, and the wrapper costs `Classify` forwarding, a longer `source()` chain, and the type
  identity that makes the pass-through substitutable.

---

## Amendment A — the capability is `OutboxEnqueue::enqueue` (2026-09-06)

The human (decision #34, `$BACKLOG_DIR/docs/analysis/decisions-2026-09-03.md`; card RELIAR-58)
renamed the write-side capability before 0.4.0 shipped. This record is unshipped, so it is amended
in place rather than superseded (`README.md`'s ship test).

### A.1 What changes

| 0.3.0 / this ADR as accepted | from 0.4.0 |
|---|---|
| trait `OutboxStaging<Tx>` | **`OutboxEnqueue<Tx>`** |
| method `stage` | **`enqueue`** |
| module `crates/reliar-outbox/src/staging.rs` | **`crates/reliar-outbox/src/enqueue.rs`** |

**Nothing else moves.** Same shape —
`fn enqueue(&self, tx: &mut Tx, envelope: &SerializedEnvelope) -> impl Future<Output =
Result<MessageId, Self::Error>> + Send` — same `Tx` type parameter, same associated
`type Error: std::error::Error + Send + Sync + 'static + Classify`, same returned `MessageId`, and
the same documented invariants word for word: persist `content_type` verbatim, no network I/O
beyond the statement, never commit or roll back the caller's transaction, and **an `Err` MAY leave
`tx` unusable** — callers treat any error from it as *abort this transaction*, and with
`reliar-store-postgres` PostgreSQL does abort it. §5's ruling stands under the new name: no
trait-level batch method.

### A.2 Why

"Staging" was jargon that appeared nowhere else in the vocabulary a host reads — SRS §19.6, the
provider's inherent `enqueue<T>` and this ADR's own headline all say *enqueue*. It also gave **one
durable path two verbs**: the host called `OutboxPublisher::enqueue`, which called `stage`, which
inserted a row a doc comment then called "staged". A reader had to learn that `stage` and `enqueue`
are the same operation at two levels of the same call. One path, one verb, and the trait now reads
as the capability the method needs: *the thing that can enqueue into a transaction*.

The rename adds no migration cost: `OutboxStaging` was published in 0.3.0, so the 0.3 → 0.4 break
already decided here simply gains one more row (contract §7), and `cargo semver-checks` reports the
trait and its method as removed alongside the routing items. A host upgrading from 0.3.0 was
already changing every call site on this path; renaming a trait it must touch anyway is work it was
doing already.

### A.3 Rejected: merge the capability into `OutboxStore`

Considered and **not** chosen — `OutboxEnqueue` stays a separate trait. `OutboxStore` is the
worker-side claim contract (`acquire`, `complete`, `fail`, `purge`, over a pool, in its own short
transactions); `OutboxEnqueue` is the host-side write contract, called on the request path inside a
transaction the library does not own. SRS §19.6 splits them for that reason, and the merge would
cost:

- **A `Tx` on a bound that never uses one.** `OutboxStore` would become `OutboxStore<Tx>` (or grow
  an associated `type Tx`), so `OutboxDispatcher<S: OutboxStore<Tx>, P>`, every host wiring, every
  fake and every generic helper on the claim path would carry a transaction type for a path that
  has no transaction to name. Where nothing constrains it, `Tx` is simply uninferable and the host
  writes a turbofish to satisfy a parameter it never uses.
- **One transaction type per store.** An associated type fixes exactly one handle per implementor.
  As a separate trait with `Tx` as a *trait* parameter, one store can implement
  `OutboxEnqueue<Transaction<'_, Postgres>>` and, later, `OutboxEnqueue<PgConnection>` or a test
  handle side by side — which the test-support store already does with `InMemoryTransaction`.
- **The GAT variant reopens a known trap.** `type Tx<'a>` on `OutboxStore` has to spell
  `&'a mut Transaction<'c, Postgres>`; that is the invariance / "implementation is not general
  enough" failure ADR 0033 Amendment D §2 escaped precisely by making the transaction a plain type
  parameter of a *separate* capability trait. Re-merging would buy it back.
- **It taxes every custom store.** `OutboxStore` is published (0.2.0) and is what a third party
  implements to run a dispatcher against another database. Merging would force such an
  implementation to provide a host-facing write method — and would force a host that only wants
  `enqueue` to implement `acquire`/`complete`/`fail`/`purge`.

Two small traits with one obligation each is also SRS §3's rule (no God trait). The single
argument *for* the merge — "one store type, one trait to implement" — is answered by the fact that
`PostgresOutboxStore` implements both anyway, and by `OutboxPublisher`'s method-level bound, which
requires `OutboxEnqueue` only where the durable path is actually used.

### A.4 Blast radius

Names only, all inside the 0.3 → 0.4 break already decided here: `crates/reliar-outbox`
(`src/enqueue.rs`, `publisher.rs`, `lib.rs` re-exports, `test_support`, the `enqueue_*` tests),
`crates/reliar-store-postgres` (the impl and its test file), `CHANGELOG.md`'s **Removed** and
migration table, the guide, and the contract
(`../architecture/outbox-publisher-contract.md` §1, §2, §4, §5, §7–§12, amended the same day).
`reliar-core` still gains nothing; the version plan in §9 is unchanged.

One consequence worth stating: on `PostgresOutboxStore` the trait method now shares its name with
the inherent typed `enqueue<T: Message>`. That is intended — same verb, same concept, one typed and
serializer-owning, one taking bytes — and Rust resolves the inherent method first, so a host with a
concrete store and a `SerializedEnvelope` uses `OutboxPublisher::enqueue` (the normal path) or
spells `OutboxEnqueue::enqueue(&store, &mut tx, &envelope)`. Both rustdocs must open by saying
which is which.

---

## Open

- A typed convenience on `OutboxPublisher` (`enqueue_typed<T: Message>` with a `Serializer`) stays
  **out** (§4). If a host asks for one, it belongs on the provider store beside the existing
  `enqueue<T>`, not here.
- A trait-level batch method on the capability — `OutboxStaging::stage_batch` as written here,
  `OutboxEnqueue::enqueue_batch` after Amendment A (a provider-side multi-row insert) — is additive
  when a measurement justifies it (§5).
- The ambient/request-scoped unit of work may return as its own story **on top of** `enqueue` —
  RELIAR-51 was closed as superseded, not as wrong forever. It would be sugar over an explicit
  call, never a second way to decide durability.
