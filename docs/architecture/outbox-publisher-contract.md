# Outbox publisher contract — `reliar-outbox` + `reliar-store-postgres` (v0.4)

**Status: FROZEN for RELIAR-55 / RELIAR-56 — 2026-09-05, decided by [ADR
0036](../decisions/0036-outbox-enqueue-and-publisher-passthrough.md).** Amended 2026-09-06 by that
ADR's *amendment A*: the write-side capability is `OutboxEnqueue<Tx>::enqueue`, renamed from
`OutboxStaging::stage` (§4). Every signature below is what the engineer builds. **Changing anything
here requires an ADR first**, then an update to this file, then a notification to everyone building
against it.

Replaces `routing-publisher-contract.md` (the frozen surface of `reliar-outbox` 0.3.0, withdrawn
with ADR 0033). Sources: story `$BACKLOG_DIR/docs/stories/RELIAR-53-outbox-enqueue-and-publish.md`
(D1–D10), decision #33, SRS §7, §12, §18, §19.4, §19.6, §20, §20.2, §21, §22, §23, §33, §36
(baseline v1.1.9; the PO's v1.2.0 amendment applies the same decision to the document).

> **The rule, in one line.** The **call site names the guarantee**. `enqueue` writes into the
> caller's transaction and is durable; `publish` forwards to the transport and is not. Nothing
> chooses between them at runtime, and no setting can.

`phase1-contract.md` and `phase2-contract.md` still govern everything they cover; their
"conventions that apply to everything below" preamble applies here verbatim and is not repeated:
rustdoc on every public item, `#[non_exhaustive]`, `impl Future + Send` in trait *declarations* and
never `async fn` there, hand-rolled errors with `source()`, a `Debug` that never prints payloads or
header values.

Four rules are specific to this slice. Each is a **blocker finding** if broken:

- **`publish` never touches the store.** No branch, no condition, no "unless". It forwards to `P`
  and nothing else. A path from `Publisher::publish` to `OutboxEnqueue::enqueue` re-opens the
  dispatcher cycle ADR 0036 §2 closed.
- **`enqueue` does no network I/O** beyond the provider's own statement, and never commits, rolls
  back or otherwise consumes the caller's transaction.
- **Nothing here serializes.** `reliar-outbox` names no wire format and holds no `Serializer` on
  this path; both operations carry the caller's `SerializedEnvelope` value unchanged. Re-serializing
  or overwriting `metadata.delivery.content_type` is a blocker.
- **Neither operation retries or sleeps.** They run on the host's request path, `enqueue` with the
  host's transaction open; a retry there would hold a database transaction across I/O (SRS §21).

---

## 1. Where it lives

```
reliar-store-postgres ──▶ reliar-outbox ──▶ reliar-core
   impl OutboxEnqueue<Transaction<'_, Postgres>>   Publisher, SerializedEnvelope, MessageId,
   inherent enqueue<T> / enqueue_with<T>           Classify, FailureKind, SettingsError
                            OutboxPublisher  ── impl reliar_core::Publisher (pass-through)
                            OutboxEnqueue<Tx>, EnqueueBatchError
```

No new crate, no new dependency. `reliar-core` gains **no item and no doc change** and is not
bumped. `reliar-outbox` still names no sqlx/postgres/broker type — the caller's transaction reaches
it only as an opaque type parameter `Tx`.

`tokio::sync::Mutex` is no longer needed on this path (it existed only to reborrow the transaction
inside `ScopedOutboxPublisher`); it stays a dependency because the dispatcher uses it.

---

## 2. `OutboxPublisher` — `reliar-outbox`, module `publisher`

```rust
/// The application's outbox handle: **`enqueue` is the durable path; `publish` bypasses the
/// outbox entirely** and forwards straight to the transport.
///
/// - [`Self::enqueue`] writes the envelope into the caller's own transaction. It becomes visible
///   when the caller commits and is published later by an [`crate::OutboxDispatcher`]: durable,
///   at-least-once, with the duplicate windows the crate docs list.
/// - The [`reliar_core::Publisher`] impl sends **now**, through the transport publisher, one
///   attempt, with no Reliar guarantee at all: no retry, no backoff, no dead state, no duplicate
///   window, and no relationship to any transaction the caller may have open.
///
/// The guarantee is chosen by which method the call site calls. Nothing decides it at runtime,
/// and no setting can (ADR 0036).
pub struct OutboxPublisher<S, P, M = NoopMetrics> {
    store: S,
    publisher: P,
    metrics: M,
}

impl<S: Clone, P: Clone, M: Clone> Clone for OutboxPublisher<S, P, M> { … }   // manual, as today
impl<S, P, M> fmt::Debug for OutboxPublisher<S, P, M> { … }                   // finish_non_exhaustive

impl<S, P> OutboxPublisher<S, P>
where
    P: Publisher,
{
    /// `store` is normally the provider store (the [`OutboxEnqueue`] capability), `publisher`
    /// the transport publisher.
    pub fn new(store: S, publisher: P) -> Self;
}

impl<S, P, M> OutboxPublisher<S, P, M>
where
    P: Publisher,
    M: OutboxMetrics,
{
    /// As [`Self::new`], with a metrics sink (§6).
    pub fn with_metrics(store: S, publisher: P, metrics: M) -> Self;
}
```

**Decided:** no policy parameter, no `policy()` accessor, no `store()`/`publisher()` accessors.
The `Debug` impl now has nothing rule-shaped to print and stays `finish_non_exhaustive()` with no
fields — it must not gain one that names `S` or `P`'s state.

The manual `Clone` stays manual for the same reason as today: a derive would condition on
`M: Clone` even for `NoopMetrics`.

### 2.1 `enqueue` / `enqueue_batch` — the durable path

```rust
impl<S, P, M> OutboxPublisher<S, P, M>
where
    M: OutboxMetrics,
{
    /// Enqueues `envelope` in the caller's transaction `tx` — **the durable path**.
    ///
    /// Atomic with whatever else the caller writes in `tx`: the message exists if and only if the
    /// transaction commits. An [`crate::OutboxDispatcher`] publishes it afterwards with the
    /// crate's at-least-once guarantee (SRS §22).
    ///
    /// The transaction is required **by type**: there is no transaction-less enqueue call, so a
    /// message cannot be enqueued outside the caller's unit of work.
    ///
    /// Issues no network I/O beyond the provider's own statement, never retries, never sleeps,
    /// and never commits, rolls back or otherwise consumes `tx` — the caller owns it throughout.
    ///
    /// Persists `envelope.metadata.delivery.content_type` verbatim: the caller serialized the
    /// body and is authoritative about its content type (SRS §12).
    ///
    /// # Errors
    ///
    /// The provider's enqueue error, unwrapped. **An `Err` MAY leave `tx` unusable**, and whether
    /// it does is the provider's contract ([`OutboxEnqueue::enqueue`]). The portable rule is: treat
    /// any enqueue error as *abort this transaction* — issue no further statement on `tx`, roll it
    /// back, and consider every earlier write in it lost. With `reliar-store-postgres` the
    /// transaction **is** aborted.
    pub async fn enqueue<Tx>(
        &self,
        tx: &mut Tx,
        envelope: &SerializedEnvelope,
    ) -> Result<(), <S as OutboxEnqueue<Tx>>::Error>
    where
        S: OutboxEnqueue<Tx>,
        Tx: Send;

    /// Enqueues `envelopes` in `tx`, in order, one statement each — **the durable path**, batched.
    ///
    /// Sequential and order-preserving. **Fails fast**: the first enqueue failure returns, naming
    /// the position in `envelopes` that failed, and the remaining envelopes are not attempted.
    ///
    /// This returns one result for the whole batch rather than one per envelope on purpose. Every
    /// row lands in the same transaction, so the batch has a single outcome — the caller's
    /// `commit` — and an enqueue failure typically aborts that transaction, voiding every row
    /// written before it. A positional `Ok` would not mean the message is durable (ADR 0036 §5).
    ///
    /// An empty slice is `Ok(())` and issues no statement.
    ///
    /// # Errors
    ///
    /// [`EnqueueBatchError`], carrying the failing index and the provider's error. The same
    /// "treat it as *abort this transaction*" rule as [`Self::enqueue`] applies.
    pub async fn enqueue_batch<Tx>(
        &self,
        tx: &mut Tx,
        envelopes: &[SerializedEnvelope],
    ) -> Result<(), EnqueueBatchError<<S as OutboxEnqueue<Tx>>::Error>>
    where
        S: OutboxEnqueue<Tx>,
        Tx: Send;
}
```

**Decided, with reasons:**

- **`Result<(), _>`, not `Result<MessageId, _>`.** `OutboxEnqueue::enqueue` returns the id it
  wrote; `OutboxPublisher::enqueue` discards it, because it is `envelope.id` — a field the caller
  passed in and already holds. Returning it would be a second way to read the same value. (The
  trait method keeps its return: it is the provider-facing one and changing it is churn with no
  gain — §4.)
- **`&SerializedEnvelope`, never a typed `Envelope<T>` + `Serializer`.** The typed path is
  `PostgresOutboxStore::enqueue<T: Message>` (SRS §19.6, ADR 0036 §4). A second typed entry point
  here would put a serializer back into `reliar-outbox` and let the bytes depend on which call the
  host picked.
- **Bounds live on the methods, not the impl block**, so `S: OutboxEnqueue<Tx>` is required only
  where the durable path is used and one `OutboxPublisher` can serve several transaction-handle
  types.
- `enqueue_batch` is implemented as a loop over the same private helper `enqueue` uses, so the two
  cannot drift and each envelope gets its own span (§6).

### 2.2 The `Publisher` impl — the pass-through

```rust
/// **Bypasses the outbox.** Forwards to the transport publisher, byte-identical, with no Reliar
/// durability: one attempt, no retry, no backoff, no dead state, no duplicate window, and no
/// relationship to any transaction the caller has open — if the caller later rolls back, the
/// message is already on the wire. Use [`OutboxPublisher::enqueue`] for the durable path.
///
/// The [`OutboxEnqueue`] capability is **never** touched here. That is what makes wiring an
/// `OutboxPublisher` into an [`crate::OutboxDispatcher`] safe (ADR 0036 §2): there is no code
/// path from a publish back into the store, so the outbox cannot drain into itself.
impl<S, P, M> Publisher for OutboxPublisher<S, P, M>
where
    S: Send + Sync,
    P: Publisher,
    M: OutboxMetrics,
{
    /// Transparent: the transport publisher's own error, unwrapped. `Classify`, `source()` and
    /// `Display` are the transport's (ADR 0036 §3).
    type Error = P::Error;

    /// **Bypasses the outbox.** Forwards `envelope` to the transport publisher, byte-identical,
    /// with no Reliar durability: one attempt, no retry, no backoff, no dead state, no duplicate
    /// window, and no relationship to any transaction the caller has open — if the caller later
    /// rolls back, the message is already on the wire. Use [`OutboxPublisher::enqueue`] for the
    /// durable path.
    ///
    /// Touches the [`OutboxEnqueue`] capability on no path.
    ///
    /// # Errors
    ///
    /// The transport publisher's error, unwrapped ([`Self::Error`]).
    fn publish(
        &self,
        envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.publisher.publish(envelope)
    }

    /// **Bypasses the outbox**, once per envelope, exactly as [`Self::publish`] does — no
    /// durability, no retry, no relationship to any open transaction.
    ///
    /// Forwarded to `P::publish_batch` rather than inherited, so a transport with a native batch
    /// API keeps it. Results stay positional, one per envelope, in order — `P`'s contract,
    /// unmodified.
    fn publish_batch(
        &self,
        envelopes: &[SerializedEnvelope],
    ) -> impl Future<Output = Vec<Result<(), Self::Error>>> + Send {
        self.publisher.publish_batch(envelopes)
    }
}
```

**Decided:** `publish_batch` is **overridden to forward**, not left to the trait default. The
default would loop over `Self::publish`, silently discarding a transport's native batch
implementation — the opposite of "byte-identical pass-through".

**Decided:** the impl records **no span and no metric** (§6).

**Decided (2026-09-06, closing review finding B1):** `publish` and `publish_batch` each carry their
**own** rustdoc, and each first line opens with **Bypasses the outbox.** (D9 / SRS §20.2 SHALL). The
doc comment on the `impl` block is *not* inherited by the methods: without a `///` of their own,
rendered docs show `reliar_core::Publisher::publish`'s text — "retry is the dispatcher's job" —
on a method that has no dispatcher behind it. An undocumented method in this impl is a blocker.

**The `S: Send + Sync` bound** exists only because `Publisher: Send + Sync` requires the whole
value to be; it does not require `S: OutboxEnqueue<_>`, and must not — the publish path never
writes to the outbox. That absence is the machine-checkable form of "`publish` never touches the
store".

> **Verified before freezing (rustc 1.98, against the real `reliar-core` 0.2.0).** The whole shape
> compiles as written: the forwarding `Publisher` impl with `type Error = P::Error`, both
> `enqueue`/`enqueue_batch` with the method-level `S: OutboxEnqueue<Tx>` bound, and an
> `assert_send` over all three returned futures. No `where` clause beyond the ones printed here is
> needed, and none of the RPITIT capture problems that bit ADR 0033's scoped view arise — there is
> no borrowed transaction in the publisher any more.

---

## 3. `EnqueueBatchError` — the one new type

```rust
/// Which envelope in an [`OutboxPublisher::enqueue_batch`] failed, and why.
#[derive(Debug)]
#[non_exhaustive]
pub struct EnqueueBatchError<E> {
    /// The position in the `envelopes` slice that failed. Envelopes after it were not attempted.
    pub index: usize,
    /// The provider's enqueue error.
    pub source: E,
}

impl<E: fmt::Display> fmt::Display for EnqueueBatchError<E> { … }
// "failed to enqueue the envelope at index {index}: {source}" — never a payload, never a header value

impl<E: std::error::Error + 'static> std::error::Error for EnqueueBatchError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> { Some(&self.source) }
}

/// Forwards to the provider's enqueue error, so a host can classify a batch failure exactly as it
/// classifies a single one. Free, because `OutboxEnqueue::Error: Classify` already.
impl<E: Classify> Classify for EnqueueBatchError<E> { … }
```

`RouteError` and `DirectPublishError` are **removed**; `ConfigError` gains nothing.

---

## 4. `OutboxEnqueue<Tx>` — renamed, otherwise unchanged

**Renamed from `OutboxStaging` (0.3.0) — ADR 0036 amendment A (2026-09-06):** the trait is
`OutboxEnqueue<Tx>`, its method is `enqueue`, and it lives in `crates/reliar-outbox/src/enqueue.rs`.
One durable path, one verb — the host calls `OutboxPublisher::enqueue`, and so does the capability
underneath it.

**Nothing else changes**: same `Tx` type parameter, same method shape
(`fn enqueue(&self, tx: &mut Tx, envelope: &SerializedEnvelope) -> impl Future<Output =
Result<MessageId, Self::Error>> + Send`), same bounds (`type Error: std::error::Error + Send + Sync +
'static + Classify`), same rustdoc invariants — including the `Err` **MAY** leave `tx` unusable
rule, which the engineer carries over verbatim. Its module doc's ADR reference is retouched
(0033 Amendment D → 0036 §6 + amendment A), and any sentence naming a "routed half",
`ScopedOutboxPublisher` or "staging" is rewritten to name `OutboxPublisher::enqueue`.

The trait gains **no** batch method of its own (no trait-level `enqueue_batch`; ADR 0036 §5 — a
provider-side multi-row insert stays additive).

It stays a **separate trait from `OutboxStore`**, deliberately: merging the two would put a `Tx`
parameter (or a GAT) on the trait that every dispatcher bound, every custom store and every fake
already implements — a transaction type on the claim side, which never sees one. ADR 0036
amendment A records the alternatives and what each costs.

---

## 5. Provider — `reliar-store-postgres`, renamed impl, unchanged behaviour

- The `impl<'c, Ser> OutboxEnqueue<Transaction<'c, Postgres>> for PostgresOutboxStore<Ser>` stays
  exactly as shipped in 0.3.0 apart from the trait/method names: `type Error =
  EnqueueError<Infallible>`, `content_type` written verbatim from the envelope, the private
  `insert_enqueued` helper shared with the typed path, no commit, no rollback.
- The inherent typed `enqueue<T: Message>` / `enqueue_with<T: Message>` **stay** and remain the
  typed path (they own the `Serializer`; SRS §19.6).
- **The name now collides with the inherent typed method, on purpose.** `PostgresOutboxStore` has
  both an inherent `enqueue<T: Message>(&mut tx, &Envelope<T>)` (owns the `Serializer`, SRS §19.6)
  and the trait `OutboxEnqueue::enqueue(&mut tx, &SerializedEnvelope)`. Same verb, same concept,
  two argument types. Rust resolves the **inherent** method first, so a host holding a concrete
  `PostgresOutboxStore` and a `SerializedEnvelope` reaches the trait method through
  `OutboxPublisher::enqueue` (the normal path) or by spelling
  `OutboxEnqueue::enqueue(&store, &mut tx, &envelope)`. Both rustdocs must say which is which in
  their first line.
- **No migration, no SQL-text change, no `.sqlx/` change.** If the engineer's diff touches any of
  the three, something is wrong with the diff.
- Rustdoc that refers to `ScopedOutboxPublisher`, "the routed path", "staging" or
  "route-independent bytes" is rewritten to name `OutboxPublisher::enqueue`; the substantive
  sentence — *this path persists the caller's `content_type`, unlike the inherent `enqueue`* —
  survives unchanged and is still the one semantic difference between the two.

---

## 6. Observability

| operation | span | fields |
|---|---|---|
| `enqueue` | `debug_span!("reliar.outbox.enqueue")` | `message.id`, `message.type` |
| `enqueue_batch` | `debug_span!("reliar.outbox.enqueue_batch")` | `batch.size` |
| `publish` / `publish_batch` | **none** | — |

- `enqueue_batch`'s per-envelope `reliar.outbox.enqueue` spans nest inside its own span, because
  the batch calls the same helper; a subscriber therefore sees both the batch shape and each
  message id.
- The span wraps the whole call, so the provider's own spans nest under it.
- No event on success; the publisher never logs an error it also returns. Never a payload, a header
  value, a tenant id or a connection string.
- **`publish` opens no outbox span on purpose.** It adds no behaviour; a `reliar.outbox.*` span on
  a path that never touches the outbox would be misleading in a trace, and the transport publisher
  already instruments itself.

Metrics hook (`OutboxMetrics`, ADR 0020):

```rust
/// Called once per envelope successfully enqueued through [`crate::OutboxPublisher::enqueue`] or
/// [`crate::OutboxPublisher::enqueue_batch`], with `n = 1` — the same call shape as
/// [`Self::published`]. Labels stay bounded: `message_type` is already an accepted label.
fn enqueued(&self, _n: usize, _message_type: &MessageType) {}
```

`fn routed(&self, RouteKind, &MessageType)` is **removed** with `RouteKind`. The `Publisher` impl
calls **no** hook: the same sink is routinely wired to the dispatcher, which already counts
`published`, and counting a forward there would double-count.

---

## 7. Removals — the complete list (one place, checked by `cargo semver-checks`)

`reliar-outbox` public surface:

| removed | replacement |
|---|---|
| `OutboxPolicy` (whole type, module `policy`) | none — the rule is withdrawn |
| `RouteKind` | none |
| `ScopedOutboxPublisher` | none — `OutboxPublisher` is itself the `Publisher` |
| `OutboxPublisher::in_transaction` | `OutboxPublisher::enqueue(&mut tx, &e)` |
| `OutboxPublisher::publish_direct` | `Publisher::publish(&e)` |
| `OutboxPublisher::policy` | none |
| `OutboxPublisher::new`'s third parameter | two-argument `new(store, publisher)` |
| `OutboxPublisher::with_metrics`'s third parameter | three-argument `with_metrics(store, publisher, metrics)` |
| `OutboxStaging` (trait, module `staging`) | `OutboxEnqueue`, module `enqueue` (ADR 0036 amendment A) |
| `OutboxStaging::stage` | `OutboxEnqueue::enqueue` |
| `RouteError` | `enqueue` returns the provider's enqueue error directly |
| `DirectPublishError` | `publish` returns `P::Error` directly |
| `OutboxSettings::enabled` (field + setter) | none |
| `OutboxSettings::allowed_types` (field + setter) | none |
| `OutboxSettings::disallowed_types` (field + setter) | none |
| `MessageTypeNames` (whole type) | none |
| `OutboxMetrics::routed` | `OutboxMetrics::enqueued` |
| `RecordingMetrics::routed` (test-support accessor) | `RecordingMetrics::enqueued` |
| env `RELIAR_OUTBOX_ENABLED` / `_ALLOWED_TYPES` / `_DISALLOWED_TYPES` | none — not read, not rejected |

Internal, not public surface but deleted with them: `policy::check_disjoint`, the whole `policy`
module, `settings::OutboxSettingsRepr` + its `TryFrom` + `default_enabled_true`.

**Config-document behaviour:** `OutboxSettings` deserialization keeps `deny_unknown_fields`, so a
document still carrying `enabled` / `allowed_types` / `disallowed_types` **fails to deserialize**,
naming the offending field. Deliberate (ADR 0036 §7) — a retired durability key must never be
silently ignored. `from_env` neither reads nor rejects the retired keys: an environment is an open
namespace, so a retired-key deny-list would be a permanent tax that could collide with a host's own
variable.

`OutboxSettings` keeps `dispatcher` and `retention` with their `Default`, builder, serde and
`from_env` behaviour **unchanged**. With `enabled` gone, its hand-written `Default` (which existed
only to force `enabled: true`) becomes a derive, and the struct derives `Deserialize` directly with
`#[serde(default, deny_unknown_fields)]` like its two nested structs.

---

## 8. `test-support` — `reliar-outbox`

- `InMemoryTransaction`, `InMemoryOutboxStore`'s `OutboxEnqueue<InMemoryTransaction>` impl,
  `fail_next_enqueue`, `enqueue_call_count`, `RecordingPublisher`, `ScriptedPublisher`: **kept
  unchanged**. They were never rule-shaped.
- `RecordingMetrics`: `routed()` accessor and hook impl → `enqueued()`, recording
  `(usize, MessageType)` pairs.
- No new fake is needed. The D3 cycle test needs only the existing `InMemoryOutboxStore` used as
  *both* the dispatcher's store and the publisher's [`OutboxEnqueue`] capability.

---

## 9. Test matrix (RELIAR-55 / RELIAR-56; the `reviewer` audits it)

Ids are new (`E1…`) — the 0.3.0 `R*` ids died with the rule. The **AC** column cites the story's
D1–D10 (which the PO's SRS v1.2.0 §43.D adopts).

| id | kind | crate / file | what it proves | AC |
|---|---|---|---|---|
| E1 | unit | outbox | `publish` forwards every envelope to the transport exactly once, byte-identical (body and `content_type` compared field by field), and the store's enqueue call count is **0** | D1 |
| E2 | unit | outbox | `publish_batch` returns one result per envelope, in order, and reaches `P::publish_batch` (a `RecordingPublisher` that distinguishes the batch entry point), store untouched | D1 |
| E3 | unit | outbox | **Mutation guard:** with a store and a publisher that both record, swapping the two code paths fails — asserted as "publisher saw N, store saw 0" *and* the mirror in E4 | D1, D2 |
| E4 | unit | outbox | `enqueue(&mut tx, &e)` writes exactly one row through `OutboxEnqueue` in the given transaction; the transport publisher's call count is **0** | D2 |
| E5 | unit | outbox | `enqueue_batch` writes in order; an empty slice is `Ok(())` with zero calls | D2 |
| E6 | unit | outbox | `enqueue_batch` with `fail_next_enqueue` armed at position *k* returns `EnqueueBatchError { index: k }` and the store saw exactly `k + 1` calls — the tail was not attempted. It asserts **fail-fast and the index only**: the in-memory fake has no transaction to abort, so it *can* still show the first *k* rows, and asserting their absence there would assert the fake, not the contract. The "earlier `Ok`s are not durable" half of the invariant is asserted on Postgres by **E18** (the earlier write is discarded when the aborted transaction is rolled back). Ratified 2026-09-06 — see ADR 0036 §5's dated note | D2, D4 |
| E7 | unit | outbox | An `OutboxPublisher` is accepted as `OutboxDispatcher`'s publisher: seed *N* rows in **one** `InMemoryOutboxStore` used as both the dispatcher's store and the publisher's `OutboxEnqueue` capability; run to drain under a `CancellationToken`; assert all *N* reached the transport, the store's row count did **not** grow, and `enqueue_call_count() == 0` | D3 |
| E8 | unit | outbox | `enqueue` returns the provider's enqueue error unwrapped, `source()` wired, `Classify` verdict preserved; `Display` contains no payload bytes and no header value | D4 |
| E9 | unit | outbox | `publish` returns `P::Error` unwrapped with its `Classify` verdict; a `ScriptedPublisher` set to fail once is called **exactly once** — no retry, and `#[tokio::test(start_paused = true)]` proves no sleep | D4 |
| E10 | unit | outbox | `EnqueueBatchError` `Display`/`Debug`/`source()`/`Classify` behave as §3 says | D4 |
| E11 | unit | outbox | `OutboxSettings`: `Default`, builder, serde round-trip and `from_env` unchanged for `dispatcher`/`retention` | D5 |
| E12 | unit | outbox | Retired env keys are inert: with `RELIAR_OUTBOX_ENABLED`, `_ALLOWED_TYPES` and `_DISALLOWED_TYPES` all set to hostile values, `from_env` returns exactly the `Default` settings and no error (single-threaded, env-isolated as the existing `from_env` tests are) | D5 |
| E13 | unit | outbox | A serde document carrying `enabled` **fails** to deserialize, and the error names the field | D5 |
| E14 | obs | outbox | One `reliar.outbox.enqueue` span per envelope with `message.id`/`message.type`; one `reliar.outbox.enqueue_batch` with `batch.size` wrapping them; **no span** emitted by `publish`/`publish_batch`; no payload or header value in any field, event or `Debug` (recording-subscriber test, as `dispatcher_never_logs_payload_or_header_values.rs`) | D8 |
| E15 | obs | outbox | `RecordingMetrics` sees one `enqueued(1, type)` per enqueued envelope and **nothing** from a `publish` | D8 |
| E16 | unit | outbox | Send-safety: `OutboxPublisher`'s `publish` and `enqueue` futures are `Send` when spawned (extends `store_and_publisher_satisfy_send_when_spawned.rs`) | D3 |
| E17 | pg | store-postgres | `enqueue` inside the caller's transaction is invisible before commit and absent after rollback; persists `content_type` verbatim; a reused `MessageId` is `EnqueueError::Duplicate` (carries over 0.3.0's D7/R14) | D6 |
| E18 | pg | store-postgres | An enqueue failure leaves the transaction aborted: the next statement on `tx` fails and an earlier business write in the same transaction is gone after `commit`. **This is where "an earlier `Ok` is not durable" is proven** — the half E6 cannot reach on a fake | D6 |
| E19 | pg | store-postgres | The `OutboxEnqueue` impl's future is `Send` when spawned (keeps 0.3.0's R23 regression guard) | D6 |
| E20 | e2e | tests/system | Postgres + JetStream: an **enqueued** envelope lands in `outbox` and reaches the stream through the dispatcher | D7 |
| E21 | e2e | tests/system | A **published** envelope reaches the stream immediately and never appears in `outbox` | D7 |
| E22 | e2e | tests/system | A `publish` followed by a **rollback** of the surrounding business transaction is still on the stream — the pass-through's non-transactional nature asserted, not documented | D7 |
| E23 | review | — | `cargo semver-checks` reports every §7 removal; the bump is 0.4.0; `reliar-core` and `reliar-transport-nats` untouched; CI purity + versioning jobs green | D5, D10 |

E20–E22 **replace** `e5_routing_stages_and_streams_together.rs` and
`e6_disallow_wins_and_the_switch.rs`. One system-test binary, one Postgres and one NATS container,
as today (ADR 0031 §6).

---

## 10. Engineer handoff

### RELIAR-55 — `reliar-outbox` (0.3.0 → **0.4.0**)

**Delete**

- `src/policy.rs` (whole module) and its `mod`/`pub use` lines in `lib.rs`.
- `tests/policy_construction.rs`, `policy_matching.rs`, `policy_precedence.rs`,
  `settings_routing_overlap.rs`, `routing_disabled.rs`, `routing_selective.rs`, `routing_all.rs`,
  `routing_delegates_to_the_policy.rs`, `routing_requires_transaction.rs`, `routing_settings.rs`.

**Rewrite**

- `src/publisher.rs` — §2: strip the policy, the scoped view, `in_transaction`, `publish_direct`,
  `RouteError`, `DirectPublishError`; add `enqueue`, `enqueue_batch`, `EnqueueBatchError` and the
  `Publisher` impl. The first rustdoc line of the type and of `publish` must say that `publish`
  **bypasses the outbox** and `enqueue` is the durable path (D9). Replace the doctest: build a
  `SerializedEnvelope`, `enqueue` it into an `InMemoryTransaction`, then `publish` one — same
  `test-support` gating as today. The 0.3.0 `compile_fail` doctest (the scoped view cannot reach a
  dispatcher) is **deleted**; E7 replaces it with the positive assertion.
- `src/settings.rs` — §7: remove the three fields, their setters, `MessageTypeNames`, the repr +
  `TryFrom` + `default_enabled_true`, and the three `from_env` blocks; derive `Default` and
  `Deserialize`.
- `src/metrics.rs` — `routed` → `enqueued`.
- `src/staging.rs` → **`src/enqueue.rs`** — trait `OutboxStaging` → `OutboxEnqueue`, method
  `stage` → `enqueue`, module doc and invariant wording retouched; no other change (§4, ADR 0036
  amendment A). Update the `mod`/`pub use` lines in `lib.rs` with it.
- `src/lib.rs` — crate-doc paragraph 2 rewritten to the two-operation rule; the *Guarantees* list
  loses the "routing rule's direct path" bullet and gains a "**`publish` bypasses the outbox**"
  bullet in the same place; `pub use` lines updated.
- `src/test_support/metrics.rs` — `routed` → `enqueued` (§8).

**Add tests** E1–E16 (§9), one file per scenario, named for the behaviour, in `tests/` against the
public API — the existing `routing_*.rs` names are replaced by `enqueue_*.rs` / `publish_*.rs`
equivalents. **No test name carries the retired `stage*` vocabulary either** (§4): E5's file is
`enqueue_batch_writes_in_order_and_empty_is_a_noop.rs`, not
`enqueue_batch_stages_in_order_and_empty_is_a_noop.rs`.

**Manifest** `version = "0.4.0"`.

### RELIAR-56 — provider, system tests, examples, guides

- `crates/reliar-store-postgres` → **0.4.0**; §5's doc retouch only; rename
  `tests/postgres/routing_enqueue.rs` → `outbox_publisher_enqueue.rs` and rewrite its header
  comment and assertions to E17–E19.
- `tests/system/tests/e2e/` — delete `e5_*.rs` and `e6_*.rs`, add the E20–E22 files (register them
  in `main.rs`).
- `examples/axum-outbox` — the handler calls `outbox.enqueue(&mut tx, &serialized)`.
- `examples/nats-pub-sub` — drop the `RELIAR_OUTBOX_*` routing demo; show the two call sites side
  by side (`enqueue` inside a transaction, `publish` outside one) and say which guarantee each has.
- `examples/outbox-basic` — check for policy references.
- **Guides:** delete `docs/guides/outbox-routing.md`, add `docs/guides/outbox-enqueue-and-publish.md`
  (the two operations, the guarantee each carries, the 0.3 → 0.4 migration table from §7, and the
  "an app that never uses the outbox wires `NatsPublisher` directly" pointer). Fix the inbound
  links in `README.md:61`, `docs/guides/getting-started.md:80`, `docs/guides/nats.md:57`.
- **Architecture:** `docs/architecture/overview.md:11` points at this file.
- **Root `Cargo.toml`:** `reliar-outbox` and `reliar-store-postgres` pins → `0.4.0`.
- **`CHANGELOG.md`:** the *Unreleased* section currently describes `reliar-outbox` 0.3.0,
  `reliar-store-postgres` 0.3.0 and `reliar-transport-nats` 0.1.1 — all three already tagged and
  published. Close that text into a dated release section
  `## 2026-09-05 — reliar-outbox 0.3.0 · reliar-store-postgres 0.3.0 · reliar-transport-nats 0.1.1`
  **without rewriting it** (it is the record of what 0.3.0 shipped), then open a fresh *Unreleased*:

  - **Removed** — §7's table, verbatim, plus the three environment keys.
  - **Changed** — `OutboxPublisher` is now a `reliar_core::Publisher` whose `publish` bypasses the
    outbox; `new`/`with_metrics` lose the policy argument; `OutboxMetrics::routed` →
    `enqueued`; `OutboxSettings` deserialization rejects a document carrying the retired keys.
  - **Added** — `enqueue` / `enqueue_batch`, `EnqueueBatchError`, `OutboxMetrics::enqueued`, the
    `reliar.outbox.enqueue`/`enqueue_batch` spans, the new guide.
  - **Migration 0.3 → 0.4** — `outbox.in_transaction(&mut tx).publish(&e).await?` →
    `outbox.enqueue(&mut tx, &e).await?`; `outbox.publish_direct(&e).await?` →
    `outbox.publish(&e).await?`; delete `RELIAR_OUTBOX_ENABLED` / `_ALLOWED_TYPES` /
    `_DISALLOWED_TYPES` from every deployment and every config document (a document that keeps them
    now fails to load); a type you had disallowed is now a `publish` call site, changed in code.

  The changelog bookkeeping fix is flagged to the PO as scope to ratify — it is a pre-existing gap,
  folded in because this change edits the same section.

### Gates before handing back

`cargo fmt --all --check` · `cargo clippy --workspace --all-targets --all-features -- -D warnings` ·
`cargo test --workspace` · `cargo hack check --feature-powerset -p reliar-outbox` ·
`cargo sqlx prepare --check` (expect **no** `.sqlx/` change) · `RUSTDOCFLAGS="-D warnings" cargo doc
--no-deps` · `cargo semver-checks check-release` for both bumped crates (expect the §7 removals,
and only those).

---

## 11. Decided here (so nobody re-litigates it)

1. `publish`'s error is **`P::Error`**, transparent — not a wrapper (ADR 0036 §3).
2. `enqueue`'s error is **`S::Error`**, transparent.
3. `enqueue_batch` **fails fast with an index**, not `Vec<Result<…>>` (ADR 0036 §5).
4. `enqueue` returns `()`, not `MessageId`.
5. `enqueue` takes `&SerializedEnvelope` only — no typed overload, no serializer (ADR 0036 §4).
6. `OutboxEnqueue<Tx>` (renamed from `OutboxStaging`, ADR 0036 amendment A) is otherwise
   unchanged, stays separate from `OutboxStore`, and gains no trait-level batch method.
7. `PostgresOutboxStore::enqueue`/`enqueue_with` stay.
8. `publish_batch` is overridden to forward, not inherited.
9. `publish` emits no span and no metric; `enqueue` emits both.
10. serde rejects a retired key; `from_env` ignores one.
11. `reliar-core` and `reliar-transport-nats` are untouched and unbumped.

## 12. Not in this contract

- Any ambient or request-scoped transaction (superseded RELIAR-51; may return as sugar over
  `enqueue`).
- Per-type durability configuration of any kind, in any crate — including the future
  `reliar-messaging` facade (SRS §36).
- Deferred-after-commit publishing (superseded RELIAR-52).
- A trait-level `OutboxEnqueue` batch method, a typed `enqueue_typed`, an
  `EnqueueOptions`/`ordering_key` on the `OutboxEnqueue` path.
- Merging `OutboxEnqueue` into `OutboxStore` (weighed and rejected — ADR 0036 amendment A).
