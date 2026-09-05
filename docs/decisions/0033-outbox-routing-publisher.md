# 0033 — The outbox routing publisher: `OutboxRouter`, `OutboxEnqueue`/`OutboxEnqueueIn<Cx>`

- **Status:** accepted
- **Date:** 2026-09-05
- **Story / cards:** RELIAR-43 (story), RELIAR-44 (this ADR + contract), RELIAR-45 (build)
- **SRS:** v1.1.4 §7, §12, §18, §19.4, §19.6, §20, §22, §23, §31, §33, §36
- **Related:** ADR 0001 (static dispatch), 0008 (`Publisher`), 0010 (`Serializer`), 0019 (settings +
  opt-in `from_env`), 0020 (metrics hook), 0027 (routing concepts are the transport's), 0032
  (`Publisher` in `reliar-core`, the §18 kind test)
- **Contract:** `../architecture/routing-publisher-contract.md`

## Context

A host wants **one object** whose "publish" call either stages the message in the transactional
outbox — inside the host's own transaction — or sends it straight to the transport, decided by
configuration: an `enabled` switch, a list of routed message types, an empty list meaning *all*
types. It lets an operator turn the outbox on per deployment and widen it per message type without
touching call sites (RELIAR-43 stories 1–2).

Three facts constrain the design.

1. **`enqueue` is not on `OutboxStore`** (SRS §19.6): it takes the provider's own transaction handle
   (`&mut sqlx::Transaction<'_, Postgres>`), which `reliar-outbox` may not name. The routed half of
   the router therefore needs a transaction it cannot type.
2. **The direct half needs no transaction at all**, and `reliar_core::Publisher::publish` takes a
   `&SerializedEnvelope` — bytes — while `PostgresOutboxStore::enqueue` takes a typed
   `&Envelope<T>` and serializes internally. The two halves start from different shapes.
3. The unit acceptance criteria D1–D5 must run **without a database**, against the existing
   `InMemoryOutboxStore`/`RecordingPublisher` fakes.

## Decision

### 1. Placement: `reliar-outbox`

The router and its settings live in `reliar-outbox`, not `reliar-core` and not a provider crate.
Applying the §18 kind test (ADR 0032): the router names no storage engine, no broker and no
transport routing concept — but it *is* outbox mechanics ("does this message get outbox durability?"
is a statement about how the outbox works, not shared vocabulary two capabilities need to talk to
each other). That is exactly the kind of item §18 keeps **out** of core and ADR 0032 lists on the
`OutboxStore`/`RetryPolicy`/dispatcher side of the line. `reliar-core` gains **no new item**; the
only core change this decision implies is a one-sentence rustdoc clarification on
`DeliveryMetadata::content_type` (§6 below).

Fact 3 also settles it against a provider-side router (`PostgresOutboxPublisher`): a router that
lives in `reliar-store-postgres` cannot be unit-tested with `reliar-outbox`'s in-memory fakes, so
D1–D5 would each need a container.

### 2. The transaction is a **type parameter of the capability trait**, not an sqlx type

Two traits in `reliar-outbox`:

```rust
/// The identity half: what an enqueue capability fails with. Named without a context, so
/// `OutboxRouter` can state its error type once and `publish` (which has no transaction) can
/// name it too.
pub trait OutboxEnqueue: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
}

/// The operation half, once per transaction-context type the provider supports.
pub trait OutboxEnqueueIn<Cx>: OutboxEnqueue {
    fn enqueue_serialized(
        &self,
        cx: Cx,
        envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<MessageId, Self::Error>> + Send;
}
```

`Cx` is supplied by the provider's impl —
`impl<'a, 'c, Ser> OutboxEnqueueIn<&'a mut Transaction<'c, Postgres>> for PostgresOutboxStore<Ser>`
— so `reliar-outbox` names no sqlx type and the two lifetimes of `&'a mut Transaction<'c, _>` stay
free parameters of the impl. `OutboxRouter::publish_in` carries `where E: OutboxEnqueueIn<Cx>` at
the **method**, so one router type serves any provider and any context.

**Why the split into two traits:** the router's transaction-less `publish` must name `E::Error`
without a `Cx` in scope; an associated `type Error` on `OutboxEnqueueIn<Cx>` alone cannot be named
there. The base trait is deliberately method-free.

**Why not a GAT** (`type Context<'a>`): `sqlx::Transaction<'c, DB>` is invariant in `'c`, so a
single-lifetime GAT cannot express `&'a mut Transaction<'c, Postgres>` without collapsing the two
lifetimes and rejecting the `Transaction<'static, _>` that `PgPool::begin()` returns.

### 3. Serialize once in the router; the enqueue capability takes **bytes**

`OutboxRouter<E, P, Ser>` owns the `Ser: Serializer`, serializes the typed `Envelope<T>` into a
`SerializedEnvelope` **before** the routing decision is acted on, and then either enqueues those
bytes or publishes them.

This makes "the wire format does not depend on the route" **structural** rather than documented: a
message routed through the outbox today and published directly tomorrow is byte-identical and
carries the same `content_type`. The rejected alternative — a typed `enqueue<T: Message>` on the
trait, with the store serializing on the routed path and the router serializing on the direct path —
puts two `Serializer` instances behind one call site and makes a silent wire divergence possible
whenever they differ.

It also costs the fakes nothing: `InMemoryOutboxStore::insert` already takes a `SerializedEnvelope`.

The provider gains **no new inherent method** — only the trait impl, whose method is named
`enqueue_serialized` so it can never be confused with (or shadow) the existing typed
`PostgresOutboxStore::enqueue`.

### 4. `OutboxRouter` does **not** implement `reliar_core::Publisher`

The router is not a `Publisher`. Implementing it would let a host pass the router to
`OutboxDispatcher` as its publisher, and every routed message the dispatcher drained would be
re-enqueued — a cycle, or (with the "routed type without a transaction is an error" rule) a
permanent failure that kills every message the outbox exists to protect. The rustdoc says so
explicitly. The dispatcher's publisher is always the transport publisher.

The API is two inherent methods on the router:

- `publish_in(cx, &Envelope<T>) -> Result<Routed, RouteError<E::Error, P::Error>>` — the normal
  call. Routed → `enqueue_serialized(cx, …)`. Direct → `publisher.publish(…)` **now**, with `cx`
  untouched (no statement is issued on it).
- `publish(&Envelope<T>) -> Result<Routed, RouteError<…>>` — for call sites that have no
  transaction. A type that routes to the outbox returns `RouteError::TransactionRequired`; it
  **never** silently downgrades to a direct publish, because that would quietly cancel the
  durability the operator configured.

So it is impossible to enqueue without a transaction (D4): the only path to the store is
`publish_in`, and the tx-less method refuses loudly.

### 5. Matching rule: exact `MessageType` **name**, case-sensitive, version-agnostic

The key is `envelope.message_type.name()` — `"orders.created"`, not `"orders.created.v1"`. Every
version of a message contract gets the same durability; per-version routing is not a use case worth
an ambiguous key. No wildcards, no prefixes in v0.2 (a prefix syntax is additive later; adding
matches is semver-safe, removing them is not).

Because `MessageType`'s `Display` renders `orders.created.v1`, an operator writing that form into
the routed list is the predictable mistake, and under name-matching it would silently match
**nothing** — a routed message quietly published direct. So a configured entry ending in `.v<digits>`
is a **loud configuration error** (`ConfigError::VersionedRoutedType` / `SettingsError::Parse`), not
a silent miss. Rejecting now and relaxing later is the semver-safe direction.

Empty entries are dropped (`"a,,b"` → `[a, b]`, `""` → empty list); duplicates are tolerated;
entries are trimmed of surrounding whitespace.

### 6. Settings: a third section on `OutboxSettings`

```rust
pub struct RoutingSettings { pub enabled: bool, pub routed_types: RoutedTypes }
```

`OutboxSettings` gains `routing: RoutingSettings` (additive — the struct is `#[non_exhaustive]` with
`#[serde(default)]`). Defaults: `enabled = true`, `routed_types` empty = **all types routed** —
the safe default, matching the human's rule 3.

Env keys, flat under the caller's prefix like every other outbox key:
`{prefix}ROUTING_ENABLED` and `{prefix}ROUTED_TYPES` (comma-separated). **`ROUTING_ENABLED`, not
`ENABLED`** as the story proposed: `RELIAR_OUTBOX_ENABLED=false` reads as "the outbox is off", but
the dispatcher must keep running to drain rows already staged. The switch turns *routing* off, never
the drain.

`RoutedTypes` validates on **every** construction path — `parse` (comma list), `try_from_iter`, and
serde (`#[serde(try_from = "Vec<String>", into = "Vec<String>")]`) — so a config file cannot smuggle
in a `.vN` entry that the env loader would reject.

Consequence on `reliar-core`: `DeliveryMetadata::content_type`'s rustdoc currently says it is
"authoritatively set by the store at enqueue". With this ADR it is set by **whoever serialized the
body** — the store on `PostgresOutboxStore::enqueue`, the router on `publish_in`/`publish` — and
`enqueue_serialized` persists the value the envelope carries verbatim. That is a doc-only change to
core; no signature moves.

### 7. Guarantees, retries and observability

- **The direct path is not transactional.** It publishes at the moment of the call; if the caller's
  transaction later rolls back, the message is already on the wire. Stated in the rustdoc of both
  methods, in the crate docs' *Guarantees* list, and in the guide.
- **The router never retries.** Open question 2 is answered "surface the error", and the reason is
  stronger than taste: `publish_in` is normally called with the caller's transaction **open**, so a
  retry loop inside the router would hold a database transaction open across broker I/O — the one
  thing `team/engineering-conventions.md` §6 and SRS §21 forbid outright. The same reasoning becomes
  a documented **hazard**: even a single direct publish inside a transaction holds it across network
  I/O, so the router's docs tell hosts to set a publisher-side timeout and to prefer calling
  direct-routed publishes outside the transaction. `RouteError::Publish` leaves the caller's
  transaction intact and committable; `RouteError::Enqueue` has already aborted it (Postgres aborts
  on a failed statement) and the caller must roll back.
- **At-least-once is unchanged for routed messages** and *not claimed* for direct ones: a direct
  publish is exactly one attempt, with whatever the transport's own client guarantees, and no
  Reliar-side retry, backoff, dead state, or duplicate window.
- **Observability:** one `debug_span!("reliar.outbox.route")` per call with `message.id`,
  `message.type` and `route = "outbox" | "direct"`; no payload, no header values, no event on
  success, and the library never logs an error it also returns. `OutboxMetrics` gains one
  default-bodied hook, `routed(&self, route: RouteKind, message_type: &MessageType)` — additive by
  construction (ADR 0020), label cardinality bounded (2 × message types).
- **Cost:** one `Serializer::serialize` per call (the same one the store used to do), one linear
  scan of a handful of names, no allocation for the decision, no clone of the settings per call.

## Consequences

- `reliar-outbox` gains `OutboxEnqueue`, `OutboxEnqueueIn<Cx>`, `OutboxRouter`, `Routed`,
  `RouteKind`, `RouteError`, `RoutingSettings`, `RoutedTypes`, two `ConfigError` variants and one
  `OutboxMetrics` hook. All additive; `cargo semver-checks` should report a minor bump.
- `reliar-store-postgres` gains one trait impl and a small internal generalization (`insert_row`
  already ignores the body type; it takes a `&SerializedEnvelope` on the new path). No SQL, no
  migration, no `.sqlx/` change.
- `reliar-outbox`'s `test-support` gains an `InMemoryTransaction` marker, an `OutboxEnqueueIn`
  impl on `InMemoryOutboxStore`, and a `fail_next_enqueue(n)` knob.
- A host on a provider that has not implemented `OutboxEnqueueIn` cannot use the router. Today that
  is nobody: Postgres is the only store.
- The router carries **no** `EnqueueOptions`/`ordering_key` in v0.2. `Ordering::PerKey` is already a
  configuration error in this release, so nothing is lost; a host that needs an ordering key calls
  `PostgresOutboxStore::enqueue_with` directly. Adding a `publish_in_with` later is additive.
- Three generic parameters (`OutboxRouter<E, P, Ser>`) with no default for `Ser`: a default would
  require a `json` feature on `reliar-outbox` (and a new `cargo hack` axis) purely for ergonomics.
  Hosts write `JsonSerializer` explicitly.
- `RouteError<E, P>` boxes the direct path's serialization failure
  (`Serialize { source: Box<dyn Error + Send + Sync> }`) rather than adding a third type parameter.
  Cold path; boxing a *source* is house-legal.
- The provider impl must **not** carry an explicit `where 'c: 'a` outlives bound. It is implied by
  `&'a mut Transaction<'c, _>` and looks harmless, but writing it makes the impl non-general to the
  higher-ranked check that runs when a host asserts the `publish_in` future is `Send`, and every
  `tokio::spawn`/Axum call site fails with "implementation of `OutboxEnqueueIn` is not general
  enough" — an error that points at the caller, not the impl. Verified on rustc 1.98 against a
  stand-in with `sqlx::Transaction`'s invariance; the contract carries the warning and a
  spawnability regression test (R14a).
- `publish_in`/`publish` require `T: Message + Sync` — the future holds `&Envelope<T>` across an
  await, and `&T: Send` needs `T: Sync`. Nearly free for message bodies, but it is a real bound and
  it is in the contract.

## Alternatives considered

- **Typed `enqueue<T: Message>` on the trait, store serializes (option (a) in the brief).** Rejected:
  two serializers behind one call site, so the bytes on the wire could differ by route; and
  `InMemoryOutboxStore` would need a serializer (dragging `reliar-core/json` into `test-support`).
- **Provider-side router, `PostgresOutboxPublisher` (option (c)).** Rejected: D1–D5 would each need a
  container, the rule would be re-implemented per provider, and the host call site is identical
  either way — so the provider-side version buys nothing and costs testability.
- **Host-supplied closure / `Enqueuer` fn (option (b)).** Rejected: an async closure that borrows a
  transaction and returns a `Send` future is the worst ergonomics in this list, at every call site.
- **Router implements `Publisher`, `publish` errors on routed types (option (d)).** Rejected in §4 —
  it makes a dispatcher-feedback cycle wireable by accident.
- **A GAT `type Context<'a>` on one trait.** Rejected in §2 — invariance in `Transaction<'c, _>`.
- **Buffer direct publishes and flush after the caller commits.** Rejected: it is an in-memory outbox
  with none of the durability, it needs an explicit `flush` (so it is not one call site after all),
  and a crash between commit and flush loses the message silently — the exact failure the outbox
  exists to prevent.
- **`enabled` gating the dispatcher too.** Rejected in §6: it would strand already-staged rows.

## Open

- Prefix/wildcard matching (`orders.*`) — additive, deferred until an operator asks.
- A `publish_in_with(cx, envelope, EnqueueOptions)` overload, once `Ordering::PerKey` ships.
- Whether the future `reliar-messaging` facade's `publish`/`send` (SRS §36) delegates here or
  re-states the rule. It must delegate — see the amendment draft.
