# 0033 — The outbox routing publisher: `OutboxPublisher`, `OutboxStaging<Tx>`

- **Status:** **Superseded by [0036](0036-outbox-enqueue-and-publisher-passthrough.md)
  (2026-09-05).** The routing rule this record decided was withdrawn by the human one day after it
  shipped in `reliar-outbox` 0.3.0 / `reliar-store-postgres` 0.3.0 — configuration deciding a
  message's durability behind an identical call site. 0036 replaces it with two explicitly named
  operations (`OutboxPublisher::enqueue` in the caller's transaction; a pass-through
  `reliar_core::Publisher`). **Everything below is the record of what 0.3.0 shipped and is kept
  verbatim; none of it describes 0.4.0 or later.** The one part that survives unchanged is
  `OutboxStaging<Tx>` (§2, Amendment D §2). Because this record had shipped, it is superseded
  rather than amended (`README.md`'s ship test).
- **Superseded status at the time:** accepted (Amendment A, 2026-09-05 — settings naming; Amendment B, 2026-09-05 — allow +
  disallow lists; Amendment C, 2026-09-05 — the rule is its own type, `OutboxPolicy`; **Amendment D,
  2026-09-05 — the routing publisher *is* a `Publisher`: `OutboxRouter` → `OutboxPublisher` +
  `ScopedOutboxPublisher`, one staging trait, the caller serializes**)
- **Date:** 2026-09-05
- **Story / cards:** RELIAR-43 (story), RELIAR-44 (this ADR + contract), RELIAR-45 (build)
- **SRS:** v1.1.8 §0.7, §0.8, §7, §12, §18, §19.4, §19.6, §20, §20.2, §22, §23, §31, §33, §36
- **Related:** ADR 0001 (static dispatch), 0008 (`Publisher`), 0010 (`Serializer`), 0019 (settings +
  opt-in `from_env`), 0020 (metrics hook), 0027 (routing concepts are the transport's), 0032
  (`Publisher` in `reliar-core`, the §18 kind test)
- **Contract:** `../architecture/routing-publisher-contract.md` — **withdrawn**; the live
  contract is `../architecture/outbox-publisher-contract.md` (ADR 0036).

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

> **Amendment C** splits this section's subject in two: the **rule** is `OutboxPolicy`
> (`crates/reliar-outbox/src/policy.rs`) and the **router** composes it with a store and a
> publisher. Both placements are the same, for the same reason.

The rule, the router and their settings live in `reliar-outbox`, not `reliar-core` and not a
provider crate. Applying the §18 kind test (ADR 0032): they name no storage engine, no broker and no
transport routing concept — but they *are* outbox mechanics ("does this message get outbox durability?"
is a statement about how the outbox works, not shared vocabulary two capabilities need to talk to
each other). That is exactly the kind of item §18 keeps **out** of core and ADR 0032 lists on the
`OutboxStore`/`RetryPolicy`/dispatcher side of the line. `reliar-core` gains **no new item**; the
only core change this decision implies is a one-sentence rustdoc clarification on
`DeliveryMetadata::content_type` (§6 below).

Fact 3 also settles it against a provider-side router (`PostgresOutboxPublisher`): a router that
lives in `reliar-store-postgres` cannot be unit-tested with `reliar-outbox`'s in-memory fakes, so
D1–D5 would each need a container.

### 2. The transaction is a **type parameter of the capability trait**, not an sqlx type

> **Amendment D** keeps this section's core decision — the transaction is a type parameter, never an
> sqlx type — and changes its shape twice: the two traits below collapse into **one**
> (`OutboxStaging<Tx>`), and the parameter is the **transaction type** `Tx` rather than a whole
> context type `Cx = &'a mut Transaction<'c, _>`. The `'c`-invariance reasoning and the
> "no `where 'c: 'a`" hazard below are historical from D onward: with `&mut Tx` in the method
> signature the impl carries a single lifetime and the higher-ranked check never runs. See
> Amendment D §2.

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

> **Amendment D** keeps the conclusion (**the capability takes bytes**) and moves the serialization
> step out of the type entirely: `reliar_core::Publisher::publish` takes a `SerializedEnvelope`, so
> the **caller** serializes — exactly as it already does for `NatsPublisher` — and hands the *same*
> buffer to whichever path the policy picks. The guarantee below gets stronger, not weaker: it is no
> longer "one serializer", it is **one buffer**. `Ser` disappears as a type parameter. See
> Amendment D §3.

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

> **Revised by Amendment D**, and it is the only ruling in this ADR that a later amendment reverses
> in part. The hazard below is real and is *still* guarded against — but the guard moved from "no
> `Publisher` impl anywhere" to "the `Publisher` impl lives on the **transaction-scoped view**,
> which borrows a transaction and therefore is neither `'static` nor `Clone`, so
> `OutboxDispatcher`'s `P: Publisher + Send + Sync + 'static` bound rejects it". The un-scoped
> `OutboxPublisher` still does **not** implement `Publisher`. Read §4 as the reason the guard
> exists; read Amendment D §4 for the guard that ships.

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
either list is the predictable mistake, and under name-matching it would silently match
**nothing** — a message quietly taking the wrong route. So a configured entry ending in `.v<digits>`
is a **loud configuration error** (`SettingsError::Parse`, Amendment B), not a silent miss.
Rejecting now and relaxing later is the semver-safe direction.

Empty entries are dropped (`"a,,b"` → `[a, b]`, `""` → empty list); duplicates are tolerated;
entries are trimmed of surrounding whitespace.

### 6. Settings: two top-level fields on `OutboxSettings`

> Superseded in part by **Amendment A** (2026-09-05, the `RoutingSettings` sub-section keyed
> `{prefix}ROUTING_ENABLED` — recorded under *Alternatives*), by **Amendment B** (2026-09-05,
> `routed_types` → `allowed_types` plus a `disallowed_types` list) and by **Amendment C**
> (2026-09-05 — the fields stay exactly as below, but `OutboxSettings` no longer *evaluates* or
> *validates* them: `OutboxPolicy` does). The shape below is the thrice-amended one; the rule it
> feeds is Amendment B's, and the type that implements it is Amendment C's.

```rust
pub struct OutboxSettings {
    pub dispatcher: DispatcherSettings,
    pub retention: RetentionSettings,
    pub enabled: bool,                      // new
    pub allowed_types: MessageTypeNames,    // new
    pub disallowed_types: MessageTypeNames, // new
}
```

All three are **top-level fields of `OutboxSettings`**, not a nested section (additive — the struct
is `#[non_exhaustive]`). Defaults: `enabled = true`, both lists empty — an empty `allowed_types`
means **all types routed**, the safe default, matching the human's rule 3.

Env keys, flat under the caller's prefix like every other outbox key: `{prefix}ENABLED`,
`{prefix}ALLOWED_TYPES` and `{prefix}DISALLOWED_TYPES` (comma-separated).

`enabled = false` stops **new** messages entering the outbox — they publish directly — and changes
nothing about draining: the dispatcher keeps claiming and publishing rows already staged. That
distinction is carried by **documentation, not by the name**: the rustdoc on `enabled`, the guide,
and SRS §20.2 each state it in one sentence.

`MessageTypeNames` validates on **every** construction path — `parse` (comma list),
`try_from_iter`, and `OutboxSettings`'s serde deserializer — so a config file cannot smuggle in a
`.vN` entry, or an overlap, that the env loader would reject (Amendment B).

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

- *(Amendment D renames and prunes this list: `OutboxStaging<Tx>`, **`OutboxPolicy`**,
  `OutboxPublisher`, `ScopedOutboxPublisher`, `RouteKind`, `RouteError`, `DirectPublishError`,
  `MessageTypeNames`, three `OutboxSettings` fields and one `OutboxMetrics` hook.)*
  `reliar-outbox` gains `OutboxEnqueue`, `OutboxEnqueueIn<Cx>`, **`OutboxPolicy`** (Amendment C),
  `OutboxRouter`, `Routed`, `RouteKind`, `RouteError`, `MessageTypeNames`, three `OutboxSettings`
  fields and one `OutboxMetrics` hook — and **no `ConfigError` variant** (Amendment B), **no
  `OutboxSettings::route_for` and no `OutboxSettings::validate_routing`** (Amendment C). All additive;
  `cargo semver-checks` should report a minor bump, and because `reliar-outbox` is already
  published the change ships as **0.3.0** (ADR 0034).
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
- *(Withdrawn by Amendment D §3 — there is no `Ser` parameter.)* Three generic parameters
  (`OutboxRouter<E, P, Ser>`) with no default for `Ser`: a default would
  require a `json` feature on `reliar-outbox` (and a new `cargo hack` axis) purely for ergonomics.
  Hosts write `JsonSerializer` explicitly.
- *(Withdrawn by Amendment D §3 — the caller serializes, so there is no `Serialize` variant.)*
  `RouteError<E, P>` boxes the direct path's serialization failure
  (`Serialize { source: Box<dyn Error + Send + Sync> }`) rather than adding a third type parameter.
  Cold path; boxing a *source* is house-legal.
- *(Void from Amendment D §2 — `OutboxStaging<Tx>`'s impl carries one lifetime and the
  higher-ranked check never runs; R14a is deleted with the trap.)* The provider impl must **not**
  carry an explicit `where 'c: 'a` outlives bound. It is implied by
  `&'a mut Transaction<'c, _>` and looks harmless, but writing it makes the impl non-general to the
  higher-ranked check that runs when a host asserts the `publish_in` future is `Send`, and every
  `tokio::spawn`/Axum call site fails with "implementation of `OutboxEnqueueIn` is not general
  enough" — an error that points at the caller, not the impl. Verified on rustc 1.98 against a
  stand-in with `sqlx::Transaction`'s invariance; the contract carries the warning and a
  spawnability regression test (R14a).
- *(Void from Amendment D §3 — no method here is generic over a body type.)* `publish_in`/`publish`
  require `T: Message + Sync` — the future holds `&Envelope<T>` across an
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
  it makes a dispatcher-feedback cycle wireable by accident. **Amendment D revisits this**: the
  `Publisher` impl goes on the transaction-scoped view, which the dispatcher's `'static` bound
  rejects; the un-scoped type still has no impl, so option (d) as written stays rejected.
- **A GAT `type Context<'a>` on one trait.** Rejected in §2 — invariance in `Transaction<'c, _>`.
- **Buffer direct publishes and flush after the caller commits.** Rejected: it is an in-memory outbox
  with none of the durability, it needs an explicit `flush` (so it is not one call site after all),
  and a crash between commit and flush loses the message silently — the exact failure the outbox
  exists to prevent.
- **`enabled` gating the dispatcher too.** Rejected in §6: it would strand already-staged rows.
- **A single list with a `!name` negation prefix** (`allowed_types = ["!c"]`). Rejected in
  Amendment B: it invents a mini-language inside a comma-separated env value, it makes `""` vs
  `"!c"` vs `"a,!c"` each mean something subtly different, and it cannot be expressed in a typed
  config file without the same string parsing. Two named lists say the same thing in the schema.
- **Allow wins over disallow**, or a configurable precedence. Rejected in Amendment B: an operator
  reaches for the disallow list to *stop* something, so the deny must be the one that holds; a
  configurable precedence is a fourth input whose only job is to make the other three ambiguous.
- **Silently resolving an overlap** (either direction). Rejected in Amendment B — see there.
- **The rule inline in the router** — the pre-Amendment-C shape, where `OutboxRouter` held
  `enabled` plus both lists and answered `route_for` itself. **Rejected by the human on 2026-09-05**
  (Amendment C), and the design agrees: the router would carry the rule *and* serialization *and*
  two publish paths *and* three error mappings *and* the span, the §2.1 table could only be observed
  through a store and a publisher, and the second consumer (the `reliar-messaging` facade, SRS §36)
  would have to re-state it. "Bigger and hard to follow" is the accurate short version.
- **The rule as a closure or a `RoutingPolicy` trait parameter** (`impl Fn(&MessageType) ->
  RouteKind`, or `OutboxRouter<E, P, Ser, Pol: RoutingPolicy>`). Rejected: it buys host-supplied
  rules nobody asked for while costing the things a value gives free — the closure is not `Debug`,
  not `PartialEq`, not previewable in a log, not constructible from `OutboxSettings`, and it makes
  the router's type harder to name at every call site. Worse, it would let a host install a rule the
  SRS §20.2 truth table does not describe while the rustdoc still claims those semantics; the rule
  is normative, so it is a concrete type, not an extension point (SRS §31). A trait remains additive
  if a genuine second rule ever appears.
- **Keeping `OutboxSettings::route_for` as a thin delegation to the policy.** Rejected in
  Amendment C: two public entry points to one rule, one of them on a type whose job is to be data.
  A delegation costs a line today and is the shape drift starts from — the day someone "fixes" one
  of them.
- **A plural `OutboxPolicies` collection now.** Rejected: one rule exists; a collection with one
  element is an abstraction for a hypothetical (SRS §31). The name is reserved.
- **The policy holding `HashSet<String>` lookups.** Rejected: it duplicates the validated lists,
  allocates at construction, and loses to a linear `&str` scan at the sizes this feature is
  configured with. The decision is documented so the trade-off is re-openable with a bench, not a
  hunch.
- **A `routing: RoutingSettings` sub-section with `{prefix}ROUTING_ENABLED`.** This ADR's original
  decision, **rejected by the human on 2026-09-05** (Amendment A). The switch and the list are
  top-level `OutboxSettings` fields keyed `{prefix}ENABLED` / `{prefix}ROUTED_TYPES`; the ambiguity
  the longer name was buying is closed by documentation instead.

## Amendment A — settings naming (2026-09-05)

> **Renamed by Amendment B** later the same day: `routed_types` / `{prefix}ROUTED_TYPES` below is
> now **`allowed_types` / `{prefix}ALLOWED_TYPES`**, joined by `disallowed_types` /
> `{prefix}DISALLOWED_TYPES`. Read the names in this section as historical; the placement decision
> (top-level fields, no `RoutingSettings`) and the entry-vs-drain reasoning still stand.

**Decided by the human**, overruling §6 as originally written. Recorded here so the reasoning on
both sides survives.

- **Decision.** No `RoutingSettings` type. `OutboxSettings` carries `enabled: bool` and
  `routed_types: RoutedTypes` as top-level fields, with builder methods `enabled(bool)` /
  `routed_types(…)` and env keys `{prefix}ENABLED` / `{prefix}ROUTED_TYPES` — i.e.
  `RELIAR_OUTBOX_ENABLED` and `RELIAR_OUTBOX_ROUTED_TYPES`. Semantics are unchanged from §6:
  `enabled = false` stops *new* messages entering the outbox (they publish directly) and the
  dispatcher keeps draining what is already staged.
- **The concern this ADR raised, and how it is answered.** `RELIAR_OUTBOX_ENABLED=false` can be read
  as "the outbox is off", which would wrongly suggest the dispatcher should stop too — the failure
  mode is an operator who flips the switch and also stops the worker, stranding staged rows. The
  answer is **documentation, not naming**: the rustdoc on `OutboxSettings::enabled`, the rollout
  guide, and SRS §20.2 each say in one sentence that disabling stops **entry**, never **draining**,
  and the guide's rollout section spells out the "keep the dispatcher running" step. A name cannot
  carry that nuance either way (`ROUTING_ENABLED` is equally silent about the drain); prose can, and
  the shorter key is the one an operator will guess.
- **Blast radius.** Contract-level only, before any code exists: `RoutingSettings` never shipped, so
  this is not a breaking change to a published surface. The SRS is already amended (v1.1.6 §0.7,
  §7.2, §20.2, §43.D D5) and the contract
  (`../architecture/routing-publisher-contract.md`) is updated in the same change. RELIAR-45 builds
  against the amended shape.

## Amendment B — allow and disallow lists (2026-09-05)

**Decided by the human**, applied as SRS v1.1.7 §0.8 (§7.2 rows, §20.2 rules 2–4 + truth table,
§43.D D2/D3/D5 reworded, D12/D13 new). It renames Amendment A's list and adds a second one.

- **Decision.** The routed list takes the human's name **`allowed_types`**
  (`RELIAR_OUTBOX_ALLOWED_TYPES`), and a **`disallowed_types`** list
  (`RELIAR_OUTBOX_DISALLOWED_TYPES`) sits beside it — both top-level on `OutboxSettings`, next to
  `enabled`, both `MessageTypeNames`, both defaulting to empty.
- **Precedence**, in this order (SRS §20.2's truth table, reproduced in the contract §2.1):
  1. `enabled = false` → every type publishes **direct**; both lists are ignored.
  2. a name in `disallowed_types` → **direct**. *Disallow wins.*
  3. an empty `allowed_types` → **outbox** (every type).
  4. a name in a non-empty `allowed_types` → **outbox**; anything else → **direct**.
  5. a name in **both** lists → a `SettingsError` on every construction path.
- **Why disallow wins.** The two lists answer different questions: the allow list is a *policy*
  ("these are durable"), the disallow list is an *escape hatch* ("not this one, not right now"). An
  operator reaches for the escape hatch to stop something that is currently happening — usually
  under pressure, because one message type is flooding the outbox or its consumer. If allow won,
  the escape hatch would silently fail exactly when it is being used in anger. Explicit deny beating
  implicit or explicit allow is also what every firewall, ACL and `.gitignore` an operator has ever
  used already does; matching that intuition costs nothing and being novel here would cost an
  outage.
- **Why "everything except `c`" is the shape that matters.** It is the primary rollout use case and
  it is the one the pair exists for: `allowed_types = []`, `disallowed_types = [c]`. Before this
  amendment an operator who wanted the outbox for everything *except* one noisy type had to
  enumerate every other type in `allowed_types` and keep that list correct forever — a list that is
  silently wrong the day someone adds a message type, and wrong in the *unsafe* direction (the new
  type quietly publishes direct). With the disallow list the default stays "everything is durable"
  and the exception is one name.
- **Why an overlap is an error rather than a tie-break.** A name in both lists is not a preference,
  it is a **contradiction**: the operator has stated two incompatible intentions and at least one of
  them will not happen. Silently applying rule 2 would make the config file *look* like it routes
  `c` durably while `c` goes direct — the same class of silent-miss failure the `.v<digits>`
  rejection (§5) exists to prevent, and the same remedy: fail loudly at construction, where a
  deployment can catch it, rather than per message, where nobody looks. It also keeps the rule
  total: with overlaps rejected, `route_for` is a pure function of three well-defined inputs and
  needs no "and if both, then…" clause in its documentation.
- **The decision now evaluates four inputs** — `enabled`, the two lists, and the message type's name
  — rather than three. The evaluation order above **is** the precedence; the contract states it as
  code so an implementation cannot reorder steps 2 and 3 without failing a test (contract §2.1,
  test R18). *(Amendment C: the method is `OutboxPolicy::decide`, not `OutboxSettings::route_for`,
  and R18 asserts the table on the policy alone — no store, no publisher.)*
- **One newtype, not two.** Both fields are `MessageTypeNames` — the same validation (trim, drop
  empties, reject `.v<digits>`), the same matching, the same accessors. Two distinct newtypes
  (`AllowedTypes`/`DisallowedTypes`) would buy type-safety against swapping the two arguments at a
  call site, and there is no such call site: the two fields are set by two separately named methods
  and read by two separately named accessors, so the swap a newtype pair would prevent is not
  expressible. The neutral name also keeps the "empty means all" reading where it belongs — on the
  `allowed_types` field, not on the list type, since an empty *disallow* list means the opposite.
- **One error type: `reliar_core::SettingsError`.** SRS §43.D13 requires the overlap error to name
  the field or env key, and `SettingsError` is the house's only *key + reason* error. Rather than
  split the routing configuration across two error types — `ConfigError` for a bad entry,
  `SettingsError` for a bad pair — every routing-configuration failure is a `SettingsError`:
  `Parse { key, value_kind }` for a `.v<digits>` or empty entry, `OutOfRange { key, message }` for
  an overlap. `MessageTypeNames::parse`/`try_from_iter` therefore take the field or env-variable
  name as their first argument. Consequence: `ConfigError` gains **no** variant from this slice
  (Amendment A's drafted `EmptyRoutedType`/`VersionedRoutedType` are withdrawn before any code
  existed), it stays the dispatcher's cross-field error, and a host wiring `from_env`, a config
  file and the builder handles one `Err` type. No value is ever echoed (ADR 0019).
- **Where the pair is validated.** The invariant is *cross-field*, so it cannot live in the newtype:
  (a) the two list setters become fallible and each checks the incoming list against the one already
  configured — every overlap is caught, because the other list is always known; (b) `from_env`
  validates after reading both keys; (c) serde deserializes `OutboxSettings` through a private repr
  with `Vec<String>` lists and runs the same validation in `TryFrom`, so a config file cannot
  bypass it; (d) `OutboxSettings::validate_routing()` is public, because the fields are public.
  *(Amendment C: (a)–(c) stand and share one crate-private helper in `policy.rs`; (d) is replaced by
  `OutboxPolicy::from_settings`, which validates and returns the rule in one call.)*
- **`OutboxRouter::new` becomes fallible** (`Result<Self, SettingsError>`), and so does
  `with_metrics`. `#[non_exhaustive]` stops an out-of-crate struct literal but not
  `settings.allowed_types = …` on an owned value, so the setters alone cannot make the invariant
  type-level while §7 keeps settings fields public. Constructing a router **is** constructing the
  rule, so the router validates: no `OutboxRouter` ever holds an ambiguous pair, and its
  `route_for` is total and unambiguous by construction. The cost is one `?` per application wiring.
  The alternative — private fields with getters — was rejected as a worse trade: it breaks §7's
  uniform public-field settings shape for one invariant, and it still needs the serde repr.
  *(**Superseded by Amendment C:** the validation moved to `OutboxPolicy::from_settings`, so
  `OutboxRouter::new`/`with_metrics` are infallible again — they take a policy that is valid by
  construction. The reasoning above is why *something* must validate; C only moves *which* type
  does, onto the one that also evaluates.)*
- **Blast radius.** Contract-level only, before any code exists: neither `routed_types` nor
  `RoutedTypes` ever shipped, so this is not a breaking change to a published surface. The SRS is
  already amended (v1.1.7 §0.8) and
  `../architecture/routing-publisher-contract.md` is updated in the same change (§2, §2.1–§2.4,
  §4, §5, §9–§13). RELIAR-45 builds against the amended shape; its slice 1 grows the disallow
  list, the repr and tests R18/R19.

## Amendment C — the rule is its own type, `OutboxPolicy` (2026-09-05)

**Decided by the human**, third instruction of the day on this rule: the enable/allow/disallow
evaluation gets its own type and module, reused by the router, "otherwise that will make it bigger
and hard to follow". Amendments A and B settled *what* the rule is; C settles *where it lives*.

- **Decision.** `crates/reliar-outbox/src/policy.rs` defines **`OutboxPolicy`**: a validated,
  immutable value built once by `OutboxPolicy::from_settings(&OutboxSettings) -> Result<Self,
  SettingsError>`, with one decision method `decide(&MessageType) -> RouteKind` implementing §2.1's
  evaluation order, three accessors (`enabled`, `allowed_types`, `disallowed_types`), and
  `Clone + Debug + Default + PartialEq + Eq`. `OutboxRouter::new(enqueuer, publisher, serializer,
  policy)` **owns one and delegates**; the router keeps no flag, no list and no branch of the table,
  and exposes exactly one rule-shaped accessor, `policy()`.
- **Why a type and not a method.** The rule and the plumbing have different reasons to change and
  different ways to be wrong. The rule is a pure function of four inputs with a truth table an
  operator can read; the router is I/O choreography — serialize, pick a collaborator, map three
  error shapes, emit a span, hold no transaction across a network call. Fused, the router carries
  both, and the rule can only be observed through a store and a publisher: to assert "`c` goes
  direct" you must construct two fakes, run an async test, and inspect two recordings. Split, the
  rule's entire test suite is `assert_eq!(policy.decide(&t), Direct)` — the six table rows in one
  array, no fakes, no runtime.
- **What it makes possible.** A host can build a policy without a store or a transport and **preview**
  the rule at startup ("these types are durable, these are not") — a real operator need during the
  rollout this feature exists for, and impossible when the rule is reachable only through a
  configured router. The future `reliar-messaging` facade (SRS §36) can hold the same policy value
  rather than re-stating the table, which was this ADR's third *Open* item and is now closed by
  construction rather than by a promise.
- **One home, enforced.** Amendment B put the rule in two places: `OutboxSettings::route_for`
  evaluated it and `OutboxSettings::validate_routing` guarded it, while `OutboxRouter::new` also
  validated and the router also evaluated. Both `OutboxSettings` methods are **removed** — neither
  ever shipped. Evaluating the rule is `OutboxPolicy::decide`; checking a settings value is
  `OutboxPolicy::from_settings(&settings)?`, which hands back the very rule it validated, so the
  check and its result are the same call instead of two that can disagree. The cross-field
  disjointness check that the two list setters, `from_env` and the serde `TryFrom` still run
  eagerly (so the error names the field the operator just wrote, SRS §43.D13) is one crate-private
  helper in `policy.rs` with four call sites — one implementation, not four.
- **`OutboxRouter::new` becomes infallible again**, reversing Amendment B's `Result<Self,
  SettingsError>`. An `OutboxPolicy` that exists is unambiguous, so there is nothing left for the
  router to reject. The host's `?` moves one line earlier, onto the thing that actually fails, and
  buys a value it can also log. There is deliberately **no** `OutboxRouter::from_settings`
  convenience: a second constructor would re-introduce the fallible path and give the rule two
  entry points again, which is precisely what this amendment removes.
- **Names.** Singular **`OutboxPolicy`** — there is exactly one rule, and a collection holding one
  element is the speculative abstraction SRS §31 forbids; `OutboxPolicies` is reserved and can wrap
  this type additively if rules ever compose. `decide` returns the existing **`RouteKind`**, not a
  new `Route`: one concept keeps one name, and `Route` sitting beside `Routed` and `RouteError`
  would read as a third thing.
- **Cost.** One more public type and module in `reliar-outbox`, one extra line in a host's wiring,
  and one `OutboxPolicy` (a `bool` and two `Vec<String>`) cloned into the router at construction.
  Per decision the cost is unchanged: the policy keeps the two `MessageTypeNames` as validated and
  scans them, which for a handful of names beats a `HashSet` (no hasher setup, no second copy of the
  data) and allocates nothing either way. If a deployment ever configures hundreds of names, the
  policy is the single place a set would be introduced — another consequence of it being a type.
- **§18 kind test (confirmed).** `OutboxPolicy` names no storage engine, no broker and no transport
  routing concept, so it passes the first half — but it answers "does this message get **outbox**
  durability?", which is outbox mechanics, not vocabulary two capabilities need to talk to each
  other. It stays in `reliar-outbox`, on the `OutboxStore`/`RetryPolicy`/dispatcher side of ADR
  0032's line, exactly as §1 places the router. `reliar-core` still gains no item.
- **§33 (confirmed).** The policy emits **nothing** — no span, no event, no metric, no `tracing`
  dependency use at all. `reliar.outbox.route` and the `OutboxMetrics::routed` hook stay on the
  router, which is the layer that has a message id, a route taken and an outcome worth recording.
- **Blast radius.** Contract-level only, before any code exists: no removed item ever shipped, so
  nothing here is a breaking change to a published surface. The contract
  (`../architecture/routing-publisher-contract.md`) is updated in the same change — §2 retitled,
  §2.5 added, §4/§4.2 reduced to composition, §9–§13 and the test matrix and slices adjusted. SRS
  §20.2's rules and truth table are **unchanged**; only the sentence naming the mechanism gains the
  policy, proposed to the PO in the amendment draft §1.

## Amendment D — the routing publisher **is** a `Publisher` (2026-09-05)

**Decided by the human**, fourth instruction of the day on this feature, in two parts: *"does we need
outbox enqueue"* (is the `OutboxEnqueue`/`OutboxEnqueueIn<Cx>` pair necessary?) and *"the outbox
routing is actually an implementation of `Publisher` and it can have name `OutboxPublisher` … we can
publish via outbox and inside outbox that have routing policy; for using without outbox use NATS
transport directly, that is `NatsPublisher`."*

Amendments A–C settled the rule. D settles **what the application holds**: not a router with two
bespoke methods, but a `reliar_core::Publisher` whose implementation happens to consult a policy.

### D.1 The obstacle, stated honestly

`Publisher::publish(&self, envelope: &SerializedEnvelope)` carries **no transaction**, and a routed
message must be staged **inside the caller's** transaction (SRS §20, §19.6). One type cannot both
satisfy that signature and receive a transaction — so the `Publisher` impl goes on a second,
**transaction-scoped** type that the first one hands out:

```rust
let published = outbox.in_transaction(&mut tx);   // ScopedOutboxPublisher<'_, S, P, Tx>
published.publish(&serialized).await?;            // <- reliar_core::Publisher
tx.commit().await?;
```

The scoped value is a `Publisher` for the duration of the borrow. Generic host code written against
`impl Publisher` accepts it unchanged; that is the human's model, delivered where it is meaningful —
inside a transaction, which is the only place the outbox exists at all.

### D.2 One staging trait, `OutboxStaging<Tx>` — the pair collapses

**The honest answer to "do we need it":** *some* trait with a transaction the crate cannot name is
unavoidable. `reliar-outbox` may not depend on sqlx, and SRS §19.6 deliberately keeps `enqueue` off
`OutboxStore` because it takes the provider's own transaction handle. The choice is not
"trait or no trait" — it is **how many**.

```rust
pub trait OutboxStaging<Tx>: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static + Classify;
    fn stage(&self, tx: &mut Tx, envelope: &SerializedEnvelope)
        -> impl Future<Output = Result<MessageId, Self::Error>> + Send;
}
```

Two things change, and both are consequences of D.1 rather than new preferences.

- **The base trait `OutboxEnqueue` is deleted.** Its whole job was to name `E::Error` *without* a
  transaction in scope, for the transaction-less `publish`. Under D, the only method that can stage
  lives on the scoped type — which has `Tx` in scope — so `<S as OutboxStaging<Tx>>::Error` is always
  nameable where it is needed, and the un-scoped `OutboxPublisher::publish_direct` never touches the
  store, so its error does not mention the staging error at all. The reason for the split evaporated;
  the split goes with it.
- **The parameter is `Tx`, not `Cx`.** `stage` takes `&mut Tx`, so the Postgres impl is
  `impl<'c, Ser> OutboxStaging<Transaction<'c, Postgres>> for PostgresOutboxStore<Ser>` — **one**
  lifetime, universally quantified by the method rather than by the impl. This deletes the
  `'c`-invariance hazard §2 documented and the "**never write `where 'c: 'a`**" trap of the
  consequences list: there is no higher-ranked check to be non-general to. It also makes the scoped
  type's reborrow (`stage(&mut **guard, …)`) expressible, which an opaque by-value `Cx` is not —
  a generic `Cx` cannot be reborrowed, so `Cx` could not have survived D.1 in any case.

**Not on `OutboxStore` with a GAT `type Tx<'a>`.** Adding a required method to a **published** trait
breaks every external implementor and every test double, and the GAT would have to spell
`&'a mut Transaction<'c, Postgres>` — reintroducing the second lifetime and the invariance §2
rejected. A separate one-method trait costs one public item and keeps the claim side (`OutboxStore`)
and the staging side independently implementable, which is what SRS §19.6 asked for in the first
place.

Verified by compiling the shape (rustc 1.98, a stand-in with `sqlx::Transaction`'s invariance): the
provider impl, the scoped `Publisher` impl, `publish_batch`'s default, a generic
`fn f<P: Publisher>(&P)` accepting the scoped value, and the `Send`-ness of the returned future all
hold, with no HRTB and no outlives bound anywhere.

### D.3 The caller serializes; `Ser` disappears

`Publisher::publish` takes bytes, so `OutboxPublisher` holds no `Serializer`, has no `Ser` type
parameter, and has no serialization step. The caller builds the `SerializedEnvelope` once — the same
three lines it already writes for `NatsPublisher` — and both paths receive **that value**:

```rust
let bytes = serializer.serialize(&envelope.body)?;
let mut serialized = envelope.map_body(|_| bytes);
serialized.metadata.delivery.content_type = serializer.content_type().clone();
```

§3's guarantee survives in a stronger form. It was "one serializer, so the bytes cannot differ by
route"; it is now **one buffer, so the bytes are the same object** — route-independence is no longer
an argument about configuration, it is an identity. SRS §12's "`content_type` is set by whoever
serialized the body" holds unchanged, and it is now the *only* rule: `stage` persists
`metadata.delivery.content_type` verbatim, and the transport mapper reads the same field.

Consequences: `RouteError::Serialize` disappears (nothing in this crate serializes), the `Arc<Ser>`
and the manual `Clone`/`Debug` gymnastics around it disappear, two type parameters leave every call
site, and `T: Message + Sync` — the bound §"consequences" apologised for — disappears too, because
no method here is generic over a body type any more.

The one cost is ergonomic: a host that used to pass `&Envelope<T>` now serializes first. That is
deliberate symmetry with `NatsPublisher` (the human's second point), and it is the price of being a
`Publisher` rather than a look-alike. A `Serializer::serialize_envelope` provided method in
`reliar-core` would remove even that, and is left **Open** — it is additive, it is a `reliar-core`
change, and it should not ride inside this reshape.

### D.4 Which type implements `Publisher`, and the guard that actually holds

- **`ScopedOutboxPublisher<'a, S, P, Tx>` implements `reliar_core::Publisher`.** Routed types are
  staged in the borrowed transaction; direct types are forwarded to the transport publisher.
- **`OutboxPublisher<S, P, M>` does not.** It exposes `in_transaction(&mut tx)` and
  `publish_direct(&SerializedEnvelope)`, whose routed types return
  `DirectPublishError::TransactionRequired` — never a silent downgrade (§4's D4 rule, intact).

The human's "it is a `Publisher`" is honoured by the scoped type; §4's cycle hazard is answered by
the same choice, and this time **the compiler enforces it**. `OutboxDispatcher` requires
`P: Publisher + Send + Sync + 'static`, and `ScopedOutboxPublisher<'a, …>` borrows a transaction: it
is `'static` only if a host deliberately leaks both the publisher and the transaction, and it is not
`Clone`. An accidental `OutboxDispatcher::builder(store, outbox.in_transaction(&mut tx))` does not
compile. That is strictly better than ADR 0033 §4's original guard, which was rustdoc plus a
review rule.

Weighed and rejected for the un-scoped type:

- **`impl Publisher for OutboxPublisher`, routed types → `TransactionRequired`.** It is the shape the
  human's sentence most literally describes, and it is the one §4 rejected: it makes the
  dispatcher-feedback cycle wireable, and with a `Permanent` classification every message the outbox
  exists to protect would go **dead**. It is also a `Publisher` that fails a subset of its inputs
  *decided by configuration* — the same call site works in one deployment and errors in another,
  which no implementor of that trait should do.
- **A `TransportPublisher` marker bound on `OutboxDispatcher`** to make the cycle unrepresentable
  while still implementing `Publisher` on the un-scoped type. Rejected: it tightens the bound of a
  **published** trait (breaking `reliar-outbox`), forces every present and future transport to
  implement a marker whose only purpose is to exclude one type, and buys nothing once the un-scoped
  type is not a `Publisher`.
- **A runtime refusal inside `OutboxDispatcher::build`.** Not expressible — there is nothing to
  inspect without a marker trait, and `type_name` matching is not a design.

Adding the impl later, if a host ever produces a case, is **additive** and semver-safe. Removing it
would not be. The safe direction is the one taken.

### D.5 Interior mutability: `tokio::sync::Mutex<&'a mut Tx>`

`Publisher::publish` takes `&self`; staging needs `&mut Tx`. The scoped type therefore holds
`tokio::sync::Mutex<&'a mut Tx>` and reborrows through the guard.

- **`std::sync::Mutex` is unusable**: its guard is not `Send`-safe to hold across an await, so the
  returned future would not be `Send` and callers could not spawn or use it in a multi-thread
  runtime task.
- **`RefCell`/`Cell` are unusable**: not `Sync`, and `Publisher: Send + Sync`.
- **`Send`/`Sync` proof.** `tokio::sync::Mutex<T>` is `Send + Sync` when `T: Send`; `T = &'a mut Tx`
  is `Send` when `Tx: Send`, which `sqlx::Transaction<'_, Postgres>` is. The guard is `Send` under
  the same condition, so `publish`'s future is `Send`. The bound the contract carries is exactly
  `Tx: Send` — nothing wider. Compile-verified.
- **No contention by construction.** A transaction is not a concurrency point: two publishes on one
  scoped value serialize, which is the only correct behaviour, and the mutex is never held across
  anything but the single `stage` call. `publish_batch`'s inherited default loops over `publish`, so
  a batch stages sequentially in the caller's transaction — again the only correct behaviour, and the
  reason **the default is deliberately not overridden**.
- **`tokio::sync` is already a dependency** of `reliar-outbox` (the dispatcher's `Semaphore`). No new
  dependency.

### D.6 Positional batch results are not durability

`publish_batch` returns one `Result` per envelope, positionally. On the scoped view an `Ok` means
*the statement was accepted*, **not** that the message is durable: durability is decided by the
caller's `commit`, and on Postgres a failed statement aborts the whole transaction, so an
`Err(RouteError::Stage(_))` anywhere in the vector invalidates the `Ok`s before it. This is stated in
the rustdoc, in the contract, and it gets its own test (R22) — it is exactly the kind of quiet
false-positive the project's honest-semantics rule exists to prevent.

### D.7 What this costs and what it deletes

Deleted: `OutboxEnqueue`, `OutboxEnqueueIn<Cx>`, `OutboxRouter`, `Routed`, `RouteError::Serialize`,
`RouteError::TransactionRequired`, the `Ser` type parameter and its `Arc`, the `T: Message + Sync`
bound, the `where 'c: 'a` trap and its regression test, and the whole serialization step (§4.3).
Added: `OutboxStaging<Tx>`, `OutboxPublisher`, `ScopedOutboxPublisher`, `DirectPublishError`. Net
public items: unchanged. Net concepts: fewer, and one of them (`Publisher`) the host already knows.

Unchanged by D: `OutboxPolicy` and every rule in §2.1 (Amendments B and C stand untouched),
`MessageTypeNames`, the three `OutboxSettings` fields and their env keys, the "no retry, no sleep"
rule, the direct path's non-transactional honesty, `reliar.outbox.route` and `OutboxMetrics::routed`,
and the §18 placement in `reliar-outbox`.

Two ergonomic consequences a host feels, both documented in the guide:

1. The scoped value borrows the transaction for its lifetime, so a host that interleaves its own SQL
   between publishes writes the one-expression form
   `outbox.in_transaction(&mut tx).publish(&e).await?` (the temporary ends with the statement) rather
   than holding the scope open.
2. Dropping the scoped value **neither commits nor rolls back**. The caller owns the transaction, as
   it always did.

### D.8 Blast radius

No published surface breaks: nothing named in D ever shipped — RELIAR-45's slices 1–5 are built but
unreleased. `reliar-outbox` still ships **0.3.0** and `reliar-store-postgres` **0.3.0** (ADR 0034);
`reliar-core` is untouched, including its `DeliveryMetadata::content_type` rustdoc, which already
says "whoever serialized the body". The contract
(`../architecture/routing-publisher-contract.md`) is rewritten in the same change — preamble, §1,
§3–§5, §6, §7, §9–§13 — and §11 becomes a **reshaping** list over the existing code rather than a
build-from-nothing one. SRS §20.2's rules and truth table are unchanged; the mechanism sentence and
the "SHALL NOT implement `Publisher`" clause need revising, proposed to the PO in the amendment
draft §1.

## Open

- Prefix/wildcard matching (`orders.*`) — additive, deferred until an operator asks. It would apply
  to both lists, and disallow would still win. Amendment C makes this a change to one method body.
- A `stage_with(tx, envelope, EnqueueOptions)` overload, once `Ordering::PerKey` ships. A host that
  needs an ordering key today calls `PostgresOutboxStore::enqueue_with` directly.
- A `Serializer::serialize_envelope` provided method in `reliar-core` (Amendment D §3) — additive,
  removes the three-line serialize-then-set-`content_type` pattern from every call site, and needs
  its own slice and a `reliar-core` bump.
- A `Publisher` impl on the un-scoped `OutboxPublisher` (Amendment D §4) — additive if a real case
  appears, and it would need an enforceable dispatcher guard first.
- ~~Whether the future `reliar-messaging` facade's `publish`/`send` (SRS §36) delegates here or
  re-states the rule.~~ **Closed by Amendment C:** the facade holds an `OutboxPolicy`. There is
  nothing to re-state.
