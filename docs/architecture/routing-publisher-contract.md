# Routing-publisher contract — `reliar-outbox` + `reliar-store-postgres` (v0.2)

**Status: FROZEN for RELIAR-45 — 2026-09-05.** Every signature below is what the engineer builds.
**Changing anything here requires an ADR first**, then an update to this file, then a notification
to everyone building against it.

Decided by **ADR 0033**; extracted from `../srs.md` v1.1.4 §7, §12, §19.4, §19.6, §20, §22, §23,
§33 and story `$BACKLOG_DIR/docs/stories/RELIAR-43-outbox-routing-publisher.md` (AC D1–D8).

`phase1-contract.md` and `phase2-contract.md` still govern everything they cover; this file adds a
composition on top of them and changes **no existing signature**. Their "conventions that apply to
everything below" preamble applies here verbatim and is not repeated: rustdoc on every public item,
`#[non_exhaustive]`, `impl Future + Send` in traits and never `async fn` in a trait *declaration*,
hand-rolled errors with `source()`, a `Debug` that never prints payloads or header values.

Two rules are specific to this slice:

- **`OutboxRouter` must never implement `reliar_core::Publisher`.** A `Publisher` impl would let a
  host wire the router into `OutboxDispatcher` and feed the outbox back into itself (ADR 0033 §4).
  Adding one is a blocker finding.
- **The router never retries and never sleeps.** It is called on the host's request path, usually
  with the host's transaction open; a retry would hold a database transaction across broker I/O
  (conventions §6, SRS §21).

---

## 1. Where it lives

```
reliar-store-postgres ──▶ reliar-outbox ──▶ reliar-core
   impl OutboxEnqueueIn<&mut Transaction<'_, Postgres>>
                            OutboxRouter, OutboxEnqueue, RoutingSettings
```

No new crate. No new dependency in any crate. `reliar-core` gains **no item** — only a one-sentence
rustdoc clarification (§8). `reliar-outbox` still names no sqlx/postgres/broker type: the caller's
transaction reaches it only as an opaque type parameter `Cx`.

## 2. Settings — `reliar-outbox`, module `settings`

```rust
/// Whether a message is staged in the outbox or published straight to the transport.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default, deny_unknown_fields))]
#[non_exhaustive]
pub struct RoutingSettings {
    /// `true` (the default): the routed-types rule applies. `false`: **every** message publishes
    /// directly and the store is never touched — `routed_types` is ignored.
    ///
    /// This switches *routing*, never the dispatcher: rows already staged must still be drained,
    /// so a deployment that flips this to `false` keeps running its `OutboxDispatcher`.
    pub enabled: bool,
    /// The message-type names that route through the outbox. **Empty (the default) means every
    /// type is routed** — the durable default.
    pub routed_types: RoutedTypes,
}

impl Default for RoutingSettings {                 // enabled: true, routed_types: empty
    fn default() -> Self { … }
}

impl RoutingSettings {
    #[must_use] pub const fn enabled(self, enabled: bool) -> Self;
    #[must_use] pub fn routed_types(self, routed_types: RoutedTypes) -> Self;

    /// The routing decision for one message type. `RouteKind::Outbox` when routing is enabled and
    /// either the list is empty or it contains `name` exactly.
    #[must_use] pub fn route_for(&self, message_type: &MessageType) -> RouteKind;
}

/// A validated list of message-type **names** (`"orders.created"`), never `Display` forms
/// (`"orders.created.v1"`). Order is irrelevant; duplicates are tolerated.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "Vec<String>", into = "Vec<String>"))]
pub struct RoutedTypes(Vec<String>);

impl RoutedTypes {
    /// Empty — every type routes through the outbox.
    #[must_use] pub const fn all() -> Self;

    /// Parses a comma-separated list. Entries are trimmed; empty entries are dropped, so `""`
    /// yields [`Self::all`] and `"a,,b"` yields `[a, b]`.
    ///
    /// # Errors
    /// [`ConfigError::VersionedRoutedType`] for an entry ending in `.v<digits>` — that is
    /// `MessageType`'s `Display` form, and matching is on the name alone, so accepting it would
    /// silently match nothing (ADR 0033 §5).
    pub fn parse(list: &str) -> Result<Self, ConfigError>;

    /// Same validation, from any iterator of names.
    ///
    /// # Errors
    /// As [`Self::parse`].
    pub fn try_from_iter<I, S>(names: I) -> Result<Self, ConfigError>
    where I: IntoIterator<Item = S>, S: Into<String>;

    /// `true` when the list is empty — every type routes through the outbox.
    #[must_use] pub fn is_all(&self) -> bool;

    /// Exact, case-sensitive. `O(n)` over a list expected to hold a handful of names; allocates
    /// nothing.
    #[must_use] pub fn contains(&self, name: &str) -> bool;

    /// The configured names, for diagnostics.
    #[must_use] pub fn names(&self) -> &[String];
}

impl TryFrom<Vec<String>> for RoutedTypes { type Error = ConfigError; … }
impl From<RoutedTypes> for Vec<String> { … }
```

`OutboxSettings` gains one field and one builder method (additive; the struct is `#[non_exhaustive]`
with `#[serde(default)]`):

```rust
pub struct OutboxSettings {
    pub dispatcher: DispatcherSettings,
    pub retention: RetentionSettings,
    pub routing: RoutingSettings,          // new
}
impl OutboxSettings { #[must_use] pub fn routing(self, routing: RoutingSettings) -> Self; }
```

### 2.1 Environment (`OutboxSettings::from_env(prefix)`, flat under `prefix` as everywhere else)

| Key | Type | Default | Notes |
|---|---|---|---|
| `{prefix}ROUTING_ENABLED` | bool | `true` | `true`/`false`/`1`/`0`, case-insensitive, trimmed. Anything else → `SettingsError::Parse { value_kind: "a boolean (\"true\" or \"false\")" }`. **Not** `{prefix}ENABLED` — see ADR 0033 §6. |
| `{prefix}ROUTED_TYPES` | comma list | empty = all | Parsed by `RoutedTypes::parse`. A `.v<digits>` entry → `SettingsError::Parse { value_kind: "message type names without a version suffix" }` (the offending value is never echoed, per ADR 0019). |

Absent variable → the default, never a reset. Present-but-invalid → `Err`, never a silent fallback.

## 3. The enqueue capability — `reliar-outbox`, module `enqueue`

```rust
/// What an outbox-enqueue capability fails with. Method-free on purpose: it names the error
/// **without** a transaction-context type, so [`OutboxRouter`] can state its error once and
/// [`OutboxRouter::publish`] — which has no transaction — can name it too.
pub trait OutboxEnqueue: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
}

/// Staging a message in the caller's transaction, once per context type the provider supports.
///
/// `Cx` is the provider's own transaction handle — `&mut sqlx::Transaction<'_, Postgres>` for
/// `reliar-store-postgres`. It is a type parameter precisely so this crate names no storage type
/// (SRS §19.6, ADR 0033 §2).
pub trait OutboxEnqueueIn<Cx>: OutboxEnqueue {
    /// Stages `envelope` — already serialized — in `cx`. Returns the id written, so the caller can
    /// use it as the next message's `causation_id` in the same transaction.
    ///
    /// The implementation SHALL persist `envelope.metadata.delivery.content_type` verbatim: the
    /// caller serialized the body and is authoritative about its content type.
    ///
    /// # Errors
    /// Provider-defined. A failure has typically aborted `cx`; the caller must roll back.
    fn enqueue_serialized(
        &self,
        cx: Cx,
        envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<MessageId, Self::Error>> + Send;
}
```

Name note: `enqueue_serialized`, never `enqueue` — `PostgresOutboxStore` keeps its inherent typed
`enqueue`/`enqueue_with`, and an identically named trait method there would be a call-site trap.

## 4. `OutboxRouter` — `reliar-outbox`, module `router`

```rust
/// One publish call that either stages the message in the outbox or sends it straight to the
/// transport, decided by [`RoutingSettings`].
///
/// **Not a [`reliar_core::Publisher`], and it never will be** — a `Publisher` impl would let a
/// host pass this to `OutboxDispatcher` and feed the outbox back into itself (ADR 0033 §4). The
/// dispatcher's publisher is always the transport publisher, and so is `P` here.
pub struct OutboxRouter<E, P, Ser> { … }          // holds E, P, Arc<Ser>, RoutingSettings

impl<E, P, Ser> Clone for OutboxRouter<E, P, Ser> where E: Clone, P: Clone { … }   // manual: never bounds Ser
impl<E, P, Ser> fmt::Debug for OutboxRouter<E, P, Ser> { … }                       // settings only, finish_non_exhaustive

impl<E, P, Ser> OutboxRouter<E, P, Ser>
where
    E: OutboxEnqueue,
    P: Publisher,
    Ser: Serializer,
{
    /// `enqueuer` is normally the provider store, `publisher` the transport publisher, `serializer`
    /// the **same** serializer the store was built with (the router serializes both routes, so the
    /// bytes on the wire do not depend on the route).
    #[must_use]
    pub fn new(enqueuer: E, publisher: P, serializer: Ser, settings: RoutingSettings) -> Self;

    /// The routing decision this router would make for `message_type`, without publishing.
    #[must_use]
    pub fn route_for(&self, message_type: &MessageType) -> RouteKind;

    /// The configured rule.
    #[must_use]
    pub fn settings(&self) -> &RoutingSettings;

    /// Publishes `envelope` with the caller's transaction available.
    ///
    /// - **Routed** → serialize, then `enqueue_serialized(cx, …)`. The message becomes visible when
    ///   the caller commits and is published later by an `OutboxDispatcher`: durable, at-least-once,
    ///   with the documented duplicate windows.
    /// - **Direct** → serialize, then `Publisher::publish` **immediately**. `cx` is not touched — no
    ///   statement is issued on it. This publish is **not part of the caller's transaction**: if the
    ///   transaction later rolls back, the message is already on the wire. It is one attempt with no
    ///   Reliar-side retry, backoff, dead state or duplicate window.
    ///
    /// A direct publish here runs while the caller's transaction is open — network I/O holding a
    /// database transaction. Configure a publisher-side timeout, and prefer calling this before
    /// opening (or after committing) the transaction for types you route directly.
    ///
    /// # Errors
    /// [`RouteError::Serialize`] before either path is taken (the transaction is untouched);
    /// [`RouteError::Enqueue`] — `cx` has typically been aborted, roll back;
    /// [`RouteError::Publish`] — `cx` is untouched and still committable.
    pub async fn publish_in<T, Cx>(
        &self,
        cx: Cx,
        envelope: &Envelope<T>,
    ) -> Result<Routed, RouteError<E::Error, P::Error>>
    where
        T: Message + Sync,
        E: OutboxEnqueueIn<Cx>;

    /// Publishes `envelope` from a call site that has no transaction.
    ///
    /// Only the direct path is reachable. A type the rule routes through the outbox returns
    /// [`RouteError::TransactionRequired`] — this method **never** falls back to a direct publish,
    /// because that would silently cancel the durability the operator configured.
    ///
    /// # Errors
    /// [`RouteError::TransactionRequired`], [`RouteError::Serialize`], [`RouteError::Publish`].
    pub async fn publish<T>(
        &self,
        envelope: &Envelope<T>,
    ) -> Result<Routed, RouteError<E::Error, P::Error>>
    where
        T: Message + Sync;
}
```

`T: Sync` is required because the returned future holds `&Envelope<T>` across an await and `&T: Send`
needs `T: Sync`. State it in the rustdoc; do not try to engineer it away.

### 4.1 Outcome types

```rust
/// Which way a message went.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RouteKind {
    /// Staged in the outbox, inside the caller's transaction.
    Outbox,
    /// Published straight to the transport, outside any transaction.
    Direct,
}

impl RouteKind {
    /// `"outbox"` / `"direct"` — the span field and metric label value.
    #[must_use] pub const fn as_str(self) -> &'static str;
    #[must_use] pub const fn is_outbox(self) -> bool;
}

/// What one routed publish did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
#[non_exhaustive]
pub struct Routed {
    /// The envelope's id — the id staged, or the id published.
    pub message_id: MessageId,
    /// Which path it took.
    pub route: RouteKind,
}
```

### 4.2 Serialization step (both paths, exactly once)

```
bytes            = serializer.serialize(&envelope.body)?
metadata         = envelope.metadata.clone() with delivery.content_type = serializer.content_type()
SerializedEnvelope::from_parts(envelope.id, envelope.message_type.clone(), bytes, metadata,
                               envelope.headers().cloned())
```

## 5. Errors

```rust
/// Why a routed publish failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum RouteError<E, P> {
    /// The rule routes this type through the outbox, but the call site has no transaction
    /// ([`OutboxRouter::publish`]). Use [`OutboxRouter::publish_in`], or stop routing this type.
    TransactionRequired {
        /// The type that requires a transaction.
        message_type: MessageType,
    },
    /// The configured `Serializer` rejected the body. Boxed rather than a third type parameter —
    /// a cold path (ADR 0033 consequences).
    Serialize {
        /// The serializer's own error.
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// The store rejected the staged row. The caller's transaction has typically been aborted.
    Enqueue(E),
    /// The transport rejected the direct publish. The caller's transaction is untouched.
    Publish(P),
}
```

`Display`/`Error` hand-rolled; `source()` returns the inner error for all but
`TransactionRequired`. **Never** prints payload bytes or header values. No `Classify` impl in v0.2 —
the caller sees the transport's own error and can classify it itself; adding one later is additive.

`ConfigError` (existing enum in `reliar-outbox`, `#[non_exhaustive]`) gains:

```rust
/// A routed-types entry was empty after trimming. (Reserved: the parser drops empties, so this
/// only fires from `try_from_iter`/serde with an explicitly empty name.)
EmptyRoutedType,
/// A routed-types entry ends in `.v<digits>` — that is `MessageType`'s `Display` form and would
/// match nothing. Configure the name alone.
VersionedRoutedType {
    /// The offending entry (a message type name — not sensitive).
    value: String,
},
```

## 6. Provider direction — `reliar-store-postgres`

One impl, no SQL change, no migration, no `.sqlx/` change:

```rust
impl<Ser> OutboxEnqueue for PostgresOutboxStore<Ser> {
    /// Serialization cannot fail on this path — the caller serialized the body — so the error's
    /// serializer parameter is uninhabited.
    type Error = EnqueueError<core::convert::Infallible>;
}

// NO `where 'c: 'a` — see the warning below.
impl<'a, 'c, Ser> OutboxEnqueueIn<&'a mut Transaction<'c, Postgres>> for PostgresOutboxStore<Ser>
where Ser: Serializer
{
    async fn enqueue_serialized(
        &self,
        cx: &'a mut Transaction<'c, Postgres>,
        envelope: &SerializedEnvelope,
    ) -> Result<MessageId, Self::Error> { … }
}
```

> **Do not add an explicit `where 'c: 'a` outlives bound to that impl.** It is implied by
> `&'a mut Transaction<'c, _>` and therefore looks harmless, but writing it makes the impl
> non-general to the higher-ranked check that runs when a host asserts the returned future is
> `Send` — every `tokio::spawn`/Axum call site then fails with
> *"implementation of `OutboxEnqueueIn` is not general enough … but it actually implements
> `OutboxEnqueueIn<&mut Transaction<'2, Postgres>>`, for some specific lifetime `'2`"*, pointing at
> the caller rather than at the impl. Verified on rustc 1.98 against a stand-in with the same
> invariance as `sqlx::Transaction`: with the bound the spawn fails, without it everything compiles.
> A regression test belongs in slice 4 (R14a): a test helper that takes the router and asserts the
> `publish_in` future is `Send` from inside a plain (non-`'static`) transaction scope.

- Reuse the existing `enqueue_with` body: `search_path` handling, `insert_row`, the "restore only on
  success" rule, `map_enqueue_error`. `insert_row` already ignores the body type — generalize it to
  take the payload and the envelope separately (or call it with the `SerializedEnvelope` directly);
  the SQL text must not change, so `.sqlx/` stays valid.
- `content_type` written = `envelope.metadata.delivery.content_type` (the router's), **not**
  `self.content_type()`. This is the only semantic difference from the inherent `enqueue`, and it is
  what makes the route-independent-bytes guarantee hold. Document it on the impl.
- No `EnqueueOptions`/`ordering_key` on this path in v0.2 (ADR 0033 consequences).

## 7. `test-support` additions — `reliar-outbox`

```rust
/// A stand-in for a provider transaction in fake-driven tests: it carries no state, it exists so
/// a test exercises the same `publish_in(cx, …)` shape a real host does.
#[derive(Clone, Copy, Debug, Default)]
pub struct InMemoryTransaction;

impl OutboxEnqueue for InMemoryOutboxStore { type Error = InMemoryStoreError; }
impl OutboxEnqueueIn<&mut InMemoryTransaction> for InMemoryOutboxStore { … }   // delegates to insert
```

Plus one knob, matching the existing `fail_next*` family: `fail_next_enqueue(&self, n: usize)` and
`enqueue_call_count(&self) -> usize`. `RecordingPublisher` already records publishes and counts.

## 8. `reliar-core` — doc-only

`DeliveryMetadata::content_type`'s rustdoc says it is "authoritatively set by the store at enqueue".
Reword to: set by **whoever serialized the body** — the store on `PostgresOutboxStore::enqueue`, the
router on `OutboxRouter::publish_in`/`publish` — and persisted verbatim by `enqueue_serialized`. No
signature, no item, no dependency changes in `reliar-core`.

## 9. Observability

- One span per call: `debug_span!("reliar.outbox.route", message.id = %…, message.type = %…,
  route = …)`, `route` recorded as `RouteKind::as_str()`. Nothing else — no payload, no header
  values, no tenant id, no connection string. The span wraps the whole call so the store's or
  publisher's own spans nest under it.
- No event on success. The router never logs an error it also returns.
- `OutboxMetrics` gains a default-bodied hook (additive, ADR 0020):
  `fn routed(&self, _route: RouteKind, _message_type: &MessageType) {}`. Labels stay bounded —
  `route` has two values, `message_type` is already an accepted label. The router is generic over
  `M: OutboxMetrics` with `NoopMetrics` as its default type parameter, mirroring the dispatcher:
  `OutboxRouter<E, P, Ser, M = NoopMetrics>` with a `metrics(m)` constructor variant
  (`with_metrics(enqueuer, publisher, serializer, settings, metrics)`).

## 10. Test matrix (RELIAR-45; `reviewer` audits it)

The **AC** column cites the **story's** D1–D8 (RELIAR-43). The proposed SRS §43.D list renumbers
these into D1–D11 — the mapping is in the amendment draft, which cites the R ids below.

All unit tests live in `crates/reliar-outbox/tests/`, exercise the public API, and use
`InMemoryOutboxStore` + `RecordingPublisher` + `InMemoryTransaction`. No inline `#[cfg(test)]`.

| id | AC | Where | What must fail if the code breaks |
|---|---|---|---|
| R1 | D1 | `routing_disabled.rs` | `enabled = false`: every envelope reaches `RecordingPublisher` exactly once, `InMemoryOutboxStore` records **zero** enqueues, and `Routed::route == Direct`. Runs through both `publish_in` and `publish`. |
| R2 | D1 | `routing_disabled.rs` | `enabled = false` **with a non-empty** `routed_types` containing the type: still direct — the list is ignored when disabled. |
| R3 | D2 | `routing_all.rs` | `enabled = true`, empty list: every envelope is enqueued, `RecordingPublisher` count is **0**, `Routed::route == Outbox`, returned `message_id == envelope.id`. |
| R4 | D3 | `routing_selective.rs` | list `[a, b]`: `a`/`b` enqueued, `c` published; assert on **both** the store contents and the publisher recording, so swapping the two paths fails the test. |
| R5 | D3 | `routing_selective.rs` | Matching is by **name**: `MessageType::new("a", 1)` and `("a", 2)` both route; `("A", 1)` (case) and `("a.b", 1)` (prefix) do **not**. |
| R6 | D4 | `routing_requires_transaction.rs` | `publish` on a routed type returns `RouteError::TransactionRequired { message_type }`, the store is untouched **and the publisher was not called** (no silent downgrade). |
| R7 | D4 | doctest / `routing_selective.rs` | `publish_in` is the only path that reaches the store — a compile-fail or doc note is enough; the runtime proof is R6. |
| R8 | D5 | `routing_settings.rs` | `RoutingSettings::default()` is enabled + empty; builder round-trips; `RoutedTypes::parse("")` is `all()`; `"a,,b, c "` → `[a, b, c]`; duplicates tolerated; `"a.v1"` → `ConfigError::VersionedRoutedType`. |
| R9 | D5 | `routing_settings.rs` | `from_env`: absent keys keep defaults; `ROUTING_ENABLED=FALSE`/`0` parse; `ROUTING_ENABLED=maybe` → `SettingsError::Parse` naming the full key; `ROUTED_TYPES=a.v1` → `SettingsError::Parse`; the error **never echoes the value**. Serial/isolated env handling as in the existing settings tests. |
| R10 | D5 | `routing_settings.rs` | serde (feature on): `routed_types: ["a.v1"]` fails deserialization — validation is not bypassable through a config file. |
| R11 | — | `routing_errors.rs` | `fail_next_enqueue(1)` → `RouteError::Enqueue`, `source()` wired, `Display` mentions neither payload nor headers. A failing `RecordingPublisher`/`ScriptedPublisher` → `RouteError::Publish`. A serializer that rejects the body → `RouteError::Serialize` **before** either path runs (store and publisher both untouched). |
| R12 | — | `routing_errors.rs` | The router never retries: a `ScriptedPublisher` scripted to fail once then succeed is called **exactly once** and the call returns `Err`. |
| R13 | — | `routing_observability.rs` | With a recording subscriber: exactly one `reliar.outbox.route` span per call carrying `route`, and no payload bytes / header values in any field, event, or `Debug` output (mirrors the existing §43.A.26 test). |
| R14a | D6 | `crates/reliar-store-postgres/tests/postgres/routing_enqueue.rs` | The `publish_in` future is `Send` from a non-`'static` transaction scope — the spawnability regression the §6 warning describes. A compile-time assertion is enough; it must not be `'static`-flavoured or it proves nothing. |
| R14 | D6 | `crates/reliar-store-postgres/tests/postgres/routing_enqueue.rs` | Real Postgres: `enqueue_serialized` inside a caller transaction — the row is invisible before commit and present after; a rollback leaves nothing; `content_type` equals the **envelope's**, not the store's default; a reused `MessageId` → `EnqueueError::Duplicate`. |
| R15 | D6 | `tests/system` (e2e, Postgres + JetStream) | Routed type: `publish_in` + commit → row in `outbox` → dispatcher → message on the stream. Non-routed type: `publish_in` → on the stream **immediately**, and `SELECT count(*) FROM outbox` for that id is `0`. |
| R16 | D6 | `tests/system` | Direct path is not transactional: `publish_in` a non-routed type, then **roll back** — the message is still on the stream. The honest-guarantee test; it must be named so nobody "fixes" it. |
| R17 | D7 | doc | `cargo doc -D warnings`; the router's rustdoc doctest compiles a fake-backed example showing both routes. |

Determinism: no wall-clock sleeps; the router has no timing behavior to test with paused time.

## 11. Slices for RELIAR-45 (one engineer, in this order)

1. **Settings** — `RoutingSettings`, `RoutedTypes`, `OutboxSettings.routing`, the two `ConfigError`
   variants, `from_env` keys. Tests R8–R10. (No dependency on the rest; lands alone.)
2. **Capability + router** — `OutboxEnqueue`, `OutboxEnqueueIn<Cx>`, `OutboxRouter`, `Routed`,
   `RouteKind`, `RouteError`, the `OutboxMetrics::routed` hook, `lib.rs` re-exports and the crate-doc
   *Guarantees* bullet for the direct path. Tests R1–R7, R11–R13, R17.
3. **`test-support`** — `InMemoryTransaction`, the two impls, `fail_next_enqueue`,
   `enqueue_call_count`. (Needed by slice 2's tests; land it with them.)
4. **Postgres impl** — the two impls in `reliar-store-postgres`, `insert_row` generalization. Tests
   R14, R14a (write R14a **first** — it is the one that catches the outlives-bound trap). Verify `cargo sqlx prepare --check` is still clean (the SQL must not change).
5. **e2e + docs** — R15, R16; `docs/guides/` rollout section (start with `ROUTED_TYPES` empty or a
   short list, widen, and what "direct" costs you); `examples/axum-outbox` switched to the router as
   the reference call site (§20.1); `CHANGELOG.md` under *Unreleased*.

Feature-powerset check after slice 1 and slice 3 (`cargo hack check --feature-powerset` on
`reliar-outbox`: `serde`, `test-support`, `metrics`).

## 12. Decided here

1. Placement `reliar-outbox`, not core, not the provider (ADR 0033 §1).
2. `Cx` as a trait type parameter, base trait for the error (§2–§3).
3. The router serializes once; the capability takes bytes (§4.2, §6).
4. No `Publisher` impl on the router, ever (§4).
5. Matching by name, `.vN` entries rejected loudly (§2).
6. `ROUTING_ENABLED` rather than `ENABLED` (§2.1).
7. No retry, no timeout, no buffering in the router (preamble, §4).
8. `EnqueueError<Infallible>` as the Postgres impl's error (§6).
9. No explicit outlives bound on the provider impl, with a spawnability test guarding it (§6, R14a).

## 13. Not in this contract

Prefix/wildcard matching · per-type retry policies · runtime-mutable rules · `EnqueueOptions`/
`ordering_key` on the router · the inbox side · a `Classify` impl for `RouteError` · a `Ser` default
type parameter (would need a `json` feature on `reliar-outbox`).
