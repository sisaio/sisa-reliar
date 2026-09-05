# Routing-publisher contract — `reliar-outbox` + `reliar-store-postgres` (v0.2)

**Status: FROZEN for RELIAR-45 — 2026-09-05, reshaped by ADR 0033 Amendment D.** Every signature
below is what the engineer builds. **Changing anything here requires an ADR first**, then an update
to this file, then a notification to everyone building against it.

Decided by **ADR 0033** (incl. Amendments A, B, C and **D**, 2026-09-05); extracted from `../srs.md`
v1.1.8 §0.7, §0.8, §7, §12, §19.4, §19.6, §20, §20.2, §22, §23, §33 and story
`$BACKLOG_DIR/docs/stories/RELIAR-43-outbox-routing-publisher.md` (AC D1–D8).

> **Amendment D in one paragraph.** The application-facing object **is** a
> `reliar_core::Publisher`. `OutboxPublisher` owns the `OutboxPolicy`, a staging capability and the
> transport publisher; `outbox.in_transaction(&mut tx)` hands out a `ScopedOutboxPublisher` that
> implements `Publisher` for the borrow — routed types staged in `tx`, direct types forwarded.
> `Publisher::publish` takes bytes, so **the caller serializes** (exactly as for `NatsPublisher`) and
> there is no `Serializer` here. The `OutboxEnqueue`/`OutboxEnqueueIn<Cx>` pair collapses into one
> trait, `OutboxStaging<Tx>`. Everything about the **rule** (§2, §2.1–§2.5) is unchanged.

`phase1-contract.md` and `phase2-contract.md` still govern everything they cover; this file adds a
composition on top of them and changes **no existing signature**. Their "conventions that apply to
everything below" preamble applies here verbatim and is not repeated: rustdoc on every public item,
`#[non_exhaustive]`, `impl Future + Send` in traits and never `async fn` in a trait *declaration*,
hand-rolled errors with `source()`, a `Debug` that never prints payloads or header values.

Four rules are specific to this slice:

- **Only `ScopedOutboxPublisher` implements `reliar_core::Publisher`.** The un-scoped
  `OutboxPublisher` must not — a `Publisher` impl on a `'static`, `Clone`-able type lets a host wire
  it into `OutboxDispatcher` and feed the outbox back into itself (ADR 0033 §4, Amendment D §4).
  Adding one, or making the scoped type `'static`/`Clone`, is a blocker finding.
- **Nothing here serializes.** `reliar-outbox` names no wire format and holds no `Serializer` on this
  path; both routes carry the caller's `SerializedEnvelope` value unchanged (Amendment D §3).
  Re-serializing, or overwriting `metadata.delivery.content_type`, is a blocker finding.
- **The publisher never retries and never sleeps.** It is called on the host's request path, usually
  with the host's transaction open; a retry would hold a database transaction across broker I/O
  (conventions §6, SRS §21).
- **The rule lives in `OutboxPolicy`, and nowhere else** (ADR 0033 Amendment C). The publisher owns a
  policy and calls `policy.decide(…)`; it holds no `enabled` flag, no list, and no branch of the
  §2.1 table. Re-implementing, mirroring or "just inlining for one case" the rule anywhere —
  the publisher, `OutboxSettings`, a provider, the future `reliar-messaging` facade — is a blocker
  finding.

---

## 1. Where it lives

```
reliar-store-postgres ──▶ reliar-outbox ──▶ reliar-core
   impl OutboxStaging<Transaction<'_, Postgres>>        Publisher, SerializedEnvelope, Serializer,
                            OutboxPolicy (the rule),    Classify, SettingsError
                            OutboxPublisher (composition) ──┐
                            ScopedOutboxPublisher ─────────┘ impl reliar_core::Publisher
                            OutboxStaging<Tx>, MessageTypeNames
```

No new crate. No new dependency in any crate (`tokio::sync::Mutex` is already used by the
dispatcher). `reliar-core` gains **no item and no doc change** —
`DeliveryMetadata::content_type` already reads "authoritatively set by whoever serialized the body",
which is exactly what Amendment D makes the only rule. `reliar-outbox` still names no
sqlx/postgres/broker type: the caller's transaction reaches it only as an opaque type parameter `Tx`.

## 2. Settings and the rule — `reliar-outbox`, modules `settings` and `policy`

`OutboxSettings` carries the rule's **inputs** (§2–§2.4); `OutboxPolicy` **is** the rule (§2.5).
The split is ADR 0033 Amendment C: configuration is data, the rule is a value, and the publisher is
only a composition. Everything downstream — the publisher, a rule preview, the future
`reliar-messaging` facade — asks the policy.

The switch and the two lists are **top-level fields of `OutboxSettings`** — there is no
`RoutingSettings` sub-section (ADR 0033 Amendment A, SRS §7.2). `OutboxSettings` is
`#[non_exhaustive]`, so all three are additive; append them after `retention` rather than reordering
the existing ones.

```rust
pub struct OutboxSettings {
    pub dispatcher: DispatcherSettings,
    pub retention: RetentionSettings,
    /// `true` (the default): the routing rule applies to messages published through
    /// [`OutboxPublisher`]. `false`: **every** message publishes directly and the store is never
    /// touched — [`Self::allowed_types`] and [`Self::disallowed_types`] are both ignored.
    ///
    /// **This stops new messages entering the outbox; it never stops draining.** Rows already
    /// staged are still claimed and published by [`OutboxDispatcher`], so a deployment that flips
    /// this to `false` keeps its dispatcher running until the backlog is empty. This sentence is
    /// the whole reason the field is called `enabled` and not something longer — the rustdoc
    /// carries the nuance a name cannot (ADR 0033 Amendment A).
    pub enabled: bool,                      // new — default true
    /// The message-type names that route through the outbox. **Empty (the default) means every
    /// type is routed** — the durable default. Ignored when [`Self::enabled`] is `false`, and
    /// overridden per type by [`Self::disallowed_types`].
    pub allowed_types: MessageTypeNames,    // new — default empty
    /// The message-type names that publish **directly** even while routing is enabled.
    /// **Disallow wins over allow**, so "everything except `c`" is an empty
    /// [`Self::allowed_types`] plus `disallowed_types = [c]` — the primary rollout shape.
    ///
    /// A name present in **both** lists is a configuration error at construction, never a silent
    /// tie-break ([`OutboxPolicy::from_settings`]).
    pub disallowed_types: MessageTypeNames, // new — default empty
}

impl OutboxSettings {
    #[must_use] pub const fn enabled(self, enabled: bool) -> Self;

    /// Sets [`Self::allowed_types`].
    ///
    /// # Errors
    /// [`SettingsError::OutOfRange`] with `key = "allowed_types"` when `allowed` names a type
    /// that the already-configured [`Self::disallowed_types`] also names.
    pub fn allowed_types(self, allowed: MessageTypeNames) -> Result<Self, SettingsError>;

    /// Sets [`Self::disallowed_types`].
    ///
    /// # Errors
    /// As above, with `key = "disallowed_types"`.
    pub fn disallowed_types(self, disallowed: MessageTypeNames) -> Result<Self, SettingsError>;
}
```

**`OutboxSettings` has no `route_for` and no `validate_routing`** (ADR 0033 Amendment C — both were
contract-level only and never shipped). Evaluating the rule is [`OutboxPolicy::decide`]; checking a
settings value is `OutboxPolicy::from_settings(&settings)?`, which returns the rule it just
validated. A `route_for` delegation on the settings type would be a second public entry point to
one rule and the seed of the drift this amendment exists to prevent.

The two list setters stay fallible so a mistake is caught at the line that made it, with the field
name in the error (SRS §43.D13). They do **not** re-implement anything: the disjointness check is a
single crate-private helper in `policy.rs` that the setters, `from_env`, the serde `TryFrom` and
`OutboxPolicy::from_settings` all call. One implementation, four call sites.

`Default` keeps `enabled = true` and both lists empty, so `OutboxSettings::default()` still means
"everything durable". The existing `dispatcher(…)` / `retention(…)` builder methods are unchanged,
and neither they nor `DispatcherSettings`/`RetentionSettings` have a field or method named
`enabled`, `allowed_types` or `disallowed_types` — no shadowing, and no existing `from_env` key ends
in `ENABLED`, `ALLOWED_TYPES` or `DISALLOWED_TYPES`.

`OutboxDispatcherBuilder::build` does **not** check the routing pair: routing is not the
dispatcher's concern, and a deployment that only drains never configures the two lists. The policy
is the component that validates (§2.5).

### 2.1 The rule — SRS §20.2's truth table, verbatim

| `enabled` | `allowed_types` | `disallowed_types` | `OutboxPolicy::decide(t)` |
|---|---|---|---|
| `false` | any | any | `Direct` |
| `true` | `[]` | `[]` | `Outbox` |
| `true` | `[]` | contains *t* | `Direct` |
| `true` | contains *t* | not *t* | `Outbox` |
| `true` | non-empty, not *t* | any | `Direct` |
| `true` | contains *t* | contains *t* | configuration error at construction |

It is implemented **once**, as the body of [`OutboxPolicy::decide`] (§2.5), in this evaluation
order — it reproduces every row, and the order **is** the precedence:

```text
1. !enabled                       -> Direct
2. disallowed_types.contains(n)   -> Direct      // disallow wins
3. allowed_types.is_empty()       -> Outbox      // empty allow list = route everything
4. allowed_types.contains(n)      -> Outbox
5. otherwise                      -> Direct      // a non-empty allow list is exhaustive
```

`n` is `message_type.name()` — exact, case-sensitive, version-agnostic (§5 of ADR 0033).

Row 6 is unreachable through any `OutboxPolicy`: [`OutboxPolicy::from_settings`] refuses to build
one from an overlapping pair, so **no policy — and therefore no publisher — ever tie-breaks**. The
`decide` body is nevertheless total, and its rustdoc records that were such a pair to exist step 2
would fire and the message would go `Direct`.

### 2.2 `MessageTypeNames` — one newtype, both fields

```rust
/// A validated list of message-type **names** (`"orders.created"`), never `Display` forms
/// (`"orders.created.v1"`). Order is irrelevant; duplicates are tolerated.
///
/// One type serves both [`OutboxSettings::allowed_types`] and
/// [`OutboxSettings::disallowed_types`]: the validation, the matching and the accessors are
/// identical, and the two fields are set by two separately named methods, so a distinct newtype
/// per field would guard against an argument swap that no signature makes possible (ADR 0033
/// Amendment B).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(into = "Vec<String>"))]
pub struct MessageTypeNames(Vec<String>);

impl MessageTypeNames {
    /// The empty list. On [`OutboxSettings::allowed_types`] that means *every* type routes; on
    /// [`OutboxSettings::disallowed_types`] it means *no* type is excluded. The neutral name is
    /// deliberate — "all" is a property of the field, not of the list.
    #[must_use] pub const fn empty() -> Self;

    /// Parses a comma-separated list. Entries are trimmed; empty entries are dropped, so `""`
    /// yields [`Self::empty`] and `"a,,b"` yields `[a, b]`.
    ///
    /// `field` is the field or environment-variable name reported in the error —
    /// `"allowed_types"` or `"RELIAR_OUTBOX_ALLOWED_TYPES"`. It exists so the error names the
    /// thing the operator has to edit (SRS §43.D13).
    ///
    /// # Errors
    /// [`SettingsError::Parse`] with `value_kind = "message type names without a version suffix"`
    /// for an entry ending in `.v<digits>` — that is `MessageType`'s `Display` form, and matching
    /// is on the name alone, so accepting it would silently match nothing (ADR 0033 §5). The
    /// offending value is never echoed.
    pub fn parse(field: &str, list: &str) -> Result<Self, SettingsError>;

    /// Same validation, from any iterator of names. An entry that is empty after trimming is
    /// [`SettingsError::Parse`] with `value_kind = "non-empty message type names"` — unlike
    /// [`Self::parse`], which drops empties, an explicit empty name is a mistake worth reporting.
    ///
    /// # Errors
    /// As [`Self::parse`], plus the empty-name case.
    pub fn try_from_iter<I, S>(field: &str, names: I) -> Result<Self, SettingsError>
    where I: IntoIterator<Item = S>, S: AsRef<str>;

    /// `true` when the list holds no names.
    #[must_use] pub fn is_empty(&self) -> bool;

    /// Exact, case-sensitive. `O(n)` over a list expected to hold a handful of names; allocates
    /// nothing.
    #[must_use] pub fn contains(&self, name: &str) -> bool;

    /// The configured names, for diagnostics.
    #[must_use] pub fn names(&self) -> &[String];
}

impl From<MessageTypeNames> for Vec<String> { … }
```

**One error type for the whole routing configuration: `reliar_core::SettingsError`.** It is the
only house error whose shape is *key + reason*, which is exactly what SRS §43.D13 asks for, and it
keeps a host wiring `from_env`, a config file and a builder on one `Err` type. `ConfigError` gains
**no** variant from this slice — it stays the dispatcher's cross-field error. There is deliberately
no `TryFrom<Vec<String>>` and no `Deserialize` on `MessageTypeNames`: a bare list cannot name its
own field, so every validated construction goes through `parse`/`try_from_iter` or through
`OutboxSettings`'s deserializer (§2.4).

### 2.3 Environment (`OutboxSettings::from_env(prefix)`, flat under `prefix` as everywhere else)

| Key | Type | Default | Notes |
|---|---|---|---|
| `{prefix}ENABLED` | bool | `true` | `RELIAR_OUTBOX_ENABLED` with the conventional prefix. `true`/`false`/`1`/`0`, case-insensitive, trimmed. Anything else → `SettingsError::Parse { value_kind: "a boolean (\"true\" or \"false\")" }`. |
| `{prefix}ALLOWED_TYPES` | comma list | empty = all types routed | `MessageTypeNames::parse("{prefix}ALLOWED_TYPES", …)`. |
| `{prefix}DISALLOWED_TYPES` | comma list | empty | `MessageTypeNames::parse("{prefix}DISALLOWED_TYPES", …)`. |

With the conventional prefix the three keys are `RELIAR_OUTBOX_ENABLED`,
`RELIAR_OUTBOX_ALLOWED_TYPES` and `RELIAR_OUTBOX_DISALLOWED_TYPES`.

Absent variable → the default, never a reset. Present-but-invalid → `Err`, never a silent fallback.
After both lists are read, `from_env` runs the shared disjointness check, so an overlap surfaces as
`SettingsError::OutOfRange { key: "{prefix}DISALLOWED_TYPES", … }` — the full key, no value echoed.

### 2.4 serde (feature `serde`)

Validation is not bypassable through a config file. `OutboxSettings` deserializes through a private
repr:

```rust
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(try_from = "OutboxSettingsRepr"))]
pub struct OutboxSettings { … }

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct OutboxSettingsRepr {
    #[serde(default)]
    dispatcher: DispatcherSettings,
    #[serde(default)]
    retention: RetentionSettings,
    #[serde(default = "default_enabled_true")]
    enabled: bool,
    #[serde(default)]
    allowed_types: Vec<String>,
    #[serde(default)]
    disallowed_types: Vec<String>,
}

fn default_enabled_true() -> bool {
    true
}

impl TryFrom<OutboxSettingsRepr> for OutboxSettings { type Error = SettingsError; … }
```

The `TryFrom` runs `MessageTypeNames::try_from_iter("allowed_types", …)`, then the same for
`"disallowed_types"`, then the shared disjointness check.
`deny_unknown_fields` moves from `OutboxSettings` to the repr, and so does defaulting — but **per
field**, never as a container `#[serde(default)]`: the container form fills every missing field from
`Default for OutboxSettingsRepr`, whose derive yields `enabled = false` and would silently invert the
durable default, so a document that simply omits `enabled` would stop routing through the outbox.
`#[serde(default = "default_enabled_true")]` is what keeps that document durable. (These are
deserialize-side attributes; `Serialize` still derives on the real struct and emits both lists as
arrays of strings, so a document round-trips.) Field names are unchanged, so an existing
`{dispatcher, retention}` document still deserializes.

### 2.5 `OutboxPolicy` — the rule as a value, `reliar-outbox`, module `policy`

```rust
/// The routing rule of SRS §20.2: given a [`MessageType`], does this message go through the
/// outbox or straight to the transport?
///
/// It is a **validated, immutable value**, not behaviour bolted onto a publisher. Built once from
/// an [`OutboxSettings`], it answers [`Self::decide`] for the rest of the process's life.
/// [`OutboxPublisher`] owns one and delegates every decision to it; the publisher itself holds no
/// flag, no list and no branch of the table.
///
/// Because it needs neither a store nor a transport, a host can build one to **preview** the rule
/// at startup (log which types are durable), and the rule's tests are ordinary unit tests over a
/// pure function.
///
/// # Guarantee
/// A policy that exists is unambiguous: [`Self::from_settings`] rejects an overlapping
/// allow/disallow pair, so [`Self::decide`] is total and needs no tie-break.
///
/// # Examples
/// ```
/// # use reliar_outbox::{MessageTypeNames, OutboxPolicy, OutboxSettings, RouteKind};
/// # use reliar_core::MessageType;
/// let settings = OutboxSettings::default()
///     .disallowed_types(MessageTypeNames::parse("disallowed_types", "audit.logged")?)?;
/// let policy = OutboxPolicy::from_settings(&settings)?;
/// assert_eq!(policy.decide(&MessageType::new("orders.created", 1)), RouteKind::Outbox);
/// assert_eq!(policy.decide(&MessageType::new("audit.logged", 1)), RouteKind::Direct);
/// # Ok::<_, reliar_core::SettingsError>(())
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OutboxPolicy { /* private: enabled, allowed, disallowed */ }

impl OutboxPolicy {
    /// Reads [`OutboxSettings::enabled`], [`OutboxSettings::allowed_types`] and
    /// [`OutboxSettings::disallowed_types`] and validates the pair. The dispatcher and retention
    /// sections are ignored — they belong to the worker, not to the rule — so a host passes the
    /// one `OutboxSettings` it already built.
    ///
    /// This is the **only** constructor, so the rule can never drift from the settings shape that
    /// documents it. `OutboxSettings::default()` is the cheap way to name a rule in a test.
    ///
    /// # Errors
    /// [`SettingsError::OutOfRange`] with `key = "disallowed_types"` and
    /// `message = "a message type may not appear in both allowed_types and disallowed_types"` when
    /// the two lists intersect. The offending name is **not** echoed (ADR 0019). This is the
    /// backstop for the one path the setters cannot cover — a host assigning the public fields
    /// directly; a value from `default()`, the setters, `from_env` or serde always passes.
    pub fn from_settings(settings: &OutboxSettings) -> Result<Self, SettingsError>;

    /// The routing decision for one message type — §2.1's table, evaluated in §2.1's order.
    /// Total, infallible, allocation-free, and the single implementation of the rule.
    ///
    /// Matching is on [`MessageType::name`]: exact, case-sensitive, version-agnostic.
    #[must_use] pub fn decide(&self, message_type: &MessageType) -> RouteKind;

    /// Whether routing is on — the copy of [`OutboxSettings::enabled`] this policy was built with.
    #[must_use] pub const fn enabled(&self) -> bool;

    /// The allow list this policy was built with.
    #[must_use] pub fn allowed_types(&self) -> &MessageTypeNames;

    /// The disallow list this policy was built with.
    #[must_use] pub fn disallowed_types(&self) -> &MessageTypeNames;
}
```

- **`Default`** is the default settings' rule — enabled, both lists empty, every type durable — and
  is the only way to obtain a policy without a `Result`, because that pair cannot overlap.
- **`PartialEq`/`Eq`** so a test (and R21) can assert that a publisher carries the policy it was given.
  `Debug` prints `enabled` and both lists; there is nothing else to print and no payload to leak.
- **No allocation and no hashing per decision.** The policy keeps the two `MessageTypeNames` as they
  were validated and uses their `contains` — a linear scan of `&str` over a handful of names, which
  beats a `HashSet` at this size (no hasher setup, no duplicated storage, cache-friendly) and
  allocates nothing either way. If a deployment ever configures hundreds of names, the policy is the
  **one** place a set would be swapped in — which is itself part of why it exists.
- **`decide` returns [`RouteKind`]**, the type this module owns and `OutboxMetrics::routed`
  already labels with. The story's shorthand `Route` is not introduced: one concept, one name, and
  `Route` next to `RouteError`/`RouteKind` would read as a third thing (ADR 0033 Amendment C).
- **The policy emits nothing** — no span, no event, no metric. It is a pure function object; the
  span `reliar.outbox.route` and the `routed` hook are the publisher's (§9).
- Singular **`OutboxPolicy`**: there is exactly one rule today, and a collection holding one element
  is the speculative abstraction SRS §31 forbids. If several rules ever compose, `OutboxPolicies`
  is the reserved name and can wrap this type additively.

### 2.6 `RouteKind` — the decision, `reliar-outbox`, module `policy`

Defined beside the rule that returns it (before Amendment D it sat in the router's §4.1; it never
belonged to the composition).

```rust
/// Which way a message goes.
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
```

## 3. The staging capability — `reliar-outbox`, module `staging`

**One** trait (ADR 0033 Amendment D §2 — the `OutboxEnqueue`/`OutboxEnqueueIn<Cx>` pair is deleted).

```rust
/// Staging a serialized message in the caller's own transaction — the routed half of
/// [`OutboxPublisher`].
///
/// `Tx` is the provider's transaction type: `sqlx::Transaction<'_, Postgres>` for
/// `reliar-store-postgres`. It is a **type parameter** precisely so this crate names no storage
/// type (SRS §19.6, ADR 0033 §2 + Amendment D §2), and one implementor may support several.
///
/// Deliberately **not** a method on [`OutboxStore`]: staging takes a transaction handle the claim
/// side never sees, `OutboxStore` is already published, and a GAT `type Tx<'a>` would have to
/// spell `&'a mut Transaction<'c, _>` and reintroduce the invariance ADR 0033 §2 rejected.
pub trait OutboxStaging<Tx>: Send + Sync {
    /// What staging fails with. `Classify` is required because
    /// [`ScopedOutboxPublisher`]'s `Publisher::Error` is built from it, and
    /// `reliar_core::Publisher::Error: Classify`.
    type Error: std::error::Error + Send + Sync + 'static + Classify;

    /// Stages `envelope` in `tx`. Returns the id written, so the caller can use it as the next
    /// message's `causation_id` in the same transaction.
    ///
    /// The implementation SHALL persist `envelope.metadata.delivery.content_type` **verbatim**:
    /// the caller serialized the body and is authoritative about its content type (SRS §12).
    /// It SHALL issue no network I/O other than the statement itself, and SHALL NOT commit,
    /// roll back or otherwise consume `tx` — the caller owns it.
    ///
    /// # Errors
    ///
    /// Provider-defined. A failure has typically aborted `tx`; the caller must roll back.
    fn stage(
        &self,
        tx: &mut Tx,
        envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<MessageId, Self::Error>> + Send;
}
```

Name note: `stage`, never `enqueue` — `PostgresOutboxStore` keeps its inherent typed
`enqueue`/`enqueue_with`, and an identically named trait method there would be a call-site trap.
`&mut Tx` rather than a by-value context is not a style choice: a generic by-value `Cx` cannot be
reborrowed, and [`ScopedOutboxPublisher`] must reborrow the transaction once per publish (§4.1).

## 4. `OutboxPublisher` — `reliar-outbox`, module `publisher`

```rust
/// The application's publisher when an outbox is in play: one `publish` call that either stages
/// the message in the outbox or sends it straight to the transport, as its [`OutboxPolicy`]
/// decides.
///
/// Composition only — it holds a staging capability, a transport [`Publisher`], the rule, and a
/// metrics sink. The rule itself lives in [`OutboxPolicy`] (ADR 0033 Amendment C): no `enabled`
/// flag, no list and no branch of the routing table here. Preview a decision with
/// [`Self::policy`].
///
/// # Publishing
///
/// The routed path needs the caller's transaction, and `Publisher::publish` has no parameter for
/// one — so **the `Publisher` impl lives on [`ScopedOutboxPublisher`]**, which
/// [`Self::in_transaction`] hands out for the life of a borrow:
///
/// ```text
/// let published = outbox.in_transaction(&mut tx);   // impl reliar_core::Publisher
/// published.publish(&serialized).await?;
/// tx.commit().await?;
/// ```
///
/// This type is deliberately **not** a [`reliar_core::Publisher`]: a `'static`, `Clone`-able
/// `Publisher` here could be wired into an [`crate::OutboxDispatcher`], which would drain the
/// outbox back into itself (ADR 0033 §4). For a call site with no transaction use
/// [`Self::publish_direct`], which refuses routed types loudly.
///
/// The caller serializes: both routes carry the same `SerializedEnvelope` value, so the bytes on
/// the wire cannot depend on the route (ADR 0033 Amendment D §3).
pub struct OutboxPublisher<S, P, M = NoopMetrics> { … }   // holds S, P, OutboxPolicy, M

impl<S: Clone, P: Clone, M: Clone> Clone for OutboxPublisher<S, P, M> { … }   // manual
impl<S, P, M> fmt::Debug for OutboxPublisher<S, P, M> { … }                  // policy only, finish_non_exhaustive

impl<S, P, M> OutboxPublisher<S, P, M> {
    /// The rule this publisher delegates to. Preview a decision with
    /// `outbox.policy().decide(&message_type)`.
    ///
    /// The **only** rule-shaped accessor: no `route_for`/`enabled`/`allowed_types`/
    /// `disallowed_types` delegation, because each would be a second public way to ask the same
    /// question (ADR 0033 Amendment C). Unbounded — reading the policy needs none of `S`/`P`/`M`'s
    /// trait bounds.
    #[must_use]
    pub const fn policy(&self) -> &OutboxPolicy;
}

impl<S, P> OutboxPublisher<S, P>
where
    P: Publisher,
{
    /// `staging` is normally the provider store, `publisher` the transport publisher, and
    /// `policy` the rule — `OutboxPolicy::from_settings(&settings)?` from the one `OutboxSettings`
    /// the host already built for its dispatcher.
    ///
    /// **Infallible**, because an `OutboxPolicy` that exists is already valid (§2.5).
    pub fn new(staging: S, publisher: P, policy: OutboxPolicy) -> Self;
}

impl<S, P, M> OutboxPublisher<S, P, M>
where
    P: Publisher,
    M: OutboxMetrics,
{
    /// As [`Self::new`], with a metrics sink (§9).
    pub fn with_metrics(staging: S, publisher: P, policy: OutboxPolicy, metrics: M) -> Self;

    /// Borrows `tx` and returns a [`reliar_core::Publisher`] that stages routed types in it and
    /// forwards direct types to the transport.
    ///
    /// The returned value borrows both `self` and `tx` — it is neither `'static` nor `Clone`, so
    /// it cannot be handed to an [`crate::OutboxDispatcher`] (that is the guard, and the compiler
    /// enforces it). Dropping it **neither commits nor rolls back**: the caller owns the
    /// transaction throughout.
    ///
    /// For a single publish, the one-expression form keeps the borrow to one statement:
    /// `outbox.in_transaction(&mut tx).publish(&serialized).await?`.
    #[must_use]
    pub fn in_transaction<'a, Tx>(
        &'a self,
        tx: &'a mut Tx,
    ) -> ScopedOutboxPublisher<'a, S, P, Tx, M>
    where
        S: OutboxStaging<Tx>,
        Tx: Send;

    /// Publishes from a call site that has **no** transaction.
    ///
    /// Only the direct path is reachable. A type the rule routes through the outbox returns
    /// [`DirectPublishError::TransactionRequired`] — this method **never** falls back to a direct
    /// publish, because that would silently cancel the durability the operator configured. It is
    /// one attempt with no Reliar-side retry, backoff, dead state or duplicate window.
    ///
    /// # Errors
    ///
    /// [`DirectPublishError::TransactionRequired`], [`DirectPublishError::Publish`].
    pub async fn publish_direct(
        &self,
        envelope: &SerializedEnvelope,
    ) -> Result<(), DirectPublishError<P::Error>>;
}
```

Note the bound placement: `in_transaction` carries `S: OutboxStaging<Tx>` **at the method**, so one
`OutboxPublisher` value serves every transaction type its store supports, and a host whose store
stages nothing can still hold one for `publish_direct`.

### 4.1 `ScopedOutboxPublisher` — the transaction-scoped view, same module

```rust
/// An [`OutboxPublisher`] scoped to one borrowed transaction — and a full
/// [`reliar_core::Publisher`] for the life of that borrow.
///
/// Returned by [`OutboxPublisher::in_transaction`]. "Scoped" is about the **borrow**, not about
/// delivery: a direct-routed publish here is still **not** part of the caller's transaction (see
/// the `Publisher` impl below).
///
/// Not `Clone`, not `'static`, and never made either: those are what stop it reaching an
/// [`crate::OutboxDispatcher`] (ADR 0033 Amendment D §4).
pub struct ScopedOutboxPublisher<'a, S, P, Tx, M = NoopMetrics> {
    // &'a OutboxPublisher<S, P, M>  +  tokio::sync::Mutex<&'a mut Tx>
}

impl<S, P, Tx, M> fmt::Debug for ScopedOutboxPublisher<'_, S, P, Tx, M> { … }  // policy only; never locks

impl<'a, S, P, Tx, M> Publisher for ScopedOutboxPublisher<'a, S, P, Tx, M>
where
    S: OutboxStaging<Tx>,
    P: Publisher,
    Tx: Send,
    M: OutboxMetrics,
{
    /// Routed and direct failures in one enum; `Classify` forwards to whichever occurred (§5).
    type Error = RouteError<<S as OutboxStaging<Tx>>::Error, P::Error>;

    /// Publishes `envelope` by the rule.
    ///
    /// - **Routed** → `staging.stage(tx, …)` in the borrowed transaction. The message becomes
    ///   visible when the caller commits and is published later by an
    ///   [`crate::OutboxDispatcher`]: durable, at-least-once, with the documented duplicate
    ///   windows.
    /// - **Direct** → the transport publisher, **immediately**. The transaction is not touched —
    ///   no statement is issued on it — and this publish is **not part of it**: if the caller
    ///   later rolls back, the message is already on the wire. One attempt, no Reliar-side retry,
    ///   backoff, dead state or duplicate window.
    ///
    /// A direct publish here runs while the caller's transaction is open — network I/O holding a
    /// database transaction. Configure a publisher-side timeout, and prefer publishing
    /// direct-routed types before opening (or after committing) the transaction.
    ///
    /// Publishes on one scoped value are **serialized**: a transaction is not a concurrency
    /// point.
    ///
    /// # Errors
    ///
    /// [`RouteError::Stage`] — the transaction has typically been aborted, roll back;
    /// [`RouteError::Publish`] — the transaction is untouched and still committable.
    fn publish(&self, envelope: &SerializedEnvelope)
        -> impl Future<Output = Result<(), Self::Error>> + Send;

    // publish_batch: NOT overridden. reliar-core's default loops over `publish`, which is the
    // only correct order for staging into one transaction. See the durability note below.
}
```

**`publish_batch` semantics (inherited default, deliberately).** Results stay positional, one per
envelope, in order. A positional `Ok` on a routed entry means *the statement was accepted*, **not**
that the message is durable: durability is the caller's `commit`, and one `Err(RouteError::Stage(_))`
aborts the whole transaction, invalidating every `Ok` before it. Rustdoc on the impl says exactly
that; test R22 proves it.

**Interior mutability.** `Publisher::publish` takes `&self`, staging needs `&mut Tx`, so the view
holds `tokio::sync::Mutex<&'a mut Tx>` and reborrows through the guard (`stage(&mut **guard, …)`).
`std::sync::Mutex` would make the future non-`Send`; `RefCell` would make the type non-`Sync` and
`Publisher` requires both. `tokio::sync::Mutex<T>: Send + Sync` when `T: Send`, and
`&'a mut Tx: Send` when `Tx: Send` — hence exactly one bound, `Tx: Send`, and nothing wider. The lock
is held across the `stage` call only, never across the direct publish.

### 4.2 The body of a publish (in this order)

```text
ScopedOutboxPublisher::publish
  1. let route = self.owner.policy.decide(&envelope.message_type);   // the ONLY rule call in the crate
  2. route == Outbox -> { let mut guard = tx.lock().await; staging.stage(&mut *guard, envelope) }
     route == Direct -> publisher.publish(envelope)
  3. on success only: metrics.routed(route, &envelope.message_type); record `route` on the span

OutboxPublisher::publish_direct
  1. let route = self.policy.decide(&envelope.message_type);
  2. route == Outbox -> Err(DirectPublishError::TransactionRequired { message_type })
  3. publisher.publish(envelope)
  4. on success only: metrics.routed(route, …); record `route`
```

There is **no serialization step** — that was §4.3 before Amendment D and it is gone (ADR 0033
Amendment D §3). Step 1 is a single expression, and there is no other `if enabled`, `contains`, or
list access in the module. A reviewer can grep: `enabled`/`allowed_types`/`disallowed_types` must not
appear in `publisher.rs` at all.

**What the caller does instead**, once, before either path — the same three lines a `NatsPublisher`
user already writes:

```rust
let bytes = serializer.serialize(&envelope.body)?;
let mut serialized = envelope.map_body(|_| bytes);
serialized.metadata.delivery.content_type = serializer.content_type().clone();
```

Both routes then carry **that value**: route-independent bytes are an identity here, not a
configuration argument. The guide and the doctest show this block verbatim.

## 5. Errors

```rust
/// Why a publish through a [`ScopedOutboxPublisher`] failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum RouteError<S, P> {
    /// The store rejected the staged row. The caller's transaction has typically been aborted.
    Stage(S),
    /// The transport rejected the direct publish. The caller's transaction is untouched.
    Publish(P),
}

impl<S: Classify, P: Classify> Classify for RouteError<S, P> { … }   // forwards to the inner error

/// Why [`OutboxPublisher::publish_direct`] failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum DirectPublishError<P> {
    /// The rule routes this type through the outbox, but the call site has no transaction.
    /// Use [`OutboxPublisher::in_transaction`], or stop routing this type.
    TransactionRequired {
        /// The type that requires a transaction.
        message_type: MessageType,
    },
    /// The transport rejected the publish.
    Publish(P),
}
```

`Display`/`Error` hand-rolled; `source()` returns the inner error for every variant but
`TransactionRequired`. **Never** prints payload bytes or header values.

Two enums rather than one, because they answer different questions and neither can hold the other's
variants honestly: inside a transaction `TransactionRequired` is unreachable, and outside one there
is no staging error to name (`publish_direct` never touches the store, so its error type does not
mention `S::Error` — which is what let the `OutboxEnqueue` base trait be deleted, Amendment D §2).

**`RouteError` implements `Classify`** — required, not optional: it is the scoped view's
`Publisher::Error`, and `reliar_core::Publisher::Error: Classify`. It forwards
(`Stage(e) => e.kind()`, `Publish(e) => e.kind()`), which is why `OutboxStaging::Error` carries the
`Classify` bound (§3). `DirectPublishError` needs no `Classify` impl and gets none; adding one later
is additive.

**`ConfigError` gains nothing.** Every routing-configuration failure — a `.v<digits>` entry, an
explicitly empty name, an overlap between the two lists — is a `reliar_core::SettingsError` naming
the field or environment key (§2.2). `ConfigError` stays the dispatcher's cross-field error.

## 6. Provider direction — `reliar-store-postgres`

One impl, no SQL change, no migration, no `.sqlx/` change:

```rust
impl<'c, Ser> OutboxStaging<Transaction<'c, Postgres>> for PostgresOutboxStore<Ser>
where
    Ser: Serializer,
{
    /// Serialization cannot fail on this path — the caller serialized the body — so the error's
    /// serializer parameter is uninhabited.
    type Error = EnqueueError<core::convert::Infallible>;

    async fn stage(
        &self,
        tx: &mut Transaction<'c, Postgres>,
        envelope: &SerializedEnvelope,
    ) -> Result<MessageId, Self::Error> { … }
}
```

> **The `where 'c: 'a` trap of the pre-Amendment-D contract is gone.** With `&mut Tx` in the method
> signature the impl carries a **single** lifetime, quantified by the impl, and the reborrow lifetime
> is quantified by the method — so the higher-ranked "implementation is not general enough" check
> never runs and there is no bound to accidentally write. Verified by compiling the whole shape
> (provider impl + scoped `Publisher` impl + a generic `fn f<P: Publisher>(&P)` + a `Send` assertion)
> against a stand-in with `sqlx::Transaction`'s invariance on rustc 1.98. Test **R14a is deleted**
> with the trap; R23 (§10) replaces it with the assertion that still matters — that the scoped view
> satisfies `impl Publisher` and its future is `Send`.

- `EnqueueError<Infallible>` must already implement `Classify` (§3's bound). It does, via
  `PostgresStoreError`; if the generic wrapper does not, adding the forwarding impl is part of this
  slice.
- Reuse the existing `enqueue_with` body: `search_path` handling, `insert_row`, the "restore only on
  success" rule, `map_enqueue_error`. `insert_row` already ignores the body type; the SQL text must
  not change, so `.sqlx/` stays valid.
- `content_type` written = `envelope.metadata.delivery.content_type` (the caller's), **not**
  `self.content_type()`. This is the only semantic difference from the inherent `enqueue`, and it is
  what makes the route-independent-bytes guarantee hold. Document it on the impl.
- `stage` SHALL NOT commit or roll back `tx`, and SHALL issue no I/O other than the insert.
- No `EnqueueOptions`/`ordering_key` on this path in v0.2 (ADR 0033 consequences).

## 7. `test-support` additions — `reliar-outbox`

```rust
/// A stand-in for a provider transaction in fake-driven tests: it carries no state, it exists so
/// a test exercises the same `in_transaction(&mut tx)` shape a real host does.
#[derive(Clone, Copy, Debug, Default)]
pub struct InMemoryTransaction;

impl OutboxStaging<InMemoryTransaction> for InMemoryOutboxStore {
    type Error = InMemoryStoreError;   // already implements Classify
    …                                  // delegates to insert
}
```

Note the parameter: `OutboxStaging<InMemoryTransaction>`, **not** `<&mut InMemoryTransaction>` — the
trait takes `&mut Tx` itself now, and the fake must mirror the Postgres shape or it stops being a
rehearsal of it.

Plus one knob, matching the existing `fail_next*` family: `fail_next_enqueue(&self, n: usize)` and
`enqueue_call_count(&self) -> usize` (names kept — they describe the store operation, which is still
an enqueue). `RecordingPublisher` already records publishes and counts.

## 8. `reliar-core` — no change

`DeliveryMetadata::content_type`'s rustdoc already reads "**authoritatively set by whoever serialized
the body**", which is exactly Amendment D's rule and now the *only* rule on this path. No item, no
signature, no doc and no dependency changes in `reliar-core`; it is not bumped by this slice.

*(Optional follow-up, out of scope here: a `Serializer::serialize_envelope` provided method would
collapse the caller's three-line block in §4.2 to one call. Additive, needs a `reliar-core` bump and
its own slice — ADR 0033 *Open*.)*

## 9. Observability

- One span per call: `debug_span!("reliar.outbox.route", message.id = %…, message.type = %…,
  route = …)`, `route` recorded as `RouteKind::as_str()`. Emitted by
  `ScopedOutboxPublisher::publish` and by `OutboxPublisher::publish_direct` — so an inherited
  `publish_batch` produces one span per envelope, which is what a per-message outcome needs.
  Nothing else — no payload, no header values, no tenant id, no connection string. The span wraps
  the whole call so the store's or publisher's own spans nest under it.
- No event on success. The publisher never logs an error it also returns.
- `OutboxMetrics` gains a default-bodied hook (additive, ADR 0020):
  `fn routed(&self, _route: RouteKind, _message_type: &MessageType) {}`. Labels stay bounded —
  `route` has two values, `message_type` is already an accepted label. Both types are generic over
  `M: OutboxMetrics` with `NoopMetrics` as the default type parameter, mirroring the dispatcher; the
  scoped view reads its owner's sink rather than carrying one.
- **`OutboxPolicy` emits nothing** — no span, no event, no metric, and no `tracing` call of any
  kind. It is a pure decision, and the caller is the one with the context worth recording.

## 10. Test matrix (RELIAR-45; `reviewer` audits it)

The **AC** column cites SRS §43.D (D1–D13, v1.1.8). Ids are stable across ADR 0033's amendments:
R1–R21 keep their meaning where the behaviour survived Amendment D, **R14a is deleted** (the
outlives trap it guarded no longer exists, §6), and **R22–R24 are new**.

Tests come in **two layers**, and that separation is the point of ADR 0033 Amendment C:

- **Rule tests** exercise `OutboxPolicy` alone — no store, no publisher, no serializer, no async, no
  fakes. They own every row of the §2.1 table. **Amendment D changes none of them.**
- **Composition tests** exercise `OutboxPublisher`/`ScopedOutboxPublisher` and assert only that they
  *delegate*: the collaborator touched matches `outbox.policy().decide(&message_type)`. They do
  **not** restate the table.

**Non-duplication check the reviewer applies.** The rule lives in one place, and two checks prove
it — neither of them "publisher tests stay green":

1. *The grep rule.* `publisher.rs` contains no `enabled`, `allowed_types` or `disallowed_types`
   identifier. It reaches the table only through `OutboxPolicy::decide`.
2. *The computed expectation.* `routing_delegates_to_the_policy.rs` (R21) derives every assertion
   from `outbox.policy().decide(&message_type)` — never from a route written into the test.

Then apply the mutation: swap the branches of `decide` (or reorder `publish_direct`'s steps 1 and 2).
The expected outcome is that the **policy** tests (R18, R2, R5) go red first and on their own, and
that the composition tests naming a concrete route (R1, R3, R4) go red *through* the policy — that is
those tests doing their job, since a publisher over a broken rule really does deliver to the wrong
collaborator. R21 stays green, because its expectation moves with `decide`.

All unit tests live in `crates/reliar-outbox/tests/` and exercise the public API. Rule tests need no
fakes; composition tests use `InMemoryOutboxStore` + `RecordingPublisher` + `InMemoryTransaction`.
No inline `#[cfg(test)]`. Envelopes are serialized by the test itself (§4.2's three lines, usually a
`tests/common/` helper) — that is now part of the call shape under test.

**Rule layer — `OutboxPolicy` (unchanged by Amendment D; these tests carry over verbatim)**

| id | AC | Where | What must fail if the code breaks |
|---|---|---|---|
| R18 | D12 | `policy_precedence.rs` | Every line of the §2.1 table, table-driven over `(enabled, allowed, disallowed, type) -> RouteKind`. |
| R2 | D1 | `policy_precedence.rs` | `enabled = false` with **both** lists non-empty and containing the type: still `Direct`. |
| R5 | D3 | `policy_matching.rs` | Matching is by **name**: `("a",1)`/`("a",2)` decide alike; `("A",1)` and `("a.b",1)` do not match `a`. |
| R20 | D5 | `policy_construction.rs` | `default()` decides `Outbox` for everything and equals `from_settings(&OutboxSettings::default())`; accessors, `Clone`/`PartialEq`/`Debug`. |

**Settings layer — `OutboxSettings` inputs (unchanged by Amendment D; carry over verbatim)**

| id | AC | Where | What must fail if the code breaks |
|---|---|---|---|
| R8 | D5 | `routing_settings.rs` | Defaults, builders, `MessageTypeNames::parse` trimming/empties/duplicates, `.vN` → `SettingsError::Parse` with the field as `key` and no echoed value. |
| R9 | D5, D13 | `routing_settings.rs` | `from_env` for the three keys, parse failures naming the full key, no value echoed. |
| R10 | D5 | `routing_settings.rs` | serde round-trip through the repr, per-field defaults, `deny_unknown_fields`, validation not bypassable. |
| R19 | D13 | `settings_routing_overlap.rs` + `policy_construction.rs` | Overlap rejected on every construction path; the policy backstop returns `Err`, so no `Err` reaches `OutboxPublisher::new`, which is infallible. |

**Composition layer — `OutboxPublisher`, `ScopedOutboxPublisher`, the provider impl and the wire**

| id | AC | Where | What must fail if the code breaks | vs. pre-D |
|---|---|---|---|---|
| R1 | D1 | `routing_disabled.rs` | Disabled policy: every envelope reaches `RecordingPublisher` exactly once, `InMemoryOutboxStore` records **zero** stages. Run through **both** the scoped view and `publish_direct`. | carries over; `Routed::route` assertions become collaborator assertions |
| R3 | D2 | `routing_all.rs` | Default policy through the scoped view: every envelope is staged, `RecordingPublisher` count is **0**, the staged row's id equals `envelope.id`. | carries over |
| R4 | D3 | `routing_selective.rs` | `allowed_types = [a, b]` → `a`/`b` reach the store, `c` reaches the publisher; assert on **both** collaborators so swapping the arms fails. | carries over |
| R21 | D12 | `routing_delegates_to_the_policy.rs` | Over a table of policies × types, exactly the collaborator matching `outbox.policy().decide(&t)` was called — expectation **computed from the policy**, never hard-coded. Plus `policy()` equals the policy constructed with. | carries over; the returned value is now `()`, so the assertion is on the collaborators |
| R6 | D4 | `routing_requires_transaction.rs` | `publish_direct` on a routed type → `DirectPublishError::TransactionRequired { message_type }`, store untouched **and the publisher not called** (no silent downgrade). | carries over, new error type |
| R7 | D4 | doctest / `routing_selective.rs` | The scoped view is the only path that reaches the store. | carries over |
| R11 | — | `routing_errors.rs` | `fail_next_enqueue(1)` → `RouteError::Stage`, `source()` wired, `Display` mentions neither payload nor headers; a failing publisher → `RouteError::Publish` / `DirectPublishError::Publish`. | carries over **minus** the `Serialize` case — nothing here serializes |
| R12 | — | `routing_errors.rs` | Never retries: a `ScriptedPublisher` scripted to fail once then succeed is called **exactly once** and the call returns `Err`. | carries over |
| R13 | — | `routing_observability.rs` | Exactly one `reliar.outbox.route` span per call carrying `route`; no payload bytes or header values in any field, event or `Debug`. Covers both types, and `publish_batch` → **n** spans. | carries over, extended |
| R14 | D6 | `crates/reliar-store-postgres/tests/postgres/routing_enqueue.rs` | Real Postgres: `stage` inside a caller transaction — invisible before commit, present after; rollback leaves nothing; `content_type` equals the **envelope's**, not the store's default; a reused `MessageId` → `EnqueueError::Duplicate`. | carries over, renamed method |
| ~~R14a~~ | — | — | **Deleted.** The `where 'c: 'a` trap it guarded cannot occur under `OutboxStaging<Tx>` (§6). Superseded by R23. | deleted |
| R15 | D6 | `tests/system` (e2e) | Routed type: scoped `publish` + commit → row in `outbox` → dispatcher → message on the stream. Direct type: on the stream immediately, `SELECT count(*) FROM outbox` for that id is `0`. | carries over |
| R16 | D6 | `tests/system` | Direct path is not transactional: scoped `publish` of a direct type, then **roll back** — the message is still on the stream. The honest-guarantee test; name it so nobody "fixes" it. | carries over |
| R17 | D7 | doc | `cargo doc -D warnings`; the rustdoc doctest compiles a fake-backed example showing the caller's serialization block and both routes. | carries over |
| **R22** | D6 | `routing_batch.rs` | **New.** `publish_batch` through the scoped view: results are **positional** (one per envelope, in order, mixed routes preserved), staging happens **sequentially**, and a mid-batch `Err(RouteError::Stage(_))` still returns `Ok` for earlier entries — with the test asserting that those entries are **not durable** after a rollback. Proves §4.1's "positional ≠ durable" note rather than assuming it. | new |
| **R23** | D6 | `routing_is_a_publisher.rs` (+ a postgres-side twin) | **New.** The scoped view **is** a `Publisher`: a generic `async fn f<P: Publisher>(&P, &SerializedEnvelope)` accepts it and both `publish` and `publish_batch` work through it; the future is asserted `Send` from a **non-`'static`** transaction scope. The postgres twin runs the same assertion over a real `Transaction<'_, Postgres>` (this is what R14a becomes). | new |
| **R24** | — | `routing_is_a_publisher.rs` (compile-fail note) or doc | **New.** The guard: `OutboxPublisher` does **not** implement `Publisher`, and the scoped view is neither `'static` nor `Clone`, so neither can be passed to `OutboxDispatcher::builder`. A `trybuild`-style compile-fail case if the crate already has one; otherwise a documented static assertion (`fn _assert_not_static` shape) plus the rustdoc statement. Do not add a new dev-dependency for this. | new |

Determinism: no wall-clock sleeps; neither publisher type has timing behaviour to test with paused
time.

## 11. Reshaping work list for RELIAR-45 (one engineer, in this order)

Slices 1 and 2 are **already built and stay as they are** — Amendment D touches neither the settings
nor the rule. What follows reshapes slices 3–5 over the existing code. Read each item as
*rename / delete / carry over*, not as a fresh build.

**A. `reliar-outbox` — capability (was slice 3a).** `src/enqueue.rs` → `src/staging.rs`:
`OutboxEnqueue` **deleted**, `OutboxEnqueueIn<Cx>` → `OutboxStaging<Tx>` with `stage(&self, &mut Tx,
&SerializedEnvelope)` and the `Classify` bound on `type Error` (§3). Update `lib.rs` re-exports.

**B. `reliar-outbox` — the publisher (was slice 3b).** `src/router.rs` → `src/publisher.rs`:

- `OutboxRouter<E, P, Ser, M>` → `OutboxPublisher<S, P, M>` — **delete** the `Ser` parameter, the
  `Arc<Ser>` field, the `serialize` helper (§4.2's block moves to the caller/guide/doctest) and the
  `Ser`-aware manual `Clone`.
- `publish_in(cx, &Envelope<T>)` → **`in_transaction(&mut tx)`** returning `ScopedOutboxPublisher`,
  whose `Publisher::publish` carries the routed/direct body (§4.1, §4.2). New file content, but the
  two-arm body, the span and the metrics call move over unchanged.
- `publish(&Envelope<T>)` → **`publish_direct(&SerializedEnvelope)`**, same refusal semantics, new
  error type.
- `Routed` **deleted** (the `Publisher` signature returns `()`; the caller already holds
  `envelope.id`, and `policy().decide` answers the route). `RouteKind` stays in `policy`.
- `RouteError` **reshaped**: `TransactionRequired` and `Serialize` removed, `Enqueue(E)` → `Stage(S)`,
  `Publish(P)` kept, **add the `Classify` impl**. New `DirectPublishError<P>` (§5).
- `policy()` accessor, the `NoopMetrics` default parameter, the "no `enabled`/`allowed_types`/
  `disallowed_types` identifier in this module" rule: unchanged.
- Rustdoc: the doctest now shows the caller's serialization block, `in_transaction(...).publish(...)`
  and `publish_direct`; the crate-doc *Guarantees* bullet for the direct path carries over verbatim.

**C. `reliar-outbox` — `test-support`.** `InMemoryTransaction` stays; its impl becomes
`OutboxStaging<InMemoryTransaction>` (§7). `fail_next_enqueue` / `enqueue_call_count` keep their
names.

**D. `reliar-store-postgres`.** One impl rewritten to `OutboxStaging<Transaction<'c, Postgres>>` with
`&mut Tx` (§6); **delete** the `where 'c: 'a` warning comment and R14a; confirm
`EnqueueError<Infallible>: Classify`. `cargo sqlx prepare --check` must still be clean — the SQL does
not change.

**E. Tests.** Carry over R1, R3, R4, R6, R7, R11 (minus its `Serialize` case), R12, R13, R14, R15,
R16, R21 with the renames above; **delete** R14a and every `Routed`/`RouteError::Serialize`
assertion; **add** R22, R23, R24. R2, R5, R8–R10, R18–R20 are untouched — do not edit those files.

**F. Docs.** `docs/guides/outbox-routing.md` sections listed in §11.1; `examples/axum-outbox` switched
to `OutboxPublisher` as the reference call site; `CHANGELOG.md` under *Unreleased* rewritten to the
Amendment D names (it has not shipped).

Feature-powerset check after B (`cargo hack check --feature-powerset` on `reliar-outbox`: `serde`,
`test-support`, `metrics`).

### 11.1 Guide sections the engineer must update (`docs/guides/outbox-routing.md`)

| section | change |
|---|---|
| *The rule — SRS §20.2, verbatim* | none |
| *Settings and environment* | none |
| *The two rollout shapes* | none |
| **`OutboxRouter`** | retitle **`OutboxPublisher`**; rewrite the wiring and call-site snippets to `let outbox = OutboxPublisher::new(store, nats, policy);` + the caller's serialization block + `outbox.in_transaction(&mut tx).publish(&serialized).await?;` + `tx.commit()`; add the borrow/one-expression note and "dropping the scope neither commits nor rolls back"; add `publish_direct` and its `TransactionRequired` refusal; add the "it **is** a `Publisher`, so `impl Publisher` code accepts it" paragraph and the "the un-scoped type is not one, and why" paragraph |
| *Previewing the rule without a store or a transport* | `router.policy()` → `outbox.policy()` only |
| **What "direct" costs you** | add the `publish_batch` positional-vs-durable paragraph (§4.1) |
| *`enabled = false` never stops the dispatcher* | none |
| **new: *Publishing without an outbox*** | one short section: if a deployment routes nothing durably, hold a `NatsPublisher` directly — the human's second point, and the honest answer to "do I need this type at all" |
| *See also* | ADR 0033 Amendment D |

**Versions (ADR 0034 — the bump lands in the change that needs it).** Unchanged by Amendment D:
`reliar-outbox` ships **0.3.0** (0.2.0 is published and this slice changes its public surface),
`reliar-store-postgres` ships **0.3.0**, both with their `[workspace.dependencies]` pins and
`CHANGELOG.md` entries. `reliar-core` is **untouched** and is not bumped. Nothing named in Amendment
D ever shipped, so no version has to move because of the reshape itself. CI's `versioning` job
enforces the freeze and `cargo semver-checks`.

## 12. Decided here

1. Placement `reliar-outbox`, not core, not the provider (ADR 0033 §1).
2. **One staging trait, `OutboxStaging<Tx>`, parameterised by the transaction type and taking
   `&mut Tx`** — the `OutboxEnqueue`/`OutboxEnqueueIn<Cx>` pair is deleted, and with it the
   `'c`-invariance trap (§3, §6, ADR 0033 Amendment D §2).
3. **The caller serializes.** Nothing in `reliar-outbox` holds a `Serializer` on this path; both
   routes carry the same `SerializedEnvelope` value, so route-independent bytes are an identity
   (§4.2, Amendment D §3).
4. **`ScopedOutboxPublisher` implements `reliar_core::Publisher`; `OutboxPublisher` does not** — the
   scoped type borrows a transaction, so it is neither `'static` nor `Clone` and the dispatcher's
   bound rejects it. That is the guard, and the compiler enforces it (§4, §4.1, Amendment D §4).
5. **`tokio::sync::Mutex<&'a mut Tx>`** for `&self` → `&mut Tx`, with `Tx: Send` as the only added
   bound; `publish_batch` keeps core's sequential default, and positional `Ok` ≠ durable (§4.1).
6. Matching by name, `.vN` entries rejected loudly (§2).
7. `enabled` / `allowed_types` / `disallowed_types` as **top-level `OutboxSettings` fields** keyed
   `{prefix}ENABLED` / `{prefix}ALLOWED_TYPES` / `{prefix}DISALLOWED_TYPES` (Amendments A, B).
8. `OutboxPublisher::new` takes an already-validated **`OutboxPolicy`** and is **infallible**; its
   only rule-shaped accessor is `policy()` (Amendment C).
9. **One newtype, `MessageTypeNames`, for both lists**, and **one error type,
   `reliar_core::SettingsError`, for the whole routing configuration** (Amendment B).
10. Disallow wins; an overlap is a construction error on every path, never a tie-break (§2.1).
11. No retry, no timeout, no buffering (preamble, §4.1).
12. `EnqueueError<Infallible>` as the Postgres impl's error, and it must be `Classify` (§6).
13. **The rule is a value, `OutboxPolicy`, in its own module** (Amendment C).
14. Singular `OutboxPolicy` and `decide -> RouteKind` (Amendment C).
15. **Two error enums, `RouteError` (scoped) and `DirectPublishError` (un-scoped)**, because neither
    can honestly hold the other's variants; `RouteError: Classify` is mandatory, `DirectPublishError`
    gets no `Classify` (§5).

## 13. Not in this contract

Prefix/wildcard matching · a `!name` negation syntax inside one list · configurable precedence
between the two lists (disallow always wins) · per-type retry policies · runtime-mutable rules ·
`EnqueueOptions`/`ordering_key` on the staging path · the inbox side · a `Classify` impl for
`DirectPublishError` · a `Publisher` impl on the un-scoped `OutboxPublisher` · a `TransportPublisher`
marker bound on `OutboxDispatcher` · a `Serializer::serialize_envelope` helper in `reliar-core`
(ADR 0033 *Open*) · a `publish_in(&mut tx, …)` convenience beside `in_transaction` (a third publish
entry point for a one-expression saving) · an override of `publish_batch` · a `RoutingPolicy`
**trait** or a host-supplied rule of any kind · `OutboxPolicies` as a composing collection · a policy
that reads anything but the three `OutboxSettings` fields · runtime replacement of a policy.
