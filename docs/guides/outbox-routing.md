# Outbox routing guide

`OutboxPublisher` (`reliar-outbox`) is one publish call that either stages a message in the outbox
or sends it straight to the transport, per message type — SRS §20.2's routing rule. It exists for
the types a deployment decides are not worth the outbox's write and the dispatcher's poll:
high-volume telemetry, audit events, anything where "at most one attempt, no durability" is an
acceptable trade for lower latency and less load on the database. Everything else keeps the
outbox's durable, at-least-once guarantee, unchanged.

The application-facing object **is** a `reliar_core::Publisher`:
`OutboxPublisher::in_transaction(&mut tx)` hands out a `ScopedOutboxPublisher` that implements it
for the life of a borrowed transaction (ADR 0033 Amendment D).

See `docs/architecture/routing-publisher-contract.md` for the frozen signatures this guide
describes, and `crates/reliar-outbox/README.md` for the crate's own quickstart.

## The rule — SRS §20.2, verbatim

| `enabled` | `allowed_types` | `disallowed_types` | `OutboxPolicy::decide(t)` |
|---|---|---|---|
| `false` | any | any | `Direct` |
| `true` | `[]` | `[]` | `Outbox` |
| `true` | `[]` | contains *t* | `Direct` |
| `true` | contains *t* | not *t* | `Outbox` |
| `true` | non-empty, not *t* | any | `Direct` |
| `true` | contains *t* | contains *t* | configuration error at construction |

Matching is on `MessageType::name()` — exact, case-sensitive, and **version-agnostic**: `orders.created.v1`
and `orders.created.v2` both match a list entry of `"orders.created"`. A name in **both** lists is
rejected on every construction path — the `allowed_types`/`disallowed_types` setters,
`OutboxSettings::from_env`, the `serde` deserializer, and `OutboxPolicy::from_settings`'s own
backstop for a value assembled by hand — never silently tie-broken: **disallow wins** is the
rule's own precedent for every combination that could exist, so a construction error is the only
way an overlap is ever seen.

The rule itself lives in one place, `OutboxPolicy` (`reliar-outbox`, module `policy`) — a
validated, immutable value built once from an `OutboxSettings` and asked `decide(&message_type)`
for the rest of the process's life. `OutboxPublisher` owns one and delegates every decision to it;
neither the publisher nor `OutboxSettings` re-implements the table.

## Settings and environment

The switch and the two lists are top-level fields of `OutboxSettings` — no `RoutingSettings`
sub-section:

| Field | Env var (`OutboxSettings::from_env(prefix)`) | Default | Notes |
|---|---|---|---|
| `enabled` | `{prefix}ENABLED` | `true` | `false` sends **every** message directly; both lists are ignored. This stops new messages *entering* the outbox — it never stops a running `OutboxDispatcher` from draining rows already staged. |
| `allowed_types` | `{prefix}ALLOWED_TYPES` | empty (comma list) | Empty means every type is routed — the durable default. |
| `disallowed_types` | `{prefix}DISALLOWED_TYPES` | empty (comma list) | Overrides `allowed_types` per type. |

With the conventional prefix: `RELIAR_OUTBOX_ENABLED`, `RELIAR_OUTBOX_ALLOWED_TYPES`,
`RELIAR_OUTBOX_DISALLOWED_TYPES`. As with every Reliar setting, nothing reads the environment
until `from_env` is called (ADR 0019), and an absent variable keeps the default rather than
resetting it.

```rust,ignore
use reliar_outbox::{MessageTypeNames, OutboxPolicy, OutboxSettings};

let settings = OutboxSettings::from_env("RELIAR_OUTBOX_")?;
let policy = OutboxPolicy::from_settings(&settings)?;
```

`MessageTypeNames::parse`/`try_from_iter` trim entries, drop empty ones (`parse` only — an
explicit empty name from `try_from_iter` is an error), tolerate duplicates, and reject a
`.v<digits>` suffix loudly — that is `MessageType`'s `Display` form, and matching is on the name
alone, so accepting it would silently match nothing. Every error is a `reliar_core::SettingsError`
naming the field or environment key, never echoing the configured value.

## The two rollout shapes

**"Everything except these"** — the primary shape, and the one to reach for by default: leave
`allowed_types` empty and name only the types you have decided don't need the outbox.

```sh
RELIAR_OUTBOX_DISALLOWED_TYPES=analytics.viewed,audit.logged
```

Every type not on the list — including one you add next month — keeps the durable default with no
further configuration.

**"Only these"** — the narrower shape, for a deliberately staged migration: name the types allowed
through the outbox and leave everything else direct.

```sh
RELIAR_OUTBOX_ALLOWED_TYPES=orders.created
```

Start narrow, and widen `allowed_types` as you gain confidence in a type's traffic and cost; there
is no configurable precedence to reach for instead of a wider list — the truth table's precedence
is fixed.

## `OutboxPublisher`

```rust,ignore
use reliar_core::{JsonSerializer, Publisher as _, Serializer as _};
use reliar_outbox::{OutboxPolicy, OutboxPublisher, OutboxSettings};
use reliar_store_postgres::PostgresOutboxStore;
use reliar_transport_nats::{NatsPublisher, NatsSettings};

let settings = OutboxSettings::from_env("RELIAR_OUTBOX_")?;
let policy = OutboxPolicy::from_settings(&settings)?;   // infallible — the pair is already valid
let outbox = OutboxPublisher::new(store, publisher, policy);

// The caller serializes once, exactly as it would for a bare `NatsPublisher` — `OutboxPublisher`
// holds no `Serializer` of its own (Amendment D §3), so the wire bytes never depend on whether a
// message goes through the outbox or direct.
let serializer = JsonSerializer;
let bytes = serializer.serialize(&envelope.body)?;
let mut serialized = envelope.map_body(|_| bytes);
serialized.metadata.delivery.content_type = serializer.content_type().clone();

// With the caller's transaction — `in_transaction` hands out a `Publisher` that reaches either
// path, whichever the policy decides. Keeping the borrow to one statement is the one-expression
// form:
let mut tx = pool.begin().await?;
outbox.in_transaction(&mut tx).publish(&serialized).await?;
tx.commit().await?;
```

Dropping the scoped value **neither commits nor rolls back** — the caller owns the transaction
throughout; `in_transaction` only borrows it for the life of one call (or one `publish_batch`).

From a call site with no transaction, only the direct path is reachable:

```rust,ignore
// A routed type returns `DirectPublishError::TransactionRequired` rather than silently
// downgrading to a direct publish.
outbox.publish_direct(&serialized).await?;
```

`store`, `publisher` and the *same* `Serializer` you serialize with are the collaborators you would
also hand to a `PostgresOutboxStore`/`NatsPublisher` pairing and an `OutboxDispatcher`.

Because it **is** a `reliar_core::Publisher`, generic code written against `impl Publisher` accepts
a `ScopedOutboxPublisher` directly — a request handler that takes `&impl Publisher` works
unmodified whether it is handed a bare `NatsPublisher` or an outbox-routed one. The un-scoped
`OutboxPublisher` is deliberately **not** one: it is `'static` and `Clone` when `S`/`P`/`M` are, and
passing something like that to `OutboxDispatcher` would feed the outbox back into itself. The
scoped view borrows both `self` and the transaction, so it can satisfy neither bound — the guard is
the compiler, not a convention.

`outbox.policy()` is the publisher's only rule-shaped accessor. There is no `enabled()`/
`allowed_types()`/`disallowed_types()` delegation on `OutboxPublisher` itself — ask the policy:

```rust,ignore
let preview = OutboxPolicy::from_settings(&settings)?;
println!("orders.created routes via {:?}", preview.decide(&MessageType::new("orders.created", 1)));
```

## Previewing the rule without a store or a transport

Because `OutboxPolicy` needs neither, a host can build one at startup — to log which types are
durable, or in a plain unit test — with nothing else in scope:

```rust,ignore
use reliar_core::MessageType;
use reliar_outbox::{OutboxPolicy, OutboxSettings, RouteKind};

let settings = OutboxSettings::from_env("RELIAR_OUTBOX_")?;
let policy = OutboxPolicy::from_settings(&settings)?;
for name in ["orders.created", "audit.logged"] {
    let route = policy.decide(&MessageType::new(name, 1));
    assert!(matches!(route, RouteKind::Outbox | RouteKind::Direct));
    println!("{name} -> {}", route.as_str());
}
```

This is exactly what `OutboxPublisher::new` does with the policy you hand it — nothing about the
rule changes once it is wrapped in a publisher, so previewing it standalone is a faithful test of
what a real publish call will do.

## What "direct" costs you

A direct publish is **not part of the caller's transaction**: the scoped view issues no statement
on the borrowed transaction for that path, so if the transaction later rolls back, the message is
already on the wire. It is **one attempt**, with no Reliar-side retry, backoff, dead state, or
duplicate window — only whatever retry the transport publisher itself performs (none, for
`NatsPublisher`). If you call `in_transaction(&mut tx).publish(..)` for a directly-routed type
while a transaction is open, that call holds the database transaction across network I/O for as
long as the publish takes — configure a publisher-side timeout (`NatsSettings::publish_timeout`,
for `NatsPublisher`), and prefer publishing direct-routed types before opening (or after
committing) the transaction.

`publish_batch` (the inherited default) keeps this same honesty: results stay positional, one per
envelope, in order, but a positional `Ok` on a routed entry means only *the statement was
accepted*, never *the message is durable* — durability is still the caller's `commit`, and one
`Err` partway through the batch aborts the whole transaction, invalidating every earlier `Ok`
along with it.

None of this is a defect to fix — it is the honest cost of skipping the outbox, and the reason
"everything except these" starts from the durable default rather than the other way around.

## `enabled = false` never stops the dispatcher

Flipping `OutboxSettings::enabled` to `false` stops *new* messages from entering the outbox; it
does not stop an already-running `OutboxDispatcher` from draining rows staged before the flip.
A deployment that disables routing keeps its dispatcher process running until the backlog it
already accumulated is empty — decommission the dispatcher only once `OutboxStore::stats` (or your
own metric on `dead`/pending counts) shows nothing left to drain.

## Publishing without an outbox

If a deployment routes nothing durably — `enabled = false`, or every type it will ever publish is
in `disallowed_types` — ask whether it needs `OutboxPublisher` at all. Holding a bare
`NatsPublisher` (or whichever transport `Publisher` you use) directly is a shorter, equally honest
answer: no policy to build, no staging capability to wire up, and one less type between your
handler and the wire. Reach for `OutboxPublisher` when *some* traffic needs the outbox's durability
— that is the whole reason the rule exists.

## See also

- `docs/guides/postgres.md` — wiring `PostgresOutboxStore`, `search_path`, `migrate()`.
- `docs/guides/nats.md` — wiring `NatsPublisher`, stream ownership, subject strategy; its
  "Standalone use" section names this guide as the middle path between a bare `NatsPublisher` and
  the full outbox.
- `examples/nats-pub-sub` — a routed and a direct message published through the same
  `OutboxPublisher`, entirely configured from `RELIAR_OUTBOX_*`.
- `examples/axum-outbox` — the §20.1 reference integration, publishing through
  `OutboxPublisher::in_transaction(&mut tx).publish(..)` from its handler.
- `docs/architecture/routing-publisher-contract.md` — the frozen contract this guide describes;
  ADR 0033 (incl. Amendment D) for the design history.
