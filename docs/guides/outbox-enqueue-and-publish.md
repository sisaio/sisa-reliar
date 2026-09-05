# Outbox enqueue and publish guide

`OutboxPublisher` (`reliar-outbox`) is the application's outbox handle. It offers exactly two
operations, and **the call site names the guarantee** — nothing decides between them at runtime,
and no setting can (ADR 0036):

- [`OutboxPublisher::enqueue`]/`enqueue_batch` **enqueues** the envelope in the caller's own
  transaction — the durable path. It becomes visible when the caller commits, and is published
  later by an `OutboxDispatcher`: at-least-once, with the duplicate windows below.
- The [`reliar_core::Publisher`] impl on `OutboxPublisher` **sends now**, through the transport
  publisher, one attempt, with no Reliar guarantee at all: no retry, no backoff, no dead state, no
  duplicate window, and no relationship to any transaction the caller may have open.

There is no third option and no policy object deciding which one a call takes. If some traffic
needs the outbox's durability and some doesn't, the call site itself picks — one line calls
`enqueue`, the other calls `publish`.

See `docs/architecture/outbox-publisher-contract.md` for the frozen signatures this guide
describes, and `crates/reliar-outbox/README.md` for the crate's own quickstart.

## `enqueue` — the durable path

```rust,ignore
use reliar_core::{JsonSerializer, Serializer as _};
use reliar_outbox::OutboxPublisher;
use reliar_store_postgres::PostgresOutboxStore;
use reliar_transport_nats::NatsPublisher;

let outbox = OutboxPublisher::new(store, publisher);

// The caller serializes once, exactly as it would for a bare `NatsPublisher` — `OutboxPublisher`
// holds no `Serializer` of its own, so the wire bytes never depend on which call the host makes.
let serializer = JsonSerializer;
let bytes = serializer.serialize(&envelope.body)?;
let mut serialized = envelope.map_body(|_| bytes);
serialized.metadata.delivery.content_type = serializer.content_type().clone();

let mut tx = pool.begin().await?;
// .. the caller's own business writes, in the same `tx` ..
outbox.enqueue(&mut tx, &serialized).await?;
tx.commit().await?;
```

Atomic with whatever else the caller writes in `tx`: the message exists **if and only if** the
transaction commits. The transaction is required *by type* — there is no transaction-less enqueue
call, so a message cannot be enqueued outside the caller's unit of work. `enqueue` issues no network
I/O beyond the provider's own statement, never retries, never sleeps, and never commits, rolls back
or otherwise consumes `tx`.

An `Err` from `enqueue` **may leave `tx` unusable** — treat any enqueue error as *abort this
transaction*: issue no further statement on it, roll it back, and consider every earlier write in
it lost. With `reliar-store-postgres` the transaction **is** aborted (a failed `INSERT` puts the
whole Postgres transaction into the aborted state).

`enqueue_batch` enqueues a slice in order, one statement each, and **fails fast**: the first enqueue
failure returns `EnqueueBatchError { index, source }`, naming the position that failed, and the
rest are never attempted. It returns one result for the whole batch, not one per envelope, on
purpose — every row lands in the same transaction, so an enqueue failure typically aborts it,
voiding every row enqueued before it. A positional `Ok` inside a batch is never itself durability;
the caller's `commit` is.

## `publish` — the pass-through

```rust,ignore
use reliar_core::Publisher as _;

// No transaction needed — `publish` bypasses the outbox entirely.
outbox.publish(&serialized).await?;
```

Forwards straight to the transport publisher, byte-identical, one attempt. **No Reliar durability
at all**: no retry, no backoff, no dead state, no duplicate window, and no relationship to any
transaction the caller has open. The enqueue capability is never touched on this path — that is
what makes wiring an `OutboxPublisher` into its own `OutboxDispatcher` safe: there is no code path
from a publish back into the store, so the outbox cannot drain into itself.

> **Warning — a `publish` call made while a transaction is open is a broker call made with that
> transaction held.** `publish` takes no transaction parameter and has no relationship to one, but
> if the call site happens to be inside an open `tx` — because the same handler also does business
> writes — that `tx` sits open, holding its connection and its locks, for as long as the network
> round trip to the transport takes. Call `publish` for these messages **before** `begin` or
> **after** `commit`, not while a business transaction is open for something else. And because
> `publish` is
> not part of any transaction's atomicity, a later rollback of that transaction does not undo it —
> the message is already on the wire.

`publish_batch` keeps the same honesty, forwarded to the transport's own batch API rather than
looped: results stay positional, one per envelope, in order — `P`'s contract, unmodified.

## Delivery guarantees on the enqueued path

`enqueue` is durable, but durable means **at-least-once**, never exactly-once. An enqueued row becomes
a candidate for the `OutboxDispatcher` once the caller commits; the dispatcher claims it, publishes
it to the transport, and only then marks it complete. If the process crashes — or the row's lease
simply expires — between a successful publish and a persisted complete, the row is still claimable
and gets republished on the next pass: the transport call already succeeded once, so the consumer
sees the same message twice. Closing this window would need a distributed transaction across the
database and the transport, which Reliar does not attempt (SRS §22, §22.1).

Consequences for whoever consumes an enqueued message: **consumers must be idempotent** — able to
see the same `message.id` twice and produce the same effect once. `Nats-Msg-Id` (set to the
envelope's `message_id`) lets JetStream collapse a duplicate for you, but only within its
configured `duplicate_window`; outside that window, or on a broker that never gets the header, the
consumer's own idempotency is what actually holds. See `docs/guides/nats.md` for how the header is
set and how to size the window.

A `publish`-only message has no duplicate window of its own kind (no retry means no second
attempt to deduplicate against), but it also has **no** delivery guarantee: a transport failure is
simply lost, because nothing durable ever recorded the attempt.

## Publishing without an outbox

If a deployment enqueues nothing — every message it will ever send goes through `publish` — ask
whether it needs `OutboxPublisher` at all. Holding a bare `NatsPublisher` (or whichever transport
`Publisher` you use) directly is a shorter, equally honest answer: no enqueue capability to wire
up, and one less type between your handler and the wire. Reach for `OutboxPublisher` when *some*
traffic needs `enqueue`'s durability — that is the whole reason the type exists.

## Propagating correlation, causation and request ids in a handler

A handler that reacts to an inbound envelope and produces a new one should carry the inbound
message's identity forward, not start a fresh, disconnected one:

- **`conversation_id`** — copy it from the inbound envelope. It groups every message in one
  business conversation; a reply that starts a new conversation breaks any consumer that traces by
  it.
- **`correlation_id`** — copy it from the inbound envelope, if it set one. It is the
  application/business correlation the caller chose; a handler has no more information than the
  message that reached it, so it has nothing better to set.
- **`request_id`** — copy it from the inbound envelope, if it set one. It names the inbound
  request that (transitively) caused this one — copying it, not regenerating it, keeps that chain
  intact across every hop.
- **`causation_id`** — set it to the inbound envelope's **own `id`**, never copied from the
  inbound's `causation_id`. This one field always changes at each hop: it names *this* message's
  immediate cause, which is the message the handler just processed, not whatever caused that one.
- **Never reuse the inbound envelope's `id`** as the new envelope's `id`. Each envelope gets its
  own fresh id (`Envelope::builder`'s default); reusing an id would collide with the inbound
  message's own identity and, on the enqueue path, `EnqueueError::Duplicate` if the same id is ever
  enqueued twice.

```rust,ignore
use reliar_core::{Envelope, Message};

fn handle(inbound: &Envelope<InboundEvent>) -> Envelope<OutboundEvent> {
    let mut builder = Envelope::builder(OutboundEvent { /* .. */ })
        .conversation(inbound.metadata.correlation.conversation_id)
        .causation(inbound.id); // the inbound message's own id — not its causation_id

    if let Some(correlation_id) = &inbound.metadata.correlation.correlation_id {
        builder = builder.correlation_id(correlation_id.clone());
    }

    let mut envelope = builder.build();
    // `request_id` has no dedicated builder setter — the field is public, so it is copied
    // directly onto the value the builder already produced.
    envelope.metadata.correlation.request_id = inbound.metadata.correlation.request_id;
    envelope
}
```

## Migrating from 0.3 (the routing rule)

0.3.0 shipped a routing rule (`OutboxPolicy`, `ScopedOutboxPublisher`) that decided, per message
type and at runtime from `RELIAR_OUTBOX_*` settings, whether a call enqueued durably or published
direct. **0.4.0 withdraws it** (ADR 0036): the call site names the guarantee instead of a
configuration table.

| 0.3.0 | 0.4.0 |
|---|---|
| `outbox.in_transaction(&mut tx).publish(&e).await?` | `outbox.enqueue(&mut tx, &e).await?` |
| `outbox.publish_direct(&e).await?` | `outbox.publish(&e).await?` |
| `OutboxPublisher::new(store, publisher, policy)` | `OutboxPublisher::new(store, publisher)` |
| `OutboxSettings::enabled` / `allowed_types` / `disallowed_types` | removed — delete every `RELIAR_OUTBOX_ENABLED` / `_ALLOWED_TYPES` / `_DISALLOWED_TYPES` from every deployment and config document; a document that still sets one now **fails to deserialize**, naming the field |
| `RouteError` / `DirectPublishError` | `enqueue` returns the enqueue error directly; `publish` returns the transport's error directly |
| `OutboxMetrics::routed` | `OutboxMetrics::enqueued` |

A type you had `disallowed_types`-listed is now a `publish` call site in code, not a configuration
entry — decide per call site which of the two operations above it needs, and call that one.

## See also

- `docs/guides/postgres.md` — wiring `PostgresOutboxStore`, `search_path`, `migrate()`.
- `docs/guides/nats.md` — wiring `NatsPublisher`, stream ownership, subject strategy.
- `examples/nats-pub-sub` — an enqueued and a published message shown side by side.
- `examples/axum-outbox` — the §20.1 reference integration, enqueuing through
  `OutboxPublisher::enqueue(&mut tx, &serialized)` from its handler.
- `docs/architecture/outbox-publisher-contract.md` — the frozen contract this guide describes;
  ADR 0036 for the design history.
