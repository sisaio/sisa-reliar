# Getting started

Reliar is a **library**, composed explicitly by your application at startup — there is no service
to deploy, no DI container, and nothing starts a background thread on its own. This guide gets a
first message enqueued and published; `docs/guides/postgres.md` covers the real, durable path.

## The crates (Phase 1)

| Crate | What it gives you |
|---|---|
| `reliar-core` | The envelope/message model: `Envelope<T>`, `Message`, `Metadata`, `Headers`, `Serializer` — no storage, no transport. |
| `reliar-outbox` | The storage-agnostic outbox: `OutboxStore`/`Publisher` traits, `OutboxDispatcher`, settings, and (behind `test-support`) in-memory fakes. |
| `reliar-store-postgres` | The PostgreSQL provider: schema, `migrate()`, `PostgresOutboxStore`. |

See `docs/architecture/overview.md` for the full crate map and the dependency rule, and
`docs/architecture/phase1-contract.md` for the frozen public API.

## The envelope model, in one paragraph

Every message you hand to Reliar is an `Envelope<T>` — your typed body `T` plus canonical
`Metadata` (correlation, trace context, routing, delivery) and optional application `Headers`. `T`
implements `Message`, which fixes a **stable wire identity** (`TYPE`/`VERSION`) independent of the
Rust type's name — never `std::any::type_name::<T>()`. A `Serializer` (the default
`JsonSerializer`) converts `Envelope<T>` to `SerializedEnvelope` (`= Envelope<bytes::Bytes>`), the
form storage and transport see. Details: `docs/architecture/envelope.md`.

```rust,ignore
use reliar_core::{Envelope, Message};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct OrderCreated { order_id: u64 }

impl Message for OrderCreated {
    const TYPE: &'static str = "orders.created";
    const VERSION: u16 = 1;
}

let envelope = Envelope::builder(OrderCreated { order_id: 1 }).build();
```

## Quickstart: in-memory, no database

`reliar-outbox`'s `test-support` feature ships the same fakes used across the workspace's own
tests: `InMemoryOutboxStore` (a full `OutboxStore`) and `RecordingPublisher` (a `Publisher` that
records what it saw). Together with `OutboxDispatcher`, they run the whole claim → publish →
complete loop with nothing external:

```sh
cargo run -p outbox-basic
```

The full source is `examples/outbox-basic/src/main.rs`. The shape:

```rust,ignore
let store = InMemoryOutboxStore::default();
let publisher = RecordingPublisher::default();

let envelope = /* build a SerializedEnvelope, as above + a Serializer */;
store.insert(envelope);

let dispatcher = OutboxDispatcher::builder(store, publisher).build()?;
let cancel = CancellationToken::new();
let handle = tokio::spawn(dispatcher.run(cancel.clone()));

// ... later, on shutdown ...
cancel.cancel();
handle.await??;
```

`run` claims due rows, publishes them with bounded concurrency, and marks them complete or
retries/dead-letters them per the configured `RetryPolicy` — see
`docs/architecture/outbox.md` for the loop in full and the three duplicate windows every
consumer must tolerate.

## Next steps

- **A real database:** `docs/guides/postgres.md` — schema, `search_path`, `migrate()`, the
  `examples/axum-outbox` reference integration.
- **Skipping the outbox for some types:** `docs/guides/outbox-routing.md` — `OutboxPublisher`, the
  `enabled`/`allowed_types`/`disallowed_types` rule, and what a direct publish costs you.
- **The guarantees:** `crates/reliar-outbox/README.md` states the duplicate windows and the
  no-ordering default plainly; read it before writing a consumer.
- **The frozen contract:** `docs/architecture/phase1-contract.md` is authoritative over this guide
  for exact signatures.
