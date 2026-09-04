//! The whole outbox loop — enqueue, claim, publish, complete — against the `test-support` fakes,
//! no database and no broker required.
//!
//! Run with:
//!
//! ```sh
//! cargo run -p outbox-basic
//! ```
//!
//! This mirrors `docs/guides/getting-started.md`'s quickstart. The production shape — a real
//! transaction, a real `PostgresOutboxStore`, graceful shutdown wired to a signal — is
//! `examples/axum-outbox`.

use std::time::Duration;

use anyhow::Result;
use reliar_core::{Envelope, JsonSerializer, Message, MessageId, Serializer};
use reliar_outbox::{
    DispatcherSettings, InMemoryOutboxStore, OutboxDispatcher, RecordingPublisher,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

/// A minimal application message. `TYPE`/`VERSION` are the stable wire identity (never
/// `type_name::<T>()`, ADR 0010) — a caller downstream matches on `"orders.created"`, not on the
/// Rust type's name.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct OrderCreated {
    order_id: u64,
}

impl Message for OrderCreated {
    const TYPE: &'static str = "orders.created";
    const VERSION: u16 = 1;
}

#[tokio::main]
async fn main() -> Result<()> {
    // In a real application this is `PostgresOutboxStore` and rows are inserted inside the
    // caller's own transaction (see `examples/axum-outbox`). Here it is the in-memory fake, so
    // this example needs neither Postgres nor a broker.
    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let serializer = JsonSerializer;

    let mut seeded: Vec<MessageId> = Vec::with_capacity(3);
    for order_id in 1..=3 {
        let envelope = Envelope::builder(OrderCreated { order_id }).build();
        // `SerializedEnvelope` is what a store persists; `PostgresOutboxStore::enqueue` does
        // this same conversion internally with the store's configured `Serializer`.
        let bytes = serializer.serialize(&envelope.body)?;
        seeded.push(store.insert(envelope.map_body(|_| bytes)).id);
    }
    println!("enqueued {} messages: {seeded:?}", seeded.len());

    // Short polling intervals so this example settles in well under a second of wall-clock time
    // — a real deployment keeps the library defaults (500 ms / 5 s).
    let settings = DispatcherSettings::default()
        .poll_interval(Duration::from_millis(10))
        .idle_poll_interval(Duration::from_millis(10));

    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .settings(settings)
        .build()?;

    let cancel = CancellationToken::new();
    let worker = tokio::spawn(dispatcher.run(cancel.clone()));

    // Poll for "every seeded message published" instead of guessing a fixed sleep, bounded by an
    // overall deadline so a broken dispatcher fails this example loudly rather than hanging.
    let deadline = Duration::from_secs(2);
    let poll_every = Duration::from_millis(5);
    let awaited = tokio::time::timeout(deadline, async {
        while publisher.published().len() < seeded.len() {
            tokio::time::sleep(poll_every).await;
        }
    })
    .await;
    if awaited.is_err() {
        anyhow::bail!(
            "timed out after {deadline:?} waiting for all {} messages to publish",
            seeded.len()
        );
    }

    // Shut down gracefully — `run` drains in-flight publishes before returning `Ok(())`.
    cancel.cancel();
    worker.await??;

    println!("published: {:?}", publisher.published());
    Ok(())
}
