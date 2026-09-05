//! `ScopedOutboxPublisher` **is** a [`reliar_core::Publisher`] (§43.D, R23, ADR 0033 Amendment D
//! §4): a generic function bounded only by the trait accepts it, and the future its `publish`/
//! `publish_batch` return is `Send` even though the value borrows a non-`'static` transaction
//! scope. `crates/reliar-store-postgres/tests/postgres/routing_enqueue.rs` runs the same
//! assertion over a real `sqlx::Transaction<'_, Postgres>` — the postgres twin this file's doc
//! comment promises, and what R14a became once the `&mut Tx` shape retired its `'c: 'a` trap
//! (contract §6).
//!
//! The other half of R23/R24 — that `OutboxPublisher` itself does **not** implement `Publisher`,
//! and that the scoped view is therefore unreachable from `OutboxDispatcher::run` — is a negative
//! fact no passing `#[test]` can prove; it is the `compile_fail` doctest on
//! [`reliar_outbox::ScopedOutboxPublisher`]'s own rustdoc.

#![cfg(feature = "test-support")]

mod common;

use reliar_core::{Envelope, Publisher, SerializedEnvelope};
use reliar_outbox::{
    InMemoryOutboxStore, InMemoryTransaction, OutboxPolicy, OutboxPublisher, RecordingPublisher,
};

/// Accepts anything that implements [`Publisher`] — the shape a real caller (or `reliar-core`'s
/// own `publish_batch` default) uses. If `ScopedOutboxPublisher` did not implement `Publisher`,
/// this file would fail to compile at every call site below.
async fn publish_through_generic<P: Publisher>(
    publisher: &P,
    envelope: &SerializedEnvelope,
) -> Result<(), P::Error> {
    publisher.publish(envelope).await
}

/// Compiles only if `T: Send` — a positive, ordinary trait-bound check, unlike the negative
/// "does not implement" fact `OutboxPublisher`'s guard is (see the module doc).
fn assert_send<T: Send>(_: &T) {}

#[tokio::test]
async fn scoped_publisher_is_a_publisher_through_a_generic_function() {
    let outbox = OutboxPublisher::new(
        InMemoryOutboxStore::default(),
        RecordingPublisher::default(),
        OutboxPolicy::default(),
    );
    let serialized =
        common::serialize(Envelope::builder(common::OrderCreated { order_id: 1 }).build());

    let mut tx = InMemoryTransaction;
    let scoped = outbox.in_transaction(&mut tx);
    publish_through_generic(&scoped, &serialized)
        .await
        .expect("publish succeeds");
}

#[tokio::test]
async fn scoped_publisher_publish_future_is_send_from_a_non_static_scope() {
    let outbox = OutboxPublisher::new(
        InMemoryOutboxStore::default(),
        RecordingPublisher::default(),
        OutboxPolicy::default(),
    );
    let serialized =
        common::serialize(Envelope::builder(common::OrderCreated { order_id: 1 }).build());

    // `tx` and `outbox` are both local to this function's stack frame — `scoped` borrows both for
    // a lifetime tied to this scope, never `'static`. `assert_send` type-checks `fut`'s type
    // without awaiting or spawning it, so this proves `Send` alone, independent of `'static`.
    let mut tx = InMemoryTransaction;
    let scoped = outbox.in_transaction(&mut tx);
    let fut = scoped.publish(&serialized);
    assert_send(&fut);
    fut.await.expect("publish succeeds");
}

#[tokio::test]
async fn scoped_publisher_publish_batch_future_is_send_from_a_non_static_scope() {
    let outbox = OutboxPublisher::new(
        InMemoryOutboxStore::default(),
        RecordingPublisher::default(),
        OutboxPolicy::default(),
    );
    let a = common::serialize(Envelope::builder(common::TypeA).build());
    let b = common::serialize(Envelope::builder(common::TypeB).build());

    let envelopes = [a, b];
    let mut tx = InMemoryTransaction;
    let scoped = outbox.in_transaction(&mut tx);
    let fut = scoped.publish_batch(&envelopes);
    assert_send(&fut);
    let results = fut.await;
    assert!(results.iter().all(Result::is_ok));
}
