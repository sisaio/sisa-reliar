//! Concurrent publishes never exceed `max_in_flight` (§43.A.23, SRS §26) — proven twice, because
//! it is two different bounds meeting at the same number (architect ruling, RELIAR-15 review 3,
//! blocker 2): the claim gate bounds how many rows this worker holds leased; the `Semaphore`
//! bounds how many `Publisher::publish` calls run concurrently. With a *conforming* store the
//! gate alone caps what gets claimed at `max_in_flight`, so nothing meaningfully queues behind
//! the semaphore — that case is proven first. An *over-delivering* store (one that does not
//! honor the requested `batch_size`, standing in for a third-party `OutboxStore` Reliar cannot
//! audit) can still hand back more rows than there is concurrency budget for; the semaphore is
//! the only thing left bounding `Publisher::publish` concurrency then, proven second.
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{InMemoryOutboxStore, OutboxDispatcher, RecordingPublisher};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn with_a_conforming_store_the_claim_gate_alone_caps_the_peak() {
    let store = InMemoryOutboxStore::default();
    for _ in 0..20 {
        store.insert(common::distinct_envelope());
    }
    // Every publish parks for the same paused delay, so all candidates claimed at once are
    // genuine candidates to overlap — without this, nothing would ever be in flight at the same
    // instant to prove a bound on.
    let publisher = RecordingPublisher::with_concurrency_probe(Duration::from_millis(5));

    let settings = common::fast_dispatcher_settings()
        .batch_size(20)
        .max_in_flight(4);
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .settings(settings)
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // Small, repeated hops rather than one big jump: each wave of 4 only registers its own
    // `sleep` once the semaphore frees a permit released by the *previous* wave, so the clock
    // must advance in steps small enough to let that registration happen in between.
    for _ in 0..40 {
        common::advance_and_settle(Duration::from_millis(5)).await;
    }

    assert_eq!(
        publisher.published().len(),
        20,
        "every row was published exactly once"
    );
    assert_eq!(
        publisher.in_flight_peak(),
        4,
        "the claim gate never asks for more than max_in_flight from a store that honors the \
         request, so the peak never exceeds it — with or without the semaphore"
    );

    cancel.cancel();
    handle
        .await
        .expect("dispatcher task joins")
        .expect("run() returns Ok(()) after cancellation");
}

#[tokio::test(start_paused = true)]
async fn an_over_delivering_store_is_still_bounded_by_the_semaphore() {
    let store = InMemoryOutboxStore::default();
    for _ in 0..20 {
        store.insert(common::distinct_envelope());
    }
    // Hands back up to 20 rows regardless of the `batch_size` the dispatcher actually asks for —
    // standing in for a third-party `OutboxStore` that does not honor the request. `outstanding`
    // now genuinely exceeds `max_in_flight`, so only the semaphore keeps concurrent `publish`
    // calls bounded. Replacing `Semaphore::new(settings.max_in_flight)` with
    // `Semaphore::new(1_000_000)` in dispatcher.rs must make this test fail.
    let over_delivering = common::OverDeliveringStore::new(store, 20);
    let publisher = RecordingPublisher::with_concurrency_probe(Duration::from_millis(5));

    let settings = common::fast_dispatcher_settings().max_in_flight(4);
    let dispatcher = OutboxDispatcher::builder(over_delivering, publisher.clone())
        .settings(settings)
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    for _ in 0..40 {
        common::advance_and_settle(Duration::from_millis(5)).await;
    }

    assert_eq!(
        publisher.published().len(),
        20,
        "every row was still published exactly once, just serialized into waves of 4"
    );
    assert_eq!(
        publisher.in_flight_peak(),
        4,
        "twenty rows claimed at once against four permits: the semaphore is the only thing \
         capping concurrent publish() calls here"
    );

    cancel.cancel();
    handle
        .await
        .expect("dispatcher task joins")
        .expect("run() returns Ok(()) after cancellation");
}
