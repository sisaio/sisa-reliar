//! N9 (contract §4.2, review gap 1): dropping `publish_batch` mid-window yields **no** results at
//! all (the future's output is the whole vector), leaves the stream holding some prefix of the
//! batch, and does not panic. The connection stays usable afterward, and this crate spawns no
//! task the drop could otherwise leak.
//!
//! As in `n8_publish_cancellation.rs`, a race against a normal (acking) stream cannot reliably
//! catch a future genuinely mid-flight — a local round trip is fast enough that a short outer
//! timeout can resolve after the batch already finished. This scenario publishes to a
//! `no_ack: true` stream instead: every ack in the window can **never** arrive, so
//! `publish_batch`'s future is *guaranteed* still pending at any point up to
//! [`NatsSettings::publish_timeout`]'s own much longer deadline, making the drop deterministic
//! rather than a timing gamble.
//!
//! Review minor 1: `issue_window_sends` issues every send in a window *before* awaiting any ack
//! (ADR 0028 §3), and a local send is far faster than the 20ms drop below — so all three sends in
//! this single-window batch (`batch_pipeline_depth(8)` comfortably covers 3 envelopes) have already
//! reached the server by the time the future is dropped. The stored count after the drop is
//! therefore not merely bounded by the batch length, it is the batch length exactly. Republishing
//! the same three envelopes (same ids, so the same `Nats-Msg-Id`s) into the same stream proves the
//! rest of the claim: `JetStream`'s own duplicate suppression — not this test's bookkeeping —
//! keeps the count unchanged, which is the whole point of carrying an idempotent `Nats-Msg-Id`
//! through a cancellation. This time the publisher is configured with a short `publish_timeout` so
//! the republish is *awaited to completion* (not raced against an outer drop): a `no_ack` stream
//! still never acks, so every result is a genuine `NatsPublishError::Timeout`, never a dropped
//! future. The duplicate-window collapse this relies on is also proven, independently, by
//! `n3_duplicate_suppression.rs`.

use std::time::Duration;

use async_nats::jetstream::stream::Config as StreamConfig;
use reliar_core::Publisher;
use reliar_transport_nats::{NatsPublisher, NatsSettings};

use crate::common::{self, TestStream};

async fn dropping_publish_batch_mid_window_never_panics_and_the_connection_stays_usable() {
    let noack_stream = TestStream::create_with(
        common::jetstream_context().await,
        StreamConfig {
            no_ack: true,
            ..StreamConfig::default()
        },
    )
    .await;
    let publisher = NatsPublisher::new(
        noack_stream.context.clone(),
        NatsSettings::default()
            .subject_prefix(noack_stream.subject_prefix.clone())
            .batch_pipeline_depth(8)
            .publish_timeout(Duration::from_secs(10)),
    )
    .expect("valid settings");

    let envelopes: Vec<_> = (0..3).map(|_| common::distinct_envelope()).collect();

    // The acks can never arrive on a `no_ack` stream, so this window is provably still pending —
    // not racing anything — at the 20ms mark.
    let outcome = tokio::time::timeout(
        Duration::from_millis(20),
        publisher.publish_batch(&envelopes),
    )
    .await;
    assert!(
        outcome.is_err(),
        "dropping publish_batch mid-window yields no result vector at all"
    );

    // `issue_window_sends` issues every send in a window before awaiting any ack, and a local send
    // (µs) is far faster than the 20ms drop above, so all 3 sends in this one window (3 envelopes
    // fit inside `batch_pipeline_depth(8)`) already reached the server — the exact count, not merely an
    // upper bound.
    let stored_after_drop = noack_stream.message_count().await;
    assert_eq!(
        stored_after_drop,
        envelopes.len() as u64,
        "every send in this single window is issued before any ack is awaited, so all 3 must \
         already be stored by the time the future is dropped"
    );

    // Republishing the same envelopes (same ids, so the same `Nats-Msg-Id`s) proves the point of
    // carrying that dedup key through a cancellation: `JetStream` collapses every one of them, so
    // the stored count is unchanged even though this publisher fully awaits every result this
    // time (a short `publish_timeout` makes each one a genuine `Timeout`, not a raced drop — a
    // `no_ack` stream still never acks).
    let republish_publisher = NatsPublisher::new(
        noack_stream.context.clone(),
        NatsSettings::default()
            .subject_prefix(noack_stream.subject_prefix.clone())
            .batch_pipeline_depth(8)
            .publish_timeout(Duration::from_millis(200)),
    )
    .expect("valid settings");
    let republish_results = republish_publisher.publish_batch(&envelopes).await;
    assert!(
        republish_results.iter().all(|r| matches!(
            r,
            Err(reliar_transport_nats::NatsPublishError::Timeout { .. })
        )),
        "a no_ack stream never acks, so every republished envelope must time out: {republish_results:?}"
    );
    assert_eq!(
        noack_stream.message_count().await,
        envelopes.len() as u64,
        "republishing the same ids must dedup, not grow the stored count"
    );

    // No poisoned connection: the same underlying connection publishes a batch successfully to a
    // *different*, normally-acking stream right after.
    let healthy_stream = TestStream::create(noack_stream.context.clone()).await;
    let healthy_publisher = NatsPublisher::new(
        healthy_stream.context.clone(),
        NatsSettings::default().subject_prefix(healthy_stream.subject_prefix.clone()),
    )
    .expect("valid settings");
    let recovery_envelopes: Vec<_> = (0..3).map(|_| common::distinct_envelope()).collect();
    let results = healthy_publisher.publish_batch(&recovery_envelopes).await;
    assert!(
        results.iter().all(Result::is_ok),
        "the connection remains healthy after a dropped batch: {results:?}"
    );
    assert_eq!(healthy_stream.message_count().await, 3);

    noack_stream.delete().await;
    healthy_stream.delete().await;
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![libtest_mimic::Trial::test(
        "n9_publish_batch_cancellation::dropping_publish_batch_mid_window_never_panics_and_the_connection_stays_usable",
        move || {
            rt.block_on(
                dropping_publish_batch_mid_window_never_panics_and_the_connection_stays_usable(),
            );
            Ok(())
        },
    )]
}
