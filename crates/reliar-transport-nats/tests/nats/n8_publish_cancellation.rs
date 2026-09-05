//! N8 (contract §4.2, review gap 1): dropping the `publish` future mid-ack must not panic, and
//! leaves the stream holding **either zero or one** copy of the message — accepting both outcomes
//! is the point, since this proves SRS §22's duplicate window rather than a delivery guarantee.
//! This crate never `spawn`s a task, so there is nothing a drop could leak; what a drop *can* do
//! is poison the shared connection, so this scenario also proves the connection stays usable
//! afterward.
//!
//! A race against a **normal** (acking) stream is not a reliable way to catch a future genuinely
//! mid-flight: a local `JetStream` round trip over loopback is fast enough (observed ~100µs in
//! this crate's own bench) that a short outer timeout can resolve *after* the operation already
//! completed, regardless of scheduling. This scenario instead publishes to a `no_ack: true`
//! stream — the ack can **never** arrive (not merely arrives late), so the publish future is
//! *guaranteed* still pending, awaiting that ack, at any point up to
//! [`NatsSettings::publish_timeout`]'s own much longer deadline. Dropping it there is therefore a
//! deterministic proof of drop-safety, not a race. "The connection stays usable" is then proven
//! separately, on a normal stream sharing the same underlying connection: N3 already proves the
//! duplicate-window collapse this scenario's dropped attempt would otherwise rely on.

use std::time::Duration;

use async_nats::jetstream::stream::Config as StreamConfig;
use reliar_core::Publisher;
use reliar_transport_nats::{NatsPublisher, NatsSettings};

use crate::common::{self, TestStream};

async fn dropping_publish_mid_ack_never_panics_and_the_connection_stays_usable() {
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
            // Comfortably longer than the drop below, so the future is *guaranteed* pending —
            // never a race against how fast the round trip happens to be.
            .publish_timeout(Duration::from_secs(10)),
    )
    .expect("valid settings");

    let envelope = common::serialized_envelope();

    // The ack can never arrive on a `no_ack` stream, so this future is provably still pending —
    // not racing anything — at the 20ms mark. Dropping it here is deterministic mid-ack
    // cancellation, not a timing gamble.
    let dropped = tokio::time::timeout(Duration::from_millis(20), publisher.publish(&envelope));
    let outcome = dropped.await;
    assert!(
        outcome.is_err(),
        "a no_ack stream never acks within 20ms, so the inner future must still be pending"
    );

    let count_after_drop = noack_stream.message_count().await;
    assert!(
        count_after_drop == 0 || count_after_drop == 1,
        "expected 0 or 1 stored copies after a dropped publish, got {count_after_drop}"
    );

    // The connection stays usable: the same underlying connection publishes successfully to a
    // *different*, normally-acking stream right after. (The duplicate-window collapse this
    // dropped attempt would otherwise rely on is already proven, independently, by
    // `n3_duplicate_suppression.rs`.)
    let healthy_stream = TestStream::create(noack_stream.context.clone()).await;
    let healthy_publisher = NatsPublisher::new(
        healthy_stream.context.clone(),
        NatsSettings::default().subject_prefix(healthy_stream.subject_prefix.clone()),
    )
    .expect("valid settings");
    healthy_publisher
        .publish(&common::distinct_envelope())
        .await
        .expect("the connection remains healthy after a dropped publish future");
    assert_eq!(healthy_stream.message_count().await, 1);

    noack_stream.delete().await;
    healthy_stream.delete().await;
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![libtest_mimic::Trial::test(
        "n8_publish_cancellation::dropping_publish_mid_ack_never_panics_and_the_connection_stays_usable",
        move || {
            rt.block_on(dropping_publish_mid_ack_never_panics_and_the_connection_stays_usable());
            Ok(())
        },
    )]
}
