//! N5 (story C8): every classification cell ADR 0030's table promises, proven against real
//! `JetStream` behavior rather than asserted from the source alone.

use std::time::Duration;

use async_nats::jetstream::stream::Config as StreamConfig;
use bytes::Bytes;
use reliar_core::{Classify, FailureKind, Publisher};
use reliar_transport_nats::{NatsPublishError, NatsPublisher, NatsSettings};

use crate::common::{self, TestStream};
use crate::{NATS_IMAGE, NATS_TAG};

/// No stream captures the subject at all — `JetStream`'s own "no responders" (ADR 0029 §3).
async fn no_stream_for_the_subject_is_transient() {
    let context = common::jetstream_context().await;
    let prefix = format!("reliar.test.unbound.{}", uuid::Uuid::now_v7().simple());
    let publisher = NatsPublisher::new(context, NatsSettings::default().subject_prefix(prefix))
        .expect("valid settings");

    let err = publisher
        .publish(&common::serialized_envelope())
        .await
        .expect_err("no stream captures this subject");

    assert!(
        matches!(err, NatsPublishError::StreamNotFound { .. }),
        "expected StreamNotFound, got {err:?}"
    );
    assert_eq!(err.kind(), FailureKind::Transient);
}

/// The pre-flight `max_payload` guard rejects an oversized message before any I/O — permanent,
/// and never even reaches the server (proven by the stream staying empty).
async fn preflight_payload_too_large_is_permanent() {
    let stream = TestStream::create(common::jetstream_context().await).await;
    let publisher = NatsPublisher::new(
        stream.context.clone(),
        NatsSettings::default()
            .subject_prefix(stream.subject_prefix.clone())
            .max_payload(Some(32)),
    )
    .expect("valid settings");

    let mut envelope = common::serialized_envelope();
    envelope.body = Bytes::from(vec![b'x'; 4096]);

    let err = publisher
        .publish(&envelope)
        .await
        .expect_err("the pre-flight guard rejects this locally");

    assert!(
        matches!(err, NatsPublishError::PayloadTooLarge { .. }),
        "expected PayloadTooLarge, got {err:?}"
    );
    assert_eq!(err.kind(), FailureKind::Permanent);
    assert_eq!(stream.message_count().await, 0, "never reached the server");

    stream.delete().await;
}

/// No local `max_payload` is configured, so the client checks the message against the server's
/// own account-wide `max_payload` (advertised in its `INFO` greeting, 1 MiB by default for
/// `nats-server`) **before** any `JetStream` round-trip — `PublishErrorKind::MaxPayloadExceeded`,
/// permanent. This is a distinct guard from a stream's own `max_message_size` (a JetStream-level
/// precondition the server enforces after the round-trip, which surfaces as an unrecognised
/// `Response::Err` — `Broker`/transient — not `MaxPayloadExceeded`; ADR 0030 §"Other").
async fn server_max_payload_exceeded_is_permanent() {
    let stream = TestStream::create(common::jetstream_context().await).await;
    let publisher = NatsPublisher::new(
        stream.context.clone(),
        NatsSettings::default().subject_prefix(stream.subject_prefix.clone()),
    )
    .expect("valid settings");

    let mut envelope = common::serialized_envelope();
    // Over `nats-server`'s default 1 MiB account `max_payload` — headers add a little more still.
    envelope.body = Bytes::from(vec![b'x'; 2 * 1024 * 1024]);

    let err = publisher
        .publish(&envelope)
        .await
        .expect_err("the server's max_payload rejects this before any JetStream round-trip");

    assert!(
        matches!(err, NatsPublishError::MaxPayloadExceeded { .. }),
        "expected MaxPayloadExceeded, got {err:?}"
    );
    assert_eq!(err.kind(), FailureKind::Permanent);

    stream.delete().await;
}

/// A server that goes away mid-run classifies as transient — regardless of the exact broker
/// variant (`Connection`, `Timeout` and `Broker` are all legitimate depending on exactly when the
/// connection drops), which is what the dispatcher's retry decision actually depends on. Uses a
/// **dedicated** container so no other, possibly-parallel trial against the shared server is
/// disturbed.
async fn a_server_that_stops_mid_run_is_transient() {
    let (container, url) = common::start_isolated_container(NATS_IMAGE, NATS_TAG).await;
    let context = common::jetstream_context_at(&url).await;
    let stream = TestStream::create(context).await;
    let publisher = NatsPublisher::new(
        stream.context.clone(),
        NatsSettings::default()
            .subject_prefix(stream.subject_prefix.clone())
            .publish_timeout(Duration::from_secs(2)),
    )
    .expect("valid settings");

    publisher
        .publish(&common::serialized_envelope())
        .await
        .expect("the dedicated server is healthy before it is stopped");

    container
        .stop_with_timeout(Some(0))
        .await
        .expect("stop the dedicated server");

    let err = publisher
        .publish(&common::distinct_envelope())
        .await
        .expect_err("the server is gone");
    assert_eq!(err.kind(), FailureKind::Transient, "got {err:?}");

    drop(container);
}

/// Review gap 2 / m1: `after_ms` is the **measured** elapsed time, not the configured setting —
/// distinguished here by making the two numbers wildly different. The host `Context` is built
/// with its own short internal ack-await timeout (100ms), while `NatsSettings::publish_timeout`
/// is set to 5s — comfortably longer, so it never fires. Publishing to a `no_ack` stream (the ack
/// can never arrive) therefore always times out via async-nats' own internal `Context::timeout`,
/// which `classify_publish_error`'s `TimedOut` arm maps using [`elapsed_ms`]. If `after_ms` instead
/// reported the *configured* `publish_timeout` (the bug review m1 found), it would read ~5000, not
/// ~100 — the two are unmistakably different, so this cannot pass by coincidence the way a test
/// that only compares `after_ms` against its own configured setting would.
async fn a_short_publish_timeout_reports_measured_elapsed_time() {
    use async_nats::jetstream::context::ContextBuilder;

    let client = common::connect_retrying(common::admin_url()).await;
    let short_ack_timeout = Duration::from_millis(100);
    let context = ContextBuilder::new()
        .timeout(short_ack_timeout)
        .build(client);
    let stream = TestStream::create_with(
        context,
        StreamConfig {
            no_ack: true,
            ..StreamConfig::default()
        },
    )
    .await;
    let publisher = NatsPublisher::new(
        stream.context.clone(),
        NatsSettings::default()
            .subject_prefix(stream.subject_prefix.clone())
            .publish_timeout(Duration::from_secs(5)),
    )
    .expect("valid settings");

    let started = std::time::Instant::now();
    let err = publisher
        .publish(&common::serialized_envelope())
        .await
        .expect_err("a no_ack stream never acks, so the host's own ack timeout must fire");
    let measured_upper_bound = started.elapsed().as_millis() as u64;

    let NatsPublishError::Timeout { after_ms, .. } = err else {
        panic!("expected Timeout, got {err:?}");
    };
    assert!(
        after_ms <= measured_upper_bound,
        "after_ms ({after_ms}) must not exceed this scenario's own measured elapsed time ({measured_upper_bound})"
    );
    assert!(
        after_ms < 2_000,
        "after_ms ({after_ms}) should track the host's ~100ms ack timeout, not the 5s configured publish_timeout"
    );

    stream.delete().await;
}

/// Review gap 4: exhausting the host context's own ack-permit pool. A `Context` built with
/// `max_ack_inflight(1)` and `backpressure_on_inflight(false)` (async-nats's own default is
/// `true`, so this is deliberately opted into for the test) grants exactly one outstanding-ack
/// permit; `publish_batch`'s window issues every send before awaiting any ack (ADR 0028 §3), so
/// the second and third sends in a 3-envelope window find the permit pool empty and fail
/// immediately with `PublishErrorKind::MaxAckPending` — transient, never permanent (the pool
/// frees up once earlier acks are awaited or dropped, so a retry can succeed).
async fn max_ack_pending_is_transient_when_the_ack_permit_pool_is_exhausted() {
    use async_nats::jetstream::context::ContextBuilder;

    let client = common::connect_retrying(common::admin_url()).await;
    let context = ContextBuilder::new()
        .max_ack_inflight(1)
        .backpressure_on_inflight(false)
        .build(client);
    let stream = TestStream::create(context).await;
    let publisher = NatsPublisher::new(
        stream.context.clone(),
        NatsSettings::default()
            .subject_prefix(stream.subject_prefix.clone())
            .batch_pipeline_depth(8),
    )
    .expect("valid settings");

    let envelopes = vec![
        common::distinct_envelope(),
        common::distinct_envelope(),
        common::distinct_envelope(),
    ];
    let results = publisher.publish_batch(&envelopes).await;

    assert!(
        results
            .iter()
            .any(|r| matches!(r, Err(NatsPublishError::MaxAckPending { .. }))),
        "expected at least one MaxAckPending among {results:?}"
    );
    for result in &results {
        if let Err(err) = result {
            assert_eq!(err.kind(), FailureKind::Transient, "got {err:?}");
        }
    }

    stream.delete().await;
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "n5_error_classification::no_stream_for_the_subject_is_transient",
            move || {
                rt.block_on(no_stream_for_the_subject_is_transient());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "n5_error_classification::preflight_payload_too_large_is_permanent",
            move || {
                rt.block_on(preflight_payload_too_large_is_permanent());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "n5_error_classification::server_max_payload_exceeded_is_permanent",
            move || {
                rt.block_on(server_max_payload_exceeded_is_permanent());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "n5_error_classification::a_server_that_stops_mid_run_is_transient",
            move || {
                rt.block_on(a_server_that_stops_mid_run_is_transient());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "n5_error_classification::a_short_publish_timeout_reports_measured_elapsed_time",
            move || {
                rt.block_on(a_short_publish_timeout_reports_measured_elapsed_time());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "n5_error_classification::max_ack_pending_is_transient_when_the_ack_permit_pool_is_exhausted",
            move || {
                rt.block_on(max_ack_pending_is_transient_when_the_ack_permit_pool_is_exhausted());
                Ok(())
            },
        ),
    ]
}
