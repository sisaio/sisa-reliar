//! Review minor 2: `publish_batch`'s per-window `Timeout` arms (`issue_window_sends`'s send-phase
//! arm and `await_window_acks`'s ack-await arm, `src/publisher.rs:267-270, 291-294`) fire for
//! every envelope in every window, and a later window still runs after an earlier window's own
//! deadline has already elapsed — the pipelining promise "a slow neighbour's timeout never
//! overrides an already-acked sibling" extends to "one window timing out never stops the next
//! window from starting" (ADR 0028 §3).
//!
//! A `no_ack` stream never acks, so every send's ack-await genuinely runs out its window's own
//! `publish_timeout` — deterministic, not a race against how fast a local round trip happens to
//! be (same reasoning as N8/N9). A local send itself is far faster than the 200ms window deadline
//! below, so in practice this proves the ack-await arm (`await_window_acks`, lines 291-294) for
//! every envelope in both windows; the send-phase arm (`issue_window_sends`, lines 267-270) is the
//! same `timeout_at` pattern one stage earlier; both share the identical proof (the whole window
//! genuinely runs out its deadline) and neither one bails out early.

use std::time::Duration;

use async_nats::jetstream::stream::Config as StreamConfig;
use reliar_core::{Classify, FailureKind, Publisher};
use reliar_transport_nats::{NatsPublishError, NatsPublisher, NatsSettings};

use crate::common::{self, RecordingSubscriber, TestStream};

async fn every_envelope_in_every_window_times_out_and_later_windows_still_run() {
    let noack_stream = TestStream::create_with(
        common::jetstream_context().await,
        StreamConfig {
            no_ack: true,
            ..StreamConfig::default()
        },
    )
    .await;
    let publish_timeout = Duration::from_millis(200);
    let publisher = NatsPublisher::new(
        noack_stream.context.clone(),
        NatsSettings::default()
            .subject_prefix(noack_stream.subject_prefix.clone())
            .batch_pipeline_depth(2)
            .publish_timeout(publish_timeout),
    )
    .expect("valid settings");

    let envelopes: Vec<_> = (0..4).map(|_| common::distinct_envelope()).collect();

    let (subscriber, _guard) = RecordingSubscriber::install();
    let started = std::time::Instant::now();
    let results = publisher.publish_batch(&envelopes).await;
    let measured = started.elapsed();

    assert_eq!(
        results.len(),
        envelopes.len(),
        "positional, always full length"
    );
    for (i, result) in results.iter().enumerate() {
        match result {
            Err(NatsPublishError::Timeout { after_ms, .. }) => {
                assert!(
                    *after_ms >= publish_timeout.as_millis() as u64,
                    "envelope {i}: after_ms ({after_ms}) must be at least the {publish_timeout:?} \
                     window deadline it actually waited out"
                );
                assert_eq!(result.as_ref().unwrap_err().kind(), FailureKind::Transient);
            }
            other => panic!("envelope {i}: expected Timeout, got {other:?}"),
        }
    }

    // `batch_pipeline_depth(2)` over 4 envelopes pipelines two windows; the second window only starts
    // once the first's `take` is drained, so the measured wall time must cover both windows'
    // deadlines in turn — proof the second window is not skipped once the first has elapsed.
    assert!(
        measured >= publish_timeout * 2,
        "two pipelined windows must each run out their own {publish_timeout:?} deadline in turn, \
         took {measured:?}"
    );

    let transcript = subscriber.text();
    assert!(
        transcript.contains("windows=2"),
        "batch_pipeline_depth(2) over 4 envelopes must take 2 pipelined windows, got:\n{transcript}"
    );

    noack_stream.delete().await;
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![libtest_mimic::Trial::test(
        "n11_publish_batch_window_deadline::every_envelope_in_every_window_times_out_and_later_windows_still_run",
        move || {
            rt.block_on(every_envelope_in_every_window_times_out_and_later_windows_still_run());
            Ok(())
        },
    )]
}
