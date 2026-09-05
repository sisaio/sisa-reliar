//! N4 (story C7): `publish_batch` returns one positional result per envelope; an envelope that
//! cannot be encoded never fails its neighbours; a `batch_pipeline_depth` smaller than the batch
//! exercises more than one pipelined window (ADR 0028 §3).

use reliar_core::{Classify, Envelope, FailureKind, MessageId, Publisher};
use reliar_transport_nats::{NatsPublisher, NatsSettings, headers};

use crate::common::{self, OrderCreated, RecordingSubscriber, TestStream};

/// A custom header in NATS's reserved `Nats-` namespace — core accepts it (only `reliar-` is
/// reserved there), and this crate's mapper rejects it at `encode` (`NatsMapError::
/// ReservedHeaderName`), a permanent `NatsPublishError::Map`. A ready-made "bad envelope" other
/// scenarios in this file reuse at whichever index they need.
fn bad_envelope() -> reliar_core::SerializedEnvelope {
    let mut bad = Envelope::builder(OrderCreated { order_id: 99 })
        .header("Nats-Bad", "x")
        .expect("a plain ASCII key/value is always accepted by core")
        .build()
        .map_body(|_| bytes::Bytes::from_static(b"{}"));
    bad.id = MessageId::new();
    bad
}

async fn one_bad_envelope_leaves_its_neighbours_ok_across_several_windows() {
    let stream = TestStream::create(common::jetstream_context().await).await;
    let publisher = NatsPublisher::new(
        stream.context.clone(),
        NatsSettings::default()
            .subject_prefix(stream.subject_prefix.clone())
            // Smaller than the 5-envelope batch below, so this exercises >1 pipelined window.
            .batch_pipeline_depth(2),
    )
    .expect("valid settings");

    let mut envelopes = Vec::new();
    for _ in 0..2 {
        envelopes.push(common::distinct_envelope());
    }
    envelopes.push(bad_envelope());
    for _ in 0..2 {
        envelopes.push(common::distinct_envelope());
    }
    let bad_index = 2;

    // Review m6: the `publish_batch` span records how many pipelined windows this run took —
    // `batch_pipeline_depth(2)` over a 5-envelope batch must take more than one.
    let (subscriber, _guard) = RecordingSubscriber::install();
    let results = publisher.publish_batch(&envelopes).await;

    assert_eq!(
        results.len(),
        envelopes.len(),
        "positional, always full length"
    );
    for (i, result) in results.iter().enumerate() {
        if i == bad_index {
            let err = result.as_ref().expect_err("the bad envelope must fail");
            assert_eq!(err.kind(), FailureKind::Permanent);
        } else {
            assert!(
                result.is_ok(),
                "envelope {i} must not be affected by its bad neighbour: {result:?}"
            );
        }
    }

    assert_eq!(
        stream.message_count().await,
        envelopes.len() as u64 - 1,
        "every good envelope was actually stored"
    );

    // The bad envelope fails at `prepare()` (before any window opens), so only the 4 good
    // envelopes are ever windowed: `batch_pipeline_depth(2)` over 4 is 2 pipelined windows.
    let transcript = subscriber.text();
    assert!(
        transcript.contains("windows=2"),
        "expected 2 pipelined windows (2 + 2) over batch_pipeline_depth(2), got:\n{transcript}"
    );

    stream.delete().await;
}

/// Review gap 5: two bad envelopes at **non-adjacent** indices, not just one — proving the
/// positional invariant holds pairwise, and that every surviving envelope's own id (not merely
/// its count) is exactly the one the caller expects at that index.
async fn two_non_adjacent_bad_envelopes_leave_every_good_index_intact() {
    let stream = TestStream::create(common::jetstream_context().await).await;
    let publisher = NatsPublisher::new(
        stream.context.clone(),
        NatsSettings::default().subject_prefix(stream.subject_prefix.clone()),
    )
    .expect("valid settings");

    // indices: 0 good, 1 bad, 2 good, 3 bad, 4 good.
    let good: Vec<_> = (0..3).map(|_| common::distinct_envelope()).collect();
    let envelopes = vec![
        good[0].clone(),
        bad_envelope(),
        good[1].clone(),
        bad_envelope(),
        good[2].clone(),
    ];
    let bad_indices = [1usize, 3];

    let results = publisher.publish_batch(&envelopes).await;

    assert_eq!(results.len(), envelopes.len());
    for (i, result) in results.iter().enumerate() {
        if bad_indices.contains(&i) {
            let err = result.as_ref().expect_err("the bad envelope must fail");
            assert_eq!(err.kind(), FailureKind::Permanent);
        } else {
            assert!(
                result.is_ok(),
                "envelope {i} must not be affected: {result:?}"
            );
        }
    }

    // Every good envelope's id is stored at exactly the sequence its index implies (1-based
    // JetStream sequences, in slice order — SRS §22.2, ADR 0013), never a neighbour's.
    let stored_1 = stream.raw_message(1).await;
    let stored_2 = stream.raw_message(2).await;
    let stored_3 = stream.raw_message(3).await;
    assert_eq!(
        common::header_value(&stored_1.headers, headers::MESSAGE_ID),
        good[0].id.to_string(),
        "index 0's envelope id"
    );
    assert_eq!(
        common::header_value(&stored_2.headers, headers::MESSAGE_ID),
        good[1].id.to_string(),
        "index 2's envelope id"
    );
    assert_eq!(
        common::header_value(&stored_3.headers, headers::MESSAGE_ID),
        good[2].id.to_string(),
        "index 4's envelope id"
    );
    assert_eq!(stream.message_count().await, 3);

    stream.delete().await;
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "n4_publish_batch_positional::one_bad_envelope_leaves_its_neighbours_ok_across_several_windows",
            move || {
                rt.block_on(one_bad_envelope_leaves_its_neighbours_ok_across_several_windows());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "n4_publish_batch_positional::two_non_adjacent_bad_envelopes_leave_every_good_index_intact",
            move || {
                rt.block_on(two_non_adjacent_bad_envelopes_leave_every_good_index_intact());
                Ok(())
            },
        ),
    ]
}
