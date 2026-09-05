//! N1 (story C5): `publish` stores exactly one message with the expected headers and raw body.
//! N2 (review B2): `publish` genuinely **awaits** the server's ack rather than merely writing to
//! a socket — proven with a `no_ack: true` stream, which the server never acks: an implementation
//! that awaited the ack times out; one that did not would return `Ok` regardless. (A same-
//! connection `message_count()` check, as this file used to rely on, cannot distinguish the two —
//! the server's own ordering makes the write visible to a subsequent query whether or not the ack
//! was awaited, which is why the round-2 review flagged that proof as unsound — review n5.)

use std::time::Duration;

use async_nats::jetstream::stream::Config as StreamConfig;
use reliar_core::{EnvelopeMapper, Publisher};
use reliar_transport_nats::{
    NatsEnvelopeMapper, NatsPublishError, NatsPublisher, NatsSettings, headers,
};

use crate::common::{self, TestStream};

async fn publish_stores_one_message_with_the_expected_headers_and_body() {
    let stream = TestStream::create(common::jetstream_context().await).await;
    let publisher = NatsPublisher::new(
        stream.context.clone(),
        NatsSettings::default().subject_prefix(stream.subject_prefix.clone()),
    )
    .expect("valid settings");

    let envelope = common::serialized_envelope();
    let expected_wire = NatsEnvelopeMapper::default()
        .encode(&envelope)
        .expect("encodable");

    publisher.publish(&envelope).await.expect("publish acks");

    assert_eq!(stream.message_count().await, 1, "N1: exactly one message");

    let stored = stream.raw_message(1).await;
    assert_eq!(stored.payload, envelope.body, "N1: raw body, byte for byte");
    assert_eq!(
        common::header_value(&stored.headers, headers::MESSAGE_ID),
        common::header_value(&expected_wire.headers, headers::MESSAGE_ID),
        "N1: message id header matches the mapper's own projection"
    );
    assert_eq!(
        common::header_value(&stored.headers, headers::MESSAGE_TYPE),
        common::header_value(&expected_wire.headers, headers::MESSAGE_TYPE),
    );
    assert_eq!(
        common::header_value(&stored.headers, headers::NATS_MSG_ID),
        common::header_value(&expected_wire.headers, headers::NATS_MSG_ID),
    );

    stream.delete().await;
}

/// N2 (review B2): a `no_ack: true` stream never sends a `PubAck` back, so a publisher that
/// genuinely awaits the ack **must** time out here — the only way this test can pass is if
/// `publish` really did wait. A publisher that merely fired the bytes onto the wire and returned
/// would report `Ok` regardless of `no_ack`, which is exactly the false positive this proves
/// absent.
async fn publish_to_a_no_ack_stream_times_out_because_the_ack_is_genuinely_awaited() {
    let stream = TestStream::create_with(
        common::jetstream_context().await,
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
            .publish_timeout(Duration::from_millis(500)),
    )
    .expect("valid settings");

    let err = publisher
        .publish(&common::serialized_envelope())
        .await
        .expect_err("a no_ack stream never sends a PubAck, so this must time out");

    assert!(
        matches!(err, NatsPublishError::Timeout { .. }),
        "expected Timeout, got {err:?}"
    );

    stream.delete().await;
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "n1_publish_awaits_the_ack::publish_stores_one_message_with_the_expected_headers_and_body",
            move || {
                rt.block_on(publish_stores_one_message_with_the_expected_headers_and_body());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "n1_publish_awaits_the_ack::publish_to_a_no_ack_stream_times_out_because_the_ack_is_genuinely_awaited",
            move || {
                rt.block_on(
                    publish_to_a_no_ack_stream_times_out_because_the_ack_is_genuinely_awaited(),
                );
                Ok(())
            },
        ),
    ]
}
