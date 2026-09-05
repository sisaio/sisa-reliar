//! N3 (story C6): the same envelope published twice inside the stream's `duplicate_window`
//! (the server default, since this scenario never overrides it) is stored once — `Nats-Msg-Id`
//! (ADR 0026 §5) is what lets `JetStream` suppress the second write.

use reliar_core::Publisher;
use reliar_transport_nats::{NatsPublisher, NatsSettings};

use crate::common::{self, TestStream};

async fn republishing_the_same_envelope_stores_it_once() {
    let stream = TestStream::create(common::jetstream_context().await).await;
    let publisher = NatsPublisher::new(
        stream.context.clone(),
        NatsSettings::default().subject_prefix(stream.subject_prefix.clone()),
    )
    .expect("valid settings");

    let envelope = common::serialized_envelope();

    publisher
        .publish(&envelope)
        .await
        .expect("first publish acks");
    let ack_of_second = publisher.publish(&envelope).await;

    assert!(
        ack_of_second.is_ok(),
        "a duplicate is stored under the original sequence, not rejected: {ack_of_second:?}"
    );
    assert_eq!(
        stream.message_count().await,
        1,
        "the duplicate window suppressed the second write"
    );

    stream.delete().await;
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![libtest_mimic::Trial::test(
        "n3_duplicate_suppression::republishing_the_same_envelope_stores_it_once",
        move || {
            rt.block_on(republishing_the_same_envelope_stores_it_once());
            Ok(())
        },
    )]
}
