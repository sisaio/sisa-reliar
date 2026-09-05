//! N6/U10 (story C4): a custom `SubjectResolver` is honoured end to end — the stream captures the
//! resolved subject only through a wildcard, and the exact subject the resolver produced (not the
//! wildcard pattern) is what the stored message carries.

use std::convert::Infallible;

use async_nats::Subject;
use async_nats::jetstream::stream::Config as StreamConfig;
use reliar_core::{EndpointAddress, Envelope, Metadata, Publisher, SerializedEnvelope};
use reliar_transport_nats::{
    DestinationSubjects, NatsPublisher, NatsSettings, PrefixSubjects, SubjectResolver,
};

use crate::common::{self, OrderCreated, TestStream};

/// Always resolves to the same fixed subject, regardless of the envelope — a minimal custom
/// resolver, distinct from [`reliar_transport_nats::PrefixSubjects`].
#[derive(Clone, Debug)]
struct FixedSubjectResolver(Subject);

impl SubjectResolver for FixedSubjectResolver {
    type Error = Infallible;

    fn subject(&self, _envelope: &SerializedEnvelope) -> Result<Subject, Infallible> {
        Ok(self.0.clone())
    }
}

async fn a_custom_resolver_is_honoured_and_the_exact_subject_is_stored() {
    let context = common::jetstream_context().await;
    let id = uuid::Uuid::now_v7().simple();
    let exact_subject = Subject::from(format!("reliar.test.{id}.custom.exact.subject"));
    let stream = TestStream::create_with(
        context,
        StreamConfig {
            // A wildcard capture — the resolver's own exact subject is never spelled out here,
            // proving the stored subject came from the resolver, not from the stream config.
            subjects: vec![format!("reliar.test.{id}.custom.>")],
            ..StreamConfig::default()
        },
    )
    .await;

    let publisher = NatsPublisher::with_resolver(
        stream.context.clone(),
        NatsSettings::default(),
        FixedSubjectResolver(exact_subject.clone()),
    )
    .expect("valid settings");

    publisher
        .publish(&common::serialized_envelope())
        .await
        .expect("publish acks");

    let stored = stream.raw_message(1).await;
    assert_eq!(stored.subject, exact_subject);

    stream.delete().await;
}

/// Review gap 6: `DestinationSubjects` (not a hand-rolled test resolver) through a real
/// `NatsPublisher` — `RoutingMetadata.destination`, when set, wins over the wrapped
/// `PrefixSubjects` end to end, proven by the stream's own wildcard capture the same way the
/// fixed-resolver scenario above proves it.
async fn destination_subjects_is_honoured_through_a_real_publisher() {
    let context = common::jetstream_context().await;
    let id = uuid::Uuid::now_v7().simple();
    let destination_subject = format!("reliar.test.{id}.destination.exact.subject");
    let stream = TestStream::create_with(
        context,
        StreamConfig {
            subjects: vec![format!("reliar.test.{id}.destination.>")],
            ..StreamConfig::default()
        },
    )
    .await;

    let publisher = NatsPublisher::with_resolver(
        stream.context.clone(),
        NatsSettings::default(),
        DestinationSubjects::new(PrefixSubjects::default()),
    )
    .expect("valid settings");

    let mut metadata = Metadata::default();
    metadata.routing.destination =
        Some(EndpointAddress::parse(destination_subject.clone()).expect("legal endpoint address"));
    let envelope = Envelope::builder(OrderCreated { order_id: 1 })
        .metadata(metadata)
        .build()
        .map_body(|_| bytes::Bytes::from_static(b"{}"));

    publisher.publish(&envelope).await.expect("publish acks");

    let stored = stream.raw_message(1).await;
    assert_eq!(stored.subject.as_str(), destination_subject);

    stream.delete().await;
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "n6_custom_resolver_subject::a_custom_resolver_is_honoured_and_the_exact_subject_is_stored",
            move || {
                rt.block_on(a_custom_resolver_is_honoured_and_the_exact_subject_is_stored());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "n6_custom_resolver_subject::destination_subjects_is_honoured_through_a_real_publisher",
            move || {
                rt.block_on(destination_subjects_is_honoured_through_a_real_publisher());
                Ok(())
            },
        ),
    ]
}
