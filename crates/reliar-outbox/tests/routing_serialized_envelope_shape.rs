//! `ScopedOutboxPublisher`/`OutboxPublisher::publish_direct` stage/forward the caller's
//! `SerializedEnvelope` **unchanged** — body, content type and headers — on **both** routes
//! (§43.D, ADR 0033 Amendment D §3). Nothing here serializes any more, so this file uses a
//! **non-JSON** fixture (review round 2, M1): every other fixture in this crate happens to be
//! JSON, so a passing assertion against `ContentType::JSON` would be a tautology that survives
//! deleting the "persist verbatim" guarantee entirely.

#![cfg(feature = "test-support")]

mod common;

use reliar_core::{Envelope, Publisher as _, Serializer};
use reliar_outbox::{
    InMemoryOutboxStore, InMemoryTransaction, OutboxPolicy, OutboxPublisher, OutboxSettings,
    RecordingPublisher,
};

const HEADER_KEY: &str = "x-tenant";
const HEADER_VALUE: &str = "tenant-42";

#[tokio::test]
async fn a_routed_publish_stages_the_bytes_content_type_and_headers_verbatim() {
    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let outbox = OutboxPublisher::new(store.clone(), publisher, OutboxPolicy::default());

    let envelope = Envelope::builder(common::OrderCreated { order_id: 7 })
        .header(HEADER_KEY, HEADER_VALUE)
        .expect("a non-reserved header key is accepted")
        .build();
    let expected_body = common::VndSerializer
        .serialize(&envelope.body)
        .expect("serialize");
    let serialized = common::serialize_with(envelope, &common::VndSerializer).expect("serialize");

    let mut tx = InMemoryTransaction;
    outbox
        .in_transaction(&mut tx)
        .publish(&serialized)
        .await
        .expect("stage succeeds");

    let stored = store.record(serialized.id).expect("row staged").envelope;
    assert_eq!(stored.body, expected_body);
    assert_eq!(
        stored.metadata.delivery.content_type,
        *common::VndSerializer.content_type()
    );
    assert_ne!(
        stored.metadata.delivery.content_type,
        reliar_core::ContentType::JSON
    );
    assert_eq!(
        stored.headers().and_then(|h| h.get(HEADER_KEY)),
        Some(HEADER_VALUE)
    );
}

#[tokio::test]
async fn a_direct_publish_forwards_the_bytes_content_type_and_headers_verbatim() {
    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let policy = OutboxPolicy::from_settings(&OutboxSettings::default().enabled(false))
        .expect("valid settings");
    let outbox = OutboxPublisher::new(store, publisher.clone(), policy);

    let envelope = Envelope::builder(common::OrderCreated { order_id: 7 })
        .header(HEADER_KEY, HEADER_VALUE)
        .expect("a non-reserved header key is accepted")
        .build();
    let expected_body = common::VndSerializer
        .serialize(&envelope.body)
        .expect("serialize");
    let serialized = common::serialize_with(envelope, &common::VndSerializer).expect("serialize");

    outbox
        .publish_direct(&serialized)
        .await
        .expect("direct publish succeeds");

    let sent = publisher
        .envelopes()
        .into_iter()
        .next()
        .expect("exactly one publish call");
    assert_eq!(sent.body, expected_body);
    assert_eq!(
        sent.metadata.delivery.content_type,
        *common::VndSerializer.content_type()
    );
    assert_eq!(
        sent.headers().and_then(|h| h.get(HEADER_KEY)),
        Some(HEADER_VALUE)
    );
}

/// The two routes must never diverge in what they hand to the store vs. the transport: the same
/// serialized envelope, published one way then the other (via two separately configured
/// publishers), produces byte-identical `SerializedEnvelope`s.
#[tokio::test]
async fn both_routes_carry_the_same_serialized_envelope_for_the_same_input() {
    let envelope = Envelope::builder(common::OrderCreated { order_id: 9 })
        .header(HEADER_KEY, HEADER_VALUE)
        .expect("a non-reserved header key is accepted")
        .build();
    let serialized = common::serialize_with(envelope, &common::VndSerializer).expect("serialize");

    let outbox_store = InMemoryOutboxStore::default();
    let outbox_publisher = OutboxPublisher::new(
        outbox_store.clone(),
        RecordingPublisher::default(),
        OutboxPolicy::default(),
    );
    let mut tx = InMemoryTransaction;
    outbox_publisher
        .in_transaction(&mut tx)
        .publish(&serialized)
        .await
        .expect("stage succeeds");
    let routed = outbox_store
        .record(serialized.id)
        .expect("row staged")
        .envelope;

    let direct_publisher = RecordingPublisher::default();
    let direct_policy = OutboxPolicy::from_settings(&OutboxSettings::default().enabled(false))
        .expect("valid settings");
    let direct_outbox = OutboxPublisher::new(
        InMemoryOutboxStore::default(),
        direct_publisher.clone(),
        direct_policy,
    );
    direct_outbox
        .publish_direct(&serialized)
        .await
        .expect("direct publish succeeds");
    let direct = direct_publisher
        .envelopes()
        .into_iter()
        .next()
        .expect("exactly one publish call");

    assert_eq!(routed.body, direct.body);
    assert_eq!(
        routed.metadata.delivery.content_type,
        direct.metadata.delivery.content_type
    );
    assert_eq!(
        routed.headers().and_then(|h| h.get(HEADER_KEY)),
        direct.headers().and_then(|h| h.get(HEADER_KEY))
    );
}
