//! Review 1, major 3 — every `EnvelopeBuilder` setter, and the contract's rule that conversation
//! rooting is decided by the *value* of `conversation_id` (the `UNSET` sentinel), never by which
//! setter was called or in what order (contract §2.4/§2.6, ADR 0011).

mod common;

use common::OrderCreated;
use reliar_core::{ConversationId, CorrelationMetadata, Envelope, MessageId};
use time::OffsetDateTime;

#[test]
fn an_uncorrelated_message_roots_its_own_conversation() {
    let envelope = Envelope::builder(OrderCreated { order_id: 1 }).build();

    assert_eq!(
        envelope.metadata.correlation.conversation_id.as_uuid(),
        envelope.id.as_uuid()
    );
}

#[test]
fn conversation_sets_an_explicit_id_that_build_keeps() {
    let parent_conversation = ConversationId::new();

    let envelope = Envelope::builder(OrderCreated { order_id: 2 })
        .conversation(parent_conversation)
        .build();

    assert_eq!(
        envelope.metadata.correlation.conversation_id,
        parent_conversation
    );
}

#[test]
fn metadata_tweak_of_an_unrelated_field_still_roots_at_the_envelope_own_id() {
    // The common "start from Default, tweak one field" path: nothing here touches
    // `conversation_id`, so the sentinel travels through and `build` still roots it.
    let mut metadata = reliar_core::Metadata::default();
    metadata.tenant_id = Some("acme".to_string());

    let envelope = Envelope::builder(OrderCreated { order_id: 3 })
        .metadata(metadata)
        .build();

    assert_eq!(
        envelope.metadata.correlation.conversation_id.as_uuid(),
        envelope.id.as_uuid()
    );
    assert_eq!(envelope.metadata.tenant_id.as_deref(), Some("acme"));
}

#[test]
fn metadata_replaces_a_previously_set_conversation() {
    // Setter order is irrelevant: `.conversation(x).metadata(m)` yields `m`'s conversation id,
    // since `.metadata` replaces the whole struct.
    let earlier = ConversationId::new();

    let envelope = Envelope::builder(OrderCreated { order_id: 4 })
        .conversation(earlier)
        .metadata(reliar_core::Metadata::default())
        .build();

    assert_ne!(envelope.metadata.correlation.conversation_id, earlier);
    assert_eq!(
        envelope.metadata.correlation.conversation_id.as_uuid(),
        envelope.id.as_uuid()
    );
}

#[test]
fn correlation_carrying_a_real_conversation_id_is_kept_verbatim() {
    let causing_conversation = ConversationId::new();
    let mut correlation = CorrelationMetadata::default();
    correlation.conversation_id = causing_conversation;

    let envelope = Envelope::builder(OrderCreated { order_id: 5 })
        .correlation(correlation)
        .build();

    assert_eq!(
        envelope.metadata.correlation.conversation_id,
        causing_conversation
    );
}

#[test]
fn id_overrides_the_generated_message_id() {
    let fixed_id = MessageId::new();

    let envelope = Envelope::builder(OrderCreated { order_id: 6 })
        .id(fixed_id)
        .build();

    assert_eq!(envelope.id, fixed_id);
    // Rooting still uses the (overridden) envelope id.
    assert_eq!(
        envelope.metadata.correlation.conversation_id.as_uuid(),
        fixed_id.as_uuid()
    );
}

#[test]
fn causation_records_the_parent_message() {
    let parent = MessageId::new();

    let envelope = Envelope::builder(OrderCreated { order_id: 7 })
        .causation(parent)
        .build();

    assert_eq!(envelope.metadata.correlation.causation_id, Some(parent));
}

#[test]
fn trace_carries_traceparent_and_tracestate_verbatim() {
    let envelope = Envelope::builder(OrderCreated { order_id: 8 })
        .trace("00-trace-01", Some("vendor=state".to_string()))
        .build();

    assert_eq!(
        envelope.metadata.trace.traceparent.as_deref(),
        Some("00-trace-01")
    );
    assert_eq!(
        envelope.metadata.trace.tracestate.as_deref(),
        Some("vendor=state")
    );
}

#[test]
fn expires_at_is_carried_into_delivery_metadata() {
    let at = OffsetDateTime::now_utc();

    let envelope = Envelope::builder(OrderCreated { order_id: 9 })
        .expires_at(at)
        .build();

    assert_eq!(envelope.metadata.delivery.expires_at, Some(at));
}

#[test]
fn headers_mut_lazily_allocates_and_set_headers_replaces_the_whole_map() {
    let mut envelope = Envelope::builder(OrderCreated { order_id: 10 }).build();
    assert!(envelope.headers().is_none());

    envelope.headers_mut().insert("x-a", "1").unwrap();
    assert_eq!(envelope.headers().unwrap().get("x-a"), Some("1"));

    let mut replacement = reliar_core::Headers::default();
    replacement.insert("x-b", "2").unwrap();
    envelope.set_headers(Some(replacement));

    assert_eq!(envelope.headers().unwrap().get("x-a"), None);
    assert_eq!(envelope.headers().unwrap().get("x-b"), Some("2"));

    envelope.set_headers(None);
    assert!(envelope.headers().is_none());
}
