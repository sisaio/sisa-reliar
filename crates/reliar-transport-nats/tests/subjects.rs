//! `SubjectResolver`: the default `<prefix>.<message_type>` resolver, its §3.1 validation, the
//! `destination`-aware resolver, and honouring a caller-supplied resolver (U9, U10 — story C4).

mod common;

use bytes::Bytes;
use reliar_core::{EndpointAddress, Envelope, Metadata};
use reliar_transport_nats::{DestinationSubjects, PrefixSubjects, SubjectError, SubjectResolver};

use common::OrderCreated;

fn envelope() -> reliar_core::SerializedEnvelope {
    Envelope::builder(OrderCreated { order_id: 1 })
        .build()
        .map_body(|_| Bytes::from_static(b"{}"))
}

#[test]
fn default_prefix_is_reliar() {
    assert_eq!(PrefixSubjects::default().prefix(), "reliar");
}

#[test]
fn prefix_subjects_yields_prefix_dot_message_type() {
    let resolver = PrefixSubjects::new("app").expect("legal prefix");
    let subject = resolver.subject(&envelope()).expect("legal subject");
    assert_eq!(subject.as_str(), "app.orders.created.v1");
}

#[test]
fn empty_prefix_is_rejected() {
    assert_eq!(
        PrefixSubjects::new("").expect_err("empty prefix"),
        SubjectError::Empty
    );
}

#[test]
fn a_prefix_with_an_empty_token_is_rejected() {
    let err = PrefixSubjects::new("a..b").expect_err("empty token between two dots");
    assert_eq!(
        err,
        SubjectError::EmptyToken {
            subject: "a..b".to_string()
        }
    );
}

#[test]
fn a_prefix_with_a_wildcard_token_is_rejected() {
    let err = PrefixSubjects::new("a.*").expect_err("a bare wildcard token");
    assert_eq!(
        err,
        SubjectError::Wildcard {
            subject: "a.*".to_string()
        }
    );

    let err = PrefixSubjects::new("a.>").expect_err("a full-wildcard token");
    assert_eq!(
        err,
        SubjectError::Wildcard {
            subject: "a.>".to_string()
        }
    );
}

#[test]
fn a_prefix_with_whitespace_is_rejected() {
    let err = PrefixSubjects::new("a b").expect_err("whitespace is outside 0x21..=0x7E");
    assert_eq!(
        err,
        SubjectError::IllegalCharacter {
            subject: "a b".to_string()
        }
    );
}

#[test]
fn a_prefix_with_a_control_character_is_rejected() {
    let err = PrefixSubjects::new("a\tb").expect_err("a tab is outside 0x21..=0x7E");
    assert_eq!(
        err,
        SubjectError::IllegalCharacter {
            subject: "a\tb".to_string()
        }
    );
}

#[test]
fn an_overlong_prefix_is_rejected() {
    let overlong = "a".repeat(SubjectError::MAX_LEN + 1);
    let err = PrefixSubjects::new(overlong.clone()).expect_err("over MAX_LEN");
    assert_eq!(
        err,
        SubjectError::TooLong {
            len: overlong.len(),
            limit: SubjectError::MAX_LEN,
        }
    );
}

#[test]
fn destination_subjects_prefers_the_destination_when_set() {
    let resolver = DestinationSubjects::new(PrefixSubjects::default());
    let mut metadata = Metadata::default();
    metadata.routing.destination = Some(EndpointAddress::parse("custom.subject").expect("legal"));
    let envelope = Envelope::builder(OrderCreated { order_id: 1 })
        .metadata(metadata)
        .build()
        .map_body(|_| Bytes::from_static(b"{}"));

    let subject = resolver.subject(&envelope).expect("legal subject");
    assert_eq!(subject.as_str(), "custom.subject");
}

#[test]
fn destination_subjects_falls_back_to_prefix_subjects_when_unset() {
    let resolver = DestinationSubjects::new(PrefixSubjects::new("app").expect("legal"));
    let subject = resolver.subject(&envelope()).expect("legal subject");
    assert_eq!(subject.as_str(), "app.orders.created.v1");
}

#[test]
fn destination_subjects_still_validates_the_destination() {
    let resolver = DestinationSubjects::new(PrefixSubjects::default());
    let mut metadata = Metadata::default();
    metadata.routing.destination =
        Some(EndpointAddress::parse("bad subject").expect("legal endpoint address"));
    let envelope = Envelope::builder(OrderCreated { order_id: 1 })
        .metadata(metadata)
        .build()
        .map_body(|_| Bytes::from_static(b"{}"));

    let err = resolver
        .subject(&envelope)
        .expect_err("whitespace is not a legal subject");
    assert_eq!(
        err,
        SubjectError::IllegalCharacter {
            subject: "bad subject".to_string()
        }
    );
}

// U10 ("a caller-supplied resolver is honoured") is exercised for real against `NatsPublisher`
// by S2's N6 (`tests/nats/n6_custom_resolver_subject.rs`); a standalone test here would only
// prove that a fake resolver returns what it was told to return, not that this crate honours one
// anywhere (S1 review, major 5).

/// Minor 13: a wildcard token in the **message type** — not just in a configured prefix — is
/// rejected the same way, reached through `PrefixSubjects`'s own `<prefix>.<message_type>`
/// composition rather than through a hand-built subject string.
#[test]
fn a_wildcard_message_type_is_rejected_via_prefix_subjects() {
    let envelope = reliar_core::SerializedEnvelope::from_parts(
        reliar_core::MessageId::new(),
        reliar_core::MessageType::from_parts("orders.*", 1),
        Bytes::from_static(b"{}"),
        Metadata::default(),
        None,
    );

    let err = PrefixSubjects::default()
        .subject(&envelope)
        .expect_err("a `*` token anywhere in the resolved subject is a wildcard");
    assert_eq!(
        err,
        SubjectError::Wildcard {
            subject: "reliar.orders.*.v1".to_string()
        }
    );
}

/// Minor 13: an over-length message type carries the resolved subject past
/// [`SubjectError::MAX_LEN`], reached the same way — through `PrefixSubjects`, not a hand-built
/// subject string.
#[test]
fn an_overlong_message_type_is_rejected_via_prefix_subjects() {
    let long_name = "a".repeat(SubjectError::MAX_LEN);
    let envelope = reliar_core::SerializedEnvelope::from_parts(
        reliar_core::MessageId::new(),
        reliar_core::MessageType::from_parts(long_name.clone(), 1),
        Bytes::from_static(b"{}"),
        Metadata::default(),
        None,
    );
    let expected_subject = format!("reliar.{long_name}.v1");
    assert!(expected_subject.len() > SubjectError::MAX_LEN);

    let err = PrefixSubjects::default()
        .subject(&envelope)
        .expect_err("the resolved subject exceeds MAX_LEN");
    assert_eq!(
        err,
        SubjectError::TooLong {
            len: expected_subject.len(),
            limit: SubjectError::MAX_LEN,
        }
    );
}
