//! `OutboxPolicy::decide` matches by [`reliar_core::MessageType::name`] alone: exact,
//! case-sensitive, version-agnostic (§43.D3, ADR 0033 §5).

use reliar_core::MessageType;
use reliar_outbox::{MessageTypeNames, OutboxPolicy, OutboxSettings, RouteKind};

// A test helper, not itself a `#[test]` function: clippy's "allow unwrap/expect in tests"
// exemption only covers `#[test]` bodies, so it is granted explicitly here.
#[allow(clippy::expect_used)]
fn policy_allowing(name: &str) -> OutboxPolicy {
    let settings = OutboxSettings::default()
        .allowed_types(MessageTypeNames::try_from_iter("test", [name]).expect("valid test name"))
        .expect("no overlap");
    OutboxPolicy::from_settings(&settings).expect("valid settings")
}

#[test]
fn matches_every_version_of_the_same_name() {
    let policy = policy_allowing("a");
    assert_eq!(policy.decide(&MessageType::new("a", 1)), RouteKind::Outbox);
    assert_eq!(policy.decide(&MessageType::new("a", 2)), RouteKind::Outbox);
    assert_eq!(policy.decide(&MessageType::new("a", 99)), RouteKind::Outbox);
}

#[test]
fn is_case_sensitive() {
    let policy = policy_allowing("a");
    assert_eq!(policy.decide(&MessageType::new("A", 1)), RouteKind::Direct);
}

#[test]
fn does_not_match_by_prefix() {
    let policy = policy_allowing("a");
    assert_eq!(
        policy.decide(&MessageType::new("a.b", 1)),
        RouteKind::Direct
    );
}

/// `RouteKind::as_str`/`is_outbox` — the span field and metric label value, and the boolean
/// shorthand a caller uses on the route `OutboxPolicy::decide` returns (review round 1, minor:
/// untested).
#[test]
fn route_kind_as_str_and_is_outbox() {
    assert_eq!(RouteKind::Outbox.as_str(), "outbox");
    assert!(RouteKind::Outbox.is_outbox());

    assert_eq!(RouteKind::Direct.as_str(), "direct");
    assert!(!RouteKind::Direct.is_outbox());
}
