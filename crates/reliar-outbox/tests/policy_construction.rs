//! `OutboxPolicy::default`, its accessors, and the construction backstop over public
//! [`OutboxSettings`] fields (§43.D5, §43.D13, ADR 0033).

use reliar_core::MessageType;
use reliar_outbox::{MessageTypeNames, OutboxPolicy, OutboxSettings, RouteKind};

#[test]
fn default_routes_every_type_through_the_outbox() {
    let policy = OutboxPolicy::default();
    assert!(policy.enabled());
    assert!(policy.allowed_types().is_empty());
    assert!(policy.disallowed_types().is_empty());
    assert_eq!(
        policy.decide(&MessageType::new("anything", 1)),
        RouteKind::Outbox
    );
}

#[test]
fn default_equals_from_default_settings() {
    let from_settings = OutboxPolicy::from_settings(&OutboxSettings::default())
        .expect("default settings never overlap");
    assert_eq!(OutboxPolicy::default(), from_settings);
}

#[test]
fn accessors_return_exactly_what_the_settings_held() {
    let settings = OutboxSettings::default()
        .allowed_types(MessageTypeNames::try_from_iter("test", ["a", "b"]).expect("valid"))
        .expect("no overlap")
        .disallowed_types(MessageTypeNames::try_from_iter("test", ["c"]).expect("valid"))
        .expect("no overlap")
        .enabled(false);
    let policy = OutboxPolicy::from_settings(&settings).expect("valid settings");

    assert!(!policy.enabled());
    assert_eq!(policy.allowed_types(), &settings.allowed_types);
    assert_eq!(policy.disallowed_types(), &settings.disallowed_types);
}

/// R20: `Debug` shows the whole rule — the flag and both lists — not just `enabled`.
#[test]
fn clone_partial_eq_and_debug() {
    let settings = OutboxSettings::default()
        .allowed_types(MessageTypeNames::try_from_iter("test", ["a"]).expect("valid"))
        .expect("no overlap")
        .disallowed_types(MessageTypeNames::try_from_iter("test", ["b"]).expect("valid"))
        .expect("no overlap");
    let policy = OutboxPolicy::from_settings(&settings).expect("valid settings");
    let cloned = policy.clone();
    assert_eq!(policy, cloned);

    let debug = format!("{policy:?}");
    assert!(debug.contains("enabled"), "Debug output was: {debug}");
    assert!(debug.contains("\"a\""), "Debug output was: {debug}");
    assert!(debug.contains("\"b\""), "Debug output was: {debug}");
}

/// §43.D13 backstop: [`OutboxPolicy::from_settings`] rejects an overlap even when the settings'
/// **public fields** were assigned directly rather than through the fallible setters — the one
/// path the setters cannot cover.
#[test]
fn from_settings_rejects_an_overlap_assigned_through_public_fields() {
    let mut settings = OutboxSettings::default();
    settings.allowed_types = MessageTypeNames::try_from_iter("test", ["a"]).expect("valid");
    settings.disallowed_types = MessageTypeNames::try_from_iter("test", ["a"]).expect("valid");

    let err = OutboxPolicy::from_settings(&settings).expect_err("overlap must be rejected");
    assert_eq!(err.key(), "disallowed_types");
}

/// A valid (non-overlapping) pair assigned the same way always succeeds — the backstop rejects
/// only a genuine overlap, never a well-formed value reached by a path other than the setters.
#[test]
fn from_settings_accepts_a_non_overlapping_pair_assigned_through_public_fields() {
    let mut settings = OutboxSettings::default();
    settings.allowed_types = MessageTypeNames::try_from_iter("test", ["a"]).expect("valid");
    settings.disallowed_types = MessageTypeNames::try_from_iter("test", ["b"]).expect("valid");

    assert!(OutboxPolicy::from_settings(&settings).is_ok());
}
