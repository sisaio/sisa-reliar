//! `ScopedOutboxPublisher` never decides a route itself — it delegates to its owner's
//! `OutboxPolicy`, and every assertion here computes the expected route from the policy, never a
//! hard-coded value (§43.D12, ADR 0033 Amendment C — this is what keeps the rule from being
//! duplicated into the composition layer).

#![cfg(feature = "test-support")]

mod common;

use reliar_core::{Envelope, Publisher as _};
use reliar_outbox::{
    InMemoryOutboxStore, InMemoryTransaction, MessageTypeNames, OutboxPolicy, OutboxPublisher,
    OutboxSettings, RecordingPublisher, RouteKind,
};

// A test helper, not itself a `#[test]` function: clippy's "allow unwrap/expect in tests"
// exemption only covers `#[test]` bodies, so it is granted explicitly here.
#[allow(clippy::expect_used)]
fn policies() -> Vec<OutboxPolicy> {
    vec![
        OutboxPolicy::default(),
        OutboxPolicy::from_settings(&OutboxSettings::default().enabled(false))
            .expect("valid settings"),
        OutboxPolicy::from_settings(
            &OutboxSettings::default()
                .allowed_types(MessageTypeNames::try_from_iter("test", ["a"]).expect("valid"))
                .expect("no overlap"),
        )
        .expect("valid settings"),
        OutboxPolicy::from_settings(
            &OutboxSettings::default()
                .disallowed_types(MessageTypeNames::try_from_iter("test", ["a"]).expect("valid"))
                .expect("no overlap"),
        )
        .expect("valid settings"),
    ]
}

#[tokio::test]
async fn scoped_publish_always_follows_the_policys_own_decision() {
    for policy in policies() {
        let store = InMemoryOutboxStore::default();
        let publisher = RecordingPublisher::default();
        let outbox = OutboxPublisher::new(store.clone(), publisher.clone(), policy.clone());
        assert_eq!(outbox.policy(), &policy);

        let mut tx = InMemoryTransaction;
        let scoped = outbox.in_transaction(&mut tx);

        let a = common::serialize(Envelope::builder(common::TypeA).build());
        let expected_a = outbox.policy().decide(&a.message_type);
        scoped
            .publish(&a)
            .await
            .expect("publish succeeds for this fixture");
        match expected_a {
            RouteKind::Outbox => assert!(store.record(a.id).is_some()),
            RouteKind::Direct => assert_eq!(publisher.count(a.id), 1),
            other => panic!("unexpected RouteKind variant: {other:?}"),
        }

        let b = common::serialize(Envelope::builder(common::TypeB).build());
        let expected_b = outbox.policy().decide(&b.message_type);
        scoped
            .publish(&b)
            .await
            .expect("publish succeeds for this fixture");
        match expected_b {
            RouteKind::Outbox => assert!(store.record(b.id).is_some()),
            RouteKind::Direct => assert_eq!(publisher.count(b.id), 1),
            other => panic!("unexpected RouteKind variant: {other:?}"),
        }
    }
}
