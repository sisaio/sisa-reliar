//! N7 (was U12, withdrawn — contract §7): `NatsConfigError` for a zero `batch_pipeline_depth`, a zero
//! `publish_timeout`, a `max_payload(Some(0))`, and a bad `subject_prefix`. Construction never
//! touches the network, but a real `Context` is the only way to obtain one, so this lives in the
//! NATS binary alongside every other `NatsPublisher` scenario (ADR 0031 §4) rather than in a
//! standard-harness `tests/*.rs` file.

use std::time::Duration;

use reliar_transport_nats::{NatsConfigError, NatsPublisher, NatsSettings, PrefixSubjects};

use crate::common;

async fn zero_batch_pipeline_depth_is_rejected() {
    let context = common::jetstream_context().await;
    let err = NatsPublisher::new(context, NatsSettings::default().batch_pipeline_depth(0))
        .expect_err("zero batch_pipeline_depth must be rejected");
    assert_eq!(err, NatsConfigError::ZeroBatchPipelineDepth);
}

async fn zero_publish_timeout_is_rejected() {
    let context = common::jetstream_context().await;
    let err = NatsPublisher::new(
        context,
        NatsSettings::default().publish_timeout(Duration::ZERO),
    )
    .expect_err("zero publish_timeout must be rejected");
    assert_eq!(err, NatsConfigError::ZeroPublishTimeout);
}

/// N7's own reason to exist: with `max_payload(Some(1))` accepted at construction, every envelope
/// fails `PayloadTooLarge` and nothing is ever stored, which is why a zero limit is rejected here
/// instead — the one payload limit that is unusable for **every** possible envelope, including one
/// with an empty body (ADR 0030 Amendment A).
async fn zero_max_payload_is_rejected() {
    let context = common::jetstream_context().await;
    let err = NatsPublisher::new(context, NatsSettings::default().max_payload(Some(0)))
        .expect_err("zero max_payload must be rejected");
    assert_eq!(err, NatsConfigError::ZeroMaxPayload);
}

async fn an_illegal_subject_prefix_is_rejected() {
    let context = common::jetstream_context().await;
    let err = NatsPublisher::new(
        context,
        NatsSettings::default().subject_prefix("bad.*.prefix"),
    )
    .expect_err("a wildcard prefix must be rejected");
    assert!(matches!(err, NatsConfigError::Subject(_)), "got {err:?}");
}

/// Review minor 3: `build` (shared by both constructors) enforces every zero-guard the same way
/// regardless of which public constructor calls it — proven here through `with_resolver` too, not
/// only through `new`.
async fn zero_batch_pipeline_depth_is_rejected_through_with_resolver() {
    let context = common::jetstream_context().await;
    let err = NatsPublisher::with_resolver(
        context,
        NatsSettings::default().batch_pipeline_depth(0),
        PrefixSubjects::default(),
    )
    .expect_err("zero batch_pipeline_depth must be rejected");
    assert_eq!(err, NatsConfigError::ZeroBatchPipelineDepth);
}

async fn zero_publish_timeout_is_rejected_through_with_resolver() {
    let context = common::jetstream_context().await;
    let err = NatsPublisher::with_resolver(
        context,
        NatsSettings::default().publish_timeout(Duration::ZERO),
        PrefixSubjects::default(),
    )
    .expect_err("zero publish_timeout must be rejected");
    assert_eq!(err, NatsConfigError::ZeroPublishTimeout);
}

async fn zero_max_payload_is_rejected_through_with_resolver() {
    let context = common::jetstream_context().await;
    let err = NatsPublisher::with_resolver(
        context,
        NatsSettings::default().max_payload(Some(0)),
        PrefixSubjects::default(),
    )
    .expect_err("zero max_payload must be rejected");
    assert_eq!(err, NatsConfigError::ZeroMaxPayload);
}

async fn with_resolver_never_validates_the_unused_subject_prefix() {
    let context = common::jetstream_context().await;
    // `subject_prefix` is documented as unused when an explicit resolver is supplied — an
    // otherwise-illegal prefix must not fail construction through this path.
    NatsPublisher::with_resolver(
        context,
        NatsSettings::default().subject_prefix("bad.*.prefix"),
        PrefixSubjects::default(),
    )
    .expect("subject_prefix is ignored by with_resolver");
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "n7_config_validation::zero_batch_pipeline_depth_is_rejected",
            move || {
                rt.block_on(zero_batch_pipeline_depth_is_rejected());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "n7_config_validation::zero_publish_timeout_is_rejected",
            move || {
                rt.block_on(zero_publish_timeout_is_rejected());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "n7_config_validation::zero_max_payload_is_rejected",
            move || {
                rt.block_on(zero_max_payload_is_rejected());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "n7_config_validation::an_illegal_subject_prefix_is_rejected",
            move || {
                rt.block_on(an_illegal_subject_prefix_is_rejected());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "n7_config_validation::with_resolver_never_validates_the_unused_subject_prefix",
            move || {
                rt.block_on(with_resolver_never_validates_the_unused_subject_prefix());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "n7_config_validation::zero_batch_pipeline_depth_is_rejected_through_with_resolver",
            move || {
                rt.block_on(zero_batch_pipeline_depth_is_rejected_through_with_resolver());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "n7_config_validation::zero_publish_timeout_is_rejected_through_with_resolver",
            move || {
                rt.block_on(zero_publish_timeout_is_rejected_through_with_resolver());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "n7_config_validation::zero_max_payload_is_rejected_through_with_resolver",
            move || {
                rt.block_on(zero_max_payload_is_rejected_through_with_resolver());
                Ok(())
            },
        ),
    ]
}
