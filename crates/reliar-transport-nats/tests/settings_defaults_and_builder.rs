//! `NatsSettings::default` matches the contract's documented values, every builder method sets
//! exactly the field it names, and no constructor or builder reads the environment — only calling
//! `NatsSettings::from_env` does (SRS §7.2, ADR 0019). Mirrors `reliar-outbox`'s
//! `settings_defaults_match_srs.rs` / `settings_construction_never_reads_env.rs`.

mod common;

use std::time::Duration;

use reliar_transport_nats::NatsSettings;

#[test]
fn defaults_match_the_contract() {
    let settings = NatsSettings::default();
    assert_eq!(settings.subject_prefix, "reliar");
    assert_eq!(settings.publish_timeout, Duration::from_secs(10));
    assert_eq!(settings.batch_pipeline_depth, 64);
    assert_eq!(settings.max_payload, None);
}

#[test]
fn builder_methods_set_only_the_named_field() {
    let defaults = NatsSettings::default();

    let settings = NatsSettings::default().subject_prefix("app");
    assert_eq!(settings.subject_prefix, "app");
    assert_eq!(settings.publish_timeout, defaults.publish_timeout);
    assert_eq!(settings.batch_pipeline_depth, defaults.batch_pipeline_depth);
    assert_eq!(settings.max_payload, defaults.max_payload);

    let settings = NatsSettings::default().publish_timeout(Duration::from_secs(3));
    assert_eq!(settings.publish_timeout, Duration::from_secs(3));
    assert_eq!(settings.subject_prefix, defaults.subject_prefix);

    let settings = NatsSettings::default().batch_pipeline_depth(8);
    assert_eq!(settings.batch_pipeline_depth, 8);
    assert_eq!(settings.subject_prefix, defaults.subject_prefix);

    let settings = NatsSettings::default().max_payload(Some(1_048_576));
    assert_eq!(settings.max_payload, Some(1_048_576));
    assert_eq!(settings.batch_pipeline_depth, defaults.batch_pipeline_depth);
}

/// The documented conventional prefix (contract §4.1) — deliberately **not** a test-only prefix,
/// so this proves construction ignores exactly the variables a real host would have set. See
/// `tests/common::run_scenario_in_child` for why this runs in a child process.
const REAL_PREFIX: &str = "RELIAR_NATS_";

#[test]
fn default_and_builders_ignore_absurd_values_under_the_real_prefix() {
    if common::is_child() {
        let settings = NatsSettings::default();
        assert_eq!(settings.subject_prefix, "reliar");
        assert_eq!(settings.publish_timeout, Duration::from_secs(10));
        assert_eq!(settings.batch_pipeline_depth, 64);
        assert_eq!(settings.max_payload, None);

        let settings = NatsSettings::default().subject_prefix("app");
        assert_eq!(settings.subject_prefix, "app");
        return;
    }

    let ok = common::run_scenario_in_child(
        "default_and_builders_ignore_absurd_values_under_the_real_prefix",
        &[
            (&format!("{REAL_PREFIX}SUBJECT_PREFIX"), "not-the-default"),
            (&format!("{REAL_PREFIX}PUBLISH_TIMEOUT_MS"), "1"),
            (&format!("{REAL_PREFIX}BATCH_PIPELINE_DEPTH"), "999999"),
            (&format!("{REAL_PREFIX}MAX_PAYLOAD_BYTES"), "1"),
        ],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}
