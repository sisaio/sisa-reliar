//! `NatsSettings::from_env` overrides only the variables present under its prefix (SRS §7.2).
//! See `tests/common::run_scenario_in_child` for why each scenario runs in a child process.

mod common;

use std::time::Duration;

use reliar_transport_nats::NatsSettings;

const PREFIX: &str = "RELIAR_NATS_TEST_OVERRIDE_";

#[test]
fn overrides_only_present_variables() {
    if common::is_child() {
        let settings = NatsSettings::from_env(PREFIX).expect("valid overrides parse");

        assert_eq!(settings.subject_prefix, "app");
        assert_eq!(settings.publish_timeout, Duration::from_secs(3));

        // Absent: keeps `Default`'s value.
        let defaults = NatsSettings::default();
        assert_eq!(settings.batch_pipeline_depth, defaults.batch_pipeline_depth);
        assert_eq!(settings.max_payload, defaults.max_payload);
        return;
    }

    let ok = common::run_scenario_in_child(
        "overrides_only_present_variables",
        &[
            (&format!("{PREFIX}SUBJECT_PREFIX"), "app"),
            (&format!("{PREFIX}PUBLISH_TIMEOUT_MS"), "3000"),
        ],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

#[test]
fn overrides_batch_pipeline_depth_and_max_payload_independently() {
    if common::is_child() {
        let settings = NatsSettings::from_env(PREFIX).expect("valid overrides parse");

        assert_eq!(settings.batch_pipeline_depth, 4);
        assert_eq!(settings.max_payload, Some(1_048_576));

        let defaults = NatsSettings::default();
        assert_eq!(settings.subject_prefix, defaults.subject_prefix);
        assert_eq!(settings.publish_timeout, defaults.publish_timeout);
        return;
    }

    let ok = common::run_scenario_in_child(
        "overrides_batch_pipeline_depth_and_max_payload_independently",
        &[
            (&format!("{PREFIX}BATCH_PIPELINE_DEPTH"), "4"),
            (&format!("{PREFIX}MAX_PAYLOAD_BYTES"), "1048576"),
        ],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

#[test]
fn absent_variables_change_nothing() {
    if common::is_child() {
        let settings = NatsSettings::from_env(PREFIX).expect("no variables set");
        assert_eq!(settings, NatsSettings::default());
        return;
    }

    let ok = common::run_scenario_in_child("absent_variables_change_nothing", &[])
        .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}
