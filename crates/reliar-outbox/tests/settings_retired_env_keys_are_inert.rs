//! The retired `RELIAR_OUTBOX_ENABLED` / `_ALLOWED_TYPES` / `_DISALLOWED_TYPES` environment keys
//! are inert: `from_env` returns exactly the `Default` settings and no error, even when all three
//! are set to hostile values (ADR 0036 §7, contract §7, E12).
//!
//! See `tests/common::run_scenario_in_child` for why this runs in a child process.

mod common;

use reliar_outbox::OutboxSettings;

const PREFIX: &str = "RELIAR_OUTBOX_TEST_RETIRED_";

#[test]
fn retired_keys_are_neither_read_nor_rejected() {
    if common::is_child() {
        let settings = OutboxSettings::from_env(PREFIX).expect("retired keys must not error");
        let defaults = OutboxSettings::default();

        assert_eq!(
            settings.dispatcher.batch_size,
            defaults.dispatcher.batch_size
        );
        assert_eq!(settings.dispatcher.lease, defaults.dispatcher.lease);
        assert_eq!(
            settings.dispatcher.max_in_flight,
            defaults.dispatcher.max_in_flight
        );
        assert_eq!(settings.dispatcher.ordering, defaults.dispatcher.ordering);
        assert_eq!(
            settings.retention.published_retention,
            defaults.retention.published_retention
        );
        assert_eq!(
            settings.retention.dead_retention,
            defaults.retention.dead_retention
        );
        return;
    }

    let ok = common::run_scenario_in_child(
        "retired_keys_are_neither_read_nor_rejected",
        &[
            (&format!("{PREFIX}ENABLED"), "false"),
            (&format!("{PREFIX}ALLOWED_TYPES"), "orders.created"),
            (
                &format!("{PREFIX}DISALLOWED_TYPES"),
                "not,a,message,type,.v9",
            ),
        ],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}
