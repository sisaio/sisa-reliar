//! `OutboxSettings::from_env` itself accepts `POLL_INTERVAL_MS=0` — it only validates a value's
//! *shape* (SRS §7.2), never a dispatcher-level invariant. The zero is instead caught one layer
//! down, at [`OutboxDispatcherBuilder::build`], by [`ConfigError::ZeroPollInterval`] (S4 review
//! 7): the env-to-dispatcher pipeline as a whole still rejects it, just at the point that
//! actually knows the invariant.
//!
//! See `tests/common::run_scenario_in_child` for why this runs in a child process — env vars are
//! process-global and tests run concurrently.
#![cfg(feature = "test-support")]

mod common;

use reliar_outbox::{
    ConfigError, InMemoryOutboxStore, OutboxDispatcher, OutboxSettings, RecordingPublisher,
};

const PREFIX: &str = "RELIAR_OUTBOX_TEST_ZERO_POLL_";

#[test]
fn zero_poll_interval_ms_from_env_is_rejected_when_the_dispatcher_is_built() {
    if common::is_child() {
        let settings = OutboxSettings::from_env(PREFIX).expect("from_env itself accepts 0");

        let err = OutboxDispatcher::builder(
            InMemoryOutboxStore::default(),
            RecordingPublisher::default(),
        )
        .settings(settings.dispatcher)
        .build()
        .expect_err("a zero poll_interval must be rejected once a dispatcher is built");
        assert_eq!(
            err,
            ConfigError::ZeroPollInterval {
                field: "poll_interval"
            }
        );
        return;
    }

    let ok = common::run_scenario_in_child(
        "zero_poll_interval_ms_from_env_is_rejected_when_the_dispatcher_is_built",
        &[(&format!("{PREFIX}POLL_INTERVAL_MS"), "0")],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}
