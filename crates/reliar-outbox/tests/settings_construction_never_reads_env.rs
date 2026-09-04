//! No constructor, `Default` or builder method reads the environment — only calling
//! `OutboxSettings::from_env` does (SRS §7.2, ADR 0019, §43.A.30's settings half).
//!
//! Every `RELIAR_OUTBOX_*` variable below is the **real** key `OutboxSettings::from_env` would
//! read under the documented `RELIAR_OUTBOX_` prefix (§7.2), set to a value that would visibly
//! change the result if anything but `from_env` looked at it. See
//! `tests/common::run_scenario_in_child` for why this runs in a child process.

mod common;

use std::time::Duration;

use reliar_outbox::{
    AcquireRequest, DeadQuery, ExponentialBackoff, OutboxSettings, PurgeRequest, WorkerId,
};

/// The documented default prefix (SRS §7.2) — deliberately **not** a test-only prefix, so this
/// scenario proves construction ignores exactly the variables a real host would have set.
const PREFIX: &str = "RELIAR_OUTBOX_";

#[test]
fn default_and_builders_ignore_absurd_values_under_the_real_prefix() {
    if common::is_child() {
        // `Default` (and everything built from it) must equal the hardcoded documented values,
        // not anything derived from the environment above.
        let settings = OutboxSettings::default();
        assert_eq!(settings.dispatcher.batch_size, 100);
        assert_eq!(settings.dispatcher.lease, Duration::from_secs(30));
        assert_eq!(
            settings.retention.published_retention,
            Duration::from_secs(7 * 24 * 60 * 60)
        );
        assert_eq!(settings.dispatcher.worker_id, None);

        let retry = ExponentialBackoff::default();
        assert_eq!(retry.max_attempts, 10);
        assert!((retry.jitter - 0.2).abs() < f64::EPSILON);

        // Builders and other public constructors never touch `std::env` either.
        let request = AcquireRequest::new(WorkerId::generate());
        assert_eq!(request.batch_size, 100);
        assert_eq!(request.lease, Duration::from_secs(30));

        assert_eq!(PurgeRequest::default().batch_size, 1_000);
        assert_eq!(DeadQuery::default().limit, 100);

        // `WorkerId::generate()` never reads `WORKER_ID` (or `HOSTNAME`, or anything else): it
        // is always `pid:uuid7`, never the absurd value set below.
        let worker_id = WorkerId::generate();
        assert_ne!(worker_id.as_str(), "not-generated");
        assert!(worker_id.as_str().contains(':'), "pid:uuid7 shape");
        return;
    }

    let ok = common::run_scenario_in_child(
        "default_and_builders_ignore_absurd_values_under_the_real_prefix",
        &[
            (&format!("{PREFIX}BATCH_SIZE"), "999999"),
            (&format!("{PREFIX}LEASE_MS"), "1"),
            (&format!("{PREFIX}MAX_IN_FLIGHT"), "999999"),
            (&format!("{PREFIX}ORDERING"), "per_key"),
            (&format!("{PREFIX}PUBLISHED_RETENTION_MS"), "1"),
            (&format!("{PREFIX}DEAD_RETENTION_MS"), "1"),
            (&format!("{PREFIX}PURGE_BATCH_SIZE"), "1"),
            (&format!("{PREFIX}WORKER_ID"), "not-generated"),
            (&format!("{PREFIX}RETRY_MAX_ATTEMPTS"), "999999"),
            (&format!("{PREFIX}RETRY_JITTER"), "0.9"),
        ],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}
