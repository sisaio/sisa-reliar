//! Neither a hung `complete` nor a hung `fail` blocks the loop from reaching the lease-renewal
//! tick: before S4 review 4 (major 3), `retry_unwritten_outcomes` ran as an unconditional tail
//! call *after* `select!` resolved, so a store call stuck for the full `store_timeout` — worse,
//! **two** stuck calls in the same round, if both a `complete` and a `fail` are outstanding —
//! could occupy up to `2 * store_timeout` of one loop iteration, blocking the loop from ever
//! reaching the lease tick and starving renewal for that whole stretch (RELIAR-26, S4 review 4,
//! major 3; S4 review 5, major — the original version of this test only ever hung `complete`,
//! so it could not tell the fixed version apart from the regression it was meant to catch).
//! Racing the retry inside the same `select!` as the lease tick means a due tick simply wins that
//! round regardless of how long the hung writes would otherwise take.
//!
//! # Why two waves
//!
//! M2 bounds how long *any one row* keeps being retried (and therefore renewed) to `lease`, and
//! the lease-renewal tick fires every `lease / 2` — so a single permanently-hung row can be
//! renewed **at most twice** (at `lease / 2` and at `lease`) before M2 abandons it. Proving the
//! renewal tick's cadence over **four** tick periods (S4 review 6, major — the previous version
//! of this test only ran long enough to observe one tick) therefore needs a **second** wave of
//! rows, seeded upfront but only claimed once the first wave's entries age out of M2's tracking
//! (freeing the `max_in_flight = 2` capacity this test pins). The lease-renewal tick itself fires
//! on a fixed schedule regardless of *which* row it renews, so the four observations below — two
//! from wave 1, two from wave 2 — form one continuous, evenly spaced cadence.
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{
    DispatcherSettings, InMemoryOutboxStore, OutboxDispatcher, PublishStep, ScriptedPublisher,
};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn hung_outcome_writes_do_not_starve_renewal_across_four_tick_periods() {
    let store = InMemoryOutboxStore::default();
    // Wave 1: claimed immediately. Wave 2: seeded upfront too, but `max_in_flight = 2` (matching
    // wave 1's own row count) holds it back until wave 1 ages out of M2's tracking and frees
    // capacity.
    let completing_1 = store.insert(common::distinct_envelope());
    let failing_1 = store.insert(common::distinct_envelope());
    let completing_2 = store.insert(common::distinct_envelope());
    let failing_2 = store.insert(common::distinct_envelope());
    let publisher = ScriptedPublisher::keyed([
        (completing_1.id, PublishStep::Ok),
        (failing_1.id, PublishStep::Permanent),
        (completing_2.id, PublishStep::Ok),
        (failing_2.id, PublishStep::Permanent),
    ]);

    // `store_timeout` sits just under the `lease / 2` boundary `validate_shared` enforces (S4
    // review 4, major 3) — the tightest gap the config allows.
    let lease = Duration::from_millis(150);
    let tick_period = lease / 2; // 75 ms
    let store_timeout = Duration::from_millis(70);
    assert!(
        store_timeout < lease / 2,
        "test setup must respect the boundary itself"
    );

    // Both `complete` and `fail` share this hang budget (S4 review 5, major) — armed generously
    // so it lasts the whole test, across both waves.
    store.hang_next(1_000_000, Duration::from_secs(3600));

    let settings = DispatcherSettings::default()
        .poll_interval(Duration::from_millis(5))
        .idle_poll_interval(Duration::from_millis(5))
        .lease(lease)
        .publish_timeout(Duration::from_millis(50))
        .store_timeout(store_timeout)
        .max_in_flight(2)
        .batch_size(2);
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .settings(settings)
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // Claim + publish wave 1, then capture its lease set — before any renewal tick has had a
    // chance to fire.
    common::advance_both(&store, Duration::from_millis(10)).await;
    let claimed_until = store
        .record(completing_1.id)
        .expect("row still exists")
        .locked_until
        .expect("a freshly claimed row is leased");

    // Sample the highest `locked_until` across every row just after each of the first four
    // lease-tick periods (75, 150, 225, 300 ms), with a small margin so the tick has had a
    // chance to actually run before each sample.
    let checkpoints_at = [
        tick_period + Duration::from_millis(10),
        2 * tick_period + Duration::from_millis(10),
        3 * tick_period + Duration::from_millis(10),
        4 * tick_period + Duration::from_millis(10),
    ];
    let mut samples = Vec::new();
    let mut elapsed = Duration::from_millis(10); // already advanced above
    for target in checkpoints_at {
        while elapsed < target {
            common::advance_both(&store, Duration::from_millis(5)).await;
            elapsed += Duration::from_millis(5);
        }
        let max_locked_until = [completing_1.id, failing_1.id, completing_2.id, failing_2.id]
            .into_iter()
            .filter_map(|id| store.record(id).and_then(|record| record.locked_until))
            .max()
            .expect("at least one row is still leased at every checkpoint");
        samples.push(max_locked_until);
    }

    // Neither original row's publish ever landed — every `complete` attempt hangs until
    // `store_timeout` for the whole test.
    for id in [completing_1.id, completing_2.id] {
        assert!(
            store
                .record(id)
                .expect("row still exists")
                .published_at
                .is_none(),
            "every complete attempt hung and timed out — none has landed"
        );
    }

    assert!(
        samples[0] > claimed_until,
        "no renewal observed by the first tick period ({tick_period:?}) — starved from the \
         very first tick"
    );
    for window in samples.windows(2) {
        let (before, after) = (window[0], window[1]);
        assert!(
            after > before,
            "locked_until did not advance between consecutive tick-period checkpoints \
             ({before:?} -> {after:?}) — hung outcome writes starved the lease-renewal tick \
             across the wave 1 -> wave 2 handoff"
        );
        let gap_ms = (after - before).whole_milliseconds();
        assert!(
            (50..=100).contains(&gap_ms),
            "consecutive renewals were {gap_ms} ms apart — expected roughly one tick period \
             ({tick_period:?})"
        );
    }

    // Every row is left to its lease on a clean shutdown rather than released — no write was
    // ever classified `Permanent`, so `run()` exits `Ok(())`.
    cancel.cancel();
    let outcome = handle.await.expect("dispatcher task joins");
    assert!(
        outcome.is_ok(),
        "a transiently-timing-out outcome write must not itself end run() with Err — only a \
         Permanent-classified write does that (M1)"
    );
}
