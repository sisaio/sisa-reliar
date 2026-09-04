//! `ExponentialBackoff`'s invariants, proptested without a database (SRS §23.1, ADR 0009).
//!
//! Permanent → dead immediately, whatever `attempts`; attempts exhausted → dead; otherwise a
//! delay in `[0, max_delay × (1 + jitter)]`, monotonic in `attempts` when jitter is disabled, and
//! never overflowing even at a saturating-huge `attempts`. With jitter enabled: the delay stays
//! in `[capped × (1 - jitter), capped × (1 + jitter)]`, repeated calls are not all equal (the
//! regression this file exists for — a jitter unit that barely moved between calls, review 1
//! blocker 1), and the cap itself is `max_delay × (1 + jitter)`.

use std::time::Duration;

use proptest::prelude::*;
use reliar_outbox::{DeadReason, ExponentialBackoff, FailureKind, FailureOutcome, RetryPolicy};

/// A backoff with jitter disabled, so delays are exactly reproducible for the monotonicity and
/// bound checks.
fn backoff_without_jitter(
    base_millis: u64,
    max_delay_millis: u64,
    max_attempts: u32,
) -> ExponentialBackoff {
    ExponentialBackoff::default()
        .base(Duration::from_millis(base_millis.max(1)))
        .max_delay(Duration::from_millis(max_delay_millis.max(1)))
        .max_attempts(max_attempts.max(1))
        .jitter(0.0)
}

proptest! {
    #[test]
    fn permanent_failures_are_always_dead_regardless_of_attempts(
        base_millis in 1u64..10_000,
        max_delay_millis in 1u64..100_000,
        max_attempts in 1u32..100,
        attempts in 0u32..1_000,
    ) {
        let policy = backoff_without_jitter(base_millis, max_delay_millis, max_attempts);
        let outcome = policy.next(attempts, FailureKind::Permanent);
        prop_assert_eq!(outcome, FailureOutcome::Dead { reason: DeadReason::PermanentError });
    }

    #[test]
    fn attempts_exhausted_goes_dead_and_never_earlier(
        base_millis in 1u64..10_000,
        max_delay_millis in 1u64..100_000,
        max_attempts in 1u32..50,
        attempts in 0u32..200,
    ) {
        let policy = backoff_without_jitter(base_millis, max_delay_millis, max_attempts);
        let outcome = policy.next(attempts, FailureKind::Transient);

        if attempts.saturating_add(1) >= max_attempts {
            prop_assert_eq!(outcome, FailureOutcome::Dead { reason: DeadReason::AttemptsExhausted });
        } else {
            let is_retry = matches!(outcome, FailureOutcome::Retry { .. });
            prop_assert!(is_retry);
        }
    }

    #[test]
    fn retry_delay_is_bounded_and_never_zero(
        base_millis in 1u64..10_000,
        max_delay_millis in 1u64..100_000,
        max_attempts in 2u32..50,
        attempts in 0u32..49,
    ) {
        let policy = backoff_without_jitter(base_millis, max_delay_millis, max_attempts);
        // Stay strictly below the exhaustion boundary so the outcome is always `Retry`.
        prop_assume!(attempts.saturating_add(1) < max_attempts);

        let FailureOutcome::Retry { delay } = policy.next(attempts, FailureKind::Transient) else {
            unreachable!("attempts is below the exhaustion boundary by construction");
        };

        prop_assert!(delay > Duration::ZERO);
        prop_assert!(delay <= Duration::from_millis(max_delay_millis));
    }

    #[test]
    fn retry_delay_is_monotonic_in_attempts_without_jitter(
        base_millis in 1u64..10_000,
        max_delay_millis in 1u64..100_000,
        max_attempts in 3u32..50,
        attempts in 0u32..47,
    ) {
        prop_assume!(attempts.saturating_add(2) < max_attempts);
        let policy = backoff_without_jitter(base_millis, max_delay_millis, max_attempts);

        let FailureOutcome::Retry { delay: earlier } = policy.next(attempts, FailureKind::Transient) else {
            unreachable!("attempts is below the exhaustion boundary by construction");
        };
        let FailureOutcome::Retry { delay: later } = policy.next(attempts + 1, FailureKind::Transient) else {
            unreachable!("attempts + 1 is below the exhaustion boundary by construction");
        };

        prop_assert!(later >= earlier);
    }

    #[test]
    fn a_huge_attempts_count_saturates_at_the_cap_instead_of_overflowing(
        base_millis in 1u64..10_000,
        max_delay_millis in 1u64..100_000,
    ) {
        let policy = backoff_without_jitter(base_millis, max_delay_millis, u32::MAX);
        let FailureOutcome::Retry { delay } = policy.next(u32::MAX - 2, FailureKind::Transient) else {
            unreachable!("max_attempts is u32::MAX, so no attempts count exhausts it here");
        };

        prop_assert_eq!(delay, Duration::from_millis(max_delay_millis));
    }

    #[test]
    fn jittered_delay_stays_within_capped_times_one_minus_and_one_plus_jitter(
        base_millis in 1u64..10_000,
        max_delay_millis in 1u64..100_000,
        jitter in 0.01f64..1.0,
    ) {
        let policy = ExponentialBackoff::default()
            .base(Duration::from_millis(base_millis))
            .max_delay(Duration::from_millis(max_delay_millis))
            .max_attempts(u32::MAX)
            .jitter(jitter);
        let capped = Duration::from_millis(base_millis).min(Duration::from_millis(max_delay_millis));
        let lower = capped.mul_f64(1.0 - jitter);
        let upper = capped.mul_f64(1.0 + jitter);

        // Repeat: the entropy source is fresh per call, so one sample could land near a bound by
        // chance — the bound must hold on every one of them, not just on average.
        for _ in 0..16 {
            let FailureOutcome::Retry { delay } = policy.next(0, FailureKind::Transient) else {
                unreachable!("attempts=0 with max_attempts=u32::MAX is always Retry");
            };
            // `+ 1ns` absorbs `mul_f64`'s own floating-point rounding, not the policy's.
            prop_assert!(delay + Duration::from_nanos(1) >= lower);
            prop_assert!(delay <= upper + Duration::from_nanos(1));
        }
    }

    #[test]
    fn jitter_varies_the_delay_across_repeated_calls(
        base_millis in 100u64..10_000,
        max_delay_millis in 100_000u64..1_000_000,
    ) {
        let policy = ExponentialBackoff::default()
            .base(Duration::from_millis(base_millis))
            .max_delay(Duration::from_millis(max_delay_millis))
            .max_attempts(u32::MAX)
            .jitter(0.2);

        let mut delays = std::collections::HashSet::new();
        for _ in 0..32 {
            let FailureOutcome::Retry { delay } = policy.next(0, FailureKind::Transient) else {
                unreachable!("attempts=0 with max_attempts=u32::MAX is always Retry");
            };
            delays.insert(delay);
        }
        prop_assert!(
            delays.len() > 1,
            "32 calls produced only {} distinct delay(s) — jitter looks constant",
            delays.len()
        );
    }

    #[test]
    fn jittered_delay_lands_both_below_and_above_capped_over_many_samples(
        base_millis in 1_000u64..10_000,
        max_delay_millis in 100_000u64..1_000_000,
    ) {
        // Fixed, generous jitter and a large sample: a source biased into half (or any other
        // strict sub-range) of `[1-j, 1+j]` must be caught here, not just "more than one value"
        // (review 2 blocker 2 — the old assertions passed even on a biased entropy source).
        let policy = ExponentialBackoff::default()
            .base(Duration::from_millis(base_millis))
            .max_delay(Duration::from_millis(max_delay_millis))
            .max_attempts(u32::MAX)
            .jitter(0.3);
        let capped = Duration::from_millis(base_millis).min(Duration::from_millis(max_delay_millis));

        let mut saw_below = false;
        let mut saw_above = false;
        for _ in 0..200 {
            let FailureOutcome::Retry { delay } = policy.next(0, FailureKind::Transient) else {
                unreachable!("attempts=0 with max_attempts=u32::MAX is always Retry");
            };
            saw_below |= delay < capped;
            saw_above |= delay > capped;
        }

        prop_assert!(saw_below, "200 samples never landed below the unjittered `capped` delay");
        prop_assert!(saw_above, "200 samples never landed above the unjittered `capped` delay");
    }

    #[test]
    fn jittered_delay_mean_is_close_to_capped(
        base_millis in 1_000u64..10_000,
        max_delay_millis in 100_000u64..1_000_000,
    ) {
        // A symmetric jitter factor in `[1-j, 1+j]` averages to 1 over enough samples, so the
        // mean delay should land near `capped` itself. A source stuck in, say, `[0.5, 0.75]` of
        // that range would instead average well below `capped` and fail this bound.
        let policy = ExponentialBackoff::default()
            .base(Duration::from_millis(base_millis))
            .max_delay(Duration::from_millis(max_delay_millis))
            .max_attempts(u32::MAX)
            .jitter(0.3);
        let capped = Duration::from_millis(base_millis).min(Duration::from_millis(max_delay_millis));

        let samples = 200u32;
        let mut total = Duration::ZERO;
        for _ in 0..samples {
            let FailureOutcome::Retry { delay } = policy.next(0, FailureKind::Transient) else {
                unreachable!("attempts=0 with max_attempts=u32::MAX is always Retry");
            };
            total += delay;
        }
        let mean = total / samples;

        // Generous tolerance (±10% of `capped`) — this is a distribution-shape smoke test, not a
        // precise statistical bound; it only needs to reject "mean stuck near one edge."
        let tolerance = capped.mul_f64(0.10);
        let diff = mean.abs_diff(capped);
        prop_assert!(
            diff <= tolerance,
            "mean delay {mean:?} is not within {tolerance:?} of capped {capped:?}"
        );
    }

    #[test]
    fn the_jitter_cap_is_max_delay_times_one_plus_jitter(
        max_delay_millis in 1u64..100_000,
        jitter in 0.01f64..1.0,
    ) {
        // `base` is deliberately far above `max_delay` so the pre-jitter value is always capped.
        let policy = ExponentialBackoff::default()
            .base(Duration::from_millis(max_delay_millis.saturating_add(1).saturating_mul(1_000)))
            .max_delay(Duration::from_millis(max_delay_millis))
            .max_attempts(u32::MAX)
            .jitter(jitter);
        let cap = Duration::from_millis(max_delay_millis).mul_f64(1.0 + jitter);

        for _ in 0..16 {
            let FailureOutcome::Retry { delay } = policy.next(0, FailureKind::Transient) else {
                unreachable!("attempts=0 with max_attempts=u32::MAX is always Retry");
            };
            prop_assert!(delay <= cap + Duration::from_nanos(1));
        }
    }
}
