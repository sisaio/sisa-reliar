//! `OutboxStats::lag`'s three branches: no claimable backlog (`None`), the normal case (`as_of -
//! oldest_pending_available_at`), and a negative diff clamped to zero rather than underflowing
//! (phase1-contract.md §3.3).

use std::time::Duration;

use reliar_outbox::OutboxStats;

#[test]
fn lag_is_none_when_there_is_no_oldest_pending_row() {
    let stats = OutboxStats::new(0, 0, 0, None, time::OffsetDateTime::now_utc());
    assert_eq!(stats.lag(), None);
}

#[test]
fn lag_is_the_gap_between_as_of_and_the_oldest_pending_row() {
    let as_of = time::OffsetDateTime::now_utc();
    let oldest = as_of - time::Duration::seconds(30);
    let stats = OutboxStats::new(5, 0, 0, Some(oldest), as_of);

    assert_eq!(stats.lag(), Some(Duration::from_secs(30)));
}

#[test]
fn lag_clamps_a_negative_diff_to_zero_instead_of_underflowing() {
    // `as_of` before `oldest_pending_available_at` should not happen in practice (both come from
    // the same `stats()` query), but `lag` must not panic or wrap if it ever does.
    let as_of = time::OffsetDateTime::now_utc();
    let oldest = as_of + time::Duration::seconds(5);
    let stats = OutboxStats::new(1, 0, 0, Some(oldest), as_of);

    assert_eq!(stats.lag(), Some(Duration::ZERO));
}
