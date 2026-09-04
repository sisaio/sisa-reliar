//! `PurgeReport::is_complete`: `false` when any of the three counts (published deleted, dead
//! deleted, expired swept to dead) hit `batch_size` — the signal the caller uses to repeat
//! `OutboxStore::purge` (phase1-contract.md §3.3, §3.4, ADR 0009).

use reliar_outbox::PurgeReport;

#[test]
fn complete_when_both_deletes_are_under_the_batch_size() {
    let report = PurgeReport::new(3, 4, 0);
    assert!(report.is_complete(10));
}

#[test]
fn incomplete_when_published_deleted_hit_the_batch_size() {
    let report = PurgeReport::new(10, 0, 0);
    assert!(!report.is_complete(10));
}

#[test]
fn incomplete_when_dead_deleted_hit_the_batch_size() {
    let report = PurgeReport::new(0, 10, 0);
    assert!(!report.is_complete(10));
}

#[test]
fn incomplete_when_expired_to_dead_hit_the_batch_size() {
    let report = PurgeReport::new(0, 0, 10);
    assert!(!report.is_complete(10));
}

#[test]
fn an_empty_report_is_always_complete() {
    assert!(PurgeReport::default().is_complete(1));
}
