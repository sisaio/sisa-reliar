//! `PurgeRequest::default()` and `DeadQuery::default()` are **hand-written**, never derived: a
//! derived `Default` would give `PurgeRequest { batch_size: 0, .. }` (a purge that deletes
//! nothing and reports success) and `DeadQuery { limit: 0, .. }` (a query that returns nothing)
//! (phase1-contract.md §3.3, review 2 blocker 1).

use std::time::Duration;

use reliar_outbox::{DeadQuery, PurgeRequest};

#[test]
fn purge_request_default_deletes_published_rows_after_seven_days() {
    let request = PurgeRequest::default();

    assert_eq!(
        request.published_retention,
        Some(Duration::from_secs(7 * 24 * 60 * 60))
    );
    assert_eq!(request.dead_retention, None);
    assert_eq!(request.batch_size, 1_000);
}

#[test]
fn dead_query_default_returns_the_first_page_unfiltered() {
    let query = DeadQuery::default();

    assert_eq!(query.message_type, None);
    assert_eq!(query.tenant_id, None);
    assert_eq!(query.dead_before, None);
    assert_eq!(query.limit, 100);
    assert_eq!(query.after_sequence, None);
}
