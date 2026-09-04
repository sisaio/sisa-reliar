//! `acquire` claims `ORDER BY available_at, sequence` — the same order the SQL claim uses (review
//! 1, B1). A row inserted first but scheduled later must still lose to a row inserted second but
//! due sooner: this regression fails if the claim ever sorts by `sequence` alone.

#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{AcquireRequest, InMemoryOutboxStore, OutboxStore, WorkerId};

#[tokio::test]
async fn a_later_available_at_loses_to_an_earlier_one_despite_a_lower_sequence() {
    let store = InMemoryOutboxStore::default();

    // sequence 1, but not due until +10s.
    let later = store.insert_with(
        common::serialized_envelope(),
        time::OffsetDateTime::UNIX_EPOCH + Duration::from_secs(10),
        None,
    );
    // sequence 2, due immediately.
    let sooner = store.insert(common::serialized_envelope());

    store.advance(Duration::from_secs(10));

    let batch = store
        .acquire(AcquireRequest::new(WorkerId::generate()).batch_size(1))
        .await
        .expect("acquire succeeds");

    assert_eq!(batch.records.len(), 1);
    assert_eq!(
        batch.records[0].envelope.id, sooner.id,
        "the due-sooner row (sequence 2) must be claimed before the later-scheduled one \
         (sequence 1) — sorting by sequence alone would return the wrong row here"
    );
    assert_ne!(batch.records[0].envelope.id, later.id);
}
