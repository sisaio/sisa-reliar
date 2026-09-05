//! `Publisher::publish_batch`'s default implementation loops over `publish` in order and does
//! not stop at the first failure — every envelope gets its own positional result, so a partial
//! batch failure never loses a per-message verdict (contract §2.8). Moved here from
//! `reliar-outbox` when `Publisher`/`Classify`/`FailureKind` moved to `reliar-core` (ADR 0032).

mod common;

use std::fmt;
use std::sync::Mutex;

use reliar_core::{Classify, FailureKind, Publisher, SerializedEnvelope};

#[derive(Debug)]
struct FakeError;

impl fmt::Display for FakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("fake publish failure")
    }
}

impl std::error::Error for FakeError {}

impl Classify for FakeError {
    fn kind(&self) -> FailureKind {
        FailureKind::Transient
    }
}

/// Fails the message at the given zero-based positions, succeeds everywhere else, and records
/// call order.
#[derive(Default)]
struct PositionalPublisher {
    fail_at: Vec<usize>,
    calls: Mutex<Vec<usize>>,
}

impl Publisher for PositionalPublisher {
    type Error = FakeError;

    // A plain `fn` returning `impl Future`, not `async fn`: the body is synchronous (recording a
    // call and returning), so there is no `.await` point to make it worth suspending on.
    fn publish(
        &self,
        _envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let position = {
            let mut calls = self
                .calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let position = calls.len();
            calls.push(position);
            position
        };
        std::future::ready(if self.fail_at.contains(&position) {
            Err(FakeError)
        } else {
            Ok(())
        })
    }
}

#[tokio::test]
async fn every_envelope_is_attempted_in_order_even_after_a_failure() {
    let envelopes: Vec<SerializedEnvelope> =
        (0..4).map(|_| common::serialized_envelope()).collect();
    let publisher = PositionalPublisher {
        fail_at: vec![1],
        ..Default::default()
    };

    let results = publisher.publish_batch(&envelopes).await;

    assert_eq!(results.len(), 4);
    assert!(results[0].is_ok());
    assert!(results[1].is_err());
    assert!(results[2].is_ok(), "a failure must not stop the loop");
    assert!(results[3].is_ok());
    assert_eq!(*publisher.calls.lock().unwrap(), vec![0, 1, 2, 3]);
}

#[tokio::test]
async fn an_empty_batch_publishes_nothing_and_returns_no_results() {
    let publisher = PositionalPublisher::default();

    let results = publisher.publish_batch(&[]).await;

    assert!(results.is_empty());
    assert!(publisher.calls.lock().unwrap().is_empty());
}
