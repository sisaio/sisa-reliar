---
name: rust-lib-testing
description: >-
  Reliar's testing convention for library crates - no inline cfg(test) modules in src/, every crate
  tests its PUBLIC API from tests/ with shared helpers in tests/common/, in-memory fakes
  (InMemoryOutboxStore, RecordingPublisher, ScriptedPublisher for transient/permanent failures, a
  controllable clock) shipped behind a test-support feature so provider crates and examples reuse
  them, tokio::test(start_paused = true) + tokio::time::advance for backoff/idle/lease timing,
  cancellation and bounded-concurrency tests, doctests on public items, proptest for pure logic
  (backoff bounds, header validation), criterion benches under benches/, and the reviewer's
  "with all green, what's still broken?" checklist. Use when writing or reviewing tests, fakes,
  benches, or the test layout of any crate.
metadata:
  audience: ENGINEER, REVIEWER, ARCHITECT
---

# Testing library crates (public API, fakes, paused time)

SRS §8: **no `#[cfg(test)] mod tests` in `src/`**. Consequence: tests can only reach the public
API — which is the point (they double as API usability checks). If something needs testing but isn't
public, either it's a pure fn worth exposing in a `pub(crate)`-free way, or it's exercised through
the public path that uses it. Doctests are allowed and encouraged on public items.

## Layout

```
crates/reliar-outbox/
├── src/…                     # production only
├── src/test_support.rs       # fakes, behind `feature = "test-support"` (pub, documented, no test-only deps leak by default)
└── tests/
    ├── common/mod.rs         # #![allow(dead_code)]; builders for envelopes/records/configs
    ├── dispatcher_publishes_claimed_batch.rs
    ├── dispatcher_retries_transient_then_dead.rs
    ├── dispatcher_permanent_error_is_dead_immediately.rs
    ├── dispatcher_respects_concurrency_limit.rs
    ├── dispatcher_cancellation_drains_in_flight.rs
    ├── backoff_policy.rs
    └── headers_reject_reserved_prefix.rs
```

One scenario per file, named as the claim it proves. Each file starts with `mod common;`.

## Fakes — `test-support` feature (reused by providers, examples, benches)

```rust
/// Records every published envelope id in order; never fails.
#[derive(Clone, Default)] pub struct RecordingPublisher { seen: Arc<Mutex<Vec<MessageId>>> }
impl Publisher for RecordingPublisher { type Error = PublishError;
    fn publish(&self, e: &SerializedEnvelope) -> impl Future<Output = Result<(), PublishError>> + Send {
        self.seen.lock().unwrap().push(e.id); async { Ok(()) } } }

/// Pops the next scripted outcome per publish; lets a test say "transient, transient, ok" or "permanent".
#[derive(Clone)] pub struct ScriptedPublisher { script: Arc<Mutex<VecDeque<Result<(), PublishError>>>> }

/// In-memory OutboxStore with the same lease/attempt semantics the SQL implements (kept in sync via the shared matrix).
#[derive(Clone, Default)] pub struct InMemoryOutboxStore { rows: Arc<Mutex<Vec<OutboxRecord>>>, now: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync> }
```

The in-memory store's `now` is injectable so lease expiry is testable deterministically. Fakes are
part of the public surface (documented, `#[doc(cfg(feature = "test-support"))]`) — keep them small.

## Time — paused Tokio clock

```rust
#[tokio::test(start_paused = true)]
async fn transient_failures_back_off_exponentially() {
    let store = InMemoryOutboxStore::default(); store.seed(one_record());
    let publisher = ScriptedPublisher::new([transient(), transient(), Ok(())]);
    let d = OutboxDispatcher::new(store.clone(), publisher.clone(), cfg(base = 100ms, max_attempts = 5));
    let cancel = CancellationToken::new(); let h = tokio::spawn(d.run(cancel.clone()));

    tokio::time::advance(Duration::from_millis(50)).await;  assert_eq!(publisher.calls(), 1);
    tokio::time::advance(Duration::from_millis(100)).await; assert_eq!(publisher.calls(), 2); // 100ms backoff
    tokio::time::advance(Duration::from_millis(200)).await; assert_eq!(publisher.calls(), 3); // 200ms backoff
    assert!(store.record(id).published_at.is_some());
    cancel.cancel(); h.await.unwrap().unwrap();
}
```

With `start_paused`, `sleep` completes only when you `advance` (or when every task is idle, which
auto-advances). Turn jitter off in tests via config (`jitter: Jitter::None`) or assert bounds.
Never `std::thread::sleep`, never wall-clock timeouts as the assertion.

## Cancellation & concurrency

- **Cancellation drains:** script the publisher to block on a `Notify`; cancel while N publishes are
  in flight; release them; assert all N outcomes were persisted and `run` returned `Ok` within the
  drain timeout; no record left `locked_by` the dead worker.
- **Bounded concurrency:** a publisher that counts concurrent calls (`AtomicUsize` high-water mark)
  proves `max_in_flight` is never exceeded.
- **Multiple dispatchers, one store:** two dispatchers on one `InMemoryOutboxStore` publish each
  record exactly once absent failures (real `SKIP LOCKED` proof lives in the provider — skill `testcontainers`).

## Pure logic — proptest

`backoff_policy.rs`: for any `attempts` ≤ `max_attempts`, `delay ∈ [0, max_delay]`, monotone
non-decreasing without jitter. `headers_reject_reserved_prefix.rs`: any key matching
`(?i)^reliar-` is rejected, any other key accepted; round-trips through JSON.

## Doctests

Public constructors and the dispatcher get a compiling example (`no_run` when it needs a DB) — they
are the first thing crates.io readers see and they break when the API drifts.

## Benches — `benches/` (criterion), never in production crates

`benches/serialization` (envelope → bytes → envelope), `benches/outbox-throughput` (dispatcher over
`InMemoryOutboxStore` + `RecordingPublisher`, messages/sec vs batch size and concurrency). Record the
baseline number in the card when a hot path changes.

## Reviewer checklist — "with all green, what's still broken?"

- Does a test fail if the `locked_by` guard is removed? If backoff becomes constant? If permanent
  errors are retried? If cancellation drops in-flight outcomes? If a `reliar-` header is accepted?
- Are assertions on **outcomes** (store state, publisher observations), not on the fake's internals?
- Any wall-clock dependence, shared static state, or order dependence between tests?
- Is the pure logic tested fast here, not only through Postgres?

## Definition of done (tests)

- [ ] No `#[cfg(test)]` in `src/`; scenarios in `tests/`, one claim per file; helpers in `tests/common/`.
- [ ] Fakes behind `test-support`, documented, reused by providers/examples/benches.
- [ ] Timing via paused clock; determinism (no sleeps/wall clock); concurrency and cancellation covered.
- [ ] Pure logic property-tested; public API has doctests; hot paths benched.
