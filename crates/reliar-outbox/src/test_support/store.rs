//! [`InMemoryOutboxStore`]: an in-memory [`OutboxStore`]/[`OutboxDeadLetters`] with the same
//! lease/attempt/dead-letter semantics `reliar-store-postgres` implements in SQL (§43.A.27).

use core::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use reliar_core::{Classify, FailureKind, MessageId, SerializedEnvelope};
use time::OffsetDateTime;

use crate::enqueue::OutboxEnqueue;
use crate::record::{OutboxRecord, truncate_error};
use crate::store::{
    AcquireRequest, AcquiredBatch, CompletedMessage, DeadLetterPage, DeadQuery, DeadReason,
    FailedMessage, FailureOutcome, MessageRef, OutboxDeadLetters, OutboxStats, OutboxStore,
    PurgeReport, PurgeRequest,
};
use crate::worker::WorkerId;

/// [`OutboxRecord::last_error`] left by [`InMemoryOutboxStore::purge`]'s expiry sweep (§12.2,
/// M1) — the only place this fake supplies its own error text rather than one a caller passed
/// in.
const EXPIRED_ERROR_MESSAGE: &str = "reliar: expired before publication";

/// Full in-memory [`OutboxStore`] + [`OutboxDeadLetters`]. Keeps its own instant
/// ([`Self::advance`]) so lease expiry and `available_at` can be driven forward without a
/// database — the fake's substitute for SQL time-travel.
///
/// The clock starts at [`OffsetDateTime::UNIX_EPOCH`], not the wall clock: every test using this
/// store is therefore deterministic and independent of when it runs. `sequence` is assigned
/// monotonically per [`Self::insert`]/[`Self::insert_with`] call, exactly as a provider would
/// assign it.
///
/// This store implements only [`crate::Ordering::Unordered`] — the only strategy [`crate::Ordering::validate`]
/// accepts in this release, so [`AcquireRequest::ordering`] is read but never changes the claim.
///
/// **Every mutation happens eagerly, at call time — not lazily on first poll of the returned
/// future.** Every [`OutboxStore`]/[`OutboxDeadLetters`] method here locks and mutates its rows
/// (and, on the `OutboxStore` side, consumes one unit of [`Self::fail_next`]) *before* building
/// the `std::future::ready(..)` it returns, so a caller that constructs but never awaits the
/// future still sees every effect applied. A real provider cannot offer this: its future's first
/// poll is what sends the SQL, so an unpolled future does nothing at all. This asymmetry is
/// deliberate — the fake would otherwise need genuinely async, poll-driven state machinery to
/// match a database it is not — and is safe because nothing in this crate ever constructs a
/// store future without immediately awaiting it. [`RecordingPublisher`](crate::RecordingPublisher)
/// and [`ScriptedPublisher`](crate::ScriptedPublisher) are the opposite: their side effects wait
/// for the first poll, matching how a real transport's call only starts once awaited.
#[derive(Clone, Debug, Default)]
pub struct InMemoryOutboxStore {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug)]
struct Inner {
    rows: Vec<OutboxRecord>,
    next_sequence: i64,
    now: OffsetDateTime,
    fail_next: usize,
    fail_next_permanent: usize,
    hang_next: usize,
    hang_duration: Duration,
    complete_calls: usize,
    fail_next_complete: usize,
    fail_next_enqueue: usize,
    enqueue_calls: usize,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            rows: Vec::new(),
            next_sequence: 1,
            now: OffsetDateTime::UNIX_EPOCH,
            fail_next: 0,
            fail_next_permanent: 0,
            hang_next: 0,
            hang_duration: Duration::ZERO,
            complete_calls: 0,
            fail_next_complete: 0,
            fail_next_enqueue: 0,
            enqueue_calls: 0,
        }
    }
}

impl InMemoryOutboxStore {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Seeds a pending row, immediately claimable, and returns its [`MessageRef`]. The fake
    /// assigns `sequence` and `created_at` exactly as a provider would.
    pub fn insert(&self, envelope: reliar_core::SerializedEnvelope) -> MessageRef {
        self.insert_row(envelope, None, None)
    }

    /// Seeds a row with an explicit `available_at` and `ordering_key`.
    pub fn insert_with(
        &self,
        envelope: reliar_core::SerializedEnvelope,
        available_at: OffsetDateTime,
        ordering_key: Option<String>,
    ) -> MessageRef {
        self.insert_row(envelope, Some(available_at), ordering_key)
    }

    fn insert_row(
        &self,
        envelope: reliar_core::SerializedEnvelope,
        available_at: Option<OffsetDateTime>,
        ordering_key: Option<String>,
    ) -> MessageRef {
        let mut inner = self.lock();
        let sequence = inner.next_sequence;
        inner.next_sequence += 1;
        let created_at = inner.now;
        let id = envelope.id;

        let record = OutboxRecord::builder(envelope, sequence, created_at)
            .ordering_key(ordering_key)
            .available_at(available_at.unwrap_or(created_at))
            .build();
        inner.rows.push(record);

        MessageRef::new(id, created_at)
    }

    /// Moves the fake's notion of "now" forward — expires leases and makes retried rows due. The
    /// store-side counterpart of `tokio::time::advance`.
    pub fn advance(&self, by: Duration) {
        self.lock().now += by;
    }

    /// Full row inspection for assertions, ordered by `sequence`.
    #[must_use]
    pub fn records(&self) -> Vec<OutboxRecord> {
        let mut rows = self.lock().rows.clone();
        rows.sort_by_key(|row| row.sequence);
        rows
    }

    /// The row for one message id, if it was inserted.
    #[must_use]
    pub fn record(&self, id: MessageId) -> Option<OutboxRecord> {
        self.lock()
            .rows
            .iter()
            .find(|row| row.envelope.id == id)
            .cloned()
    }

    /// Makes the next `n` calls to any [`OutboxStore`] method fail with
    /// [`InMemoryStoreError::Injected`] (`Transient`) — drives the "`run` survives a store
    /// error" test (§43.A.18). Never affects [`OutboxDeadLetters`] methods, which this fake
    /// never fails.
    pub fn fail_next(&self, n: usize) {
        self.lock().fail_next = n;
    }

    /// Makes the next `n` calls to any [`OutboxStore`] method fail with
    /// [`InMemoryStoreError::InjectedPermanent`] (`Permanent`) — drives the test proving `run`
    /// drains best-effort and returns `Err(DispatchError::Store(_))` (S4 review, major 6).
    /// Checked before [`Self::fail_next`] when both are pending.
    pub fn fail_next_permanent(&self, n: usize) {
        self.lock().fail_next_permanent = n;
    }

    /// Makes the next `n` calls to [`OutboxStore::complete`] **or** [`OutboxStore::fail`] sleep
    /// for `duration` (on the paused clock) **before** the mutation applies, rather than after —
    /// a caller that times out the call (as the dispatcher's own bounded-store-call helper does)
    /// drops the future mid-sleep, so the write never lands at all. Drives the test proving
    /// `store_timeout` bounds a hung outcome write so drain still finishes within
    /// `drain_timeout`, and the "publish succeeded but complete never landed" row is left to its
    /// lease (S4 review 3, K4/L3). One shared budget, not two: arming `n = 2` with one row's
    /// `complete` and another's `fail` both unwritten at once lets a single retry round consume
    /// both units — the same round `retry_unwritten_outcomes` occupies for up to `2 *
    /// store_timeout` — proving lease renewal is never starved even then (S4 review 5, major).
    ///
    /// This is the one deliberate exception to this fake's documented eager-mutation guarantee
    /// (§ above): only while a hang is armed, the mutation moves from call time to after the
    /// sleep resolves, because the whole point is to let a caller's timeout drop the future
    /// before the mutation ever runs.
    pub fn hang_next(&self, n: usize, duration: Duration) {
        let mut inner = self.lock();
        inner.hang_next = n;
        inner.hang_duration = duration;
    }

    /// How many times `complete`'s mutation logic has actually run — every call,
    /// whether it went on to fail an injected failure or succeed. Drives the test proving a
    /// fast-failing `complete` cannot retry faster than `outcome_retry_interval` allows (S4
    /// review 5, blocker): a CPU-speed spin would run this into the tens of thousands within a
    /// few seconds of virtual time; a properly paced retry stays in the single digits.
    #[must_use]
    pub fn complete_call_count(&self) -> usize {
        self.lock().complete_calls
    }

    /// Makes the next `n` calls to [`OutboxStore::complete`] specifically fail immediately
    /// (`InMemoryStoreError::Injected`, `Transient`) — unlike [`Self::fail_next`] (shared across
    /// every `OutboxStore` method, including `acquire`), this only ever affects `complete`, so a
    /// test can arm it from the very start without starving the initial claim. Drives the test
    /// proving a *fast*-failing (never hung) `complete` still cannot retry faster than
    /// `outcome_retry_interval` allows (S4 review 5, blocker).
    pub fn fail_next_complete(&self, n: usize) {
        self.lock().fail_next_complete = n;
    }

    /// Makes the next `n` calls to [`OutboxEnqueue::enqueue`] fail with
    /// [`InMemoryStoreError::Injected`] (`Transient`) — drives
    /// [`crate::OutboxPublisher::enqueue`]'s error path (SRS §43.D) without touching any other
    /// `OutboxStore` method.
    pub fn fail_next_enqueue(&self, n: usize) {
        self.lock().fail_next_enqueue = n;
    }

    /// How many times [`OutboxEnqueue::enqueue`] has been called, whether it went on to fail an
    /// injected failure or succeed.
    #[must_use]
    pub fn enqueue_call_count(&self) -> usize {
        self.lock().enqueue_calls
    }

    /// Consumes one unit of an armed enqueue-specific failure, if any is pending.
    fn take_fail_next_enqueue(&self) -> bool {
        let mut inner = self.lock();
        if inner.fail_next_enqueue > 0 {
            inner.fail_next_enqueue -= 1;
            true
        } else {
            false
        }
    }

    fn try_enqueue(&self, envelope: &SerializedEnvelope) -> Result<MessageId, InMemoryStoreError> {
        self.lock().enqueue_calls += 1;
        if self.take_fail_next_enqueue() {
            return Err(InMemoryStoreError::Injected);
        }
        let message_ref = self.insert(envelope.clone());
        Ok(message_ref.id)
    }

    /// Consumes one unit of an armed complete-specific fast failure, if any is pending.
    fn take_fail_next_complete(&self) -> bool {
        let mut inner = self.lock();
        if inner.fail_next_complete > 0 {
            inner.fail_next_complete -= 1;
            true
        } else {
            false
        }
    }

    /// Consumes one unit of an armed hang, if any is pending.
    fn take_hang(&self) -> Option<Duration> {
        let mut inner = self.lock();
        if inner.hang_next > 0 {
            inner.hang_next -= 1;
            Some(inner.hang_duration)
        } else {
            None
        }
    }

    /// Consumes one unit of an injected failure, if any is pending. Permanent failures are
    /// checked first: a test setting both up front should see the permanent one exhaust before
    /// the transient one, matching the order the fields are documented in.
    fn take_injected_failure(&self) -> Option<InMemoryStoreError> {
        let mut inner = self.lock();
        if inner.fail_next_permanent > 0 {
            inner.fail_next_permanent -= 1;
            Some(InMemoryStoreError::InjectedPermanent)
        } else if inner.fail_next > 0 {
            inner.fail_next -= 1;
            Some(InMemoryStoreError::Injected)
        } else {
            None
        }
    }

    /// Whether `row` is due, unlocked (or lease-expired) and unexpired at `now` — the claim
    /// predicate every other query (stats, purge) also uses. The lease boundary is **strict**
    /// (`locked_until < now`), matching the SQL claim's `locked_until IS NULL OR locked_until <
    /// now()`: a lease is held through the instant it names, not merely up to it.
    fn is_claimable(row: &OutboxRecord, now: OffsetDateTime) -> bool {
        row.published_at.is_none()
            && row.dead_at.is_none()
            && row.available_at <= now
            && Self::lease_is_free(row, now)
            && row
                .envelope
                .metadata
                .delivery
                .expires_at
                .is_none_or(|expires_at| expires_at > now)
    }

    /// `true` when `row` holds no lease, or its lease has strictly expired — the same boundary
    /// [`Self::is_claimable`] uses, shared with the expiry sweep in [`Self::try_purge`] (ADR
    /// 0009, ruling G2): a row is swept to dead only if it is *also* unowned, since one that
    /// expired mid-flight still belongs to the worker holding it until that lease lapses.
    fn lease_is_free(row: &OutboxRecord, now: OffsetDateTime) -> bool {
        row.locked_until.is_none_or(|until| until < now)
    }

    /// The row this worker still owns for `message`, if any — the guard every state-changing
    /// [`OutboxStore`] method (other than `acquire`) applies.
    fn find_owned_mut<'rows>(
        rows: &'rows mut [OutboxRecord],
        worker: &WorkerId,
        message: MessageRef,
    ) -> Option<&'rows mut OutboxRecord> {
        rows.iter_mut().find(|row| {
            row.envelope.id == message.id
                && row.created_at == message.created_at
                && row.locked_by.as_ref() == Some(worker)
        })
    }

    fn try_acquire(&self, request: &AcquireRequest) -> Result<AcquiredBatch, InMemoryStoreError> {
        if let Some(err) = self.take_injected_failure() {
            return Err(err);
        }
        let mut inner = self.lock();
        let now = inner.now;
        let locked_until = now + request.lease;
        let batch_size = request.batch_size as usize;

        let mut claimable: Vec<usize> = inner
            .rows
            .iter()
            .enumerate()
            .filter(|(_, row)| Self::is_claimable(row, now))
            .map(|(index, _)| index)
            .collect();
        // Same order as the SQL claim: `ORDER BY available_at, sequence` — due-soonest first,
        // ties broken by insertion order.
        claimable
            .sort_by_key(|&index| (inner.rows[index].available_at, inner.rows[index].sequence));
        claimable.truncate(batch_size);

        let mut records = Vec::with_capacity(claimable.len());
        for index in claimable {
            let row = &mut inner.rows[index];
            row.locked_by = Some(request.worker.clone());
            row.locked_until = Some(locked_until);
            records.push(row.clone());
        }

        Ok(AcquiredBatch::new(records, Vec::new()))
    }

    fn try_complete(
        &self,
        worker: &WorkerId,
        items: &[CompletedMessage],
    ) -> Result<u64, InMemoryStoreError> {
        self.lock().complete_calls += 1;
        if self.take_fail_next_complete() {
            return Err(InMemoryStoreError::Injected);
        }
        if let Some(err) = self.take_injected_failure() {
            return Err(err);
        }
        let mut inner = self.lock();
        let now = inner.now;
        let mut affected = 0u64;
        for item in items {
            if let Some(row) = Self::find_owned_mut(&mut inner.rows, worker, item.message) {
                row.published_at = Some(now);
                row.attempts = row.attempts.saturating_add(1);
                row.locked_by = None;
                row.locked_until = None;
                affected += 1;
            }
        }
        Ok(affected)
    }

    fn try_fail(
        &self,
        worker: &WorkerId,
        items: &[FailedMessage],
    ) -> Result<u64, InMemoryStoreError> {
        if let Some(err) = self.take_injected_failure() {
            return Err(err);
        }
        let mut inner = self.lock();
        let now = inner.now;
        let mut affected = 0u64;
        for item in items {
            if let Some(row) = Self::find_owned_mut(&mut inner.rows, worker, item.message) {
                row.attempts = row.attempts.saturating_add(1);
                row.last_error = Some(truncate_error(item.error.clone()));
                match &item.outcome {
                    FailureOutcome::Retry { delay } => {
                        row.available_at = now + *delay;
                        row.locked_by = None;
                        row.locked_until = None;
                    }
                    FailureOutcome::Dead { reason } => {
                        row.dead_at = Some(now);
                        row.dead_reason = Some(*reason);
                        row.locked_by = None;
                        row.locked_until = None;
                    }
                }
                affected += 1;
            }
        }
        Ok(affected)
    }

    fn try_release(
        &self,
        worker: &WorkerId,
        items: &[MessageRef],
    ) -> Result<u64, InMemoryStoreError> {
        if let Some(err) = self.take_injected_failure() {
            return Err(err);
        }
        let mut inner = self.lock();
        let mut affected = 0u64;
        for message in items {
            if let Some(row) = Self::find_owned_mut(&mut inner.rows, worker, *message) {
                row.locked_by = None;
                row.locked_until = None;
                affected += 1;
            }
        }
        Ok(affected)
    }

    fn try_extend_lease(
        &self,
        worker: &WorkerId,
        items: &[MessageRef],
        lease: Duration,
    ) -> Result<u64, InMemoryStoreError> {
        if let Some(err) = self.take_injected_failure() {
            return Err(err);
        }
        let mut inner = self.lock();
        let now = inner.now;
        let mut affected = 0u64;
        for message in items {
            if let Some(row) = Self::find_owned_mut(&mut inner.rows, worker, *message) {
                row.locked_until = Some(now + lease);
                affected += 1;
            }
        }
        Ok(affected)
    }

    fn try_purge(&self, request: &PurgeRequest) -> Result<PurgeReport, InMemoryStoreError> {
        if let Some(err) = self.take_injected_failure() {
            return Err(err);
        }
        let mut inner = self.lock();
        let now = inner.now;
        let batch_size = request.batch_size as usize;

        // Ruling G2: a row is swept to dead only if it is expired **and unowned** — one that
        // expired mid-flight still belongs to the worker holding it (its `complete` still wins)
        // until that lease itself lapses, exactly the boundary `Self::lease_is_free` checks.
        let mut expired_to_dead: usize = 0;
        for row in &mut inner.rows {
            if expired_to_dead >= batch_size {
                break;
            }
            if row.published_at.is_some() || row.dead_at.is_some() || !Self::lease_is_free(row, now)
            {
                continue;
            }
            let Some(expires_at) = row.envelope.metadata.delivery.expires_at else {
                continue;
            };
            if expires_at <= now {
                row.dead_at = Some(now);
                row.dead_reason = Some(DeadReason::Expired);
                // `lease_is_free` already guarantees any lease here is stale (expired, not
                // live), so clearing it is just tidying a reclaimable-but-untouched lease — the
                // same thing the SQL sweep's `locked_by = NULL, locked_until = NULL` does.
                row.locked_by = None;
                row.locked_until = None;
                row.last_error = Some(EXPIRED_ERROR_MESSAGE.to_string());
                expired_to_dead += 1;
            }
        }

        let mut published_deleted: usize = 0;
        if let Some(retention) = request.published_retention {
            let before = now - retention;
            inner.rows.retain(|row| {
                if published_deleted >= batch_size {
                    return true;
                }
                let expired = row.published_at.is_some_and(|at| at < before);
                if expired {
                    published_deleted += 1;
                }
                !expired
            });
        }

        let mut dead_deleted: usize = 0;
        if let Some(retention) = request.dead_retention {
            let before = now - retention;
            inner.rows.retain(|row| {
                if dead_deleted >= batch_size {
                    return true;
                }
                let expired = row.dead_at.is_some_and(|at| at < before);
                if expired {
                    dead_deleted += 1;
                }
                !expired
            });
        }

        Ok(PurgeReport::new(
            published_deleted as u64,
            dead_deleted as u64,
            expired_to_dead as u64,
        ))
    }

    fn try_stats(&self) -> Result<OutboxStats, InMemoryStoreError> {
        if let Some(err) = self.take_injected_failure() {
            return Err(err);
        }
        let inner = self.lock();
        let now = inner.now;

        let mut pending = 0u64;
        let mut dead = 0u64;
        let mut expired_pending = 0u64;
        let mut oldest_pending_available_at: Option<OffsetDateTime> = None;

        for row in &inner.rows {
            if row.dead_at.is_some() {
                dead += 1;
                continue;
            }
            if row.published_at.is_some() {
                continue;
            }
            let expired = row
                .envelope
                .metadata
                .delivery
                .expires_at
                .is_some_and(|expires_at| expires_at <= now);
            if expired {
                expired_pending += 1;
                continue;
            }
            if Self::is_claimable(row, now) {
                pending += 1;
                oldest_pending_available_at = Some(match oldest_pending_available_at {
                    Some(current) if current <= row.available_at => current,
                    _ => row.available_at,
                });
            }
        }

        Ok(OutboxStats::new(
            pending,
            dead,
            expired_pending,
            oldest_pending_available_at,
            now,
        ))
    }

    fn dead_ref_matches(row: &OutboxRecord, message: &MessageRef) -> bool {
        row.envelope.id == message.id && row.created_at == message.created_at
    }
}

/// [`InMemoryOutboxStore::complete`]/[`InMemoryOutboxStore::fail`]'s hang-simulation state — see
/// [`InMemoryOutboxStore::hang_next`].
enum HangState<T> {
    /// The normal path: the mutation already ran, at call time.
    Eager(T),
    /// A hang was armed: the mutation is deferred until after the sleep, so a caller whose
    /// timeout drops this future first never sees it applied.
    Deferred(Duration),
}

impl OutboxStore for InMemoryOutboxStore {
    type Error = InMemoryStoreError;

    fn acquire(
        &self,
        request: AcquireRequest,
    ) -> impl Future<Output = Result<AcquiredBatch, Self::Error>> + Send {
        let result = self.try_acquire(&request);
        std::future::ready(result)
    }

    fn complete(
        &self,
        worker: &WorkerId,
        items: &[CompletedMessage],
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        let state = match self.take_hang() {
            None => HangState::Eager(self.try_complete(worker, items)),
            Some(duration) => HangState::Deferred(duration),
        };
        let store = self.clone();
        let worker = worker.clone();
        let items = items.to_vec();
        async move {
            match state {
                HangState::Eager(result) => result,
                HangState::Deferred(duration) => {
                    tokio::time::sleep(duration).await;
                    store.try_complete(&worker, &items)
                }
            }
        }
    }

    fn fail(
        &self,
        worker: &WorkerId,
        items: &[FailedMessage],
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        let state = match self.take_hang() {
            None => HangState::Eager(self.try_fail(worker, items)),
            Some(duration) => HangState::Deferred(duration),
        };
        let store = self.clone();
        let worker = worker.clone();
        let items = items.to_vec();
        async move {
            match state {
                HangState::Eager(result) => result,
                HangState::Deferred(duration) => {
                    tokio::time::sleep(duration).await;
                    store.try_fail(&worker, &items)
                }
            }
        }
    }

    fn release(
        &self,
        worker: &WorkerId,
        items: &[MessageRef],
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        let result = self.try_release(worker, items);
        std::future::ready(result)
    }

    fn extend_lease(
        &self,
        worker: &WorkerId,
        items: &[MessageRef],
        lease: Duration,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        let result = self.try_extend_lease(worker, items, lease);
        std::future::ready(result)
    }

    fn purge(
        &self,
        request: PurgeRequest,
    ) -> impl Future<Output = Result<PurgeReport, Self::Error>> + Send {
        let result = self.try_purge(&request);
        std::future::ready(result)
    }

    fn stats(&self) -> impl Future<Output = Result<OutboxStats, Self::Error>> + Send {
        let result = self.try_stats();
        std::future::ready(result)
    }
}

impl OutboxDeadLetters for InMemoryOutboxStore {
    type Error = InMemoryStoreError;

    fn list_dead(
        &self,
        query: DeadQuery,
    ) -> impl Future<Output = Result<DeadLetterPage, Self::Error>> + Send {
        let inner = self.lock();
        let limit = query.limit as usize;

        let mut matches: Vec<&OutboxRecord> = inner
            .rows
            .iter()
            .filter(|row| row.dead_at.is_some())
            .filter(|row| {
                query
                    .message_type
                    .as_deref()
                    .is_none_or(|ty| row.envelope.message_type.name() == ty)
            })
            .filter(|row| {
                query
                    .tenant_id
                    .as_deref()
                    .is_none_or(|tenant| row.envelope.metadata.tenant_id.as_deref() == Some(tenant))
            })
            .filter(|row| {
                query
                    .dead_before
                    .is_none_or(|before| row.dead_at.is_some_and(|at| at < before))
            })
            .filter(|row| {
                query
                    .after_sequence
                    .is_none_or(|after| row.sequence > after)
            })
            .collect();
        matches.sort_by_key(|row| row.sequence);

        let page: Vec<OutboxRecord> = matches.into_iter().take(limit).cloned().collect();
        let next_after_sequence = if page.len() == limit {
            page.last().map(|row| row.sequence)
        } else {
            None
        };

        std::future::ready(Ok(DeadLetterPage::new(
            page,
            Vec::new(),
            next_after_sequence,
        )))
    }

    fn retry_dead(
        &self,
        refs: &[MessageRef],
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        let mut inner = self.lock();
        let now = inner.now;
        let mut affected = 0u64;
        for message in refs {
            if let Some(row) = inner
                .rows
                .iter_mut()
                .find(|row| Self::dead_ref_matches(row, message) && row.dead_at.is_some())
            {
                row.dead_at = None;
                row.dead_reason = None;
                row.available_at = now;
                row.attempts = 0;
                affected += 1;
            }
        }
        std::future::ready(Ok(affected))
    }

    fn purge_dead(
        &self,
        refs: &[MessageRef],
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        let mut inner = self.lock();
        let mut deleted = 0u64;
        inner.rows.retain(|row| {
            let matches = row.dead_at.is_some()
                && refs
                    .iter()
                    .any(|message| Self::dead_ref_matches(row, message));
            if matches {
                deleted += 1;
            }
            !matches
        });
        std::future::ready(Ok(deleted))
    }
}

/// [`InMemoryOutboxStore`]'s call-level failure. Never a property of one row's content.
///
/// A lost lease is a **benign** shortfall (a lower affected-row count), never an error
/// (SRS §19.1), so there is no variant for it — review 1 removed the unreachable `LeaseLost`
/// placeholder rather than inventing a call path that would contradict that documented
/// guarantee.
#[derive(Debug)]
#[non_exhaustive]
pub enum InMemoryStoreError {
    /// Injected by [`InMemoryOutboxStore::fail_next`]. Classifies `Transient`.
    Injected,
    /// Injected by [`InMemoryOutboxStore::fail_next_permanent`]. Classifies `Permanent` — the
    /// only way this fake can drive `OutboxDispatcher::run`'s permanent-store-error exit.
    InjectedPermanent,
}

impl fmt::Display for InMemoryStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Injected => f.write_str("test-support: injected store failure"),
            Self::InjectedPermanent => {
                f.write_str("test-support: injected permanent store failure")
            }
        }
    }
}

impl std::error::Error for InMemoryStoreError {}

impl Classify for InMemoryStoreError {
    fn kind(&self) -> FailureKind {
        match self {
            Self::Injected => FailureKind::Transient,
            Self::InjectedPermanent => FailureKind::Permanent,
        }
    }
}

/// A stand-in for a provider transaction in fake-driven tests: it carries no state, it exists so
/// a test exercises the same `enqueue(&mut tx, ..)` shape a real host does.
#[derive(Clone, Copy, Debug, Default)]
pub struct InMemoryTransaction;

impl OutboxEnqueue<InMemoryTransaction> for InMemoryOutboxStore {
    type Error = InMemoryStoreError;

    fn enqueue(
        &self,
        _tx: &mut InMemoryTransaction,
        envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<MessageId, Self::Error>> + Send {
        let result = self.try_enqueue(envelope);
        std::future::ready(result)
    }
}
