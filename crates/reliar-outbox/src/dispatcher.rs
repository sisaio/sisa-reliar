//! The worker loop: claim → bounded concurrent publish → batch complete/fail → idle backoff
//! (SRS §21, §21.1, §22, §22.1, §23, §26, §26.1, §33.1, ADR 0006, ADR 0007, ADR 0009, ADR 0013,
//! ADR 0014).
//!
//! [`OutboxDispatcher::run`] claims due rows with [`OutboxStore::acquire`], publishes each under
//! a [`tokio::sync::Semaphore`]-bounded [`tokio::task::JoinSet`], renews the lease of rows still
//! outstanding at least every half-lease, and applies a [`RetryPolicy`] to every failure before
//! batching the outcome back through [`OutboxStore::complete`]/[`OutboxStore::fail`].
//!
//! # The three duplicate windows
//!
//! Reliar guarantees **durable at-least-once** publication, never exactly-once (SRS §22). Three
//! distinct windows each produce a duplicate, and none is eliminable:
//!
//! 1. **The crash window** (§22): a publish reaches the broker, the process crashes before
//!    `complete` persists, the lease expires, and another worker republishes the row. A crash is
//!    not the only trigger: `lease` also bounds how long a still-healthy worker retries an
//!    outcome write that keeps failing (M2, RELIAR-26) — once that budget is spent, the row is
//!    abandoned to its lease exactly as if the worker had crashed, producing the same duplicate
//!    with no crash at all.
//! 2. **The slow-batch window** (§22.1): no crash at all — a lease shorter than a large batch
//!    takes to drain expires while the original worker is still healthily publishing; a second
//!    worker reclaims and republishes the tail while the first is still mid-flight, and the
//!    first worker's later `complete`/`fail` is rejected by the store's `locked_by` guard.
//! 3. **The drain-timeout window** (§22.1, §26.1): on cancellation, a publish already **in
//!    flight** (it has acquired its concurrency permit) is awaited for at most
//!    [`DispatcherSettings::drain_timeout`]. If it resolved as a **failure**, or never resolved
//!    at all, its row is released so the next owner may retry it. If it resolved as a
//!    **success** but the `complete` write itself never landed (it errored, hit
//!    `store_timeout`, or drain simply ran out of time), the row is **left to its lease** rather
//!    than released — releasing a row already known to be delivered would turn a *possible*
//!    duplicate into a *certain* one for nothing gained. A row whose publish task had **not
//!    yet** acquired a permit when cancellation arrived is dropped immediately rather than
//!    started, and its row is released at once (S4 review — "drain finishes what started; it
//!    never starts anything new"). The honest claim for a dropped task is **"has not completed a
//!    publish"**, not "never touched the broker": on a multi-thread runtime a task may have been
//!    polled once and begun the broker call before the drop lands, so a released row may already
//!    have been delivered — the same at-least-once window as everywhere else, not a new one.
//!    All of this is logged at `warn` with counts.
//!
//! # Ordering
//!
//! [`Ordering::Unordered`] (the only strategy this release implements) guarantees **nothing**
//! about order — not globally, not per `conversation_id`, not per aggregate, not approximately.

use core::fmt;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::time::Duration;

use reliar_core::{Classify, FailureKind, MessageType, Publisher, SerializedEnvelope};
use tokio::sync::Semaphore;
use tokio::task::{AbortHandle, Id as TaskId, JoinError, JoinSet};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;

use crate::error::ConfigError;
use crate::metrics::{NoopMetrics, OutboxMetrics};
use crate::ordering::Ordering;
use crate::retry::{ExponentialBackoff, RetryPolicy};
use crate::settings::DispatcherSettings;
use crate::store::{
    AcquireRequest, CompletedMessage, DeadReason, FailedMessage, FailureOutcome, MessageRef,
    OutboxStore,
};
use crate::worker::WorkerId;

/// Claims due outbox rows, publishes them under bounded concurrency, and persists the outcome —
/// the worker loop for the transactional outbox (SRS §21, §26). Build one with
/// [`OutboxDispatcher::builder`].
///
/// **No `Clone` bound on `P` or `M`.** Both are wrapped in an internal [`Arc`] so each spawned
/// publish task gets a cheap handle; `Arc` is not dynamic dispatch (ADR 0001). A host may still
/// pass a `Clone` type — it simply is not required to.
pub struct OutboxDispatcher<S, P, M = NoopMetrics, R = ExponentialBackoff> {
    store: S,
    publisher: Arc<P>,
    metrics: Arc<M>,
    retry: R,
    settings: DispatcherSettings,
    worker: WorkerId,
}

impl<S, P, M, R> fmt::Debug for OutboxDispatcher<S, P, M, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutboxDispatcher")
            .field("worker", &self.worker)
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl<S, P> OutboxDispatcher<S, P>
where
    S: OutboxStore,
    P: Publisher,
{
    /// Starts building a dispatcher over `store` and `publisher`, with [`NoopMetrics`] and
    /// [`DefaultRetry`] (which becomes [`ExponentialBackoff`] built from
    /// `DispatcherSettings::retry`) as the defaults.
    pub fn builder(store: S, publisher: P) -> OutboxDispatcherBuilder<S, P> {
        OutboxDispatcherBuilder {
            store,
            publisher,
            metrics: NoopMetrics,
            settings: DispatcherSettings::default(),
            retry: DefaultRetry,
        }
    }
}

/// Marker for "the retry policy comes from `DispatcherSettings::retry`" — the
/// [`OutboxDispatcherBuilder`]'s default `R`. It deliberately does **not** implement
/// [`RetryPolicy`]: that is what keeps its [`OutboxDispatcherBuilder::build`] and the generic,
/// `R: RetryPolicy`-bounded one from overlapping, so the compiler picks the right one with no
/// specialization and no second method name (S4 review; ADR-lite K1).
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultRetry;

/// Builds an [`OutboxDispatcher`]. Obtained from [`OutboxDispatcher::builder`].
#[must_use]
pub struct OutboxDispatcherBuilder<S, P, M = NoopMetrics, R = DefaultRetry> {
    store: S,
    publisher: P,
    metrics: M,
    settings: DispatcherSettings,
    retry: R,
}

impl<S, P, M, R> fmt::Debug for OutboxDispatcherBuilder<S, P, M, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutboxDispatcherBuilder")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl<S, P, M, R> OutboxDispatcherBuilder<S, P, M, R> {
    /// Replaces the whole settings struct. **Last call wins**: [`Self::ordering`] and
    /// [`Self::worker_id`] apply to whatever settings is current, so `.ordering(x).settings(s)`
    /// discards `x` while `.settings(s).ordering(x)` keeps it.
    pub fn settings(mut self, settings: DispatcherSettings) -> Self {
        self.settings = settings;
        self
    }

    /// Sets [`DispatcherSettings::ordering`].
    pub const fn ordering(mut self, ordering: Ordering) -> Self {
        self.settings.ordering = ordering;
        self
    }

    /// Sets [`DispatcherSettings::worker_id`].
    pub fn worker_id(mut self, worker: WorkerId) -> Self {
        self.settings.worker_id = Some(worker);
        self
    }

    /// Replaces the metrics hook, changing `M`.
    pub fn metrics<M2: OutboxMetrics>(self, metrics: M2) -> OutboxDispatcherBuilder<S, P, M2, R> {
        OutboxDispatcherBuilder {
            store: self.store,
            publisher: self.publisher,
            metrics,
            settings: self.settings,
            retry: self.retry,
        }
    }

    /// Replaces retry **entirely**, changing `R`. `settings.retry` is then unused, and
    /// [`Self::build`] rejects the combination of a custom policy with a non-default
    /// `settings.retry` rather than silently ignore one of the two
    /// ([`ConfigError::RetryPolicyConflict`]).
    pub fn retry_policy<R2: RetryPolicy>(self, policy: R2) -> OutboxDispatcherBuilder<S, P, M, R2> {
        OutboxDispatcherBuilder {
            store: self.store,
            publisher: self.publisher,
            metrics: self.metrics,
            settings: self.settings,
            retry: policy,
        }
    }
}

impl<S, P, M> OutboxDispatcherBuilder<S, P, M, DefaultRetry> {
    /// Validates the configuration and builds the dispatcher, constructing its
    /// [`ExponentialBackoff`] policy from `settings.retry` — the path taken whenever
    /// [`OutboxDispatcherBuilder::retry_policy`] was never called. **Never panics.**
    ///
    /// # Errors
    ///
    /// See the generic [`OutboxDispatcherBuilder::build`] for the shared rejection list; this
    /// path additionally validates `settings.retry` itself (it *is* the policy here):
    /// [`ConfigError::InvalidJitter`], [`ConfigError::ZeroMaxAttempts`],
    /// [`ConfigError::ZeroRetryBase`].
    pub fn build(self) -> Result<OutboxDispatcher<S, P, M, ExponentialBackoff>, ConfigError> {
        validate_shared(&self.settings)?;
        self.settings.retry.validate()?;
        let worker = self.settings.worker_id.clone().unwrap_or_default();
        Ok(OutboxDispatcher {
            store: self.store,
            publisher: Arc::new(self.publisher),
            metrics: Arc::new(self.metrics),
            retry: self.settings.retry,
            settings: self.settings,
            worker,
        })
    }
}

impl<S, P, M, R> OutboxDispatcherBuilder<S, P, M, R>
where
    R: RetryPolicy + 'static,
{
    /// Validates the configuration and builds the dispatcher using the custom policy supplied to
    /// [`OutboxDispatcherBuilder::retry_policy`]. **Never panics.**
    ///
    /// # Errors
    ///
    /// - [`ConfigError::ZeroInFlight`] — `max_in_flight == 0`.
    /// - [`ConfigError::ZeroBatchSize`] — `batch_size == 0`.
    /// - [`ConfigError::ZeroPollInterval`] — `poll_interval == 0` or `idle_poll_interval == 0`.
    /// - [`ConfigError::StoreTimeoutTooLong`] — `store_timeout >= lease / 2`.
    /// - [`ConfigError::LeaseTooShort`] — `lease` not longer than `publish_timeout`.
    /// - [`ConfigError::UnsupportedOrdering`] — [`Ordering::PerKey`] before 0.2.
    /// - [`ConfigError::RetryPolicyConflict`] — `settings.retry` is not
    ///   [`ExponentialBackoff::default`], which would otherwise be silently ignored now that a
    ///   custom policy is in charge.
    /// - [`ConfigError::InvalidJitter`], [`ConfigError::ZeroMaxAttempts`],
    ///   [`ConfigError::ZeroRetryBase`] — when the supplied policy **is itself** an
    ///   [`ExponentialBackoff`] (a host may reasonably pass one through `.retry_policy()` instead
    ///   of relying on `settings.retry`), its own bounds are validated too (S4 review 3, minor).
    ///
    /// **Warns**, does not fail, when `lease` is not comfortably longer than
    /// `batch_size × publish_timeout ÷ max_in_flight` (§21.1), and when `store_timeout` is
    /// longer than `drain_timeout`.
    pub fn build(self) -> Result<OutboxDispatcher<S, P, M, R>, ConfigError> {
        validate_shared(&self.settings)?;
        if self.settings.retry != ExponentialBackoff::default() {
            return Err(ConfigError::RetryPolicyConflict);
        }
        if let Some(backoff) =
            (&self.retry as &dyn core::any::Any).downcast_ref::<ExponentialBackoff>()
        {
            backoff.validate()?;
        }
        let worker = self.settings.worker_id.clone().unwrap_or_default();
        Ok(OutboxDispatcher {
            store: self.store,
            publisher: Arc::new(self.publisher),
            metrics: Arc::new(self.metrics),
            retry: self.retry,
            settings: self.settings,
            worker,
        })
    }
}

/// The validation shared by both `build()` paths (SRS §22.2, §21.1).
fn validate_shared(settings: &DispatcherSettings) -> Result<(), ConfigError> {
    if settings.max_in_flight == 0 {
        return Err(ConfigError::ZeroInFlight);
    }
    if settings.batch_size == 0 {
        return Err(ConfigError::ZeroBatchSize);
    }
    if settings.poll_interval.is_zero() {
        return Err(ConfigError::ZeroPollInterval {
            field: "poll_interval",
        });
    }
    if settings.idle_poll_interval.is_zero() {
        return Err(ConfigError::ZeroPollInterval {
            field: "idle_poll_interval",
        });
    }
    settings.ordering.validate()?;
    if settings.lease <= settings.publish_timeout {
        return Err(ConfigError::LeaseTooShort {
            lease: settings.lease,
            publish_timeout: settings.publish_timeout,
        });
    }
    // `retry_unwritten_outcomes` races the lease tick inside the same `select!` (S4 review 4,
    // major 3): a `store_timeout` this long could let one hung outcome-write attempt occupy an
    // entire renewal-tick gap, starving lease renewal.
    if settings.store_timeout >= settings.lease / 2 {
        return Err(ConfigError::StoreTimeoutTooLong {
            store_timeout: settings.store_timeout,
            lease: settings.lease,
        });
    }

    #[allow(
        clippy::cast_possible_truncation,
        reason = "max_in_flight is validated non-zero above; truncation only lowers the \
                  warning's sensitivity, never causes a false negative that matters"
    )]
    let max_in_flight = settings.max_in_flight as u32;
    let expected_batch_duration = settings
        .publish_timeout
        .saturating_mul(settings.batch_size)
        .checked_div(max_in_flight.max(1))
        .unwrap_or(Duration::MAX);
    if settings.lease <= expected_batch_duration {
        tracing::warn!(
            lease_ms = settings.lease.as_millis(),
            expected_batch_duration_ms = expected_batch_duration.as_millis(),
            "reliar.outbox: lease is not comfortably longer than batch_size × \
             publish_timeout ÷ max_in_flight; a healthy batch may still outlive its lease \
             (SRS §21.1)"
        );
    }

    if settings.store_timeout > settings.drain_timeout {
        tracing::warn!(
            store_timeout_ms = settings.store_timeout.as_millis(),
            drain_timeout_ms = settings.drain_timeout.as_millis(),
            "reliar.outbox: store_timeout is longer than drain_timeout; a slow store call can \
             still make shutdown wait past drain_timeout"
        );
    }

    Ok(())
}

/// Why [`OutboxDispatcher::run`] returned early. **Cancellation is not an error** — a cancelled
/// `run()` returns `Ok(())` after draining (§26.1, ADR 0014).
#[derive(Debug)]
#[non_exhaustive]
pub enum DispatchError<E> {
    /// Invalid configuration detected at startup. In practice unreachable through `run()` itself
    /// — [`OutboxDispatcherBuilder::build`] rejects every invalid configuration before a
    /// dispatcher can be constructed — but the variant is kept so a future, later-bound
    /// validation has somewhere to report without a breaking change.
    Configuration(ConfigError),
    /// A store error the provider [`Classify`]-ed as [`FailureKind::Permanent`] (for example,
    /// the migrations have not been run). A transient store error, and a `store_timeout` on any
    /// call, never reaches here — `run()` logs it and keeps going (ADR 0014). This exit still
    /// drains best-effort first — see [`OutboxDispatcher::run`].
    Store(E),
}

impl<E: fmt::Display> fmt::Display for DispatchError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(err) => write!(f, "invalid dispatcher configuration: {err}"),
            Self::Store(err) => write!(f, "permanent store error: {err}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for DispatchError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Configuration(err) => Some(err),
            Self::Store(err) => Some(err),
        }
    }
}

/// One claimed row not yet spawned as a publish task.
struct PendingRow {
    message: MessageRef,
    message_type: MessageType,
    attempts_before: u32,
    envelope: SerializedEnvelope,
}

/// Bookkeeping for one spawned [`publish_one`] task, keyed by its [`TaskId`] so both the
/// completion path and the cancellation path can remove it in O(1) instead of scanning a `Vec`.
struct OutstandingTask {
    message: MessageRef,
    /// Flipped to `true` by the task itself once it has acquired its concurrency permit — the
    /// signal cancellation uses to tell "likely already publishing" from "never started" (S4
    /// review, "drain finishes what started"). A **best-effort** signal, not a hard guarantee: on
    /// a multi-thread runtime a `Relaxed` load here can briefly lag the task's own store, so a
    /// task that has in fact begun the broker call could still be treated as not-yet-started and
    /// dropped. That is exactly the "has not completed a publish" window the module docs already
    /// describe, not a new one — this flag only narrows how often it happens, it does not
    /// eliminate it.
    started: Arc<AtomicBool>,
    abort: AbortHandle,
}

impl<S, P, M, R> OutboxDispatcher<S, P, M, R>
where
    S: OutboxStore + Send + Sync + 'static,
    P: Publisher + Send + Sync + 'static,
    M: OutboxMetrics + Send + Sync + 'static,
    R: RetryPolicy + Send + Sync + 'static,
{
    /// Runs until cancelled. **At-least-once**: a crash between publish and `complete`, a lease
    /// that expires mid-batch, and a drain timeout each republish the message — see the module
    /// docs for the three duplicate windows. **`Ordering::Unordered` guarantees no order of any
    /// kind** (ADR 0013).
    ///
    /// **The claim loop is bounded by `max_in_flight`** (S4 review 2): `run` claims only while
    /// `outstanding < max_in_flight`, and asks for `min(batch_size, max_in_flight - outstanding)`.
    /// Without that gate the loop would re-claim `batch_size` rows every poll regardless of how
    /// many it is still holding, so a publisher slower than the poll interval would make one
    /// dispatcher hoard leases without bound — and the tail would sit leased and unpublished
    /// until its lease expired under a healthy worker, precisely the §22.1 slow-batch duplicate
    /// window. This gate does not merely bound memory: it **shrinks that window**.
    /// **`max_in_flight` is the claim gate's ceiling on rows this worker actively holds leased —
    /// not an absolute one** (S4 review 4, minor): a row M2 (below) has abandoned still sits
    /// leased, just no longer counted here or renewed, until that lease elapses on its own.
    /// **`batch_size` only caps a single claim statement** — with the accepted defaults
    /// (100 / 16) the claim never asks for more than 16, so `batch_size` does not bind.
    ///
    /// On cancellation, `run` stops claiming immediately, drains in-flight publishes for at most
    /// [`DispatcherSettings::drain_timeout`], persists every outcome that resolved, releases the
    /// remainder, and returns `Ok(())` (§26.1, ADR 0014). A publish task that has **not yet**
    /// acquired its concurrency permit is dropped rather than awaited — the honest claim is that
    /// it **has not completed a publish**, not that it "never touched the broker": on a
    /// multi-thread runtime it may have been polled once and begun the broker call before the
    /// drop lands, so a released row may already have been delivered (the same at-least-once
    /// window as everywhere else, not a new one). Waiting to start a task that will be released
    /// anyway would only burn the drain budget for no delivery.
    ///
    /// **Every [`OutboxStore`] call is bounded by [`DispatcherSettings::store_timeout`]**;
    /// without it a hung statement would make `drain_timeout` itself unenforceable. A timeout is
    /// treated as a transient store error.
    ///
    /// **An outcome write that fails or times out keeps its rows outstanding** (S4 review 2):
    /// when `complete`/`fail` errors, the affected rows are not dropped — they stay in a
    /// pending-outcome state and the write is retried on the next loop iteration. Retrying is
    /// safe because the `locked_by` guard makes a repeated `complete`/`fail` idempotent, and a
    /// lease already lost to another worker simply affects zero rows (benign, ADR 0008). This is
    /// **not a fourth duplicate window** — it is SRS §23.2's "publish succeeded, completion
    /// failed", and **`lease` is also the outcome-write retry budget** (RELIAR-26, M2): past one
    /// `lease`'s worth of retrying, a still-unwritten outcome is abandoned rather than retried
    /// forever, and the row is left to its lease. A row whose publish **succeeded** this way
    /// reaches the crash window's exact outcome — "`complete` never landed, lease expires,
    /// another worker republishes" — through a **perfectly healthy** worker instead of a crash
    /// (see the module docs' window 1). At drain, this asymmetry continues: a row whose publish
    /// **failed or never resolved** is released immediately either way, but a row whose publish
    /// **succeeded** with an unwritten `complete` is **left to its lease** instead of released —
    /// releasing it would turn a possible duplicate into a certain one for a message already
    /// known to be delivered — whether that unwritten state was reached through drain running
    /// out of time or through M2 giving up first.
    ///
    /// **A fresh outcome is written eagerly; only a failed attempt is paced.** The due-time gate
    /// below is armed with `Instant::now() + outcome_retry_interval` only after a `complete`/
    /// `fail` attempt itself errors, so a healthy store is never throttled behind an interval
    /// unrelated to how fast messages actually publish.
    ///
    /// **The outcome-write retry is raced against the lease-renewal tick and gated behind a
    /// due-time, never a spin, once it has failed.** `outcome_retry_interval` (derived from
    /// [`DispatcherSettings::poll_interval`], capped at a quarter of `lease`) is a fixed interval
    /// between attempts, not a growing backoff — without it, a store call that fails *fast* (no
    /// hang, an immediate `Err`) would retry every loop iteration at CPU speed for the whole
    /// `lease` window. Logged at `warn`, not `error` — expected, already on its own schedule.
    ///
    /// A store error is **transient by assumption**: logged at `error`, backed off by
    /// [`DispatcherSettings::idle_poll_interval`], and the loop continues — a Postgres restart or
    /// failover SHALL NOT end the worker loop. `run` returns `Err` only when the provider
    /// classifies a store error as [`FailureKind::Permanent`], checked on **two** paths: the
    /// claim path (the path a broken deployment, e.g. missing migrations, reaches first) and the
    /// outcome-write path (M1, RELIAR-26) — a `complete`/`fail` error classified `Permanent` ends
    /// the loop immediately rather than retrying forever, because a silently wedged worker
    /// (claiming stopped, leases renewed forever, never `Err`) is the worst available outcome
    /// precisely because nothing about it looks like one. Either path, `run` drains first —
    /// persisting resolved outcomes and releasing the rest — on a **best-effort** basis, logging
    /// and discarding any error from that drain so the original diagnosis surfaces. A publish
    /// error never ends the loop; it is a per-message outcome applied through the configured
    /// [`RetryPolicy`].
    ///
    /// Idempotent under repeated cancellation; safe to run many dispatcher instances, in many
    /// processes, against one table.
    ///
    /// # Errors
    ///
    /// Returns [`DispatchError::Store`] for a permanent store error.
    #[allow(
        clippy::too_many_lines,
        reason = "the claim/publish/drain state machine reads more clearly as one function than \
                  split across helpers that would each need most of this same local state"
    )]
    pub async fn run(self, cancel: CancellationToken) -> Result<(), DispatchError<S::Error>> {
        let Self {
            store,
            publisher,
            metrics,
            retry,
            settings,
            worker,
        } = self;

        tracing::info!(
            worker.id = %worker,
            batch_size = settings.batch_size,
            max_in_flight = settings.max_in_flight,
            "reliar.outbox: dispatcher starting"
        );

        let semaphore = Arc::new(Semaphore::new(settings.max_in_flight));
        let mut pending: VecDeque<PendingRow> = VecDeque::new();
        let mut in_flight: JoinSet<PublishTaskOutcome> = JoinSet::new();
        let mut outstanding: HashMap<TaskId, OutstandingTask> = HashMap::new();
        let mut next_poll_at = Instant::now();
        let lease_tick_period = (settings.lease / 2).max(Duration::from_millis(1));
        let mut lease_ticker =
            tokio::time::interval_at(Instant::now() + lease_tick_period, lease_tick_period);
        lease_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut stats_ticker = (!settings.stats_interval.is_zero()).then(|| {
            let mut interval = tokio::time::interval_at(
                Instant::now() + settings.stats_interval,
                settings.stats_interval,
            );
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval
        });

        let mut exit_reason: Option<DispatchError<S::Error>> = None;
        // Each entry's `Instant` is when it *first* became unwritten — M2 bounds how long an
        // outcome write may be retried before the row is abandoned to its lease.
        let mut unwritten_complete: Vec<(Instant, CompletedMessage)> = Vec::new();
        let mut unwritten_fail: Vec<(Instant, FailedMessage)> = Vec::new();
        // A fast-failing (not hung) `complete`/`fail` — e.g. a connection pool immediately
        // returning an error — must not retry at CPU speed for the whole `lease` window (S4
        // review 5, blocker): a *failed* attempt pushes the next one out by
        // `outcome_retry_interval`, capped so at least four attempts still fit inside `lease`
        // before M2 gives up. A *successful* attempt does not use this interval at all — the due
        // time is left at "now" so a fresh outcome is written on the next loop iteration, not
        // paced behind a healthy store (S4 review 6, blocker). Floored at 1 ms like
        // `lease_tick_period` above (S4 review 7, defense in depth): `validate_shared` already
        // rejects `poll_interval == 0`, so this only guards a dispatcher built by some future,
        // later-bound validation path that skips it.
        let outcome_retry_interval = settings
            .poll_interval
            .min(settings.lease / 4)
            .max(Duration::from_millis(1));
        let mut next_outcome_retry_at = Instant::now();

        'run: loop {
            // `max_in_flight` is the real ceiling on outstanding rows — a row with a resolved
            // publish but an unwritten outcome still counts, since the worker still holds its
            // lease (L1, L2, S4 review 2). A row M2 has abandoned (past `lease` with no
            // persisted outcome) stops counting here — and stops being renewed below — in the
            // same step; it is no longer "the real ceiling" once M2 has let it go, only while
            // this worker still claims it.
            let outstanding_count =
                outstanding.len() + unwritten_complete.len() + unwritten_fail.len();
            let has_capacity = outstanding_count < settings.max_in_flight;
            let has_outstanding = outstanding_count > 0;
            let has_unwritten = !unwritten_complete.is_empty() || !unwritten_fail.is_empty();
            // Computed *before* `select!` (a plain owned `Vec`, not a borrow) so the lease-tick
            // branch below never needs to borrow `unwritten_complete`/`unwritten_fail` — which
            // would conflict with the retry-outcomes branch's own `&mut` borrow of the same
            // collections inside the same `select!` (S4 review 4, major 3).
            let lease_refs: Vec<MessageRef> = outstanding
                .values()
                .map(|task| task.message)
                .chain(unwritten_complete.iter().map(|(_, c)| c.message))
                .chain(unwritten_fail.iter().map(|(_, f)| f.message))
                .collect();

            tokio::select! {
                biased;

                () = cancel.cancelled() => {
                    break 'run;
                }

                Some(first) = in_flight.join_next_with_id(), if !in_flight.is_empty() => {
                    let mut results = vec![first];
                    while let Some(next) = in_flight.try_join_next_with_id() {
                        results.push(next);
                    }
                    record_publish_results(results, &mut outstanding, &mut unwritten_complete, &mut unwritten_fail, &retry, metrics.as_ref());
                    spawn_ready(&mut pending, &mut in_flight, &mut outstanding, &publisher, &metrics, &semaphore, settings.publish_timeout);
                }

                _ = lease_ticker.tick(), if has_outstanding => {
                    renew_leases(&store, &worker, &lease_refs, settings.lease, settings.store_timeout).await;
                }

                () = tick_optional(&mut stats_ticker) => {
                    report_stats(&store, metrics.as_ref(), settings.store_timeout).await;
                }

                // Raced as its own `select!` branch, not an unconditional post-`select!` tail
                // call, so a due lease tick always wins over a hung or fast-failing write attempt
                // instead of waiting behind it. Gated behind `next_outcome_retry_at` so a *fast*
                // failure (an immediate `Err`, no hang) still only retries once per
                // `outcome_retry_interval`, never at CPU speed.
                retry_result = retry_unwritten_outcomes_when_due(
                    &mut unwritten_complete,
                    &mut unwritten_fail,
                    &store,
                    &worker,
                    settings.store_timeout,
                    settings.lease,
                    next_outcome_retry_at,
                ), if has_unwritten => {
                    let RetryOutcome { permanent, any_failed } = retry_result;
                    // Pacing is armed only for a failure that leaves outcomes still unwritten —
                    // never on success, and never when this round's own retries (or
                    // `expire_past_lease`) already cleared everything, so a fresh outcome is
                    // never delayed behind a stale interval from an unrelated, now-resolved
                    // attempt (at most `outcome_retry_interval` of delay per round-wide failure).
                    let still_unwritten =
                        !unwritten_complete.is_empty() || !unwritten_fail.is_empty();
                    next_outcome_retry_at = if any_failed && still_unwritten {
                        Instant::now() + outcome_retry_interval
                    } else {
                        Instant::now()
                    };
                    if let Some(err) = permanent {
                        exit_reason = Some(DispatchError::Store(err));
                        break 'run;
                    }
                }

                () = tokio::time::sleep_until(next_poll_at), if has_capacity => {
                    // Claim only up to the room `max_in_flight` still has (L1, S4 review 2):
                    // `batch_size` caps a single claim statement, `max_in_flight` caps how much
                    // this worker holds leased at once.
                    let capacity = settings.max_in_flight.saturating_sub(outstanding_count);
                    let capacity = u32::try_from(capacity).unwrap_or(u32::MAX);
                    let want = settings.batch_size.min(capacity);
                    let request = AcquireRequest::new(worker.clone())
                        .batch_size(want)
                        .lease(settings.lease)
                        .ordering(settings.ordering);
                    let claim_span = tracing::info_span!(
                        "reliar.outbox.claim",
                        worker.id = %worker,
                        batch.requested = want,
                        batch.claimed = tracing::field::Empty,
                    );
                    let acquired = tokio::time::timeout(settings.store_timeout, store.acquire(request).instrument(claim_span.clone())).await;
                    match acquired {
                        Ok(Ok(batch)) => {
                            claim_span.record("batch.claimed", batch.records.len());
                            metrics.claimed(batch.records.len());
                            if !batch.poisoned.is_empty() {
                                for poisoned in &batch.poisoned {
                                    tracing::warn!(
                                        message.id = %poisoned.id,
                                        "reliar.outbox.dead: row undecodable, moved to dead by the store"
                                    );
                                }
                                metrics.dead(batch.poisoned.len(), DeadReason::Undecodable);
                            }
                            if batch.records.is_empty() && batch.poisoned.is_empty() {
                                next_poll_at = Instant::now() + settings.idle_poll_interval;
                            } else {
                                next_poll_at = Instant::now() + settings.poll_interval;
                                for record in batch.records {
                                    pending.push_back(PendingRow {
                                        message: record.message_ref(),
                                        message_type: record.envelope.message_type.clone(),
                                        attempts_before: record.attempts,
                                        envelope: record.envelope,
                                    });
                                }
                                spawn_ready(&mut pending, &mut in_flight, &mut outstanding, &publisher, &metrics, &semaphore, settings.publish_timeout);
                            }
                        }
                        Ok(Err(err)) => {
                            if err.kind() == FailureKind::Permanent {
                                exit_reason = Some(DispatchError::Store(err));
                                break 'run;
                            }
                            tracing::error!(error = %err, "reliar.outbox.claim failed; backing off");
                            next_poll_at = Instant::now() + settings.idle_poll_interval;
                        }
                        Err(_elapsed) => {
                            tracing::error!(
                                store_timeout_ms = settings.store_timeout.as_millis(),
                                "reliar.outbox.claim timed out; backing off"
                            );
                            next_poll_at = Instant::now() + settings.idle_poll_interval;
                        }
                    }
                }
            }
        }

        // Rows never started (their task has not yet acquired a concurrency permit) never
        // touched the broker; release them at once rather than start them now (S4 review).
        let never_started: Vec<MessageRef> = pending.drain(..).map(|row| row.message).collect();
        let not_yet_permitted: Vec<MessageRef> = {
            let mut refs = Vec::new();
            outstanding.retain(|_, task| {
                if task.started.load(AtomicOrdering::Relaxed) {
                    true
                } else {
                    task.abort.abort();
                    refs.push(task.message);
                    false
                }
            });
            refs
        };
        let release_immediately: Vec<MessageRef> =
            never_started.into_iter().chain(not_yet_permitted).collect();
        if !release_immediately.is_empty() {
            tracing::warn!(
                count = release_immediately.len(),
                "reliar.outbox: releasing rows that never started publishing — they will be \
                 reclaimed by the next owner (SRS §26.1)"
            );
            if let Err(err) = bounded(
                settings.store_timeout,
                store.release(&worker, &release_immediately),
            )
            .await
            {
                tracing::warn!(error = %err, "reliar.outbox: release of never-started rows failed; leases will expire naturally");
            }
        }

        // Drain publishes already in flight (already holding a permit) for at most
        // `drain_timeout`, persisting whatever resolves; release whatever failed or never
        // resolved. Run this regardless of `exit_reason` — a permanent store error still drains
        // best-effort before `run` returns `Err` (S4 review, ADR-lite K5).
        let drain_permanent_error = drain(
            &mut in_flight,
            &mut outstanding,
            &mut unwritten_complete,
            &mut unwritten_fail,
            &store,
            &worker,
            &retry,
            metrics.as_ref(),
            settings.drain_timeout,
            settings.store_timeout,
            settings.lease,
        )
        .await;
        // A permanent error already found by the main loop is the one that surfaces; a graceful
        // cancellation (`exit_reason` still `None`) that then meets a permanent error during
        // drain still ends in `Err` (M1) — silently swallowing it would be the exact failure M1
        // exists to prevent.
        if exit_reason.is_none() {
            exit_reason = drain_permanent_error.map(DispatchError::Store);
        }

        tracing::info!(worker.id = %worker, "reliar.outbox: dispatcher stopped");

        match exit_reason {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

/// Spawns every row currently in `pending`, moving each into `in_flight`/`outstanding`.
/// `pending` never holds more than the claim gate's `max_in_flight` allowance at the moment it
/// was populated (L1), so this drains it unconditionally rather than re-checking a capacity that
/// was already enforced at claim time.
fn spawn_ready<P, M>(
    pending: &mut VecDeque<PendingRow>,
    in_flight: &mut JoinSet<PublishTaskOutcome>,
    outstanding: &mut HashMap<TaskId, OutstandingTask>,
    publisher: &Arc<P>,
    metrics: &Arc<M>,
    semaphore: &Arc<Semaphore>,
    publish_timeout: Duration,
) where
    P: Publisher + Send + Sync + 'static,
    M: OutboxMetrics + Send + Sync + 'static,
{
    while let Some(row) = pending.pop_front() {
        let message = row.message;
        let started = Arc::new(AtomicBool::new(false));
        let job = PublishJob {
            message,
            message_type: row.message_type,
            attempts_before: row.attempts_before,
            envelope: row.envelope,
            publish_timeout,
        };
        let abort = in_flight.spawn(publish_one(
            job,
            Arc::clone(publisher),
            Arc::clone(metrics),
            Arc::clone(semaphore),
            Arc::clone(&started),
        ));
        outstanding.insert(
            abort.id(),
            OutstandingTask {
                message,
                started,
                abort,
            },
        );
    }
}

/// Drains in-flight publishes for at most `drain_timeout`, persists every outcome that
/// resolved, and releases whatever remains outstanding (§26.1, ADR 0014). Returns `Some` when an
/// outcome write is classified `Permanent` (M1) — the caller ends `run()` with that error.
#[allow(
    clippy::too_many_arguments,
    reason = "each parameter is a distinct piece of the run loop's state or a distinct timeout \
              budget (drain vs. store); bundling them would just move the count into a struct \
              with the same number of fields"
)]
async fn drain<S, M, R>(
    in_flight: &mut JoinSet<PublishTaskOutcome>,
    outstanding: &mut HashMap<TaskId, OutstandingTask>,
    unwritten_complete: &mut Vec<(Instant, CompletedMessage)>,
    unwritten_fail: &mut Vec<(Instant, FailedMessage)>,
    store: &S,
    worker: &WorkerId,
    retry: &R,
    metrics: &M,
    drain_timeout: Duration,
    store_timeout: Duration,
    lease: Duration,
) -> Option<S::Error>
where
    S: OutboxStore,
    M: OutboxMetrics,
    R: RetryPolicy,
{
    let drain_deadline = Instant::now() + drain_timeout;
    let mut permanent_error = None;

    'drain: loop {
        if in_flight.is_empty() || Instant::now() >= drain_deadline || permanent_error.is_some() {
            break 'drain;
        }
        tokio::select! {
            biased;
            Some(first) = in_flight.join_next_with_id() => {
                let mut results = vec![first];
                while let Some(next) = in_flight.try_join_next_with_id() {
                    results.push(next);
                }
                record_publish_results(results, outstanding, unwritten_complete, unwritten_fail, retry, metrics);
                permanent_error = retry_unwritten_outcomes(unwritten_complete, unwritten_fail, store, worker, store_timeout, lease).await.permanent;
            }
            () = tokio::time::sleep_until(drain_deadline) => {
                break 'drain;
            }
        }
    }

    // One last attempt to persist whatever resolved during the drain, unless a permanent error
    // already means no further attempt can help.
    if permanent_error.is_none() {
        permanent_error = retry_unwritten_outcomes(
            unwritten_complete,
            unwritten_fail,
            store,
            worker,
            store_timeout,
            lease,
        )
        .await
        .permanent;
    }

    // A row whose publish failed, or never resolved by the deadline, is released — the next
    // owner may retry it, including one already decided `Dead`: the row is released with its
    // `dead_at`/`dead_reason` never written, so the next owner's claim (or, if it somehow still
    // looks pending, its own retry policy) gets one more attempt at recording that outcome. A row
    // whose publish **succeeded** but whose `complete` still never landed is different: it is
    // left to its lease rather than released, because releasing it would turn a possible
    // duplicate into a certain one for a message already delivered (L3, SRS §23.2).
    if !unwritten_complete.is_empty() {
        tracing::warn!(
            count = unwritten_complete.len(),
            "reliar.outbox: rows published successfully but not yet marked complete are left to \
             their lease rather than released (SRS §23.2)"
        );
    }
    let mut release_now: Vec<MessageRef> = outstanding.values().map(|task| task.message).collect();
    release_now.extend(unwritten_fail.drain(..).map(|(_, failed)| failed.message));
    if !release_now.is_empty() {
        tracing::warn!(
            count = release_now.len(),
            "reliar.outbox: releasing rows whose publish failed or never resolved by the drain \
             timeout — the next owner will retry them (SRS §22.1, §26.1)"
        );
        if let Err(err) = bounded(store_timeout, store.release(worker, &release_now)).await {
            tracing::warn!(error = %err, "reliar.outbox: release on drain failed; leases will expire naturally");
        }
    }

    permanent_error
}

/// The outcome of one [`retry_unwritten_outcomes`] round: whether `run()` must end with a
/// permanent store error (M1), and whether the caller should arm the outcome-retry pacing before
/// its next attempt.
///
/// `any_failed` is `true` only when a `complete`/`fail` call actually returned an error this
/// round — never on a success, and never merely because the round had nothing to attempt: the
/// caller arms pacing only after a genuine failure, so a healthy store is never throttled.
struct RetryOutcome<E> {
    permanent: Option<E>,
    any_failed: bool,
}

/// [`retry_unwritten_outcomes`], gated behind `due_at`: the store is never touched until this
/// deadline passes, so a persistently *fast*-failing `complete`/`fail` (no hang, an immediate
/// `Err`) cannot retry faster than once per `due_at` step. The sleep has no side effect, so a
/// competing `select!` branch (the lease tick, above) can still preempt this one before `due_at`
/// elapses.
async fn retry_unwritten_outcomes_when_due<S: OutboxStore>(
    unwritten_complete: &mut Vec<(Instant, CompletedMessage)>,
    unwritten_fail: &mut Vec<(Instant, FailedMessage)>,
    store: &S,
    worker: &WorkerId,
    store_timeout: Duration,
    lease: Duration,
    due_at: Instant,
) -> RetryOutcome<S::Error> {
    tokio::time::sleep_until(due_at).await;
    retry_unwritten_outcomes(
        unwritten_complete,
        unwritten_fail,
        store,
        worker,
        store_timeout,
        lease,
    )
    .await
}

/// Attempts to persist every unwritten outcome, keeping whatever fails or times out for the next
/// attempt rather than dropping it — a row with a resolved publish but no persisted outcome stays
/// `outstanding` in a pending-outcome state (L2, ADR 0008, SRS §23.2). Two exceptions:
///
/// - **M1**: a write error [`Classify`]-ed [`FailureKind::Permanent`] is returned immediately
///   instead of retried — no amount of retrying can help, and the caller ends `run()` with it.
/// - **M2**: an entry retried for longer than `lease` is dropped (never released — just no
///   longer tracked or lease-renewed), so its lease lapses and another worker reclaims the row.
///   Dropping without also excluding it from lease renewal would leave a row nobody owns and
///   nobody can claim; this is why both live in the same function.
async fn retry_unwritten_outcomes<S: OutboxStore>(
    unwritten_complete: &mut Vec<(Instant, CompletedMessage)>,
    unwritten_fail: &mut Vec<(Instant, FailedMessage)>,
    store: &S,
    worker: &WorkerId,
    store_timeout: Duration,
    lease: Duration,
) -> RetryOutcome<S::Error> {
    let mut any_failed = false;

    if !unwritten_complete.is_empty() {
        let items: Vec<CompletedMessage> =
            unwritten_complete.iter().map(|(_, c)| c.clone()).collect();
        let count = items.len();
        match bounded(store_timeout, store.complete(worker, &items)).await {
            Ok(_affected) => unwritten_complete.clear(),
            Err(TimedOut::Store(err)) if err.kind() == FailureKind::Permanent => {
                return RetryOutcome {
                    permanent: Some(err),
                    any_failed: true,
                };
            }
            Err(err) => {
                // `warn`, not `error` (S4 review 5, minor): expected and retried on its own
                // schedule (`next_outcome_retry_at`), not an operator-actionable event by itself.
                tracing::warn!(error = %err, count, "reliar.outbox.complete failed; retrying at the next scheduled attempt");
                any_failed = true;
            }
        }
    }
    expire_past_lease(unwritten_complete, lease, "complete");

    if !unwritten_fail.is_empty() {
        let items: Vec<FailedMessage> = unwritten_fail.iter().map(|(_, f)| f.clone()).collect();
        let count = items.len();
        match bounded(store_timeout, store.fail(worker, &items)).await {
            Ok(_affected) => unwritten_fail.clear(),
            Err(TimedOut::Store(err)) if err.kind() == FailureKind::Permanent => {
                return RetryOutcome {
                    permanent: Some(err),
                    any_failed: true,
                };
            }
            Err(err) => {
                tracing::warn!(error = %err, count, "reliar.outbox.fail failed; retrying at the next scheduled attempt");
                any_failed = true;
            }
        }
    }
    expire_past_lease(unwritten_fail, lease, "fail");

    RetryOutcome {
        permanent: None,
        any_failed,
    }
}

/// Drops entries whose outcome write has been retried for longer than `lease` (M2): the row's
/// lease will lapse (it is excluded from further renewal the moment it is dropped here) and
/// another worker will reclaim it — the same §22.1 duplicate window already documented, not a
/// new one.
///
/// Measured on **this worker's own clock** (`Instant`, the same one the lease ticker and
/// `store_timeout` already use), not a round trip to the store: ownership of a row stays
/// DB-authoritative throughout (only `locked_until`, set and checked by the store, ever decides
/// who may claim a row) — this age is purely a local bound on how long *this worker* keeps
/// retrying before it gives up and stops renewing, never a claim about the row's actual
/// DB-side expiry.
fn expire_past_lease<T>(unwritten: &mut Vec<(Instant, T)>, lease: Duration, op: &'static str) {
    let now = Instant::now();
    let expired = unwritten
        .iter()
        .filter(|(since, _)| now.saturating_duration_since(*since) > lease)
        .count();
    if expired > 0 {
        tracing::warn!(
            op,
            count = expired,
            "reliar.outbox: giving up on an outcome write retried longer than the lease; the \
             row's lease will lapse and another worker may reclaim it (SRS §22.1, M2)"
        );
        unwritten.retain(|(since, _)| now.saturating_duration_since(*since) <= lease);
    }
}

/// Awaits [`tokio::time::Interval::tick`] on `interval` when `Some`, or never resolves when
/// `None` — the clean way to make a `stats_interval = Duration::ZERO` disable the tick entirely
/// inside a `tokio::select!` branch without a guard expression.
async fn tick_optional(interval: &mut Option<tokio::time::Interval>) {
    match interval {
        Some(interval) => {
            interval.tick().await;
        }
        None => std::future::pending::<()>().await,
    }
}

/// Runs `fut` bounded by `budget`. `Err` (the caller logs it like any other store failure) for a
/// timeout as much as for the store's own error — a hung statement is a transient failure, no
/// different in effect (SRS §26.1, S4 review — `store_timeout`).
async fn bounded<T, E, F>(budget: Duration, fut: F) -> Result<T, TimedOut<E>>
where
    F: Future<Output = Result<T, E>>,
{
    match tokio::time::timeout(budget, fut).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => Err(TimedOut::Store(err)),
        Err(_elapsed) => Err(TimedOut::Timeout(budget)),
    }
}

/// Either the wrapped call's own error, or `bounded` giving up first.
enum TimedOut<E> {
    Store(E),
    Timeout(Duration),
}

impl<E: fmt::Display> fmt::Display for TimedOut<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(err) => write!(f, "{err}"),
            Self::Timeout(budget) => write!(f, "timed out after {budget:?}"),
        }
    }
}

/// Renews the lease for every row still outstanding. Best-effort: a shortfall means another
/// worker already reclaimed some of these rows, which is benign, never an error (§19.2, §21.1).
async fn renew_leases<S: OutboxStore>(
    store: &S,
    worker: &WorkerId,
    outstanding: &[MessageRef],
    lease: Duration,
    store_timeout: Duration,
) {
    match bounded(
        store_timeout,
        store.extend_lease(worker, outstanding, lease),
    )
    .await
    {
        Ok(affected) => {
            let affected = usize::try_from(affected).unwrap_or(usize::MAX);
            if affected < outstanding.len() {
                tracing::warn!(
                    affected,
                    outstanding = outstanding.len(),
                    "reliar.outbox: extend_lease renewed fewer rows than outstanding; some rows' \
                     lease may already be reclaimed by another worker"
                );
            }
        }
        Err(err) => {
            tracing::warn!(error = %err, "reliar.outbox: extend_lease failed; in-flight rows may lose their lease");
        }
    }
}

/// Polls [`OutboxStore::stats`] and feeds the outbox-lag and backlog gauges. The dispatcher is
/// the **sole** caller of these three hooks — a host that never calls `stats()` itself still
/// gets the gauges (§33.1).
async fn report_stats<S: OutboxStore, M: OutboxMetrics>(
    store: &S,
    metrics: &M,
    store_timeout: Duration,
) {
    match bounded(store_timeout, store.stats()).await {
        Ok(stats) => {
            metrics.pending(stats.pending);
            metrics.expired_pending(stats.expired_pending);
            if let Some(lag) = stats.lag() {
                metrics.oldest_pending_age(lag);
            }
        }
        Err(err) => {
            tracing::warn!(error = %err, "reliar.outbox: stats poll failed");
        }
    }
}

/// One resolved publish attempt, produced by [`publish_one`] and consumed only by this module's
/// own loop.
enum PublishTaskOutcome {
    /// The publish succeeded.
    Published {
        /// The row that published successfully.
        message: MessageRef,
        /// Fed to [`OutboxMetrics::published`].
        message_type: MessageType,
    },
    /// The publish failed or timed out.
    Failed {
        /// The row whose publish failed.
        message: MessageRef,
        /// The observed-outcome count **before** this failure (SRS §23.1).
        attempts_before: u32,
        /// Whether a retry can help.
        kind: FailureKind,
        /// The failure's `Display` output (or, for a timeout, a synthetic message) — never
        /// payload bytes or header values (§33).
        error: String,
    },
}

/// The owned inputs one spawned [`publish_one`] task needs. Bundled into a struct rather than
/// passed positionally so the function itself stays under clippy's argument-count lint without
/// resorting to an `#[allow]`.
struct PublishJob {
    message: MessageRef,
    message_type: MessageType,
    attempts_before: u32,
    envelope: SerializedEnvelope,
    publish_timeout: Duration,
}

/// Publishes one envelope: waits for a concurrency permit (flipping `started` to `true` once
/// acquired — a best-effort signal cancellation uses to tell "likely already publishing" from
/// "never began", S4 review), applies [`DispatcherSettings::publish_timeout`], and reports the
/// outcome. Never panics — a closed semaphore (this dispatcher never closes its own) is treated
/// as a transient failure rather than an unwrap.
#[tracing::instrument(
    name = "reliar.outbox.publish",
    skip_all,
    fields(
        message.id = %job.message.id,
        message.type = %job.message_type,
        attempt = job.attempts_before + 1
    )
)]
async fn publish_one<P, M>(
    job: PublishJob,
    publisher: Arc<P>,
    metrics: Arc<M>,
    semaphore: Arc<Semaphore>,
    started: Arc<AtomicBool>,
) -> PublishTaskOutcome
where
    P: Publisher,
    M: OutboxMetrics,
{
    let PublishJob {
        message,
        message_type,
        attempts_before,
        envelope,
        publish_timeout,
    } = job;

    let _permit = match semaphore.acquire_owned().await {
        Ok(permit) => permit,
        Err(_closed) => {
            return PublishTaskOutcome::Failed {
                message,
                attempts_before,
                kind: FailureKind::Transient,
                error: "reliar-outbox: publish semaphore closed unexpectedly".to_string(),
            };
        }
    };
    started.store(true, AtomicOrdering::Relaxed);

    let publish_started_at = Instant::now();
    let outcome = tokio::time::timeout(publish_timeout, publisher.publish(&envelope)).await;
    metrics.publish_duration(publish_started_at.elapsed(), &message_type);

    match outcome {
        Ok(Ok(())) => PublishTaskOutcome::Published {
            message,
            message_type,
        },
        Ok(Err(err)) => {
            let kind = err.kind();
            PublishTaskOutcome::Failed {
                message,
                attempts_before,
                kind,
                error: err.to_string(),
            }
        }
        Err(_elapsed) => PublishTaskOutcome::Failed {
            message,
            attempts_before,
            kind: FailureKind::Transient,
            error: format!("reliar-outbox: publish timed out after {publish_timeout:?}"),
        },
    }
}

/// Turns a batch of resolved [`publish_one`] results into pending `complete`/`fail` writes
/// (appended to `unwritten_complete`/`unwritten_fail`, actually persisted by
/// [`retry_unwritten_outcomes`]), applying `retry` to every failure and feeding [`OutboxMetrics`]
/// along the way.
///
/// A task that panicked (`Err(JoinError)`) is logged at `error` and its row removed from
/// `outstanding` so it is not renewed forever — the row's lease simply expires and is reclaimed
/// (§19.2, S4 review). A task **we** aborted (never started, or dropped at drain) reports a
/// cancelled `JoinError` too, but that is expected shutdown behaviour, not a fault — logged at
/// `debug` instead (S4 review 3, minor).
fn record_publish_results<M, R>(
    results: Vec<Result<(TaskId, PublishTaskOutcome), JoinError>>,
    outstanding: &mut HashMap<TaskId, OutstandingTask>,
    unwritten_complete: &mut Vec<(Instant, CompletedMessage)>,
    unwritten_fail: &mut Vec<(Instant, FailedMessage)>,
    retry: &R,
    metrics: &M,
) where
    M: OutboxMetrics,
    R: RetryPolicy,
{
    for joined in results {
        let outcome = match joined {
            Ok((id, outcome)) => {
                outstanding.remove(&id);
                outcome
            }
            Err(join_error) => {
                outstanding.remove(&join_error.id());
                if join_error.is_panic() {
                    tracing::error!(
                        error = %join_error,
                        "reliar.outbox.publish task panicked; its lease will expire and the row will be reclaimed"
                    );
                } else {
                    tracing::debug!(
                        error = %join_error,
                        "reliar.outbox.publish task was aborted before completing (shutdown); its lease will expire and the row will be reclaimed"
                    );
                }
                continue;
            }
        };

        match outcome {
            PublishTaskOutcome::Published {
                message,
                message_type,
            } => {
                metrics.published(1, &message_type);
                unwritten_complete.push((Instant::now(), CompletedMessage::new(message)));
            }
            PublishTaskOutcome::Failed {
                message,
                attempts_before,
                kind,
                error,
            } => {
                let outcome = retry.next(attempts_before, kind);
                match &outcome {
                    FailureOutcome::Retry { delay } => {
                        tracing::debug!(
                            message.id = %message.id,
                            attempt = attempts_before + 1,
                            delay_ms = delay.as_millis(),
                            "reliar.outbox.retry: scheduled"
                        );
                        metrics.retried(1, kind);
                    }
                    FailureOutcome::Dead { reason } => {
                        tracing::warn!(
                            message.id = %message.id,
                            attempt = attempts_before + 1,
                            reason = ?reason,
                            "reliar.outbox.dead: message moved to dead"
                        );
                        metrics.dead(1, *reason);
                    }
                }
                unwritten_fail.push((Instant::now(), FailedMessage::new(message, error, outcome)));
            }
        }
    }
}
