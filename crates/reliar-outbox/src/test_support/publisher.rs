//! [`RecordingPublisher`] and [`ScriptedPublisher`]: [`Publisher`] fakes for dispatcher tests
//! (§43.A.18, §43.A.23).

use core::fmt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use reliar_core::{MessageId, SerializedEnvelope};

use crate::publisher::{Classify, FailureKind, Publisher};

/// Records every publish, **in order, with duplicates** — duplicates are the assertion, not a
/// bug: they are what a crash-after-publish or a reclaimed lease produces (SRS §22). Never fails.
///
/// **Timer-free by default**: [`Self::default`] never awaits a timer, so it never panics outside
/// a Tokio runtime with time support and never costs wall-clock time in a test that does not need
/// concurrency proof. [`Self::with_concurrency_probe`] opts into a paused-clock delay so several
/// concurrently spawned publishes can overlap and raise [`Self::in_flight_peak`] — without it,
/// every call would run to completion on its first poll with nothing to interleave against.
#[derive(Clone, Debug, Default)]
pub struct RecordingPublisher {
    inner: Arc<Mutex<RecordingInner>>,
    concurrency_probe: Option<Duration>,
}

#[derive(Debug, Default)]
struct RecordingInner {
    published: Vec<MessageId>,
    envelopes: Vec<SerializedEnvelope>,
    in_flight: usize,
    in_flight_peak: usize,
}

impl RecordingPublisher {
    /// A publisher whose `publish` parks on a paused Tokio timer for `delay` before completing —
    /// enough for several concurrently spawned publishes to all be in flight at once under
    /// `#[tokio::test(start_paused = true)]`. [`Self::default`] never sleeps; use this
    /// constructor only in a concurrency test.
    #[must_use]
    pub fn with_concurrency_probe(delay: Duration) -> Self {
        Self {
            inner: Arc::default(),
            concurrency_probe: Some(delay),
        }
    }

    fn lock(&self) -> MutexGuard<'_, RecordingInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Every published envelope's id, in call order, duplicates included. An id is recorded on
    /// the **first poll** of the future [`Publisher::publish`] returns — not when `publish` is
    /// called (the future may never be polled) and not when it completes (a slow or hung publish
    /// still counts as attempted).
    #[must_use]
    pub fn published(&self) -> Vec<MessageId> {
        self.lock().published.clone()
    }

    /// How many times `id` was published — `2` proves the duplicate window (SRS §22).
    #[must_use]
    pub fn count(&self, id: MessageId) -> usize {
        self.lock()
            .published
            .iter()
            .filter(|&&seen| seen == id)
            .count()
    }

    /// Every published envelope, in call order, duplicates included. Recorded on the same
    /// first poll as [`Self::published`], not at completion.
    #[must_use]
    pub fn envelopes(&self) -> Vec<SerializedEnvelope> {
        self.lock().envelopes.clone()
    }

    /// The high-water mark of concurrently in-flight `publish` calls — asserts
    /// `max_in_flight` is never exceeded (§43.A.23). Only [`Self::with_concurrency_probe`] can
    /// ever raise this past `1`.
    #[must_use]
    pub fn in_flight_peak(&self) -> usize {
        self.lock().in_flight_peak
    }
}

impl Publisher for RecordingPublisher {
    type Error = FakePublishError;

    fn publish(
        &self,
        envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let id = envelope.id;
        let envelope = envelope.clone();
        let probe = self.concurrency_probe;
        async move {
            // Everything up to the first internal `.await` runs on the future's first poll —
            // `async move` bodies are lazy, so this never runs merely because `publish` was
            // called (M4): both `published` and `envelopes` are recorded here, not at
            // completion, so a hung or never-finished publish still counts as attempted.
            {
                let mut guard = inner.lock().unwrap_or_else(PoisonError::into_inner);
                guard.in_flight += 1;
                guard.in_flight_peak = guard.in_flight_peak.max(guard.in_flight);
                guard.published.push(id);
                guard.envelopes.push(envelope);
            }

            if let Some(delay) = probe {
                tokio::time::sleep(delay).await;
            }

            inner
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .in_flight -= 1;
            Ok(())
        }
    }
}

/// One scripted publish outcome. `Hang` drives a `publish_timeout` test: the publish never
/// returns an error, it just takes `Duration` before resolving `Ok`.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum PublishStep {
    /// Publishes successfully.
    Ok,
    /// Fails with [`FakePublishError::Transient`].
    Transient,
    /// Fails with [`FakePublishError::Permanent`].
    Permanent,
    /// Resolves `Ok` only after this much (paused) time — drives `publish_timeout`.
    Hang(Duration),
    /// Panics instead of returning — drives a test asserting that a panicking publish task does
    /// not leave its row's lease renewed forever (S4 review, blocker 2).
    Panic,
}

/// Replays a script of outcomes, one per publish call.
#[derive(Clone, Debug)]
pub struct ScriptedPublisher {
    inner: Arc<Mutex<ScriptedInner>>,
}

#[derive(Debug)]
enum Script {
    /// Positional script, consumed in call order; cycles the last entry once exhausted.
    /// **Deterministic only with `max_in_flight = 1`** — with concurrent publishes the call
    /// order is a race.
    Positional {
        steps: Vec<PublishStep>,
        next: usize,
    },
    /// Per-message outcomes, order-independent and safe at any `max_in_flight`. A message id not
    /// present in the map publishes [`PublishStep::Ok`].
    Keyed(HashMap<MessageId, PublishStep>),
}

#[derive(Debug)]
struct ScriptedInner {
    script: Script,
    published: Vec<MessageId>,
}

impl ScriptedPublisher {
    /// Positional script, consumed in call order.
    #[must_use]
    pub fn new(script: impl IntoIterator<Item = PublishStep>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ScriptedInner {
                script: Script::Positional {
                    steps: script.into_iter().collect(),
                    next: 0,
                },
                published: Vec::new(),
            })),
        }
    }

    /// Per-message outcomes, order-independent and safe at any `max_in_flight`.
    #[must_use]
    pub fn keyed(steps: impl IntoIterator<Item = (MessageId, PublishStep)>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ScriptedInner {
                script: Script::Keyed(steps.into_iter().collect()),
                published: Vec::new(),
            })),
        }
    }

    /// The same outcome for every publish, at any concurrency.
    #[must_use]
    pub fn always(step: PublishStep) -> Self {
        Self::new(std::iter::once(step))
    }

    /// Every published envelope's id, in call order. An id is recorded on the **first poll** of
    /// the future [`Publisher::publish`] returns — the same point [`RecordingPublisher`] uses
    /// (M4) — not when `publish` is called and not when it completes.
    #[must_use]
    pub fn published(&self) -> Vec<MessageId> {
        self.lock().published.clone()
    }

    fn lock(&self) -> MutexGuard<'_, ScriptedInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records the call and picks this call's step. Synchronous: the lock never crosses an
    /// `.await` — the caller awaits the returned step's outcome, not this lookup.
    fn step_for(inner: &Mutex<ScriptedInner>, id: MessageId) -> PublishStep {
        let mut guard = inner.lock().unwrap_or_else(PoisonError::into_inner);
        guard.published.push(id);
        match &mut guard.script {
            Script::Positional { steps, next } => {
                let Some(last) = steps.len().checked_sub(1) else {
                    return PublishStep::Ok;
                };
                let index = (*next).min(last);
                *next = next.saturating_add(1);
                steps[index]
            }
            Script::Keyed(steps) => steps.get(&id).copied().unwrap_or(PublishStep::Ok),
        }
    }
}

impl Publisher for ScriptedPublisher {
    type Error = FakePublishError;

    #[allow(
        clippy::panic,
        reason = "PublishStep::Panic exists to simulate a publish task crashing mid-flight                   (S4 review, blocker 2) — the panic is the fake's whole purpose here, not an                   accident"
    )]
    fn publish(
        &self,
        envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let inner = Arc::clone(&self.inner);
        let id = envelope.id;
        async move {
            // `async move` bodies are lazy: this only runs on the future's first poll, not when
            // `publish` is called (M4).
            let step = Self::step_for(&inner, id);
            match step {
                PublishStep::Ok => Ok(()),
                PublishStep::Transient => Err(FakePublishError::Transient {
                    detail: "scripted transient failure",
                }),
                PublishStep::Permanent => Err(FakePublishError::Permanent {
                    detail: "scripted permanent failure",
                }),
                PublishStep::Hang(duration) => {
                    tokio::time::sleep(duration).await;
                    Ok(())
                }
                PublishStep::Panic => panic!("test-support: scripted publish panic"),
            }
        }
    }
}

/// A scripted publish failure, self-classifying via [`Classify`].
#[derive(Debug)]
#[non_exhaustive]
pub enum FakePublishError {
    /// May succeed on retry.
    Transient {
        /// Why this call was scripted to fail.
        detail: &'static str,
    },
    /// Cannot succeed on retry.
    Permanent {
        /// Why this call was scripted to fail.
        detail: &'static str,
    },
}

impl fmt::Display for FakePublishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transient { detail } => {
                write!(f, "scripted publish failure (transient): {detail}")
            }
            Self::Permanent { detail } => {
                write!(f, "scripted publish failure (permanent): {detail}")
            }
        }
    }
}

impl std::error::Error for FakePublishError {}

impl Classify for FakePublishError {
    fn kind(&self) -> FailureKind {
        match self {
            Self::Transient { .. } => FailureKind::Transient,
            Self::Permanent { .. } => FailureKind::Permanent,
        }
    }
}
