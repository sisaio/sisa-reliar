//! `NatsPublisher` — a JetStream-backed `Publisher` that awaits the server's ack before
//! returning `Ok` (SRS §19.4, §22, ADR 0028, contract §4).

use core::fmt;

use async_nats::Subject;
use async_nats::jetstream::Context;
use async_nats::jetstream::context::{PublishAckFuture, PublishError, PublishErrorKind};
use reliar_core::{EnvelopeMapper, Publisher, SerializedEnvelope};
use tracing::Instrument;

use crate::error::{NatsConfigError, NatsPublishError};
use crate::mapper::{NatsEnvelopeMapper, NatsWireMessage};
use crate::settings::NatsSettings;
use crate::subject::{PrefixSubjects, SubjectResolver};

/// An at-least-once [`Publisher`] over `JetStream`: encodes with [`NatsEnvelopeMapper`], resolves
/// the subject with `R`, publishes, and **awaits the server ack** before returning `Ok`
/// (ADR 0028).
///
/// # Guarantees
///
/// - `Ok` means the stream holds the message — never merely "written to a socket".
/// - `Nats-Msg-Id` lets `JetStream` suppress a duplicate republished inside the stream's
///   `duplicate_window`; outside it the duplicate is stored. This narrows SRS §22's duplicate
///   window; it does not close it, and this type makes no exactly-once claim.
/// - It never creates a stream, never connects, and never reads the environment (ADR 0029).
///
/// # Cancellation
///
/// Dropping a [`publish`](Publisher::publish) or [`publish_batch`](Publisher::publish_batch)
/// future — a cancelled dispatcher, a drain deadline — stops this process awaiting the ack. It
/// does **not** unsend bytes already on the wire: the stream may store the message while Reliar
/// records no outcome, so the outbox row stays claimable and the message is published again. That
/// is SRS §22's duplicate window; `Nats-Msg-Id` lets `JetStream` collapse the repeat *inside* the
/// stream's `duplicate_window`, and nothing collapses it outside. This type is **at-least-once**
/// and makes no exactly-once claim.
#[derive(Clone)]
pub struct NatsPublisher<R = PrefixSubjects> {
    context: Context,
    mapper: NatsEnvelopeMapper,
    resolver: R,
    settings: NatsSettings,
}

/// Prints the settings and the resolver — **never** the `async_nats::jetstream::Context`, whose
/// own `Debug` belongs to `async-nats` and may render the server address (a credentialed
/// `nats://user:pass@host` is exactly what §17.1 keeps out of logs).
impl<R: fmt::Debug> fmt::Debug for NatsPublisher<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NatsPublisher")
            .field("settings", &self.settings)
            .field("resolver", &self.resolver)
            .finish_non_exhaustive()
    }
}

impl NatsPublisher<PrefixSubjects> {
    /// Wraps an application-owned `JetStream` context, resolving subjects with a
    /// [`PrefixSubjects`] built from `settings.subject_prefix`.
    ///
    /// # Errors
    ///
    /// Returns [`NatsConfigError`] for a zero `batch_pipeline_depth`, a zero `publish_timeout`, a
    /// `max_payload` of `Some(0)`, or a `subject_prefix` that is not a legal subject prefix.
    pub fn new(context: Context, settings: NatsSettings) -> Result<Self, NatsConfigError> {
        let resolver = PrefixSubjects::new(settings.subject_prefix.clone())?;
        Self::build(context, settings, resolver)
    }
}

impl<R: SubjectResolver> NatsPublisher<R> {
    /// Same, with an explicit resolver. `settings.subject_prefix` is then **unused** — `R` owns
    /// subject selection entirely.
    ///
    /// # Errors
    ///
    /// Returns [`NatsConfigError`] for a zero `batch_pipeline_depth`, a zero `publish_timeout`, or a
    /// `max_payload` of `Some(0)`.
    pub fn with_resolver(
        context: Context,
        settings: NatsSettings,
        resolver: R,
    ) -> Result<Self, NatsConfigError> {
        Self::build(context, settings, resolver)
    }

    fn build(
        context: Context,
        settings: NatsSettings,
        resolver: R,
    ) -> Result<Self, NatsConfigError> {
        if settings.batch_pipeline_depth == 0 {
            return Err(NatsConfigError::ZeroBatchPipelineDepth);
        }
        if settings.publish_timeout.is_zero() {
            return Err(NatsConfigError::ZeroPublishTimeout);
        }
        if settings.max_payload == Some(0) {
            return Err(NatsConfigError::ZeroMaxPayload);
        }
        Ok(Self {
            context,
            mapper: NatsEnvelopeMapper,
            resolver,
            settings,
        })
    }

    /// The settings in force.
    #[must_use]
    pub fn settings(&self) -> &NatsSettings {
        &self.settings
    }

    /// Resolves the subject, encodes the envelope, and applies the pre-flight `max_payload`
    /// guard — every step of contract §4.2 that can fail without touching the network.
    fn prepare(
        &self,
        envelope: &SerializedEnvelope,
    ) -> Result<(Subject, NatsWireMessage), NatsPublishError> {
        let subject = self
            .resolver
            .subject(envelope)
            .map_err(|err| NatsPublishError::Subject {
                source: Box::new(err),
            })?;
        let wire = self
            .mapper
            .encode(envelope)
            .map_err(NatsPublishError::Map)?;
        // `async-nats` already rejects a payload over the server's own advertised limit locally,
        // before any I/O; this guard only buys a limit *below* the server's (RELIAR-37, M6).
        if let Some(limit) = self.settings.max_payload {
            let len = wire.wire_len();
            if len > limit {
                return Err(NatsPublishError::PayloadTooLarge { len, limit });
            }
        }
        Ok((subject, wire))
    }
}

impl<R: SubjectResolver> Publisher for NatsPublisher<R> {
    type Error = NatsPublishError;

    async fn publish(&self, envelope: &SerializedEnvelope) -> Result<(), NatsPublishError> {
        let (subject, wire) = self.prepare(envelope)?;
        let span = tracing::debug_span!(
            "reliar.transport_nats.publish",
            message.id = %envelope.id,
            message.type = %envelope.message_type,
            subject = %subject,
            jetstream.sequence = tracing::field::Empty,
            jetstream.duplicate = tracing::field::Empty,
        );
        publish_prepared(&self.context, subject, wire, self.settings.publish_timeout)
            .instrument(span)
            .await
    }

    /// Pipelines sends in `settings.batch_pipeline_depth`-sized windows, awaiting each window's
    /// acks positionally before opening the next, so returned results line up with `envelopes`.
    ///
    /// The v0.1 `reliar-outbox` `OutboxDispatcher` calls [`publish`](Publisher::publish) only, one
    /// envelope at a time — this override and `batch_pipeline_depth` are reachable today solely
    /// through a caller that invokes `publish_batch` directly; a dispatcher batch path is tracked
    /// separately (RELIAR-39).
    async fn publish_batch(
        &self,
        envelopes: &[SerializedEnvelope],
    ) -> Vec<Result<(), NatsPublishError>> {
        let span = tracing::info_span!(
            "reliar.transport_nats.publish_batch",
            batch.size = envelopes.len(),
            windows = tracing::field::Empty,
        );
        async move {
            // Every result eventually lands in `outcomes` tagged by its original index — either
            // immediately (a `prepare` failure) or after its window completes — and is sorted
            // back into place at the end. Collecting by index rather than writing into a
            // pre-sized positional slot means there is no slot to have "never been written",
            // which is what lets this stay panic-free (no unwrap/expect/index-out-of-bounds path)
            // while still proving the length/positional invariant below.
            let mut outcomes: Vec<(usize, Result<(), NatsPublishError>)> =
                Vec::with_capacity(envelopes.len());
            let mut prepared: Vec<(usize, Subject, NatsWireMessage)> = Vec::new();

            for (i, envelope) in envelopes.iter().enumerate() {
                match self.prepare(envelope) {
                    Ok((subject, wire)) => prepared.push((i, subject, wire)),
                    Err(err) => outcomes.push((i, Err(err))),
                }
            }

            let mut window_count = 0usize;
            while !prepared.is_empty() {
                window_count += 1;
                let take = prepared.len().min(self.settings.batch_pipeline_depth);
                let window: Vec<(usize, Subject, NatsWireMessage)> =
                    prepared.drain(..take).collect();
                let window_start = tokio::time::Instant::now();
                let deadline = window_start + self.settings.publish_timeout;

                let sent = issue_window_sends(&self.context, window, deadline, window_start).await;
                outcomes.extend(await_window_acks(sent, deadline, window_start).await);
            }

            tracing::Span::current().record("windows", window_count);
            outcomes.sort_by_key(|(i, _)| *i);
            outcomes.into_iter().map(|(_, result)| result).collect()
        }
        .instrument(span)
        .await
    }
}

/// Sends `wire` to `subject` and awaits the server's ack, both stages inside one `timeout`
/// (ADR 0028). A free function (not a method) so it never captures `&self` across the awaited
/// future. `context` is **borrowed** — `Context` is cheaply `Clone`, but nothing here needs an
/// owned copy, so this avoids a clone per publish (review m4).
async fn publish_prepared(
    context: &Context,
    subject: Subject,
    wire: NatsWireMessage,
    publish_timeout: std::time::Duration,
) -> Result<(), NatsPublishError> {
    let start = tokio::time::Instant::now();
    let (headers, payload) = wire.into_parts();
    let outcome = tokio::time::timeout(publish_timeout, async {
        let ack_future = context
            .publish_with_headers(subject.clone(), headers, payload)
            .await
            .map_err(|err| classify_publish_error(&subject, &err, start))?;
        ack_future
            .await
            .map_err(|err| classify_publish_error(&subject, &err, start))
    })
    .await;

    match outcome {
        Ok(Ok(ack)) => {
            let span = tracing::Span::current();
            span.record("jetstream.sequence", ack.sequence);
            span.record("jetstream.duplicate", ack.duplicate);
            Ok(())
        }
        Ok(Err(err)) => Err(err),
        Err(_elapsed) => Err(NatsPublishError::Timeout {
            subject,
            after_ms: elapsed_ms(start),
        }),
    }
}

/// Issues every send in `window` before any ack is awaited (ADR 0028 §3), each bounded against
/// the shared window `deadline`. `window_start` is the instant the window began, used to report
/// the **measured** elapsed time on a `Timeout` rather than the configured setting (review m1).
async fn issue_window_sends(
    context: &Context,
    window: Vec<(usize, Subject, NatsWireMessage)>,
    deadline: tokio::time::Instant,
    window_start: tokio::time::Instant,
) -> Vec<(usize, Subject, Result<PublishAckFuture, NatsPublishError>)> {
    let mut sent = Vec::with_capacity(window.len());
    for (i, subject, wire) in window {
        let (headers, payload) = wire.into_parts();
        let outcome = match tokio::time::timeout_at(
            deadline,
            context.publish_with_headers(subject.clone(), headers, payload),
        )
        .await
        {
            Ok(Ok(ack_future)) => Ok(ack_future),
            Ok(Err(err)) => Err(classify_publish_error(&subject, &err, window_start)),
            Err(_elapsed) => Err(NatsPublishError::Timeout {
                subject: subject.clone(),
                after_ms: elapsed_ms(window_start),
            }),
        };
        sent.push((i, subject, outcome));
    }
    sent
}

/// Awaits every send's ack positionally, each bounded against the same window `deadline` so a
/// slow neighbour never discards or delays an ack already outstanding for another index
/// (contract §4.2; "a failing ack never affects its neighbours' verdicts").
async fn await_window_acks(
    sent: Vec<(usize, Subject, Result<PublishAckFuture, NatsPublishError>)>,
    deadline: tokio::time::Instant,
    window_start: tokio::time::Instant,
) -> Vec<(usize, Result<(), NatsPublishError>)> {
    let mut outcomes = Vec::with_capacity(sent.len());
    for (i, subject, outcome) in sent {
        let result = match outcome {
            Ok(ack_future) => match tokio::time::timeout_at(deadline, ack_future).await {
                Ok(Ok(_ack)) => Ok(()),
                Ok(Err(err)) => Err(classify_publish_error(&subject, &err, window_start)),
                Err(_elapsed) => Err(NatsPublishError::Timeout {
                    subject,
                    after_ms: elapsed_ms(window_start),
                }),
            },
            Err(err) => Err(err),
        };
        outcomes.push((i, result));
    }
    outcomes
}

/// Maps an `async-nats` publish failure to its fixed verdict (ADR 0030's table). `Other`, and any
/// kind this crate does not otherwise recognise, become [`NatsPublishError::Broker`] and are
/// logged once at `warn` with the subject and a bounded kind name only — **never** the
/// `async-nats` error's own `Display`, which can carry a credentialed server URL and must never be
/// persisted or logged (ADR 0030 Amendment B, review m7/N10).
fn classify_publish_error(
    subject: &Subject,
    err: &PublishError,
    start: tokio::time::Instant,
) -> NatsPublishError {
    match err.kind() {
        PublishErrorKind::StreamNotFound => NatsPublishError::StreamNotFound {
            subject: subject.clone(),
        },
        PublishErrorKind::WrongLastMessageId | PublishErrorKind::WrongLastSequence => {
            NatsPublishError::WrongLastMessage {
                subject: subject.clone(),
            }
        }
        PublishErrorKind::TimedOut => NatsPublishError::Timeout {
            subject: subject.clone(),
            after_ms: elapsed_ms(start),
        },
        PublishErrorKind::BrokenPipe => NatsPublishError::Connection {
            subject: subject.clone(),
        },
        PublishErrorKind::MaxAckPending => NatsPublishError::MaxAckPending {
            subject: subject.clone(),
        },
        PublishErrorKind::MaxPayloadExceeded => NatsPublishError::MaxPayloadExceeded {
            subject: subject.clone(),
        },
        PublishErrorKind::Other => broker_error(subject, "other"),
        // `PublishErrorKind` is not `#[non_exhaustive]` today, but this arm keeps the mapping
        // total against a future async-nats kind this crate does not yet know, rather than
        // failing to compile or silently mis-classifying it (ADR 0030 Amendment B).
        #[allow(unreachable_patterns)]
        _ => broker_error(subject, "unrecognised"),
    }
}

/// Logs the `Broker` verdict at `warn` with only the subject and `kind` — a bounded `&'static str`
/// this crate controls, never the `async-nats` error's own `Display` (ADR 0030 Amendment B).
fn broker_error(subject: &Subject, kind: &'static str) -> NatsPublishError {
    tracing::warn!(
        subject = %subject,
        error.kind = kind,
        "reliar.transport_nats.publish: unrecognised NATS publish failure, classifying as transient"
    );
    NatsPublishError::Broker {
        subject: subject.clone(),
    }
}

/// `start.elapsed()` in milliseconds, saturating rather than panicking on an implausibly long
/// elapsed duration (review m1: the **measured** time, never the configured setting).
fn elapsed_ms(start: tokio::time::Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}
