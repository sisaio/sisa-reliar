//! The application's outbox handle: [`OutboxPublisher::enqueue`] is the durable path,
//! [`reliar_core::Publisher::publish`] bypasses the outbox entirely (ADR 0036).

use core::fmt;

use reliar_core::{Publisher, SerializedEnvelope};
use tracing::Instrument as _;

use crate::enqueue::OutboxEnqueue;
use crate::metrics::{NoopMetrics, OutboxMetrics};

/// The application's outbox handle: **`enqueue` is the durable path; `publish` bypasses the
/// outbox entirely** and forwards straight to the transport.
///
/// - [`Self::enqueue`] enqueues the envelope in the caller's own transaction. It becomes visible
///   when the caller commits and is published later by an [`crate::OutboxDispatcher`]: durable,
///   at-least-once, with the duplicate windows the crate docs list.
/// - The [`reliar_core::Publisher`] impl sends **now**, through the transport publisher, one
///   attempt, with no Reliar guarantee at all: no retry, no backoff, no dead state, no duplicate
///   window, and no relationship to any transaction the caller may have open.
///
/// The guarantee is chosen by which method the call site calls. Nothing decides it at runtime,
/// and no setting can (ADR 0036).
// Gated: the example below is only compilable with `test-support` (`InMemoryOutboxStore`,
// `InMemoryTransaction`, `RecordingPublisher`) — without the feature it becomes `ignore` so
// `cargo test -p reliar-outbox` (no `--all-features`) still compiles (review round 1, B1).
#[cfg_attr(not(feature = "test-support"), doc = "```ignore")]
#[cfg_attr(feature = "test-support", doc = "```")]
/// # use reliar_core::{ContentType, Envelope, Message, Publisher as _, Serializer};
/// # use reliar_outbox::{InMemoryOutboxStore, InMemoryTransaction, OutboxPublisher, RecordingPublisher};
/// #
/// # #[derive(serde::Serialize, serde::Deserialize)]
/// # struct OrderCreated;
/// # impl Message for OrderCreated {
/// #     const TYPE: &'static str = "orders.created";
/// #     const VERSION: u16 = 1;
/// # }
/// #
/// // A minimal `Serializer` — `reliar-outbox` names no wire format of its own, and holds none on
/// // this path: the caller serializes.
/// # struct RawJson;
/// # impl Serializer for RawJson {
/// #     type Error = serde_json::Error;
/// #     fn content_type(&self) -> &ContentType { &ContentType::JSON }
/// #     fn serialize<T: Message>(&self, body: &T) -> Result<bytes::Bytes, Self::Error> {
/// #         serde_json::to_vec(body).map(bytes::Bytes::from)
/// #     }
/// #     fn deserialize<T: Message>(&self, bytes: &[u8]) -> Result<T, Self::Error> {
/// #         serde_json::from_slice(bytes)
/// #     }
/// # }
/// #
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // The caller serializes once, exactly as it would for a bare `NatsPublisher`.
/// let envelope = Envelope::builder(OrderCreated).build();
/// let bytes = RawJson.serialize(&envelope.body)?;
/// let mut serialized = envelope.map_body(|_| bytes);
/// serialized.metadata.delivery.content_type = RawJson.content_type().clone();
///
/// let outbox = OutboxPublisher::new(InMemoryOutboxStore::default(), RecordingPublisher::default());
///
/// // The durable path: enqueued in the caller's transaction.
/// let mut tx = InMemoryTransaction;
/// outbox.enqueue(&mut tx, &serialized).await?;
///
/// // The bypass path: straight to the transport, no transaction needed.
/// outbox.publish(&serialized).await?;
/// # Ok(())
/// # }
/// ```
pub struct OutboxPublisher<S, P, M = NoopMetrics> {
    store: S,
    publisher: P,
    metrics: M,
}

/// **Manual impl, never derived**: a derived `Clone` would condition on `M: Clone` even for
/// `NoopMetrics`'s own default, and this keeps the bound list exactly `S`/`P`/`M`, nothing more.
impl<S: Clone, P: Clone, M: Clone> Clone for OutboxPublisher<S, P, M> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            publisher: self.publisher.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<S, P, M> fmt::Debug for OutboxPublisher<S, P, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutboxPublisher").finish_non_exhaustive()
    }
}

impl<S, P> OutboxPublisher<S, P>
where
    P: Publisher,
{
    /// `store` is normally the provider store, `publisher` the transport publisher.
    pub fn new(store: S, publisher: P) -> Self {
        Self {
            store,
            publisher,
            metrics: NoopMetrics,
        }
    }
}

impl<S, P, M> OutboxPublisher<S, P, M>
where
    P: Publisher,
    M: OutboxMetrics,
{
    /// As [`Self::new`], with a metrics sink.
    pub fn with_metrics(store: S, publisher: P, metrics: M) -> Self {
        Self {
            store,
            publisher,
            metrics,
        }
    }
}

impl<S, P, M> OutboxPublisher<S, P, M>
where
    M: OutboxMetrics,
{
    /// Enqueues `envelope` in the caller's transaction `tx` — **the durable path**.
    ///
    /// Atomic with whatever else the caller writes in `tx`: the message exists if and only if the
    /// transaction commits. An [`crate::OutboxDispatcher`] publishes it afterwards with the
    /// crate's at-least-once guarantee (SRS §22).
    ///
    /// The transaction is required **by type**: there is no transaction-less enqueue call, so a
    /// message cannot be enqueued outside the caller's unit of work.
    ///
    /// Issues no network I/O beyond the provider's own statement, never retries, never sleeps,
    /// and never commits, rolls back or otherwise consumes `tx` — the caller owns it throughout.
    ///
    /// Persists `envelope.metadata.delivery.content_type` verbatim: the caller serialized the
    /// body and is authoritative about its content type (SRS §12).
    ///
    /// # Errors
    ///
    /// The provider's enqueue error, unwrapped. **An `Err` MAY leave `tx` unusable**, and whether
    /// it does is the provider's contract ([`OutboxEnqueue::enqueue`]). The portable rule is: treat
    /// any enqueue error as *abort this transaction* — issue no further statement on `tx`, roll it
    /// back, and consider every earlier write in it lost. With `reliar-store-postgres` the
    /// transaction **is** aborted.
    pub async fn enqueue<Tx>(
        &self,
        tx: &mut Tx,
        envelope: &SerializedEnvelope,
    ) -> Result<(), <S as OutboxEnqueue<Tx>>::Error>
    where
        S: OutboxEnqueue<Tx>,
        Tx: Send,
    {
        self.enqueue_one(tx, envelope).await
    }

    /// Enqueues `envelopes` in `tx`, in order, one statement each — **the durable path**, batched.
    ///
    /// Sequential and order-preserving. **Fails fast**: the first enqueue failure returns, naming
    /// the position in `envelopes` that failed, and the remaining envelopes are not attempted.
    ///
    /// This returns one result for the whole batch rather than one per envelope on purpose. Every
    /// row lands in the same transaction, so the batch has a single outcome — the caller's
    /// `commit` — and an enqueue failure typically aborts that transaction, voiding every row
    /// enqueued before it. A positional `Ok` would not mean the message is durable (ADR 0036 §5).
    ///
    /// An empty slice is `Ok(())` and issues no statement.
    ///
    /// # Errors
    ///
    /// [`EnqueueBatchError`], carrying the failing index and the provider's error. The same
    /// "treat it as *abort this transaction*" rule as [`Self::enqueue`] applies.
    pub async fn enqueue_batch<Tx>(
        &self,
        tx: &mut Tx,
        envelopes: &[SerializedEnvelope],
    ) -> Result<(), EnqueueBatchError<<S as OutboxEnqueue<Tx>>::Error>>
    where
        S: OutboxEnqueue<Tx>,
        Tx: Send,
    {
        let span =
            tracing::debug_span!("reliar.outbox.enqueue_batch", batch.size = envelopes.len());
        async {
            for (index, envelope) in envelopes.iter().enumerate() {
                self.enqueue_one(tx, envelope)
                    .await
                    .map_err(|source| EnqueueBatchError { index, source })?;
            }
            Ok(())
        }
        .instrument(span)
        .await
    }

    /// The one place that enqueues a row and records its span/metric — shared by
    /// [`Self::enqueue`] and [`Self::enqueue_batch`] so the two can never drift and each envelope
    /// gets its own span even inside a batch.
    async fn enqueue_one<Tx>(
        &self,
        tx: &mut Tx,
        envelope: &SerializedEnvelope,
    ) -> Result<(), <S as OutboxEnqueue<Tx>>::Error>
    where
        S: OutboxEnqueue<Tx>,
        Tx: Send,
    {
        let span = tracing::debug_span!(
            "reliar.outbox.enqueue",
            message.id = %envelope.id,
            message.type = %envelope.message_type,
        );
        async {
            self.store.enqueue(tx, envelope).await?;
            self.metrics.enqueued(1, &envelope.message_type);
            Ok(())
        }
        .instrument(span)
        .await
    }
}

/// **Bypasses the outbox.** Forwards to the transport publisher, byte-identical, with no Reliar
/// durability: one attempt, no retry, no backoff, no dead state, no duplicate window, and no
/// relationship to any transaction the caller has open — if the caller later rolls back, the
/// message is already on the wire. Use [`OutboxPublisher::enqueue`] for the durable path.
///
/// The enqueue capability is **never** touched here. That is what makes wiring an
/// `OutboxPublisher` into an [`crate::OutboxDispatcher`] safe (ADR 0036 §2): there is no code
/// path from a publish back into the store, so the outbox cannot drain into itself.
impl<S, P, M> Publisher for OutboxPublisher<S, P, M>
where
    S: Send + Sync,
    P: Publisher,
    M: OutboxMetrics,
{
    /// Transparent: the transport publisher's own error, unwrapped. `Classify`, `source()` and
    /// `Display` are the transport's (ADR 0036 §3).
    type Error = P::Error;

    /// **Bypasses the outbox.** Sends `envelope` now, through the transport publisher, in one
    /// attempt: no retry, no backoff, no dead state, no duplicate window, and no relationship to
    /// any transaction the caller has open. This method's own doc is required — without it,
    /// rustdoc renders [`reliar_core::Publisher::publish`]'s "retry is the dispatcher's job" text
    /// here, which is false for this impl (review round 1, B1). Use [`Self::enqueue`] for the
    /// durable path.
    fn publish(
        &self,
        envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.publisher.publish(envelope)
    }

    /// Forwarded to `P::publish_batch` rather than inherited, so a transport with a native batch
    /// API keeps it. Results stay positional, one per envelope, in order — `P`'s contract,
    /// unmodified.
    fn publish_batch(
        &self,
        envelopes: &[SerializedEnvelope],
    ) -> impl Future<Output = Vec<Result<(), Self::Error>>> + Send {
        self.publisher.publish_batch(envelopes)
    }
}

/// Which envelope in an [`OutboxPublisher::enqueue_batch`] failed, and why.
#[derive(Debug)]
#[non_exhaustive]
pub struct EnqueueBatchError<E> {
    /// The position in the `envelopes` slice that failed. Envelopes after it were not attempted.
    pub index: usize,
    /// The provider's enqueue error.
    pub source: E,
}

impl<E: fmt::Display> fmt::Display for EnqueueBatchError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "failed to enqueue the envelope at index {}: {}",
            self.index, self.source
        )
    }
}

impl<E: std::error::Error + 'static> std::error::Error for EnqueueBatchError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Forwards to the enqueue error, so a host can classify a batch failure exactly as it classifies
/// a single one. Free, because `OutboxEnqueue::Error: Classify` already.
impl<E: reliar_core::Classify> reliar_core::Classify for EnqueueBatchError<E> {
    fn kind(&self) -> reliar_core::FailureKind {
        self.source.kind()
    }
}
