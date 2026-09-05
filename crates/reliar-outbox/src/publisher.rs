//! The application's publisher when an outbox is in play: one `publish` call that either stages
//! the message in the outbox or sends it straight to the transport, as its [`OutboxPolicy`]
//! decides (SRS §20.2, ADR 0033 Amendment D).

use core::fmt;

use reliar_core::{Classify, FailureKind, MessageType, Publisher, SerializedEnvelope};
use tokio::sync::Mutex;
use tracing::Instrument as _;

use crate::metrics::{NoopMetrics, OutboxMetrics};
use crate::policy::{OutboxPolicy, RouteKind};
use crate::staging::OutboxStaging;

/// The application's publisher when an outbox is in play: one `publish` call that either stages
/// the message in the outbox or sends it straight to the transport, as its [`OutboxPolicy`]
/// decides.
///
/// Composition only — it holds a staging capability, a transport [`Publisher`], the rule, and a
/// metrics sink. The rule itself lives in [`OutboxPolicy`] (ADR 0033 Amendment C): no `enabled`
/// flag, no list and no branch of the routing table here. Preview a decision with
/// [`Self::policy`].
///
/// # Publishing
///
/// The routed path needs the caller's transaction, and `Publisher::publish` has no parameter for
/// one — so **the `Publisher` impl lives on [`ScopedOutboxPublisher`]**, which
/// [`Self::in_transaction`] hands out for the life of a borrow:
///
/// ```text
/// let published = outbox.in_transaction(&mut tx);   // impl reliar_core::Publisher
/// published.publish(&serialized).await?;
/// tx.commit().await?;
/// ```
///
/// This type is deliberately **not** a [`reliar_core::Publisher`]: a `'static`, `Clone`-able
/// `Publisher` here could be wired into an [`crate::OutboxDispatcher`], which would drain the
/// outbox back into itself (ADR 0033 §4). For a call site with no transaction use
/// [`Self::publish_direct`], which refuses routed types loudly.
///
/// The caller serializes: both routes carry the same [`SerializedEnvelope`] value, so the bytes
/// on the wire cannot depend on the route (ADR 0033 Amendment D §3).
// Gated: the example below is only compilable with `test-support` (`InMemoryOutboxStore`,
// `InMemoryTransaction`, `RecordingPublisher`) — without the feature it becomes `ignore` so
// `cargo test -p reliar-outbox` (no `--all-features`) still compiles (review round 1, B1).
#[cfg_attr(not(feature = "test-support"), doc = "```ignore")]
#[cfg_attr(feature = "test-support", doc = "```")]
/// # use reliar_core::{ContentType, Envelope, Message, Publisher as _, Serializer};
/// # use reliar_outbox::{InMemoryOutboxStore, InMemoryTransaction, OutboxPolicy, OutboxPublisher, RecordingPublisher};
/// #
/// # #[derive(serde::Serialize, serde::Deserialize)]
/// # struct OrderCreated;
/// # impl Message for OrderCreated {
/// #     const TYPE: &'static str = "orders.created";
/// #     const VERSION: u16 = 1;
/// # }
/// #
/// // A minimal `Serializer` — `reliar-outbox` names no wire format of its own (ADR 0033 §13), and
/// // holds none on this path: the caller serializes (Amendment D §3).
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
/// // The caller serializes once, exactly as it would for a bare `NatsPublisher` (§4.2).
/// let envelope = Envelope::builder(OrderCreated).build();
/// let bytes = RawJson.serialize(&envelope.body)?;
/// let mut serialized = envelope.map_body(|_| bytes);
/// serialized.metadata.delivery.content_type = RawJson.content_type().clone();
///
/// // Routed: an empty allow list means every type is durable (`OutboxPolicy::default()`).
/// let outbox = OutboxPublisher::new(
///     InMemoryOutboxStore::default(),
///     RecordingPublisher::default(),
///     OutboxPolicy::default(),
/// );
/// let mut tx = InMemoryTransaction;
/// outbox.in_transaction(&mut tx).publish(&serialized).await?;
///
/// // Direct: routing disabled — the same call now goes straight to the transport.
/// let disabled = OutboxPolicy::from_settings(&reliar_outbox::OutboxSettings::default().enabled(false))?;
/// let direct_outbox = OutboxPublisher::new(
///     InMemoryOutboxStore::default(),
///     RecordingPublisher::default(),
///     disabled,
/// );
/// direct_outbox.publish_direct(&serialized).await?;
/// # Ok(())
/// # }
/// ```
pub struct OutboxPublisher<S, P, M = NoopMetrics> {
    staging: S,
    publisher: P,
    policy: OutboxPolicy,
    metrics: M,
}

/// **Manual impl, never derived**: a derived `Clone` would condition on `M: Clone` even for
/// `NoopMetrics`'s own default, and this keeps the bound list exactly `S`/`P`/`M`, nothing more.
impl<S: Clone, P: Clone, M: Clone> Clone for OutboxPublisher<S, P, M> {
    fn clone(&self) -> Self {
        Self {
            staging: self.staging.clone(),
            publisher: self.publisher.clone(),
            policy: self.policy.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<S, P, M> fmt::Debug for OutboxPublisher<S, P, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutboxPublisher")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl<S, P, M> OutboxPublisher<S, P, M> {
    /// The rule this publisher delegates to. Preview a decision with
    /// `outbox.policy().decide(&message_type)`.
    ///
    /// The **only** rule-shaped accessor: no `route_for`/`enabled`/`allowed_types`/
    /// `disallowed_types` delegation, because each would be a second public way to ask the same
    /// question (ADR 0033 Amendment C). Unbounded — reading the policy needs none of
    /// `S`/`P`/`M`'s trait bounds.
    #[must_use]
    pub const fn policy(&self) -> &OutboxPolicy {
        &self.policy
    }
}

impl<S, P> OutboxPublisher<S, P>
where
    P: Publisher,
{
    /// `staging` is normally the provider store, `publisher` the transport publisher, and
    /// `policy` the rule — `OutboxPolicy::from_settings(&settings)?` from the one `OutboxSettings`
    /// the host already built for its dispatcher.
    ///
    /// **Infallible**, because an `OutboxPolicy` that exists is already valid (§2.5).
    pub fn new(staging: S, publisher: P, policy: OutboxPolicy) -> Self {
        Self {
            staging,
            publisher,
            policy,
            metrics: NoopMetrics,
        }
    }
}

impl<S, P, M> OutboxPublisher<S, P, M>
where
    P: Publisher,
    M: OutboxMetrics,
{
    /// As [`Self::new`], with a metrics sink (§9).
    pub fn with_metrics(staging: S, publisher: P, policy: OutboxPolicy, metrics: M) -> Self {
        Self {
            staging,
            publisher,
            policy,
            metrics,
        }
    }

    /// Borrows `tx` and returns a [`reliar_core::Publisher`] that stages routed types in it and
    /// forwards direct types to the transport.
    ///
    /// The returned value borrows both `self` and `tx` — it is neither `'static` nor `Clone`, so
    /// it cannot be handed to an [`crate::OutboxDispatcher`] (that is the guard, and the compiler
    /// enforces it). Dropping it **neither commits nor rolls back**: the caller owns the
    /// transaction throughout.
    ///
    /// For a single publish, the one-expression form keeps the borrow to one statement:
    /// `outbox.in_transaction(&mut tx).publish(&serialized).await?`.
    #[must_use]
    pub fn in_transaction<'a, Tx>(
        &'a self,
        tx: &'a mut Tx,
    ) -> ScopedOutboxPublisher<'a, S, P, Tx, M>
    where
        S: OutboxStaging<Tx>,
        Tx: Send,
    {
        ScopedOutboxPublisher {
            owner: self,
            tx: Mutex::new(tx),
        }
    }

    /// Publishes from a call site that has **no** transaction.
    ///
    /// Only the direct path is reachable. A type the rule routes through the outbox returns
    /// [`DirectPublishError::TransactionRequired`] — this method **never** falls back to a direct
    /// publish, because that would silently cancel the durability the operator configured. It is
    /// one attempt with no Reliar-side retry, backoff, dead state or duplicate window.
    ///
    /// # Errors
    ///
    /// [`DirectPublishError::TransactionRequired`], [`DirectPublishError::Publish`].
    pub async fn publish_direct(
        &self,
        envelope: &SerializedEnvelope,
    ) -> Result<(), DirectPublishError<P::Error>> {
        let span = tracing::debug_span!(
            "reliar.outbox.route",
            message.id = %envelope.id,
            message.type = %envelope.message_type,
            route = tracing::field::Empty,
        );
        async {
            let route = self.policy.decide(&envelope.message_type);
            tracing::Span::current().record("route", route.as_str());
            if route.is_outbox() {
                return Err(DirectPublishError::TransactionRequired {
                    message_type: envelope.message_type.clone(),
                });
            }
            self.publisher
                .publish(envelope)
                .await
                .map_err(DirectPublishError::Publish)?;
            self.metrics.routed(route, &envelope.message_type);
            Ok(())
        }
        .instrument(span)
        .await
    }
}

/// An [`OutboxPublisher`] scoped to one borrowed transaction — and a full
/// [`reliar_core::Publisher`] for the life of that borrow.
///
/// Returned by [`OutboxPublisher::in_transaction`]. "Scoped" is about the **borrow**, not about
/// delivery: a direct-routed publish here is still **not** part of the caller's transaction (see
/// the `Publisher` impl below).
///
/// Not `Clone`, not `'static`, and never made either: those are what stop it reaching an
/// [`crate::OutboxDispatcher`] (ADR 0033 Amendment D §4). The guard is enforced by the compiler,
/// not by convention — `OutboxDispatcher::run` requires `P: Publisher + Send + Sync + 'static`,
/// and a value borrowed for the life of one transaction can never satisfy that (§43.D, R24).
///
// Gated exactly like `OutboxPublisher`'s own doctest (review round 1, B1): without
// `test-support` this becomes `ignore` rather than a `compile_fail` that would pass for the
// wrong reason (an unresolved `InMemoryOutboxStore` import, not the `'static` bound this test
// exists to prove).
#[cfg_attr(not(feature = "test-support"), doc = "```ignore")]
#[cfg_attr(feature = "test-support", doc = "```compile_fail")]
/// # use reliar_outbox::{
/// #     InMemoryOutboxStore, InMemoryTransaction, OutboxDispatcher, OutboxPolicy, OutboxPublisher,
/// #     RecordingPublisher,
/// # };
/// # use tokio_util::sync::CancellationToken;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let outbox = OutboxPublisher::new(
///     InMemoryOutboxStore::default(),
///     RecordingPublisher::default(),
///     OutboxPolicy::default(),
/// );
/// let mut tx = InMemoryTransaction;
/// let scoped = outbox.in_transaction(&mut tx);
///
/// // Does not compile: `scoped` borrows `tx` and `outbox` for a non-`'static` lifetime, so it
/// // cannot satisfy `run`'s `P: 'static` bound — the same guard that stops a host from feeding
/// // the outbox back into itself.
/// let dispatcher = OutboxDispatcher::builder(InMemoryOutboxStore::default(), scoped)
///     .build()
///     .unwrap();
/// dispatcher.run(CancellationToken::new()).await.unwrap();
/// # }
/// ```
pub struct ScopedOutboxPublisher<'a, S, P, Tx, M = NoopMetrics> {
    owner: &'a OutboxPublisher<S, P, M>,
    tx: Mutex<&'a mut Tx>,
}

impl<S, P, Tx, M> fmt::Debug for ScopedOutboxPublisher<'_, S, P, Tx, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never locks `self.tx`: a `Debug` impl must never block, and the policy is all there is
        // worth printing anyway.
        f.debug_struct("ScopedOutboxPublisher")
            .field("policy", &self.owner.policy)
            .finish_non_exhaustive()
    }
}

impl<S, P, Tx, M> Publisher for ScopedOutboxPublisher<'_, S, P, Tx, M>
where
    S: OutboxStaging<Tx>,
    P: Publisher,
    Tx: Send,
    M: OutboxMetrics,
{
    /// Routed and direct failures in one enum; `Classify` forwards to whichever occurred (§5).
    type Error = RouteError<<S as OutboxStaging<Tx>>::Error, P::Error>;

    /// Publishes `envelope` by the rule.
    ///
    /// - **Routed** → `staging.stage(tx, …)` in the borrowed transaction. The message becomes
    ///   visible when the caller commits and is published later by an
    ///   [`crate::OutboxDispatcher`]: durable, at-least-once, with the documented duplicate
    ///   windows.
    /// - **Direct** → the transport publisher, **immediately**. The transaction is not touched —
    ///   no statement is issued on it — and this publish is **not part of it**: if the caller
    ///   later rolls back, the message is already on the wire. One attempt, no Reliar-side retry,
    ///   backoff, dead state or duplicate window.
    ///
    /// A direct publish here runs while the caller's transaction is open — network I/O holding a
    /// database transaction. Configure a publisher-side timeout, and prefer publishing
    /// direct-routed types before opening (or after committing) the transaction.
    ///
    /// The transaction borrow is held only for the duration of a **staging** call
    /// (`tx.lock().await` around `staging.stage(..)`): concurrent routed publishes on one scoped
    /// value serialize on that lock, because a transaction is not a concurrency point. A direct
    /// publish never takes the lock, so concurrent direct publishes run in parallel, bounded only
    /// by whatever concurrency `P: Publisher` itself allows.
    ///
    /// # Errors
    ///
    /// [`RouteError::Stage`] — the transaction has typically been aborted, roll back;
    /// [`RouteError::Publish`] — the transaction is untouched and still committable.
    ///
    /// [`Self::publish_batch`] is the inherited default: results stay positional, one per
    /// envelope, in order. A positional `Ok` on a routed entry means *the statement was
    /// accepted*, **not** that the message is durable — durability is the caller's `commit`, and
    /// one `Err(RouteError::Stage(_))` aborts the whole transaction, invalidating every `Ok`
    /// before it. An `Err(RouteError::Publish(_))` is different: a direct publish never issues a
    /// statement on the transaction, so it neither aborts it nor invalidates any earlier `Ok` —
    /// the staged entries before and after it remain valid if the caller commits.
    fn publish(
        &self,
        envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let span = tracing::debug_span!(
            "reliar.outbox.route",
            message.id = %envelope.id,
            message.type = %envelope.message_type,
            route = tracing::field::Empty,
        );
        async move {
            // The only rule call in this crate.
            let route = self.owner.policy.decide(&envelope.message_type);
            tracing::Span::current().record("route", route.as_str());
            match route {
                RouteKind::Outbox => {
                    let mut guard = self.tx.lock().await;
                    self.owner
                        .staging
                        .stage(&mut **guard, envelope)
                        .await
                        .map_err(RouteError::Stage)?;
                }
                RouteKind::Direct => {
                    self.owner
                        .publisher
                        .publish(envelope)
                        .await
                        .map_err(RouteError::Publish)?;
                }
            }
            self.owner.metrics.routed(route, &envelope.message_type);
            Ok(())
        }
        .instrument(span)
    }
}

/// Why a publish through a [`ScopedOutboxPublisher`] failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum RouteError<S, P> {
    /// The store rejected the staged row. The caller's transaction has typically been aborted.
    Stage(S),
    /// The transport rejected the direct publish. The caller's transaction is untouched.
    Publish(P),
}

impl<S: fmt::Display, P: fmt::Display> fmt::Display for RouteError<S, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stage(err) => write!(f, "failed to stage the routed message: {err}"),
            Self::Publish(err) => write!(f, "failed to publish the message directly: {err}"),
        }
    }
}

impl<S, P> std::error::Error for RouteError<S, P>
where
    S: std::error::Error + 'static,
    P: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Stage(err) => Some(err),
            Self::Publish(err) => Some(err),
        }
    }
}

/// Forwards to whichever collaborator failed — required because [`ScopedOutboxPublisher`]'s
/// `Publisher::Error` is this type, and `reliar_core::Publisher::Error: Classify`.
impl<S: Classify, P: Classify> Classify for RouteError<S, P> {
    fn kind(&self) -> FailureKind {
        match self {
            Self::Stage(err) => err.kind(),
            Self::Publish(err) => err.kind(),
        }
    }
}

/// Why [`OutboxPublisher::publish_direct`] failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum DirectPublishError<P> {
    /// The rule routes this type through the outbox, but the call site has no transaction. Use
    /// [`OutboxPublisher::in_transaction`], or stop routing this type.
    TransactionRequired {
        /// The type that requires a transaction.
        message_type: MessageType,
    },
    /// The transport rejected the publish.
    Publish(P),
}

impl<P: fmt::Display> fmt::Display for DirectPublishError<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TransactionRequired { message_type } => write!(
                f,
                "message type {message_type} routes through the outbox, but no transaction was \
                 supplied; call OutboxPublisher::in_transaction, or stop routing this type"
            ),
            Self::Publish(err) => write!(f, "failed to publish the message directly: {err}"),
        }
    }
}

impl<P> std::error::Error for DirectPublishError<P>
where
    P: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::TransactionRequired { .. } => None,
            Self::Publish(err) => Some(err),
        }
    }
}
