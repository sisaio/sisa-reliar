# ADR 0029 — The application owns the NATS connection and the stream; publishing creates neither

**Status:** Accepted — 2026-09-04
**SRS:** §3.12 (explicit composition, no DI container), §7.2 (the library never reads the environment
implicitly), §30, §31, §35 (migrations are explicit), §45 ("transport mapping", "delivery guarantees")
**Builds on:** [ADR 0018](0018-migrations-embedded-published-and-isolated.md) (schema changes are
explicit, never a startup side effect), [ADR 0028](0028-jetstream-ack-is-the-only-publish-path.md)
**Related:** RELIAR-2 (Phase 2), RELIAR-30, contract `../architecture/phase2-contract.md` §5

## Context

A JetStream publish only succeeds if a stream captures the subject. So `NatsPublisher` has to answer
two ownership questions: who connects to NATS, and who creates the stream.

Both have a tempting convenience answer — `NatsPublisher::connect(url)` and "create the stream if it
is missing on first publish" — and Reliar has already rejected the equivalent answer once, for
databases: migrations run only through an explicit `migrate()` call, never implicitly at startup
(ADR 0018, §35). A stream's `retention`, `max_age`, `max_bytes`, `storage`, `replicas`,
`discard` and `duplicate_window` are exactly as consequential as a schema: `duplicate_window` *is*
the width of the broker-side duplicate suppression ADR 0028 relies on, and a stream auto-created
with defaults silently gives an operator a 2-minute window, memory storage, or one replica in a
cluster they believed was replicated.

The connection carries the same weight: TLS, credentials/nkeys, reconnect policy, ping intervals,
and connection-event callbacks are operational decisions, and §7.2 forbids the library from reading
`NATS_URL` — or anything else — out of the environment on its own.

## Decision

**`NatsPublisher` takes an already-built `async_nats::jetstream::Context`. It never connects, never
creates or updates a stream, and never inspects the stream's configuration.**

1. **Construction is `NatsPublisher::new(context, settings)`** (plus `with_resolver` for a custom
   `SubjectResolver`). There is no `connect`, no URL in `NatsSettings`, and no
   `NatsSettings::from_env` key for a server address. The host writes
   `async_nats::connect(url)` — or `ConnectOptions::…` for TLS and credentials — and
   `async_nats::jetstream::new(client)`, and keeps ownership of both. `Context` is cheap to clone
   and shares one multiplexed connection.
2. **Stream provisioning is the application's or the operator's job**, documented in
   `docs/guides/nats.md` with the settings that matter and a copy-pasteable
   `create_stream` snippet using `async-nats`'s own API. Reliar ships **no** provisioning helper:
   wrapping `StreamConfig` would mean tracking JetStream's configuration surface forever with no
   value added (§31).
3. **A missing stream is a loud, retryable failure**, not an implicit creation:
   `PublishErrorKind::StreamNotFound` maps to `NatsPublishError::StreamNotFound`, classified
   **transient** (ADR 0030), so the outbox row backs off, stays inspectable, and succeeds once the
   operator creates the stream — instead of the publisher inventing one.
4. **Tests own their own streams.** Each integration scenario creates a uniquely named stream over a
   uniquely prefixed subject and deletes it at the end, which is also what makes a shared CI NATS
   server safe (contract §7).

## Consequences

- Wiring a publisher is four lines of host code (`connect` → `jetstream::new` → `NatsPublisher::new`
  → hand it to the dispatcher), all of it visible at the composition root (§3.12). The
  `examples/nats-pub-sub` target is that wiring, and it creates its own stream explicitly.
- Reliar has no opinion, and no dependency, on how you authenticate: credentials files, nkeys,
  TLS and websockets are features of the *application's* `async-nats`, unified by Cargo with ours
  (see ADR 0031).
- First run against an unprovisioned server produces repeated transient failures rather than
  working by accident. That is the intended trade: an auto-created stream is a production incident
  deferred, and its symptom (silently wrong retention or replication) surfaces long after the cause.
- Reliar cannot report "your `duplicate_window` is shorter than your retry backoff". A future
  read-only diagnostic could — it would only need `Context` — and is deliberately not in Phase 2.

## Alternatives considered

- **`NatsPublisher::connect(url)` convenience constructor.** Rejected: it either hides the auth and
  TLS surface or grows to mirror `ConnectOptions`, and it invites the library to read `NATS_URL`
  itself, which §7.2 forbids.
- **Create the stream if absent on first publish.** Rejected as the direct analogue of implicit
  migrations (ADR 0018): a defaulted `duplicate_window`, storage tier or replica count is
  unrecoverable-by-then configuration, chosen by whichever process published first.
- **A `NatsStreamProvisioner` helper (`ensure_stream(config)`).** Rejected for Phase 2 under §31:
  it is a passthrough to `Context::get_or_create_stream` whose only real content is JetStream's own
  config type, and it would put Reliar in the business of tracking that type's evolution.
- **Classify `StreamNotFound` as permanent.** Rejected in ADR 0030: it is fixable server-side
  within seconds, and dead-lettering a whole batch for a provisioning gap is a worse outcome than
  backing off.
