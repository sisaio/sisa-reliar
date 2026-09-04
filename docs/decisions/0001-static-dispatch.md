# ADR 0001 — Static dispatch by default

**Status:** Accepted — 2026-09-04
**SRS:** §3, §19, §30, §32, §34, §43.B

## Context

Reliar composes a storage provider and a transport publisher into a long-running worker. The two
obvious ways to wire them are a runtime registry of `Box<dyn OutboxStore>` / `Box<dyn Publisher>`
(one compiled dispatcher, providers swapped at runtime) or generics resolved at compile time.

The wiring happens **once, at application startup** — an Axum app builds exactly one
`PostgresOutboxStore` and one publisher and never changes them (§20.1). Nothing in the product
needs to swap a provider while running. Meanwhile the publish path runs per message, per batch,
per poll interval, forever, and native `async fn` in traits (AFIT) does not object-safe-ify:
`dyn` would force `async-trait` and a `Box<dyn Future>` allocation on every call, which §3 and
§32 rule out for their own reasons.

## Decision

- Every Reliar abstraction is a **trait with generic parameters**, monomorphized at the call site:
  `OutboxDispatcher<S: OutboxStore, P: Publisher, M: OutboxMetrics = NoopMetrics>`.
- Trait methods declare `fn f(&self, …) -> impl Future<Output = Result<_, Self::Error>> + Send`.
  The `async-trait` crate is **banned** in library crates.
- `Arc<T>` is permitted for cheap sharing and is not dynamic dispatch. `Box<dyn Error + Send +
  Sync>` is permitted as an error **source**, because error paths are cold and already allocating.
- `Box<dyn Trait>` on a Reliar capability requires its own ADR. Cold, deliberate boundaries
  (a future diagnostics/plugin registry) may use it; the claim/publish/complete path may not.
- Applications compose concretely and name the type:
  `OutboxDispatcher<PostgresOutboxStore, NatsPublisher>`. There is no DI container (§3.12).

## Consequences

- No vtable, no boxed futures, and full inlining across the store/publisher boundary.
- Type signatures in host code get long. Mitigated by builders (`OutboxDispatcher::builder`) and
  by hosts type-aliasing their one concrete combination.
- **Every provider change is a recompile**, and a host cannot pick a provider from configuration.
  Accepted: provider choice is a build-time decision in this product.
- Generic code is monomorphized per combination, so compile time grows with the number of
  store × publisher pairs a host instantiates. In practice that number is one.
- `Publisher::Error: Classify` (§19.4) works only because the concrete error type is known; a
  `dyn Publisher` would have to erase the classification or re-box it.

## Alternatives considered

- **`Box<dyn OutboxStore>` everywhere.** Rejected: forces `async-trait`, allocates a future per
  publish, and buys runtime provider swapping nobody asked for.
- **Enum dispatch over the known providers.** Rejected: `reliar-outbox` would have to name every
  provider, inverting the dependency rule and making third-party providers impossible (§34).
- **A DI container / service locator.** Rejected: §3.12. A library that wires itself is a
  framework, and it hides which provider is actually running.
