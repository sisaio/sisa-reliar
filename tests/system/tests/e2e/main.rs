//! The phase's proof (RELIAR-34, ADR 0031 §6, contract `docs/architecture/phase2-contract.md` §7
//! `e2e`): a migrated Postgres outbox drained by `OutboxDispatcher<PostgresOutboxStore,
//! NatsPublisher>` into a real `JetStream` stream, and its recovery once a stream comes back.
//!
//! This is the **only** place both provider crates (`reliar-store-postgres`,
//! `reliar-transport-nats`) are exercised together — ADR 0031 §6 puts it in its own workspace
//! package rather than either provider's `tests/` precisely so neither depends on the other.
//!
//! `harness = false` + `libtest-mimic`, mirroring `reliar-store-postgres`'s
//! `tests/postgres/main.rs` and `reliar-transport-nats`'s `tests/nats/main.rs` (see either for the
//! full RELIAR-27 rationale): this `main` owns **both** shared containers as **locals**, never
//! `static`s, runs every scenario on one shared Tokio runtime, then drops both containers
//! explicitly before returning an `ExitCode` — never via `Conclusion::exit()`, which calls
//! `process::exit` and would skip those drops.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::cast_possible_truncation
)]

mod common;
mod e1_outbox_drains_into_jetstream;
mod e2_publish_recovers_after_stream_returns;
mod e3_crash_after_publish_dedupes_on_the_stream;
mod e4_unrepresentable_envelope_dead_letters;
mod e5_routing_stages_and_streams_together;
mod e6_disallow_wins_and_the_switch;

use std::process::ExitCode;

use libtest_mimic::Arguments;

/// Kept equal to `deploy/compose/docker-compose.yaml`'s `postgres:` image. Read by `ci.yaml`'s
/// pin-equality gate (keyed on the `reliar-system-tests` package existing, not on this file's
/// path), so drifting this from compose fails CI, not just a local run.
const POSTGRES_TAG: &str = "18-alpine";
/// Kept equal to `deploy/compose/docker-compose.yaml`'s `nats:` image. Read by the same gate,
/// alongside `crates/reliar-transport-nats/tests/nats/main.rs`'s own pins.
const NATS_IMAGE: &str = "nats";
const NATS_TAG: &str = "2.14-alpine";

fn main() -> ExitCode {
    let args = Arguments::from_args();

    // Leaked deliberately: this runtime is a process-lifetime singleton every trial below
    // shares, not a value with a meaningful drop (mirrors the other two providers' harnesses).
    let rt: &'static tokio::runtime::Runtime = Box::leak(Box::new(
        tokio::runtime::Runtime::new().expect("build the shared Tokio runtime"),
    ));

    // The one Postgres and one NATS container this whole run starts — **locals**, never
    // `static`s (RELIAR-27). Kept alive until every trial has finished, then dropped explicitly
    // before this function returns.
    let postgres = rt.block_on(common::start_shared_postgres(POSTGRES_TAG));
    let nats = rt.block_on(common::start_shared_nats(NATS_IMAGE, NATS_TAG));

    let mut trials = Vec::new();
    trials.extend(e1_outbox_drains_into_jetstream::trials(rt));
    trials.extend(e2_publish_recovers_after_stream_returns::trials(rt));
    trials.extend(e3_crash_after_publish_dedupes_on_the_stream::trials(rt));
    trials.extend(e4_unrepresentable_envelope_dead_letters::trials(rt));
    trials.extend(e5_routing_stages_and_streams_together::trials(rt));
    trials.extend(e6_disallow_wins_and_the_switch::trials(rt));

    let conclusion = libtest_mimic::run(&args, trials);

    // Drop both containers (and, with them, their volumes) *before* returning — never
    // `Conclusion::exit()`, which calls `process::exit` and would skip this (RELIAR-27).
    // `ContainerAsync::Drop` needs a Tokio runtime context (it calls
    // `tokio::runtime::Handle::current()` internally), which plain `main()` is not inside once
    // every `rt.block_on(..)` call above has returned — so the drop runs inside one more
    // `block_on`, not out here.
    rt.block_on(async move {
        drop(postgres);
        drop(nats);
    });
    conclusion.exit_code()
}
