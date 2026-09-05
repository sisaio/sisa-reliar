//! The single Postgres-touching test binary for `reliar-store-postgres` (RELIAR-27, architect
//! ruling `docs/analysis/architect-review.md` §9, contract §7 rows P1–P4).
//!
//! **Why this file exists.** Before this, every scenario (`tests/outbox_*.rs`, `tests/migrate.rs`)
//! was its own `[[test]]` binary, each lazily starting its own shared Postgres container in a
//! `static OnceLock<ContainerAsync<..>>`. Two facts made that leak every container, forever:
//! `testcontainers` 0.27 has **no reaper** (no Ryuk) — the *only* removal path is
//! `ContainerAsync::Drop` — and Rust **never runs destructors for `static`s** at process exit, so
//! that `Drop` never ran. ~25 scenario binaries meant ~25 leaked containers (plus volumes) per
//! `cargo test -p reliar-store-postgres` run.
//!
//! **The fix.** `harness = false` + `libtest-mimic` (`Cargo.toml`'s single `[[test]] name =
//! "postgres"` entry) so this `main` owns the shared container as a **local, not a `static`**:
//! it starts the container, runs every scenario (`mod` per former file, `Trial` per former
//! `#[tokio::test]` fn) on one shared Tokio runtime, then **drops the container before
//! exiting**. `main` returning `ExitCode` (rather than calling `Conclusion::exit()`, which calls
//! `process::exit` and would skip every destructor including this one) is what makes that
//! ordering happen — falling out of `main`'s scope runs every local's `Drop` first, and only
//! then does the runtime convert the returned `ExitCode` into the real process exit code.
//!
//! The `watchdog` dev-dependency feature (enabled in `Cargo.toml`) is the belt to this belt: it
//! removes registered containers on SIGINT/SIGTERM/SIGQUIT, which no amount of `Drop` correctness
//! can cover (a killed process runs no destructors either).

// Crate-wide (this binary has no other crate root): a `harness = false` target is not compiled
// with `rustc --test`, so it loses whatever implicit "this is test code" exemption normally
// covers restriction lints like these in a standard-harness test binary — every module below
// still needs them for exactly the reasons `unwrap`/`expect`/`panic!` are always fine in a test
// assertion (that's how a test reports failure) and a seeded test id occasionally exceeds an
// `i32`/`i64` cast target harmlessly.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::cast_possible_truncation
)]

mod common;
mod migrate;
mod outbox_acquire_skip_locked;
mod outbox_acquire_skip_locked_held_lock;
mod outbox_claim_no_lock_during_publish;
mod outbox_complete;
mod outbox_constraint_names;
mod outbox_dead_letters;
mod outbox_dispatcher_end_to_end;
mod outbox_enqueue_atomic;
mod outbox_enqueue_serialize_error;
mod outbox_epoch_millis_codec;
mod outbox_error_classification;
mod outbox_fail_retry_dead;
mod outbox_lease_management;
mod outbox_lease_recovery;
mod outbox_non_default_schema;
mod outbox_pgdog;
mod outbox_poisoned_row;
mod outbox_purge;
mod outbox_purge_concurrent_resurrection;
mod outbox_roundtrip;
mod outbox_schema_verification;
mod outbox_statement_timeout;
mod outbox_stats;
mod routing_enqueue;

use std::process::ExitCode;

use libtest_mimic::Arguments;

fn main() -> ExitCode {
    let args = Arguments::from_args();

    // Leaked deliberately: this runtime is a process-lifetime singleton (every trial below
    // shares it), not a value with a meaningful drop — `Box::leak` is the standard way to get a
    // `&'static` out of a value built at runtime, cheaper and clearer here than an `Arc` every
    // scenario module would otherwise need to clone.
    let rt: &'static tokio::runtime::Runtime = Box::leak(Box::new(
        tokio::runtime::Runtime::new().expect("build the shared Tokio runtime"),
    ));

    // The one and only container this whole run starts — a **local**, not a `static` (that
    // `static` was RELIAR-27's bug). Kept alive until every trial has finished, then dropped
    // explicitly, below, before this function returns.
    let container = rt.block_on(common::start_shared_container());

    let mut trials = Vec::new();
    trials.extend(migrate::trials(rt));
    trials.extend(outbox_acquire_skip_locked::trials(rt));
    trials.extend(outbox_acquire_skip_locked_held_lock::trials(rt));
    trials.extend(outbox_claim_no_lock_during_publish::trials(rt));
    trials.extend(outbox_complete::trials(rt));
    trials.extend(outbox_constraint_names::trials(rt));
    trials.extend(outbox_dead_letters::trials(rt));
    trials.extend(outbox_dispatcher_end_to_end::trials(rt));
    trials.extend(outbox_enqueue_atomic::trials(rt));
    trials.extend(outbox_enqueue_serialize_error::trials(rt));
    trials.extend(outbox_epoch_millis_codec::trials(rt));
    trials.extend(outbox_error_classification::trials(rt));
    trials.extend(outbox_fail_retry_dead::trials(rt));
    trials.extend(outbox_lease_management::trials(rt));
    trials.extend(outbox_lease_recovery::trials(rt));
    trials.extend(outbox_non_default_schema::trials(rt));
    trials.extend(outbox_pgdog::trials(rt));
    trials.extend(outbox_poisoned_row::trials(rt));
    trials.extend(outbox_purge::trials(rt));
    trials.extend(outbox_purge_concurrent_resurrection::trials(rt));
    trials.extend(outbox_roundtrip::trials(rt));
    trials.extend(outbox_schema_verification::trials(rt));
    trials.extend(outbox_statement_timeout::trials(rt));
    trials.extend(outbox_stats::trials(rt));
    trials.extend(routing_enqueue::trials(rt));

    let conclusion = libtest_mimic::run(&args, trials);

    // Drop the container (and, with it, its volumes) *before* returning — never
    // `Conclusion::exit()`, which calls `process::exit` and would skip this (RELIAR-27).
    // `ContainerAsync`'s own `Drop` needs a Tokio runtime context (it calls
    // `tokio::runtime::Handle::current()` internally to perform its async cleanup), which plain
    // `main()` is not inside once every `rt.block_on(..)` call above has returned — so the drop
    // itself runs inside one more `block_on`, not out here.
    rt.block_on(async move { drop(container) });
    conclusion.exit_code()
}
