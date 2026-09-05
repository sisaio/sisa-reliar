//! The single NATS-touching test binary for `reliar-transport-nats` (ADR 0031 §4, mirrors
//! `reliar-store-postgres`'s `tests/postgres/main.rs` — see that file's module docs for the full
//! RELIAR-27 rationale: `harness = false` + `libtest-mimic` so this `main` owns the shared
//! container as a **local**, never a `static`, and drops it explicitly before returning an
//! `ExitCode` rather than calling `Conclusion::exit()` (which would skip that `Drop` via
//! `process::exit`).
//!
//! `NATS_IMAGE`/`NATS_TAG` are read verbatim by `ci.yaml`'s pin-equality gate (ADR 0031 §5) —
//! keep them in sync with `deploy/compose/docker-compose.yaml`'s `nats:` service and
//! `.github/workflows/test.yaml`'s `docker run` step.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::cast_possible_truncation
)]

mod common;
mod n10_credential_and_broker_hygiene;
mod n11_publish_batch_window_deadline;
mod n1_publish_awaits_the_ack;
mod n3_duplicate_suppression;
mod n4_publish_batch_positional;
mod n5_error_classification;
mod n6_custom_resolver_subject;
mod n7_config_validation;
mod n8_publish_cancellation;
mod n9_publish_batch_cancellation;

use std::process::ExitCode;

use libtest_mimic::Arguments;

/// Read by `ci.yaml`'s pin-equality gate — keep equal to `deploy/compose/docker-compose.yaml`'s
/// `nats:` image and `.github/workflows/test.yaml`'s `docker run` image (ADR 0031 §5).
const NATS_IMAGE: &str = "nats";
/// See [`NATS_IMAGE`].
const NATS_TAG: &str = "2.14-alpine";

fn main() -> ExitCode {
    let args = Arguments::from_args();

    // Leaked deliberately: a process-lifetime singleton every trial below shares, not a value
    // with a meaningful drop (mirrors `reliar-store-postgres`'s `tests/postgres/main.rs`).
    let rt: &'static tokio::runtime::Runtime = Box::leak(Box::new(
        tokio::runtime::Runtime::new().expect("build the shared Tokio runtime"),
    ));

    // The one and only shared container this run starts — a **local**, not a `static`
    // (RELIAR-27). Kept alive until every trial has finished, then dropped explicitly below.
    let container = rt.block_on(common::start_shared_container(NATS_IMAGE, NATS_TAG));

    let mut trials = Vec::new();
    trials.extend(n1_publish_awaits_the_ack::trials(rt));
    trials.extend(n3_duplicate_suppression::trials(rt));
    trials.extend(n4_publish_batch_positional::trials(rt));
    trials.extend(n5_error_classification::trials(rt));
    trials.extend(n6_custom_resolver_subject::trials(rt));
    trials.extend(n7_config_validation::trials(rt));
    trials.extend(n8_publish_cancellation::trials(rt));
    trials.extend(n9_publish_batch_cancellation::trials(rt));
    trials.extend(n10_credential_and_broker_hygiene::trials(rt));
    trials.extend(n11_publish_batch_window_deadline::trials(rt));

    let conclusion = libtest_mimic::run(&args, trials);

    // Drop the container (and, with it, its volumes) *before* returning — never
    // `Conclusion::exit()`, which calls `process::exit` and would skip this (RELIAR-27).
    rt.block_on(async move { drop(container) });
    conclusion.exit_code()
}
