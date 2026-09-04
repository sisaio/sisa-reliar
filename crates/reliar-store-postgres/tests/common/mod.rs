#![allow(dead_code)]
//! Shared fixtures for the one test binary that has nothing to do with Postgres
//! (`outbox_settings_from_env.rs`). The real-Postgres harness lives in
//! `tests/postgres/common/mod.rs` (RELIAR-27 folded every Postgres-touching scenario into one
//! binary; this file never touches a container at all, so it stayed separate).

/// The marker `env`-var scenarios use to tell a re-executed child apart from the parent test.
pub(crate) const CHILD_MARKER: &str = "RELIAR_STORE_POSTGRES_TEST_CHILD";

/// Re-executes this same test binary, filtered to exactly `test_name`, with `envs` set only for
/// the child process, and returns whether the child's assertions passed.
///
/// `PostgresOutboxSettings::from_env` reads real process environment variables, and mutating
/// *this* process's environment safely requires `std::env::set_var` — `unsafe` since edition
/// 2024, and `unsafe_code = "forbid"` is a workspace lint applied to every target in this crate,
/// `tests/` included. Each `env`-touching scenario instead spawns a **child** copy of this
/// binary scoped to one test name, with the environment set via `Command::env` — safe, because
/// it only ever affects the child's environment, never this process's (mirrors
/// `reliar-outbox/tests/common::run_scenario_in_child`).
///
/// # Errors
///
/// Returns the underlying [`std::io::Error`] if this binary's own path cannot be resolved or the
/// child process cannot be spawned or awaited.
pub(crate) fn run_scenario_in_child(
    test_name: &str,
    envs: &[(&str, &str)],
) -> std::io::Result<bool> {
    let exe = std::env::current_exe()?;
    let mut command = std::process::Command::new(exe);
    command.arg("--exact").arg(test_name).env(CHILD_MARKER, "1");
    for (key, value) in envs {
        command.env(key, value);
    }
    let output = command.output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let ran_exactly_the_one_scenario = stdout.contains("1 passed; 0 failed");
    if !output.status.success() || !ran_exactly_the_one_scenario {
        eprintln!(
            "child scenario `{test_name}` did not cleanly report `1 passed; 0 failed`:\n{stdout}"
        );
    }
    Ok(output.status.success() && ran_exactly_the_one_scenario)
}

/// `true` inside the child process spawned by [`run_scenario_in_child`].
pub(crate) fn is_child() -> bool {
    std::env::var_os(CHILD_MARKER).is_some()
}
