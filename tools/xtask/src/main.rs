//! Repository task runner: `cargo xtask <task>` forwards to `scripts/<task>.sh`.
//!
//! The scripts are POSIX shell; on Windows run them under WSL.

use std::{env, path::PathBuf, process::Command};

const TASKS: [&str; 4] = ["lint", "test", "dev-db", "release"];

fn main() -> std::process::ExitCode {
    let Some(task) = env::args().nth(1) else {
        eprintln!("usage: cargo xtask <{}>", TASKS.join("|"));
        return std::process::ExitCode::FAILURE;
    };
    if !TASKS.contains(&task.as_str()) {
        eprintln!(
            "unknown task `{task}`; expected one of {}",
            TASKS.join(", ")
        );
        return std::process::ExitCode::FAILURE;
    }

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let script = root.join("scripts").join(format!("{task}.sh"));
    let args: Vec<String> = env::args().skip(2).collect();

    match Command::new(&script)
        .args(&args)
        .current_dir(&root)
        .status()
    {
        Ok(status) if status.success() => std::process::ExitCode::SUCCESS,
        Ok(status) => {
            eprintln!("{} exited with {status}", script.display());
            std::process::ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("failed to run {}: {error}", script.display());
            std::process::ExitCode::FAILURE
        }
    }
}
