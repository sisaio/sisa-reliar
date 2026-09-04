//! Applies Reliar's PostgreSQL migrations through this crate's public [`migrate`] API.
//!
//! ```text
//! DATABASE_URL=postgres://user:pw@host/db cargo run -p reliar-store-postgres --example migrate
//! RELIAR_SCHEMA=tenant_a DATABASE_URL=...  cargo run -p reliar-store-postgres --example migrate
//! ```
//!
//! Two jobs, one file. In CI it creates the schema before `cargo sqlx prepare --check` and the
//! provider's integration tests run against the service container — the query macros are verified
//! against a live database, so that database has to exist first. For operators it is the smallest
//! possible migration CLI: a host that would rather not call [`migrate`] from its own startup path
//! can copy this file, and a host that runs the published SQL artifact through its own pipeline
//! (ADR 0018) needs neither.
//!

use std::process::ExitCode;

use reliar_store_postgres::{MigrateOptions, migrate};
use sqlx::postgres::PgPoolOptions;

/// Schema override, matching `MigrateOptions::default().schema` when unset.
const SCHEMA_VAR: &str = "RELIAR_SCHEMA";

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(schema) => {
            println!("reliar migrate: schema `{schema}` is up to date");
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("reliar migrate: {message}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<String, String> {
    // Read, never guess: a migration tool that invents a connection string is one that migrates
    // the wrong database exactly once.
    let url = std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL is not set".to_owned())?;
    let schema =
        std::env::var(SCHEMA_VAR).unwrap_or_else(|_| MigrateOptions::default().schema.to_owned());

    // One connection: this runs once and exits, and `migrate` serializes concurrent callers on an
    // advisory lock anyway (ADR 0018).
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        // The URL carries credentials, so it is never echoed — sqlx's own Display is already
        // careful, but the rule is ours to keep (SRS §17.1, §33).
        .map_err(|e| format!("could not connect: {e}"))?;

    migrate(&pool, MigrateOptions::default().schema(&schema))
        .await
        .map_err(|e| format!("migration failed: {e}"))?;

    pool.close().await;
    Ok(schema)
}
