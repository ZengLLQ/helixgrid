use rusqlite::Connection;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("domain error: {0}")]
    Domain(#[from] helixgrid_domain::DomainError),

    #[error("database integrity check failed: {0}")]
    Corruption(String),

    #[error("database lock poisoned")]
    LockPoisoned,

    #[error("storage I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct Migration {
    pub version: i64,
    pub name: &'static str,
    pub sql: &'static str,
}

pub const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "initial_schema",
    sql: include_str!("../../../migrations/0001_initial.sql"),
}];

pub fn apply_migrations(conn: &mut Connection, now_utc: i64) -> Result<i64, StorageError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at_utc INTEGER NOT NULL
        );",
    )?;

    let mut highest: i64 =
        conn.query_row("SELECT COALESCE(MAX(version), 0) FROM schema_migrations", [], |row| {
            row.get(0)
        })?;

    for migration in MIGRATIONS {
        if migration.version > highest {
            let tx = conn.transaction()?;
            tx.execute_batch(migration.sql)?;
            tx.execute(
                "INSERT INTO schema_migrations (version, name, applied_at_utc) VALUES (?1, ?2, ?3)",
                rusqlite::params![migration.version, migration.name, now_utc],
            )?;
            tx.commit()?;
            highest = migration.version;
        }
    }

    Ok(highest)
}
