//! SQLite foundation: connection handling, pragmas and versioned migrations.
//!
//! The store owns a single [`rusqlite::Connection`] behind `Arc<Mutex<..>>`
//! (the connection is `Send` but not `Sync`). Business services reach it
//! through [`Store::conn`] and are expected to run those calls inside
//! `tokio::task::spawn_blocking`.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use rusqlite::Connection;

use kafka_core::error::StorageError;

/// Schema version recorded in `PRAGMA user_version`. Bump together with a new
/// arm in [`apply_migration`]; never renumber an already released version.
pub const SCHEMA_VERSION: i64 = 1;

/// Folder name under the platform data directory.
pub const APP_DIR_NAME: &str = "rs-kafka-ui-client";

/// Database file name inside [`APP_DIR_NAME`].
pub const DB_FILE_NAME: &str = "data.db";

/// Handle to the local database. Cloning is cheap — every clone shares one connection.
#[derive(Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
    path: Option<PathBuf>,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store").field("path", &self.path).finish_non_exhaustive()
    }
}

impl Store {
    /// Opens (creating if needed) the database at `path`, including parent directories.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| StorageError::Io(format!("cannot create data directory {}: {e}", parent.display())))?;
            }
        }

        // Opening is lazy, so a damaged file surfaces on the first pragma below;
        // mapping through `sql_error` keeps both cases classified the same way.
        let conn = Connection::open(path).map_err(|e| sql_error(&format!("cannot open database {}", path.display()), e))?;
        Self::from_connection(conn, Some(path.to_path_buf()))
    }

    /// Opens an in-memory database — used by tests and by throwaway sessions.
    pub fn open_in_memory() -> Result<Self, StorageError> {
        let conn = Connection::open_in_memory().map_err(|e| StorageError::Io(format!("cannot open in-memory database: {e}")))?;
        Self::from_connection(conn, None)
    }

    /// Where this database lives, `None` for in-memory stores.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Grants access to the shared connection. Errors only when a previous
    /// panic poisoned the lock.
    pub fn conn(&self) -> Result<MutexGuard<'_, Connection>, StorageError> {
        self.conn
            .lock()
            .map_err(|_| StorageError::Other(anyhow::anyhow!("database lock was poisoned by an earlier panic")))
    }

    /// Current schema version stored in the file (equals [`SCHEMA_VERSION`] after a
    /// successful open).
    pub fn schema_version(&self) -> Result<i64, StorageError> {
        let conn = self.conn()?;
        user_version(&conn)
    }

    fn from_connection(conn: Connection, path: Option<PathBuf>) -> Result<Self, StorageError> {
        let memory = path.is_none();
        configure(&conn, memory)?;
        migrate(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            path,
        })
    }
}

/// Applies pragmas. WAL is skipped for in-memory databases, where SQLite keeps
/// its own journal mode.
fn configure(conn: &Connection, memory: bool) -> Result<(), StorageError> {
    let sql = if memory {
        "PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 4000;"
    } else {
        "PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 4000;"
    };
    conn.execute_batch(sql).map_err(|e| sql_error("cannot configure database", e))
}

/// Runs every migration newer than the file's `user_version`, in order.
fn migrate(conn: &Connection) -> Result<(), StorageError> {
    let current = user_version(conn)?;
    if current > SCHEMA_VERSION {
        return Err(StorageError::Corrupt(format!(
            "database schema version {current} is newer than this build supports ({SCHEMA_VERSION})"
        )));
    }

    for version in (current + 1)..=SCHEMA_VERSION {
        apply_migration(conn, version)?;
        set_user_version(conn, version)?;
    }
    Ok(())
}

/// The only schema changes so far. Statements must stay idempotent so a partially
/// applied migration can be replayed.
fn apply_migration(conn: &Connection, version: i64) -> Result<(), StorageError> {
    let sql = match version {
        1 => {
            "CREATE TABLE IF NOT EXISTS clusters (
                id TEXT PRIMARY KEY,
                name TEXT,
                config_json TEXT NOT NULL,
                created_at INTEGER,
                updated_at INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_clusters_name ON clusters (name);

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL
            );"
        }
        other => {
            return Err(StorageError::Other(anyhow::anyhow!("no migration registered for schema version {other}")));
        }
    };

    conn.execute_batch(sql)
        .map_err(|e| sql_error(&format!("cannot apply schema version {version}"), e))
}

fn user_version(conn: &Connection) -> Result<i64, StorageError> {
    conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
        .map_err(|e| sql_error("cannot read schema version", e))
}

fn set_user_version(conn: &Connection, version: i64) -> Result<(), StorageError> {
    // PRAGMA statements take no bound parameters, so the value is inlined.
    conn.execute_batch(&format!("PRAGMA user_version = {version};"))
        .map_err(|e| sql_error("cannot write schema version", e))
}

/// Maps a rusqlite failure onto [`StorageError`], keeping the message short enough
/// to show in the UI and separating unreadable files from real SQL problems.
pub(crate) fn sql_error(context: &str, err: rusqlite::Error) -> StorageError {
    let text = err.to_string();
    if text.contains("not a database") || text.contains("malformed") {
        StorageError::Corrupt(format!("{context}: {text}"))
    } else {
        StorageError::Sql(format!("{context}: {text}"))
    }
}

/// Platform data directory, e.g. `%APPDATA%\rs-kafka-ui-client` on Windows or
/// `~/.local/share/rs-kafka-ui-client` on Linux.
pub fn default_data_dir() -> PathBuf {
    let base = dirs::data_local_dir()
        .or_else(dirs::data_dir)
        .or_else(dirs::config_dir)
        .unwrap_or_else(|| PathBuf::from("."));
    base.join(APP_DIR_NAME)
}

/// Default database location: [`default_data_dir`] + [`DB_FILE_NAME`].
pub fn default_db_path() -> PathBuf {
    default_data_dir().join(DB_FILE_NAME)
}
