//! T020 — SQLite foundation: opening, pragmas, versioned migrations, error mapping.

use std::fs;

use kafka_app::store::APP_DIR_NAME;
use kafka_app::store::DB_FILE_NAME;
use kafka_app::store::SCHEMA_VERSION;
use kafka_app::store::Store;
use kafka_app::store::default_db_path;
use kafka_core::error::StorageError;
use tempfile::TempDir;

fn business_tables(store: &Store) -> i64 {
    let conn = store.conn().expect("connection");
    conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name IN ('clusters', 'settings')",
        [],
        |row| row.get::<_, i64>(0),
    )
    .expect("count tables")
}

fn pragma_i64(store: &Store, sql: &str) -> i64 {
    let conn = store.conn().expect("connection");
    conn.query_row(sql, [], |row| row.get::<_, i64>(0)).expect(sql)
}

fn pragma_text(store: &Store, sql: &str) -> String {
    let conn = store.conn().expect("connection");
    conn.query_row(sql, [], |row| row.get::<_, String>(0)).expect(sql)
}

#[test]
fn open_creates_missing_directories_and_tables() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("nested").join("deep").join(DB_FILE_NAME);

    let store = Store::open(&path).expect("open");

    assert!(path.exists(), "database file should exist");
    assert_eq!(store.path(), Some(path.as_path()));
    assert_eq!(store.schema_version().expect("version"), SCHEMA_VERSION);
    assert_eq!(business_tables(&store), 2);

    // Cloning shares one connection, so the clone sees the same schema.
    let clone = store.clone();
    assert_eq!(business_tables(&clone), 2);
}

#[test]
fn reopening_keeps_rows_and_does_not_rebuild_the_schema() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("data.db");

    {
        let store = Store::open(&path).expect("first open");
        store
            .conn()
            .expect("connection")
            .execute(
                "INSERT INTO clusters (id, name, config_json, created_at, updated_at) VALUES ('c1', 'Local', '{}', 1, 2)",
                [],
            )
            .expect("insert");
    }

    let store = Store::open(&path).expect("second open");
    assert_eq!(store.schema_version().expect("version"), SCHEMA_VERSION);
    assert_eq!(business_tables(&store), 2);
    assert_eq!(pragma_i64(&store, "SELECT count(*) FROM clusters"), 1);
}

#[test]
fn file_databases_use_wal_and_enforce_foreign_keys() {
    let dir = TempDir::new().expect("temp dir");
    let store = Store::open(dir.path().join("wal.db")).expect("open");

    assert_eq!(pragma_text(&store, "PRAGMA journal_mode").to_lowercase(), "wal");
    assert_eq!(pragma_i64(&store, "PRAGMA foreign_keys"), 1);
    assert!(pragma_i64(&store, "PRAGMA busy_timeout") >= 4_000);
}

#[test]
fn in_memory_store_is_migrated_without_a_file() {
    let store = Store::open_in_memory().expect("open in memory");

    assert_eq!(store.path(), None);
    assert_eq!(store.schema_version().expect("version"), SCHEMA_VERSION);
    assert_eq!(business_tables(&store), 2);
    assert_eq!(pragma_text(&store, "PRAGMA journal_mode").to_lowercase(), "memory");
}

#[test]
fn a_file_that_is_not_a_database_is_reported_as_corrupt() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("broken.db");
    fs::write(&path, b"this is definitely not a sqlite database file").expect("write garbage");

    let err = Store::open(&path).expect_err("garbage file must not open");

    assert!(matches!(err, StorageError::Corrupt(_)), "unexpected error: {err:?}");
}

#[test]
fn a_schema_newer_than_this_build_is_refused() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().join("future.db");

    {
        let store = Store::open(&path).expect("open");
        store
            .conn()
            .expect("connection")
            .execute_batch(&format!("PRAGMA user_version = {};", SCHEMA_VERSION + 1))
            .expect("bump version");
    }

    let err = Store::open(&path).expect_err("newer schema must be refused");
    assert!(matches!(err, StorageError::Corrupt(_)), "unexpected error: {err:?}");
}

#[test]
fn default_db_path_lives_under_the_app_directory() {
    let path = default_db_path();
    let mut parts = path.components().rev();

    assert_eq!(parts.next().and_then(|c| c.as_os_str().to_str()), Some(DB_FILE_NAME));
    assert_eq!(parts.next().and_then(|c| c.as_os_str().to_str()), Some(APP_DIR_NAME));
}
