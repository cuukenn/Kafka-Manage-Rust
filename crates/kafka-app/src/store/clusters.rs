//! Cluster configuration repository: CRUD plus JSON import/export on the
//! `clusters` table created by schema v1.
//!
//! // TODO(security): `config_json` keeps SASL passwords in plaintext — the local
//! // database file is the trust boundary for now. Encryption is out of scope (T021).

use rusqlite::Connection;
use rusqlite::OptionalExtension;
use rusqlite::params;

use kafka_core::cluster::ClusterConfig;
use kafka_core::error::StorageError;
use kafka_core::protocol::now_ms;
use kafka_core::ClusterId;
use kafka_core::TimestampMs;

use super::db::Store;
use super::db::sql_error;

/// What to do when an imported cluster id already exists locally.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ImportConflict {
    /// Keep the local copy and move on.
    Skip,
    /// Replace the local copy with the imported one.
    Overwrite,
    /// Abort before writing anything (default — imports are all-or-nothing).
    #[default]
    Fail,
}

/// Outcome of one import run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImportReport {
    pub imported: usize,
    pub overwritten: usize,
    pub skipped: usize,
}

impl ImportReport {
    pub fn total(&self) -> usize {
        self.imported + self.overwritten + self.skipped
    }
}

impl Store {
    /// Inserts or replaces a cluster by id and returns the stored config, with
    /// `created_at` / `updated_at` filled in.
    pub fn save_cluster(&self, cfg: &ClusterConfig) -> Result<ClusterConfig, StorageError> {
        let conn = self.conn()?;
        save_with_conn(&conn, cfg, Stamp::Now)
    }

    /// Loads one cluster, or `None` when the id is unknown.
    pub fn get_cluster(&self, id: &ClusterId) -> Result<Option<ClusterConfig>, StorageError> {
        let conn = self.conn()?;
        let row = conn
            .query_row(
                "SELECT id, config_json FROM clusters WHERE id = ?1",
                params![id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(|e| sql_error("cannot read cluster", e))?;

        row.map(|(row_id, json)| decode_config(&row_id, &json)).transpose()
    }

    /// Every saved cluster, ordered by name (case-insensitive) then id.
    pub fn list_clusters(&self) -> Result<Vec<ClusterConfig>, StorageError> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare("SELECT id, config_json FROM clusters ORDER BY name COLLATE NOCASE ASC, id ASC")
            .map_err(|e| sql_error("cannot list clusters", e))?;
        let rows = stmt
            .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))
            .map_err(|e| sql_error("cannot list clusters", e))?;

        let mut out = Vec::new();
        for row in rows {
            let (id, json) = row.map_err(|e| sql_error("cannot list clusters", e))?;
            out.push(decode_config(&id, &json)?);
        }
        Ok(out)
    }

    pub fn find_cluster_by_name(&self, name: &str) -> Result<Option<ClusterConfig>, StorageError> {
        let conn = self.conn()?;
        let row = conn
            .query_row(
                "SELECT id, config_json FROM clusters WHERE name = ?1 COLLATE NOCASE",
                params![name],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(|e| sql_error("cannot look up cluster by name", e))?;

        row.map(|(row_id, json)| decode_config(&row_id, &json)).transpose()
    }

    pub fn has_cluster(&self, id: &ClusterId) -> Result<bool, StorageError> {
        let conn = self.conn()?;
        exists(&conn, id)
    }

    pub fn count_clusters(&self) -> Result<usize, StorageError> {
        let conn = self.conn()?;
        let count = conn
            .query_row("SELECT count(*) FROM clusters", [], |row| row.get::<_, i64>(0))
            .map_err(|e| sql_error("cannot count clusters", e))?;
        Ok(count as usize)
    }

    /// Removes a cluster; `false` means there was nothing to remove.
    pub fn delete_cluster(&self, id: &ClusterId) -> Result<bool, StorageError> {
        let conn = self.conn()?;
        let removed = conn
            .execute("DELETE FROM clusters WHERE id = ?1", params![id.as_str()])
            .map_err(|e| sql_error("cannot delete cluster", e))?;
        Ok(removed > 0)
    }

    /// All clusters as a pretty-printed JSON array, ready to hand to the user.
    pub fn export_clusters(&self) -> Result<String, StorageError> {
        let all = self.list_clusters()?;
        serde_json::to_string_pretty(&all).map_err(|e| StorageError::Other(anyhow::anyhow!("cannot serialise cluster list: {e}")))
    }

    /// Imports a JSON array of clusters. A missing or empty `id` gets a fresh one,
    /// so exporting from one machine and importing on another never collides.
    pub fn import_clusters(&self, json: &str, on_conflict: ImportConflict) -> Result<ImportReport, StorageError> {
        let incoming: Vec<ClusterConfig> = serde_json::from_str(json)
            .map_err(|e| StorageError::Corrupt(format!("import payload is not a cluster list: {e}")))?;

        let conn = self.conn()?;
        let mut report = ImportReport::default();

        // `Fail` is checked up front so a rejected import never leaves a partial set behind.
        if on_conflict == ImportConflict::Fail {
            let mut clashes = Vec::new();
            for cfg in &incoming {
                if !cfg.id.as_str().is_empty() && exists(&conn, &cfg.id)? {
                    clashes.push(format!("{} ({})", cfg.name, cfg.id));
                }
            }
            if !clashes.is_empty() {
                return Err(StorageError::Other(anyhow::anyhow!("import aborted, already stored: {}", clashes.join(", "))));
            }
        }

        for cfg in &incoming {
            let known = !cfg.id.as_str().is_empty() && exists(&conn, &cfg.id)?;
            if known && on_conflict == ImportConflict::Skip {
                report.skipped += 1;
                continue;
            }
            save_with_conn(&conn, cfg, Stamp::Preserve)?;
            if known {
                report.overwritten += 1;
            } else {
                report.imported += 1;
            }
        }

        Ok(report)
    }
}

/// How a write treats the two timestamps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stamp {
    /// A user-facing edit: `updated_at` always moves to now, because the UI sends back the
    /// config it loaded (with its stale `updated_at`) and a "last modified" that never
    /// advances would be useless.
    Now,
    /// An import: a timestamp carried by the payload survives, so export → import → export
    /// is a round trip instead of a diff generator. A zero still gets filled in.
    Preserve,
}

fn save_with_conn(conn: &Connection, cfg: &ClusterConfig, stamp: Stamp) -> Result<ClusterConfig, StorageError> {
    let now = now_ms();
    let mut stored = cfg.clone();
    if stored.id.as_str().is_empty() {
        stored.id = ClusterId::new();
    }

    if stored.created_at == 0 {
        let previous: Option<TimestampMs> = conn
            .query_row("SELECT created_at FROM clusters WHERE id = ?1", params![stored.id.as_str()], |row| {
                row.get::<_, i64>(0)
            })
            .optional()
            .map_err(|e| sql_error("cannot read cluster timestamps", e))?;
        stored.created_at = previous.unwrap_or(now);
    }
    match stamp {
        Stamp::Now => stored.updated_at = now,
        Stamp::Preserve if stored.updated_at == 0 => stored.updated_at = now,
        Stamp::Preserve => {}
    }

    let json = serde_json::to_string(&stored)
        .map_err(|e| StorageError::Other(anyhow::anyhow!("cannot serialise cluster {}: {e}", stored.id)))?;

    conn.execute(
        "INSERT INTO clusters (id, name, config_json, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(id) DO UPDATE SET
            name = excluded.name,
            config_json = excluded.config_json,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at",
        params![stored.id.as_str(), stored.name, json, stored.created_at, stored.updated_at],
    )
    .map_err(|e| sql_error("cannot save cluster", e))?;

    Ok(stored)
}

fn exists(conn: &Connection, id: &ClusterId) -> Result<bool, StorageError> {
    let found = conn
        .query_row("SELECT 1 FROM clusters WHERE id = ?1", params![id.as_str()], |row| row.get::<_, i64>(0))
        .optional()
        .map_err(|e| sql_error("cannot check cluster", e))?;
    Ok(found.is_some())
}

/// Turns a stored row back into a config, keeping the row id authoritative when the
/// JSON was written by an older or hand-edited file.
fn decode_config(id: &str, json: &str) -> Result<ClusterConfig, StorageError> {
    let mut cfg: ClusterConfig = serde_json::from_str(json)
        .map_err(|e| StorageError::Corrupt(format!("cluster {id} has unreadable config_json: {e}")))?;
    if cfg.id.as_str().is_empty() {
        cfg.id = ClusterId(id.to_string());
    }
    Ok(cfg)
}
