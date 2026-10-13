//! Application settings: a generic JSON key/value table plus the few helpers the
//! UI needs (recent clusters, mock snapshots).
//!
//! `config.get` / `config.set` map straight onto [`Store::get_json`] and
//! [`Store::set_json`], so anything the frontend can serialise is storable without
//! a schema change. Values are kept as their JSON text, which makes the table
//! readable by hand and keeps migration count at one.

use rusqlite::{OptionalExtension, params};
use serde::Serialize;
use serde::de::DeserializeOwned;

use kafka_core::ClusterId;
use kafka_core::error::StorageError;

use super::db::{Store, sql_error};

/// UI theme preference (`"light"` / `"dark"` / `"system"`).
pub const KEY_THEME: &str = "app.theme";

/// Default values used to pre-fill forms (timeouts, partition counts, ...).
pub const KEY_DEFAULTS: &str = "app.defaults";

/// Most recently used cluster ids, newest first.
pub const KEY_RECENT_CLUSTERS: &str = "app.recentClusters";

/// Prefix of the per-cluster mock snapshot slot: `mock.snapshot.<clusterId>`.
pub const KEY_MOCK_SNAPSHOT_PREFIX: &str = "mock.snapshot.";

/// How many recent clusters are kept.
pub const RECENT_LIMIT: usize = 10;

/// Settings table.
impl Store {
    /// Stores `value` under `key`, replacing whatever was there.
    pub fn set_json<T: Serialize>(&self, key: &str, value: &T) -> Result<(), StorageError> {
        let json = serde_json::to_string(value)
            .map_err(|e| StorageError::Other(anyhow::anyhow!("setting {key} cannot be serialised: {e}")))?;
        self.set_raw(key, &json)
    }

    /// Reads `key`, deserialising it into `T`. Absent keys are `Ok(None)`; a value
    /// that no longer matches `T` is reported as corrupt rather than silently
    /// replaced, so a bad migration shows up instead of resetting user settings.
    pub fn get_json<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, StorageError> {
        let raw = self.get_raw(key)?;
        match raw {
            None => Ok(None),
            Some(text) => serde_json::from_str(&text)
                .map(Some)
                .map_err(|e| StorageError::Corrupt(format!("setting {key} is unreadable: {e}"))),
        }
    }

    /// Stores an already-encoded JSON document verbatim — used for mock snapshots,
    /// which would otherwise be double-encoded as a JSON string literal.
    pub fn set_raw(&self, key: &str, json: &str) -> Result<(), StorageError> {
        // Reject malformed documents here: a snapshot slot that cannot be parsed
        // is discovered at connect time, when the user least expects it.
        serde_json::from_str::<serde_json::Value>(json)
            .map_err(|e| StorageError::Corrupt(format!("setting {key} is not valid JSON: {e}")))?;

        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO settings (key, value_json) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json;",
            params![key, json],
        )
        .map(|_| ())
        .map_err(|e| sql_error(&format!("cannot store setting {key}"), e))
    }

    /// Raw JSON text for `key`, `None` when it is absent.
    pub fn get_raw(&self, key: &str) -> Result<Option<String>, StorageError> {
        let conn = self.conn()?;
        conn.query_row("SELECT value_json FROM settings WHERE key = ?1;", params![key], |row| {
            row.get::<_, String>(0)
        })
        .optional()
        .map_err(|e| sql_error(&format!("cannot read setting {key}"), e))
    }

    /// Drops `key`. Returns whether anything was removed.
    pub fn delete_setting(&self, key: &str) -> Result<bool, StorageError> {
        let conn = self.conn()?;
        conn.execute("DELETE FROM settings WHERE key = ?1;", params![key])
            .map(|n| n > 0)
            .map_err(|e| sql_error(&format!("cannot delete setting {key}"), e))
    }

    /// How many settings are stored.
    pub fn count_settings(&self) -> usize {
        let Ok(conn) = self.conn() else { return 0 };
        conn.query_row("SELECT count(*) FROM settings;", [], |row| row.get::<_, i64>(0))
            .map(|n| n.max(0) as usize)
            .unwrap_or(0)
    }

    /// Every setting whose key starts with `prefix`, as `(key, value)` pairs
    /// ordered by key. Comparison is done in SQL (`substr`) so cluster ids and
    /// other keys never need `LIKE` escaping.
    pub fn settings_map(&self, prefix: &str) -> Result<Vec<(String, serde_json::Value)>, StorageError> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare("SELECT key, value_json FROM settings WHERE substr(key, 1, ?2) = ?1 ORDER BY key;")
            .map_err(|e| sql_error("cannot read settings", e))?;

        let rows = stmt
            .query_map(params![prefix, prefix.chars().count() as i64], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|e| sql_error("cannot read settings", e))?;

        let mut out = Vec::new();
        for row in rows {
            let (key, text) = row.map_err(|e| sql_error("cannot read settings", e))?;
            let value = serde_json::from_str(&text)
                .map_err(|e| StorageError::Corrupt(format!("setting {key} is unreadable: {e}")))?;
            out.push((key, value));
        }
        Ok(out)
    }

    /// Most recently used clusters, newest first.
    pub fn recent_clusters(&self) -> Result<Vec<ClusterId>, StorageError> {
        let ids: Vec<String> = self.get_json(KEY_RECENT_CLUSTERS)?.unwrap_or_default();
        Ok(ids.into_iter().map(ClusterId).collect())
    }

    /// Marks `cluster_id` as most recent, removing any older entry for it and
    /// trimming the list to [`RECENT_LIMIT`]. Returns the new order.
    pub fn push_recent(&self, cluster_id: &ClusterId) -> Result<Vec<ClusterId>, StorageError> {
        let mut current = self.recent_clusters()?;
        current.retain(|id| id != cluster_id);
        current.insert(0, cluster_id.clone());
        current.truncate(RECENT_LIMIT);

        let ids: Vec<&str> = current.iter().map(ClusterId::as_str).collect();
        self.set_json(KEY_RECENT_CLUSTERS, &ids)?;
        Ok(current)
    }

    /// Removes a cluster from the recent list (used when it gets deleted).
    pub fn forget_recent(&self, cluster_id: &ClusterId) -> Result<Vec<ClusterId>, StorageError> {
        let current = self.recent_clusters()?;
        let kept: Vec<&str> = current.iter().filter(|id| *id != cluster_id).map(ClusterId::as_str).collect();
        self.set_json(KEY_RECENT_CLUSTERS, &kept)?;
        Ok(current.into_iter().filter(|id| id != cluster_id).collect())
    }

    /// Persisted mock snapshot for `cluster_id`, as produced by
    /// `MockKafkaDriver::snapshot_json`.
    pub fn save_mock_snapshot(&self, cluster_id: &ClusterId, json: &str) -> Result<(), StorageError> {
        self.set_raw(&mock_snapshot_key(cluster_id), json)
    }

    /// Stored mock snapshot, `None` when the cluster has none yet.
    pub fn load_mock_snapshot(&self, cluster_id: &ClusterId) -> Result<Option<String>, StorageError> {
        self.get_raw(&mock_snapshot_key(cluster_id))
    }

    /// Drops the snapshot of one cluster.
    pub fn clear_mock_snapshot(&self, cluster_id: &ClusterId) -> Result<bool, StorageError> {
        self.delete_setting(&mock_snapshot_key(cluster_id))
    }
}

/// Settings key for the mock snapshot slot of `cluster_id`.
pub fn mock_snapshot_key(cluster_id: &ClusterId) -> String {
    format!("{KEY_MOCK_SNAPSHOT_PREFIX}{cluster_id}")
}
