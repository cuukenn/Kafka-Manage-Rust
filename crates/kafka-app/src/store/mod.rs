//! Local persistence. `db` owns the connection and migrations; the modules added
//! by later tasks (`clusters`, `settings`) are typed repositories on top of it.

pub mod clusters;
pub mod db;
pub mod settings;

pub use clusters::ImportConflict;
pub use clusters::ImportReport;
pub use db::APP_DIR_NAME;
pub use db::DB_FILE_NAME;
pub use db::SCHEMA_VERSION;
pub use db::Store;
pub use db::default_data_dir;
pub use db::default_db_path;
pub use settings::KEY_DEFAULTS;
pub use settings::KEY_MOCK_SNAPSHOT_PREFIX;
pub use settings::KEY_RECENT_CLUSTERS;
pub use settings::KEY_THEME;
pub use settings::RECENT_LIMIT;
pub use settings::mock_snapshot_key;
