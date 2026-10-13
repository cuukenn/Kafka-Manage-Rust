//! T021 — cluster CRUD, import conflict strategies and export round-trip.

use std::collections::BTreeMap;

use kafka_app::store::ImportConflict;
use kafka_app::store::Store;
use kafka_core::ClusterId;
use kafka_core::cluster::AuthConfig;
use kafka_core::cluster::ClusterConfig;
use kafka_core::cluster::SaslMechanism;
use kafka_core::cluster::SecurityProtocol;
use kafka_core::error::StorageError;

fn full_config() -> ClusterConfig {
    let mut cfg = ClusterConfig::real("Prod", ["kafka1:9092", "kafka2:9092"]);
    cfg.security_protocol = SecurityProtocol::SaslSsl;
    cfg.auth = Some(AuthConfig {
        mechanism: SaslMechanism::ScramSha256,
        username: "ops".into(),
        password: "s3cret".into(),
        sasl_protocol: None,
    });
    cfg.ssl = None;
    let mut extra: BTreeMap<String, String> = BTreeMap::new();
    extra.insert("compression.type".into(), "lz4".into());
    cfg.extra = extra;
    cfg
}

/// A store holding two clusters, plus their saved configs.
fn seeded() -> (Store, ClusterConfig, ClusterConfig) {
    let store = Store::open_in_memory().expect("store");
    let keep = store.save_cluster(&ClusterConfig::mock("keep")).expect("save keep");
    let replace = store.save_cluster(&ClusterConfig::mock("replace")).expect("save replace");
    (store, keep, replace)
}

#[test]
fn save_then_get_keeps_every_field() {
    let store = Store::open_in_memory().expect("store");

    let saved = store.save_cluster(&full_config()).expect("save");
    assert!(saved.created_at > 0, "created_at should be filled in");
    assert!(saved.updated_at >= saved.created_at);

    let loaded = store.get_cluster(&saved.id).expect("get").expect("stored");
    assert_eq!(loaded, saved);
    assert_eq!(loaded.auth.as_ref().expect("auth").password, "s3cret");
    assert_eq!(loaded.extra.get("compression.type").map(String::as_str), Some("lz4"));
    assert_eq!(loaded.bootstrap_servers, ["kafka1:9092", "kafka2:9092"]);
    assert_eq!(store.count_clusters().expect("count"), 1);
}

#[test]
fn saving_the_same_id_updates_in_place_and_keeps_created_at() {
    let store = Store::open_in_memory().expect("store");
    let first = store.save_cluster(&ClusterConfig::mock("alpha")).expect("save");

    let mut again = first.clone();
    again.name = "alpha-renamed".into();
    again.created_at = 0;
    let second = store.save_cluster(&again).expect("re-save");

    assert_eq!(second.id, first.id);
    assert_eq!(second.created_at, first.created_at, "created_at must survive an update");
    assert_eq!(store.count_clusters().expect("count"), 1);

    let loaded = store.get_cluster(&first.id).expect("get").expect("stored");
    assert_eq!(loaded.name, "alpha-renamed");
    assert!(store.find_cluster_by_name("ALPHA-RENAMED").expect("find").is_some());
}

#[test]
fn list_is_ordered_by_name_ignoring_case() {
    let store = Store::open_in_memory().expect("store");
    for name in ["zeta", "Alpha", "mid"] {
        store.save_cluster(&ClusterConfig::mock(name)).expect("save");
    }

    let names: Vec<String> = store.list_clusters().expect("list").into_iter().map(|c| c.name).collect();
    assert_eq!(names, ["Alpha", "mid", "zeta"]);
}

#[test]
fn delete_reports_whether_a_row_was_removed() {
    let store = Store::open_in_memory().expect("store");
    let saved = store.save_cluster(&ClusterConfig::mock("gone")).expect("save");

    assert!(store.has_cluster(&saved.id).expect("has"));
    assert!(store.delete_cluster(&saved.id).expect("delete"));
    assert!(!store.delete_cluster(&saved.id).expect("second delete"));
    assert!(store.get_cluster(&saved.id).expect("get").is_none());
}

#[test]
fn a_missing_id_is_replaced_by_a_generated_one() {
    let store = Store::open_in_memory().expect("store");
    let mut cfg = ClusterConfig::mock("no-id");
    cfg.id = ClusterId(String::new());

    let saved = store.save_cluster(&cfg).expect("save");
    assert!(!saved.id.as_str().is_empty());
    assert_eq!(store.get_cluster(&saved.id).expect("get").expect("stored").id, saved.id);
}

#[test]
fn unreadable_config_json_is_reported_as_corrupt() {
    let store = Store::open_in_memory().expect("store");
    store.save_cluster(&ClusterConfig::mock("ok")).expect("save");
    store
        .conn()
        .expect("connection")
        .execute(
            "INSERT INTO clusters (id, name, config_json, created_at, updated_at) VALUES ('bad1', 'Broken', '{ not json', 1, 1)",
            [],
        )
        .expect("insert garbage");

    let err = store.list_clusters().expect_err("listing must fail");
    assert!(matches!(err, StorageError::Corrupt(_)), "unexpected error: {err:?}");

    let err = store.get_cluster(&ClusterId("bad1".into())).expect_err("get must fail");
    assert!(matches!(err, StorageError::Corrupt(_)), "unexpected error: {err:?}");
}

#[test]
fn import_skip_keeps_local_copies() {
    let (store, keep, replace) = seeded();
    let payload = import_payload(&keep, &replace);

    let report = store.import_clusters(&payload, ImportConflict::Skip).expect("import");
    assert_eq!((report.imported, report.overwritten, report.skipped), (1, 0, 2));
    assert_eq!(report.total(), 3);
    assert_eq!(store.count_clusters().expect("count"), 3);
    assert_eq!(store.get_cluster(&replace.id).expect("get").expect("stored").name, "replace");
}

#[test]
fn import_overwrite_replaces_local_copies() {
    let (store, keep, replace) = seeded();
    let payload = import_payload(&keep, &replace);

    let report = store.import_clusters(&payload, ImportConflict::Overwrite).expect("import");
    assert_eq!((report.imported, report.overwritten, report.skipped), (1, 2, 0));
    assert_eq!(store.get_cluster(&replace.id).expect("get").expect("stored").name, "replaced");
}

#[test]
fn import_fail_aborts_before_writing_anything() {
    let (store, keep, replace) = seeded();
    let payload = import_payload(&keep, &replace);

    let err = store.import_clusters(&payload, ImportConflict::Fail).expect_err("conflict must abort");
    assert!(matches!(err, StorageError::Other(_)), "unexpected error: {err:?}");
    assert_eq!(store.count_clusters().expect("count"), 2, "nothing may be written");
    assert!(store.get_cluster(&ClusterId("brand-new".into())).expect("get").is_none());
}

#[test]
fn import_rejects_a_payload_that_is_not_a_cluster_list() {
    let store = Store::open_in_memory().expect("store");
    let err = store.import_clusters("{\"not\": \"an array\"}", ImportConflict::Skip).expect_err("must fail");
    assert!(matches!(err, StorageError::Corrupt(_)), "unexpected error: {err:?}");
}

#[test]
fn saving_stamps_updated_at_even_when_the_payload_carries_one() {
    let store = Store::open_in_memory().expect("store");
    let mut cfg = ClusterConfig::mock("stamper");
    cfg.updated_at = 1;

    let saved = store.save_cluster(&cfg).expect("save");
    assert!(saved.updated_at > 1, "a user edit must move updated_at to now");

    let loaded = store.get_cluster(&saved.id).expect("get").expect("stored");
    assert_eq!(loaded.updated_at, saved.updated_at, "the stamp must reach the row");
}

#[test]
fn export_then_import_round_trips_between_stores() {
    let source = Store::open_in_memory().expect("store");
    source.save_cluster(&full_config()).expect("save prod");
    source.save_cluster(&ClusterConfig::mock("Demo")).expect("save demo");

    let dump = source.export_clusters().expect("export");
    let target = Store::open_in_memory().expect("store");
    let report = target.import_clusters(&dump, ImportConflict::Fail).expect("import");

    assert_eq!((report.imported, report.overwritten, report.skipped), (2, 0, 0));
    assert_eq!(target.list_clusters().expect("target list"), source.list_clusters().expect("source list"));
}

/// Two existing clusters with renamed copies, plus one cluster with a fresh id.
fn import_payload(keep: &ClusterConfig, replace: &ClusterConfig) -> String {
    let mut renamed = replace.clone();
    renamed.name = "replaced".into();
    let fresh = ClusterConfig {
        id: ClusterId("brand-new".into()),
        ..ClusterConfig::mock("fresh")
    };
    serde_json::to_string(&[keep.clone(), renamed, fresh]).expect("payload")
}
