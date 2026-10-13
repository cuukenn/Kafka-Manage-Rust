//! T022 acceptance: settings KV, recent clusters, mock snapshot slots, concurrency.

use kafka_app::store::settings::{
    KEY_DEFAULTS, KEY_MOCK_SNAPSHOT_PREFIX, KEY_RECENT_CLUSTERS, KEY_THEME, RECENT_LIMIT, mock_snapshot_key,
};
use kafka_app::store::Store;
use kafka_core::ClusterId;
use kafka_core::error::StorageError;

fn store() -> Store {
    Store::open_in_memory().expect("in-memory store")
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
struct Theme {
    mode: String,
    accent: u32,
}

#[test]
fn set_and_get_roundtrip() {
    let store = store();

    store.set_json(KEY_THEME, &Theme { mode: "dark".into(), accent: 7 }).unwrap();
    assert_eq!(
        store.get_json::<Theme>(KEY_THEME).unwrap(),
        Some(Theme { mode: "dark".into(), accent: 7 })
    );

    // Plain JSON scalars, arrays and objects all pass through unchanged.
    store.set_json("app.count", &42u32).unwrap();
    assert_eq!(store.get_json::<u32>("app.count").unwrap(), Some(42));
    store.set_json("app.list", &vec!["a", "b"]).unwrap();
    assert_eq!(
        store.get_json::<Vec<String>>("app.list").unwrap(),
        Some(vec!["a".to_string(), "b".to_string()])
    );

    // Overwriting replaces rather than appends.
    store.set_json("app.count", &7u32).unwrap();
    assert_eq!(store.get_json::<u32>("app.count").unwrap(), Some(7));
    assert_eq!(store.count_settings(), 3);
}

#[test]
fn missing_key_is_not_an_error() {
    let store = store();
    assert_eq!(store.get_json::<String>("app.nope").unwrap(), None);
    assert_eq!(store.get_raw("app.nope").unwrap(), None);
    assert!(!store.delete_setting("app.nope").unwrap());
}

#[test]
fn unreadable_value_is_corrupt_not_defaulted() {
    let store = store();
    store.set_json(KEY_THEME, &"dark").unwrap();

    // Same key, incompatible type: the stored value must be reported, not hidden.
    let err = store.get_json::<u32>(KEY_THEME).unwrap_err();
    assert!(matches!(err, StorageError::Corrupt(_)), "{err:?}");
    assert!(err.to_string().contains(KEY_THEME));
}

#[test]
fn raw_json_is_stored_verbatim() {
    let store = store();
    let doc = r#"{"brokerCount":3,"topics":[]}"#;
    store.set_raw("mock.state", doc).unwrap();
    assert_eq!(store.get_raw("mock.raw_missing").unwrap(), None);
    assert_eq!(store.get_raw("mock.state").unwrap().as_deref(), Some(doc));

    // Malformed documents are refused at write time.
    let err = store.set_raw("mock.state", "{ not json").unwrap_err();
    assert!(matches!(err, StorageError::Corrupt(_)), "{err:?}");
    assert_eq!(store.get_raw("mock.state").unwrap().as_deref(), Some(doc));
}

#[test]
fn key_constants_match_the_protocol_contract() {
    assert_eq!(KEY_THEME, "app.theme");
    assert_eq!(KEY_DEFAULTS, "app.defaults");
    assert_eq!(KEY_RECENT_CLUSTERS, "app.recentClusters");
    assert_eq!(KEY_MOCK_SNAPSHOT_PREFIX, "mock.snapshot.");
    assert_eq!(mock_snapshot_key(&ClusterId("abc123".into())), "mock.snapshot.abc123");
}

#[test]
fn recent_clusters_dedupe_and_limit() {
    let store = store();

    let ids: Vec<ClusterId> = (0..(RECENT_LIMIT + 5)).map(|_| ClusterId::new()).collect();
    for id in &ids {
        store.push_recent(id).unwrap();
    }

    let recent = store.recent_clusters().unwrap();
    assert_eq!(recent.len(), RECENT_LIMIT);
    assert_eq!(recent[0], ids[ids.len() - 1], "newest first");
    assert_eq!(recent[1], ids[ids.len() - 2]);
    assert_eq!(recent.last().unwrap(), &ids[5], "oldest five were trimmed");

    let mut unique = recent.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), recent.len(), "no duplicates");

    // Re-visiting an existing cluster moves it to the front without growing the list.
    let moved = store.push_recent(&ids[7]).unwrap();
    assert_eq!(moved.len(), RECENT_LIMIT);
    assert_eq!(moved[0], ids[7]);
    assert_eq!(moved.iter().filter(|id| **id == ids[7]).count(), 1);

    assert_eq!(store.forget_recent(&ids[7]).unwrap().len(), RECENT_LIMIT - 1);
    assert!(!store.recent_clusters().unwrap().contains(&ids[7]));
    assert!(store.recent_clusters().unwrap().contains(&ids[6]));
}

#[test]
fn mock_snapshot_slots_are_per_cluster() {
    let store = store();
    let a = ClusterId::new();
    let b = ClusterId::new();

    assert_eq!(store.load_mock_snapshot(&a).unwrap(), None);

    store.save_mock_snapshot(&a, r#"{"brokerCount":3}"#).unwrap();
    store.save_mock_snapshot(&b, r#"{"brokerCount":1}"#).unwrap();

    assert_eq!(store.load_mock_snapshot(&a).unwrap().as_deref(), Some(r#"{"brokerCount":3}"#));
    assert_eq!(store.load_mock_snapshot(&b).unwrap().as_deref(), Some(r#"{"brokerCount":1}"#));

    // Overwrite keeps one slot per cluster.
    store.save_mock_snapshot(&a, r#"{"brokerCount":9}"#).unwrap();
    assert_eq!(store.load_mock_snapshot(&a).unwrap().as_deref(), Some(r#"{"brokerCount":9}"#));

    assert!(store.clear_mock_snapshot(&a).unwrap());
    assert!(!store.clear_mock_snapshot(&a).unwrap());
    assert_eq!(store.load_mock_snapshot(&a).unwrap(), None);
    assert!(store.load_mock_snapshot(&b).unwrap().is_some());
}

#[test]
fn settings_map_filters_by_prefix() {
    let store = store();
    store.set_json(KEY_THEME, &"light").unwrap();
    store.set_json(KEY_DEFAULTS, &1_500u32).unwrap();
    store.save_mock_snapshot(&ClusterId("zz".into()), "{}").unwrap();
    store.save_mock_snapshot(&ClusterId("aa".into()), "{}").unwrap();

    let mock: Vec<String> = store.settings_map(KEY_MOCK_SNAPSHOT_PREFIX).unwrap().into_iter().map(|(k, _)| k).collect();
    assert_eq!(mock, vec!["mock.snapshot.aa", "mock.snapshot.zz"], "sorted by key");

    let app: Vec<String> = store.settings_map("app.").unwrap().into_iter().map(|(k, _)| k).collect();
    assert_eq!(app, vec![KEY_DEFAULTS, KEY_THEME]);

    let all = store.settings_map("").unwrap();
    assert_eq!(all.len(), 4);
    let values: Vec<&serde_json::Value> = all.iter().map(|(_, v)| v).collect();
    assert!(values.iter().any(|v| **v == serde_json::json!("light")));
}

#[test]
fn concurrent_writes_do_not_tear_or_panic() {
    let store = store();
    let mut handles = Vec::new();

    for worker in 0..8u32 {
        let store = store.clone();
        handles.push(std::thread::spawn(move || {
            for round in 0..20u32 {
                // One self-consistent document per write, so a torn write would be
                // detectable as a value that no worker ever produced.
                let doc = format!("{{\"worker\":{worker},\"round\":{round}}}");
                store.set_raw("app.defaults", &doc).unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().expect("writer thread must not panic");
    }

    let final_raw = store.get_raw("app.defaults").unwrap().expect("value present");
    let parsed: serde_json::Value = serde_json::from_str(&final_raw).expect("final value is valid JSON");
    assert!(parsed.get("worker").is_some() && parsed.get("round").is_some(), "last write wins: {final_raw}");
    assert_eq!(store.count_settings(), 1);
}
