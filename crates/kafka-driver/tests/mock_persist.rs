//! Snapshot persistence contract for `MockKafkaDriver` (T017): a snapshot
//! saved by one driver instance loads back identical through a fresh one,
//! a missing file is `Ok(None)`, a corrupt file is a typed `DriverError`,
//! and atomic writes leave no `.tmp` residue behind.

use std::path::PathBuf;

use kafka_core::cluster::ClusterConfig;
use kafka_core::error::ErrorKind;
use kafka_core::group::CommitOffsetsReq;
use kafka_core::message::{ProduceMessage, ProduceReq};
use kafka_core::topic::{CreateTopicReq, TopicPartitionOffset};
use kafka_driver::driver::KafkaDriver;
use kafka_driver::mock::persist::MockPersistence;
use kafka_driver::mock::{MockKafkaDriver, MockState};

fn snapshot_path(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join("mock-snapshot.json")
}

/// A connected driver holding one two-partition topic with five stamped
/// messages and one committed consumer-group offset.
async fn connected_with_log() -> MockKafkaDriver {
    let driver = MockKafkaDriver::new();
    driver
        .connect(&ClusterConfig::mock("persist-test"))
        .await
        .expect("connect should succeed");
    let mut req = CreateTopicReq::new("persist.log");
    req.num_partitions = 2;
    driver.create_topic(&req).await.expect("create_topic should succeed");
    let messages: Vec<ProduceMessage> = (0..5)
        .map(|i| ProduceMessage { timestamp: Some(1_000 + i), ..ProduceMessage::text(format!("m{i}")) })
        .collect();
    driver
        .produce(ProduceReq { topic: "persist.log".to_string(), messages })
        .await
        .expect("produce should succeed");
    driver
        .commit_offsets(CommitOffsetsReq {
            group_id: Some("etl".to_string()),
            offsets: vec![TopicPartitionOffset {
                topic: "persist.log".to_string(),
                partition: 0,
                offset: 3,
                metadata: String::new(),
                timestamp: None,
                leader_epoch: None,
            }],
        })
        .await
        .expect("commit should succeed");
    driver
}

/// Save → load → restore through a brand-new driver instance (the in-process
/// stand-in for a restarted process): every DATA field — topics, messages,
/// committed offsets, round-robin cursor, version — must come back identical
/// (T079: identity/connection fields are live, see the restore test below).
#[tokio::test(start_paused = true)]
async fn round_trip_through_fresh_driver_restores_everything() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = snapshot_path(&dir);

    let source = connected_with_log().await;
    let state = source.state_handle().read().await.clone();
    MockPersistence::new(&path).save(&state).expect("save should succeed");

    // Fresh driver instance loading from the same file path.
    let restored = MockKafkaDriver::new();
    let json = std::fs::read_to_string(&path).expect("snapshot file should exist after save");
    // A fresh process connects first (live identity), THEN restores the data:
    // `restore_json` never fakes the `connected` flag (T079), and the API
    // checks below need a connected driver.
    restored
        .connect(&ClusterConfig::mock("persist-test"))
        .await
        .expect("connect should succeed");
    restored.restore_json(&json).await.expect("restore_json should succeed");

    let restored_state = restored.state_handle().read().await.clone();
    assert_eq!(
        restored_state.topics, state.topics,
        "round-trip through disk must be lossless for topics/messages"
    );
    assert_eq!(
        restored_state.groups, state.groups,
        "round-trip through disk must be lossless for committed offsets"
    );
    assert_eq!(
        restored_state.round_robin, state.round_robin,
        "round-trip must not lose the round-robin cursor"
    );
    assert_eq!(restored_state.version, state.version, "round-trip must not lose the version");
    let topics = restored.list_topics().await.expect("list_topics should succeed");
    assert_eq!(
        topics.iter().filter(|t| t.name == "persist.log").count(),
        1,
        "the persisted topic must exist after restore"
    );
    let groups = restored.list_consumer_groups().await.expect("list_consumer_groups should succeed");
    assert!(
        groups.iter().any(|g| g.group_id == "etl"),
        "the persisted consumer group must exist after restore"
    );
}

/// T079 — restore scope: a snapshot restore swaps DATA only. The live driver
/// keeps its current identity/connection (name, bootstrap servers, brokers,
/// cluster id, connected flag), so restoring an old snapshot after a config
/// edit must NOT resurrect the snapshot's stale brokers nor a phantom
/// `connected` flag.
#[tokio::test(start_paused = true)]
async fn restore_after_config_change_keeps_live_identity() {
    // Snapshot taken from driver A: config "persist-test" + its dataset.
    let source = connected_with_log().await;
    let json = source.snapshot_json().await.expect("snapshot_json should succeed");

    // Live driver B: connected with a DIFFERENT name and bootstrap servers.
    let live = MockKafkaDriver::new();
    let mut cfg_b = ClusterConfig::mock("live-b");
    cfg_b.bootstrap_servers = vec!["mock://10.1.2.3:9093".to_string()];
    live.connect(&cfg_b).await.expect("connect B should succeed");

    live.restore_json(&json).await.expect("restore_json should succeed");

    {
        let handle = live.state_handle();
        let state = handle.read().await;
        let cfg = state.config.as_ref().expect("the LIVE config must survive restore");
        assert_eq!(cfg.name, "live-b", "restore must keep the LIVE cluster name");
        assert_eq!(
            cfg.bootstrap_servers,
            vec!["mock://10.1.2.3:9093".to_string()],
            "restore must keep the LIVE bootstrap servers"
        );
        assert_eq!(state.cluster_id, "mock-live-b", "restore must keep the LIVE cluster id");
        assert_eq!(state.brokers.len(), 1, "restore must keep the LIVE broker list length");
        assert_eq!(state.brokers[0].host, "10.1.2.3", "restore must keep the LIVE broker host");
        assert_eq!(state.brokers[0].port, 9093, "restore must keep the LIVE broker port");
        assert!(state.connected, "restore must keep the LIVE connected flag");
        assert_eq!(state.controller_id, 1, "restore must keep the LIVE controller id");

        // …while DATA still comes from the snapshot.
        let topic = state.topics.get("persist.log").expect("snapshot topic must be restored");
        assert_eq!(topic.total_messages(), 5, "snapshot messages must be restored");
        assert_eq!(state.round_robin, 5, "snapshot round-robin cursor must be restored");
        assert!(state.groups.contains_key("etl"), "snapshot group must be restored");
    }

    // Ghost-flag guard: after a disconnect, the snapshot's `connected: true`
    // must not leak back in.
    live.disconnect().await.expect("disconnect should succeed");
    live.restore_json(&json).await.expect("second restore should succeed");
    assert!(!live.is_connected().await, "restore must not resurrect a stale connected flag");
}

/// A first run has nothing to restore: a missing snapshot file is `Ok(None)`,
/// and clearing a missing file is not an error either.
#[test]
fn load_on_missing_file_returns_ok_none() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = snapshot_path(&dir);
    let persistence = MockPersistence::new(&path);

    assert!(
        persistence.load().expect("load must not fail").is_none(),
        "a missing snapshot file must load as Ok(None)"
    );
    persistence.clear().expect("clearing a missing snapshot must succeed");
}

/// A corrupt snapshot file surfaces as a typed `DriverError` with a readable
/// message — parsing must never panic.
#[test]
fn load_on_corrupt_file_returns_typed_error() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = snapshot_path(&dir);
    std::fs::write(&path, "this is definitely not json {{{").expect("write garbage");

    let err = MockPersistence::new(&path).load().expect_err("corrupt snapshot must be an error");
    assert_eq!(err.kind(), ErrorKind::Internal, "corrupt snapshot maps to Internal, got {err:?}");
    let message = err.to_string();
    assert!(
        message.contains("corrupt mock snapshot"),
        "message should name the file problem, got: {message}"
    );
}

/// Atomicity: after saves (including an overwrite) the directory holds only
/// the target file — no `.tmp` residue — and the file holds valid JSON that
/// round-trips to the last saved state.
#[test]
fn atomic_save_leaves_no_tmp_residue_and_overwrites_cleanly() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = snapshot_path(&dir);
    let persistence = MockPersistence::new(&path);

    // NB: built by mutation, not `..Default::default()` — `MockState::active_pumps`
    // is `pub(crate)` since the review batch, so functional record update cannot
    // name it from this external test crate (E0451).
    let mut first = MockState::default();
    first.cluster_id = "first".to_string();
    persistence.save(&first).expect("first save should succeed");
    assert_eq!(persistence.load().expect("load").expect("saved"), first);

    let mut second = MockState::default();
    second.cluster_id = "second".to_string();
    persistence.save(&second).expect("overwrite save should succeed");
    assert_eq!(persistence.load().expect("load").expect("saved"), second);

    let mut names: Vec<String> = std::fs::read_dir(dir.path())
        .expect("read_dir")
        .map(|entry| entry.expect("dir entry").file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec!["mock-snapshot.json".to_string()],
        "no .tmp residue may survive a save, found: {names:?}"
    );
}

/// Saving into a directory that does not exist fails with a readable
/// `DriverError` instead of panicking.
#[test]
fn save_into_nonexistent_directory_fails_without_panic() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = dir.path().join("no-such-dir").join("snap.json");

    let err = MockPersistence::new(&path)
        .save(&MockState::default())
        .expect_err("save into a missing directory must fail");
    assert_eq!(err.kind(), ErrorKind::Internal, "save failure maps to Internal, got {err:?}");
    assert!(
        err.to_string().contains("mock snapshot write failed"),
        "message should say the write failed, got: {err}"
    );
}
