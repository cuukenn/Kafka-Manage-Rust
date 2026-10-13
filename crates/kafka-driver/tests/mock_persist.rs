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
/// stand-in for a restarted process): the whole `MockState` — topics,
/// messages, committed offsets — must come back identical.
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
    restored.restore_json(&json).await.expect("restore_json should succeed");

    assert_eq!(
        restored.state_handle().read().await.clone(),
        state,
        "round-trip through disk must be lossless"
    );
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

    let first = MockState { cluster_id: "first".to_string(), ..MockState::default() };
    persistence.save(&first).expect("first save should succeed");
    assert_eq!(persistence.load().expect("load").expect("saved"), first);

    let second = MockState { cluster_id: "second".to_string(), ..MockState::default() };
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
