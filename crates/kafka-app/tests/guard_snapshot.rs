//! T038a 验收：`ClusterService::connect` 真正包上 `GuardedDriver`，护栏转发 `as_any`
//! （T084 接缝穿透护栏仍可达 `MockKafkaDriver`），Mock 快照按 T079 裁定接 SQLite slot：
//! connect 前恢复（只换数据、不伪造 connected）、disconnect 前保存；快照读写失败仅 warn。

use kafka_app::bus::EventBus;
use kafka_app::services::clusters::ClusterService;
use kafka_app::store::Store;
use kafka_core::ClusterId;
use kafka_core::cluster::ClusterConfig;
use kafka_core::topic::{CreateTopicReq, DescribeTopicsReq};
use kafka_driver::mock::MockKafkaDriver;
use kafka_driver::{DynDriver, KafkaDriver};

fn harness() -> (ClusterService, Store) {
    let store = Store::open_in_memory().expect("in-memory store");
    (ClusterService::new(store.clone(), EventBus::new(64)), store)
}

async fn save_mock(svc: &ClusterService, name: &str) -> ClusterId {
    svc.save(&ClusterConfig::mock(name))
        .await
        .expect("save cluster")
        .id
}

/// 经护栏转发后的 `as_any` 接缝下降到 Mock 控制面；接缝断了这里就是 `None`。
fn mock_of(driver: &DynDriver) -> MockKafkaDriver {
    driver
        .as_any()
        .and_then(|any| any.downcast_ref::<MockKafkaDriver>())
        .expect("guard must forward as_any to the inner MockKafkaDriver")
        .clone()
}

/// 独立造一份含 `snap-topic` 的快照 JSON（源驱动与服务的驱动互不相干）。
async fn snapshot_with_topic(name: &str) -> String {
    let source = MockKafkaDriver::new();
    source
        .connect(&ClusterConfig::mock(name))
        .await
        .expect("source connect");
    source
        .create_topic(&CreateTopicReq::new("snap-topic"))
        .await
        .expect("source topic");
    source.snapshot_json().await.expect("source snapshot")
}

/// 1. 护栏转发：connect 后 `clusters.driver(id)` 经 `as_any` 可达 Mock 控制面，
/// `faults_handle().write()` 可触达并读回。
#[tokio::test]
async fn guard_forwards_as_any_so_the_mock_seam_stays_reachable() {
    let (svc, _store) = harness();
    let id = save_mock(&svc, "guard-seam").await;
    svc.connect(&id).await.expect("connect");

    let driver = svc.driver(&id).expect("live driver");
    let mock = mock_of(&driver);

    let mut faults = mock.faults().await;
    faults.fail_probability = 0.5;
    *mock.faults_handle().write().await = faults.clone();

    let read_back = mock.faults().await;
    assert_eq!(
        read_back.fail_probability, 0.5,
        "fault write must land on the live mock"
    );
    assert!(
        driver.is_connected().await,
        "the guarded handle still serves the trait"
    );
}

/// 2. 恢复：预存含 topic 的快照 → connect → 经接缝读驱动状态见快照 topic；
/// 身份字段（cluster_id）仍来自本次 config（T079：restore 只换数据）。
#[tokio::test]
async fn stored_snapshot_is_restored_before_connect() {
    let (svc, store) = harness();
    let id = save_mock(&svc, "snap-restore").await;
    let json = snapshot_with_topic("snap-source").await;
    store
        .save_mock_snapshot(&id, &json)
        .expect("pre-seed snapshot");

    svc.connect(&id).await.expect("connect");
    let driver = svc.driver(&id).expect("live driver");

    let topics = driver.list_topics().await.expect("list via guard");
    assert!(
        topics.iter().any(|t| t.name == "snap-topic"),
        "restored topic missing: {topics:?}"
    );

    let info = driver
        .cluster_info()
        .await
        .expect("identity from the live config");
    assert_eq!(info.cluster_id.as_deref(), Some("mock-snap-restore"));
}

/// 3. 保存：connect → 经接缝 seed → disconnect → slot 里有值且含 seed 的 fixture topic。
#[tokio::test]
async fn disconnect_saves_the_mock_snapshot_into_its_slot() {
    let (svc, store) = harness();
    let id = save_mock(&svc, "snap-save").await;
    assert!(
        store
            .load_mock_snapshot(&id)
            .expect("slot starts empty")
            .is_none()
    );

    svc.connect(&id).await.expect("connect");
    let mock = mock_of(&svc.driver(&id).expect("live driver"));
    mock.seed().await;

    svc.disconnect(&id).await.expect("disconnect");

    let stored = store
        .load_mock_snapshot(&id)
        .expect("slot")
        .expect("snapshot was saved");
    assert!(
        stored.contains("orders"),
        "seeded fixture must be in the snapshot"
    );
    assert!(stored.contains("audit-log"));
}

/// 4. 快照 JSON 合法但不是合法 `MockState`（restore 反序列化失败）→ connect 仍成功
/// （warn 语义），驱动保持空态 Mock。store 的 raw 槽本身只收合法 JSON，坏文本进不了槽。
#[tokio::test]
async fn a_corrupt_snapshot_warns_and_connects_empty() {
    let (svc, store) = harness();
    let id = save_mock(&svc, "snap-corrupt").await;
    store
        .save_mock_snapshot(&id, r#"{ "topics": "not-a-map", "groups": 7 }"#)
        .expect("store shape-invalid json");

    let info = svc
        .connect(&id)
        .await
        .expect("connect must survive a bad snapshot");
    assert!(info.cluster_id.is_some());

    let driver = svc.driver(&id).expect("live driver");
    let topics = driver.list_topics().await.expect("list");
    assert!(
        topics.is_empty(),
        "failed restore must leave the mock empty, got {topics:?}"
    );
}

/// 5.（R11）delete 清快照槽：connect+seed → disconnect 存快照（槽有值）→ delete →
/// `load_mock_snapshot` 为 None，孤儿快照不得残留。
#[tokio::test]
async fn delete_clears_the_mock_snapshot_slot() {
    let (svc, store) = harness();
    let id = save_mock(&svc, "snap-delete").await;

    svc.connect(&id).await.expect("connect");
    let mock = mock_of(&svc.driver(&id).expect("live driver"));
    mock.seed().await;
    svc.disconnect(&id).await.expect("disconnect saves the snapshot");
    assert!(
        store.load_mock_snapshot(&id).expect("slot").is_some(),
        "snapshot must exist before delete"
    );

    assert!(svc.delete(&id).await.expect("delete"));
    assert!(
        store.load_mock_snapshot(&id).expect("slot").is_none(),
        "delete must clear the mock snapshot slot (R11)"
    );
}

/// 6.（补充）完整闭环：connect → seed → disconnect → 再 connect → 数据回来了，
/// 且 `describe_topics` 经护栏可查（快照里的 topic 是活数据）。
#[tokio::test]
async fn snapshot_round_trip_survives_disconnect_and_reconnect() {
    let (svc, _store) = harness();
    let id = save_mock(&svc, "snap-round").await;

    svc.connect(&id).await.expect("first connect");
    let mock = mock_of(&svc.driver(&id).expect("live driver"));
    mock.seed().await;
    svc.disconnect(&id).await.expect("disconnect saves");

    svc.connect(&id).await.expect("reconnect restores");
    let driver = svc.driver(&id).expect("live driver");
    let details = driver
        .describe_topics(&DescribeTopicsReq {
            names: vec!["orders".into()],
            ..Default::default()
        })
        .await
        .expect("restored fixture must be describable through the guard");
    assert_eq!(details.len(), 1);
    assert_eq!(
        details[0].partitions.len(),
        3,
        "fixture partitions must survive the snapshot"
    );
}
