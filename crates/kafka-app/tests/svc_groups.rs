//! T028 — GroupService：列表 / 详情 / 位点 / 重置 + `GROUP_CHANGED` 事件。

use kafka_app::bus::{AppEvent, BusReceiver, EventBus};
use kafka_app::services::{ClusterService, GroupService};
use kafka_app::store::Store;
use kafka_core::cluster::ClusterConfig;
use kafka_core::ClusterId;
use kafka_core::error::ErrorKind;
use kafka_core::group::{CommitOffsetsReq, ListGroupOffsetsReq, ResetOffsetsReq, ResetSpec};
use kafka_core::message::{ProduceMessage, ProduceReq};
use kafka_core::topic::{CreateTopicReq, TopicPartition, TopicPartitionOffset};
use kafka_driver::driver::DynDriver;

const GROUP: &str = "demo-group";
const TOPIC: &str = "orders";

/// 已连接的 mock 集群 + 2 分区 4 条消息的 topic + 一个已提交位点的消费组。
async fn harness() -> (GroupService, ClusterId, BusReceiver) {
    let store = Store::open_in_memory().expect("in-memory store");
    let bus = EventBus::new(64);
    let mut rx = bus.subscribe();
    let clusters = ClusterService::new(store, bus);

    let saved = clusters
        .save(&ClusterConfig::mock("groups-demo"))
        .await
        .expect("save cluster");
    let cluster_id = saved.id.clone();
    clusters.connect(&cluster_id).await.expect("connect");

    let driver = clusters.driver(&cluster_id).expect("driver");
    seed(&driver).await;

    let groups = GroupService::new(clusters);
    while rx.try_recv().is_ok() {}
    (groups, cluster_id, rx)
}

async fn seed(driver: &DynDriver) {
    let mut req = CreateTopicReq::new(TOPIC);
    req.num_partitions = 2;
    req.replication_factor = 1;
    driver.create_topic(&req).await.expect("create topic");

    let produce = ProduceReq {
        topic: TOPIC.to_string(),
        messages: (0..4).map(|i| ProduceMessage::text(format!("m{i}"))).collect(),
    };
    let results = driver.produce(produce).await.expect("produce");
    assert_eq!(results.iter().filter(|r| r.error.is_none()).count(), 4);

    let committed = CommitOffsetsReq {
        group_id: Some(GROUP.to_string()),
        offsets: vec![
            TopicPartitionOffset {
                topic: TOPIC.to_string(),
                partition: 0,
                offset: 1,
                ..Default::default()
            },
            TopicPartitionOffset {
                topic: TOPIC.to_string(),
                partition: 1,
                offset: 2,
                ..Default::default()
            },
        ],
    };
    driver.commit_offsets(committed).await.expect("commit offsets");
}

fn drain(rx: &mut BusReceiver) -> Vec<AppEvent> {
    let mut out = Vec::new();
    while let Ok(event) = rx.try_recv() {
        out.push(event);
    }
    out
}

fn group_changes(events: &[AppEvent]) -> Vec<(String, String, usize)> {
    events
        .iter()
        .filter_map(|event| match event {
            AppEvent::GroupChanged {
                cluster_id,
                group_id,
                partitions,
            } => Some((cluster_id.clone(), group_id.clone(), *partitions)),
            _ => None,
        })
        .collect()
}

fn reset_req(group_id: &str, spec: ResetSpec, dry_run: bool) -> ResetOffsetsReq {
    ResetOffsetsReq {
        group_id: group_id.to_string(),
        topics: Vec::new(),
        partitions: Vec::new(),
        spec,
        dry_run,
    }
}

async fn committed_offsets(groups: &GroupService, id: &ClusterId) -> Vec<(u32, i64, Option<i64>)> {
    let req = ListGroupOffsetsReq {
        group_id: GROUP.to_string(),
        topics: Vec::new(),
        with_lag: true,
    };
    let mut rows = groups
        .offsets(id, &req)
        .await
        .expect("list group offsets")
        .into_iter()
        .map(|entry| (entry.partition, entry.offset, entry.lag))
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.0);
    rows
}

#[tokio::test]
async fn list_reports_committed_groups() {
    let (groups, cluster_id, mut rx) = harness().await;

    let listed = groups.list(&cluster_id).await.expect("list groups");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].group_id, GROUP);
    assert_eq!(listed[0].member_count, 0);
    assert!(listed[0].coordinator_id.is_some());
    assert!(!listed[0].is_local);
    assert!(drain(&mut rx).is_empty(), "list must not publish events");
}

#[tokio::test]
async fn describe_returns_members_and_rejects_unknown_group() {
    let (groups, cluster_id, mut rx) = harness().await;

    let all = groups.describe(&cluster_id, &[]).await.expect("describe all");
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].info.group_id, GROUP);
    assert!(all[0].members.is_empty());

    let one = groups
        .describe(&cluster_id, &[GROUP.to_string()])
        .await
        .expect("describe one");
    assert_eq!(one.len(), 1);

    let err = groups
        .describe(&cluster_id, &["ghost".to_string()])
        .await
        .expect_err("unknown group must fail");
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert!(drain(&mut rx).is_empty(), "describe must not publish events");
}

#[tokio::test]
async fn offsets_report_lag_only_when_asked() {
    let (groups, cluster_id, mut rx) = harness().await;

    let rows = committed_offsets(&groups, &cluster_id).await;
    assert_eq!(rows, vec![(0, 1, Some(1)), (1, 2, Some(0))]);

    let no_lag = groups
        .offsets(
            &cluster_id,
            &ListGroupOffsetsReq {
                group_id: GROUP.to_string(),
                topics: Vec::new(),
                with_lag: false,
            },
        )
        .await
        .expect("offsets without lag");
    assert!(no_lag.iter().all(|entry| entry.lag.is_none()));

    let filtered = groups
        .offsets(
            &cluster_id,
            &ListGroupOffsetsReq {
                group_id: GROUP.to_string(),
                topics: vec!["payments".to_string()],
                with_lag: true,
            },
        )
        .await
        .expect("offsets for other topic");
    assert!(filtered.is_empty());
    assert!(drain(&mut rx).is_empty(), "offset reads must not publish events");
}

#[tokio::test]
async fn reset_to_earliest_writes_offsets_and_publishes_once() {
    let (groups, cluster_id, mut rx) = harness().await;

    let result = groups
        .reset(&cluster_id, &reset_req(GROUP, ResetSpec::Earliest, false))
        .await
        .expect("reset to earliest");
    assert_eq!(result.group_id, GROUP);
    assert!(!result.dry_run);
    assert_eq!(result.error, None);
    assert_eq!(result.applied.len(), 2);
    assert!(result.applied.iter().all(|entry| entry.offset == 0));

    let events = drain(&mut rx);
    assert_eq!(events.len(), 1, "exactly one event per committed reset");
    assert_eq!(
        group_changes(&events),
        vec![(cluster_id.to_string(), GROUP.to_string(), 2)]
    );

    let json = serde_json::to_value(&events[0]).expect("serialize event");
    assert_eq!(json["type"], "group.changed");
    assert_eq!(json["payload"]["clusterId"], cluster_id.to_string());
    assert_eq!(json["payload"]["groupId"], GROUP);
    assert_eq!(json["payload"]["partitions"], 2);

    let rows = committed_offsets(&groups, &cluster_id).await;
    assert_eq!(rows, vec![(0, 0, Some(2)), (1, 0, Some(2))]);
}

#[tokio::test]
async fn dry_run_computes_without_writing_or_publishing() {
    let (groups, cluster_id, mut rx) = harness().await;

    let result = groups
        .reset(&cluster_id, &reset_req(GROUP, ResetSpec::ToOffset(2), true))
        .await
        .expect("dry run reset");
    assert!(result.dry_run);
    assert_eq!(result.applied.len(), 2);
    assert!(result.applied.iter().all(|entry| entry.offset == 2));

    assert!(drain(&mut rx).is_empty(), "dry run must not publish events");
    let rows = committed_offsets(&groups, &cluster_id).await;
    assert_eq!(rows, vec![(0, 1, Some(1)), (1, 2, Some(0))], "dry run must not write");
}

#[tokio::test]
async fn failed_reset_keeps_offsets_and_emits_nothing() {
    let (groups, cluster_id, mut rx) = harness().await;

    let err = groups
        .reset(&cluster_id, &reset_req(GROUP, ResetSpec::ToOffset(99), false))
        .await
        .expect_err("offset beyond high must fail");
    assert_eq!(err.kind(), ErrorKind::OffsetOutOfRange);

    let err = groups
        .reset(
            &cluster_id,
            &ResetOffsetsReq {
                group_id: GROUP.to_string(),
                topics: Vec::new(),
                partitions: vec![TopicPartition::new(TOPIC, 7)],
                spec: ResetSpec::Earliest,
                dry_run: false,
            },
        )
        .await
        .expect_err("unknown partition must fail");
    assert_eq!(err.kind(), ErrorKind::NotFound);

    let err = groups
        .reset(&cluster_id, &reset_req("ghost", ResetSpec::Earliest, false))
        .await
        .expect_err("unknown group must fail");
    assert_eq!(err.kind(), ErrorKind::NotFound);

    assert!(drain(&mut rx).is_empty(), "failed resets must not publish events");
    let rows = committed_offsets(&groups, &cluster_id).await;
    assert_eq!(rows, vec![(0, 1, Some(1)), (1, 2, Some(0))]);
}

#[tokio::test]
async fn reset_targets_explicit_partitions_only() {
    let (groups, cluster_id, mut rx) = harness().await;

    let result = groups
        .reset(
            &cluster_id,
            &ResetOffsetsReq {
                group_id: GROUP.to_string(),
                topics: Vec::new(),
                partitions: vec![TopicPartition::new(TOPIC, 0)],
                spec: ResetSpec::Earliest,
                dry_run: false,
            },
        )
        .await
        .expect("reset one partition");
    assert_eq!(result.applied.len(), 1);
    assert_eq!(result.applied[0].partition, 0);

    assert_eq!(
        group_changes(&drain(&mut rx)),
        vec![(cluster_id.to_string(), GROUP.to_string(), 1)]
    );
    let rows = committed_offsets(&groups, &cluster_id).await;
    assert_eq!(rows, vec![(0, 0, Some(2)), (1, 2, Some(0))]);
}

#[tokio::test]
async fn empty_group_id_fails_before_the_connection_check() {
    let store = Store::open_in_memory().expect("in-memory store");
    let bus = EventBus::new(64);
    let mut rx = bus.subscribe();
    let clusters = ClusterService::new(store, bus);
    let saved = clusters
        .save(&ClusterConfig::mock("groups-offline"))
        .await
        .expect("save cluster");
    let cluster_id = saved.id.clone();
    let groups = GroupService::new(clusters);
    while rx.try_recv().is_ok() {}

    let err = groups
        .reset(&cluster_id, &reset_req("   ", ResetSpec::Earliest, false))
        .await
        .expect_err("blank group id must fail");
    assert_eq!(err.kind(), ErrorKind::InvalidConfig);

    let err = groups
        .offsets(
            &cluster_id,
            &ListGroupOffsetsReq {
                group_id: String::new(),
                topics: Vec::new(),
                with_lag: true,
            },
        )
        .await
        .expect_err("empty group id must fail");
    assert_eq!(err.kind(), ErrorKind::InvalidConfig);

    let err = groups
        .describe(&cluster_id, &[" ".to_string()])
        .await
        .expect_err("blank group id must fail");
    assert_eq!(err.kind(), ErrorKind::InvalidConfig);

    // 三个调用的 Ok 类型互不相同，统一压成 Result<(), ErrorKind> 后才能放进同一个数组。
    for kind in [
        groups.list(&cluster_id).await.map(|_| ()).map_err(|e| e.kind()),
        groups
            .offsets(
                &cluster_id,
                &ListGroupOffsetsReq {
                    group_id: GROUP.to_string(),
                    topics: Vec::new(),
                    with_lag: true,
                },
            )
            .await
            .map(|_| ())
            .map_err(|e| e.kind()),
        groups
            .reset(&cluster_id, &reset_req(GROUP, ResetSpec::Earliest, false))
            .await
            .map(|_| ())
            .map_err(|e| e.kind()),
    ] {
        assert_eq!(kind.expect_err("offline cluster must refuse"), ErrorKind::NotConnected);
    }

    assert!(drain(&mut rx).is_empty(), "rejected calls must not publish events");
}
