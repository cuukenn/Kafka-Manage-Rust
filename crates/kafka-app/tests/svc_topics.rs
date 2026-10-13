//! T025 acceptance: TopicService — six calls, `TopicChanged` events, list cache.

use std::collections::BTreeMap;
use std::time::Duration;

use kafka_app::bus::{AppEvent, BusReceiver, EventBus, TopicChange};
use kafka_app::services::{ClusterService, NO_CACHE, TopicService};
use kafka_app::store::Store;
use kafka_core::cluster::ClusterConfig;
use kafka_core::error::ErrorKind;
use kafka_core::topic::{
    CreateTopicReq, DeleteTopicsReq, DescribeTopicsReq, ListOffsetsReq, OffsetSpec, TopicPartition,
    UpdateTopicConfigReq,
};
use kafka_core::ClusterId;

fn harness(ttl: Duration) -> (TopicService, BusReceiver) {
    let store = Store::open_in_memory().expect("in-memory store");
    let bus = EventBus::new(64);
    let rx = bus.subscribe();
    let clusters = ClusterService::new(store, bus);
    (TopicService::new(clusters, ttl), rx)
}

async fn connected(ttl: Duration) -> (TopicService, ClusterId, BusReceiver) {
    let (svc, rx) = harness(ttl);
    let saved = svc
        .clusters()
        .save(&ClusterConfig::mock("topics-demo"))
        .await
        .expect("save cluster");
    svc.clusters().connect(&saved.id).await.expect("connect");
    (svc, saved.id, rx)
}

fn drain(rx: &mut BusReceiver) -> Vec<AppEvent> {
    let mut out = Vec::new();
    while let Ok(event) = rx.try_recv() {
        out.push(event);
    }
    out
}

fn topic_changes(events: &[AppEvent]) -> Vec<(String, TopicChange)> {
    events
        .iter()
        .filter_map(|event| match event {
            AppEvent::TopicChanged {
                cluster_id,
                topic,
                change,
            } => Some((format!("{cluster_id}:{topic}"), *change)),
            _ => None,
        })
        .collect()
}

fn names(topics: &[kafka_core::topic::TopicInfo]) -> Vec<String> {
    topics.iter().map(|t| t.name.clone()).collect()
}

#[tokio::test]
async fn create_then_list_shows_it_and_publishes_created() {
    let (svc, id, mut rx) = connected(NO_CACHE).await;
    drain(&mut rx);

    let req = CreateTopicReq {
        name: "orders".into(),
        num_partitions: 3,
        replication_factor: 1,
        ..Default::default()
    };
    svc.create(&id, &req).await.expect("create topic");

    let listed = svc.list(&id).await.expect("list topics");
    assert_eq!(names(&listed), vec!["orders".to_string()]);
    assert_eq!(listed[0].partition_count, 3);

    assert_eq!(
        topic_changes(&drain(&mut rx)),
        vec![(format!("{id}:orders"), TopicChange::Created)]
    );
}

#[tokio::test]
async fn list_is_cached_within_ttl_and_refreshed_on_invalidate() {
    let (svc, id, mut rx) = connected(Duration::from_secs(60)).await;
    drain(&mut rx);

    assert!(svc.list(&id).await.expect("first list").is_empty());

    // Bypass the service: the cache must hide this from the next `list`.
    let driver = svc.clusters().driver(&id).expect("driver");
    driver
        .create_topic(&CreateTopicReq::new("direct"))
        .await
        .expect("direct create");

    assert_eq!(names(&svc.list(&id).await.expect("cached list")), Vec::<String>::new());
    assert!(svc.cached_topics(&id).is_some());

    svc.invalidate(&id);
    assert!(svc.cached_topics(&id).is_none());
    assert_eq!(
        names(&svc.list(&id).await.expect("refreshed list")),
        vec!["direct".to_string()]
    );
    assert!(topic_changes(&drain(&mut rx)).is_empty());
}

#[tokio::test]
async fn write_through_the_service_always_invalidates_the_cache() {
    let (svc, id, mut rx) = connected(Duration::from_secs(60)).await;
    drain(&mut rx);

    assert!(svc.list(&id).await.expect("warm cache").is_empty());
    svc.create(&id, &CreateTopicReq::new("orders"))
        .await
        .expect("create");
    assert!(svc.cached_topics(&id).is_none());
    assert_eq!(
        names(&svc.list(&id).await.expect("list after create")),
        vec!["orders".to_string()]
    );

    svc.update_config(
        &id,
        &UpdateTopicConfigReq {
            topic: "orders".into(),
            set: BTreeMap::from([("retention.ms".to_string(), "604800000".to_string())]),
            unset: Vec::new(),
        },
    )
    .await
    .expect("update config");
    assert!(svc.cached_topics(&id).is_none());

    svc.delete(&id, &DeleteTopicsReq { names: vec!["orders".into()] })
        .await
        .expect("delete");
    assert!(svc.cached_topics(&id).is_none());

    assert_eq!(
        topic_changes(&drain(&mut rx)),
        vec![
            (format!("{id}:orders"), TopicChange::Created),
            (format!("{id}:orders"), TopicChange::ConfigChanged),
            (format!("{id}:orders"), TopicChange::Deleted),
        ]
    );
}

#[tokio::test]
async fn no_cache_by_default_and_cache_expires_after_ttl() {
    let (svc, id, mut rx) = connected(NO_CACHE).await;
    drain(&mut rx);
    assert!(svc.list(&id).await.expect("list").is_empty());

    let driver = svc.clusters().driver(&id).expect("driver");
    driver
        .create_topic(&CreateTopicReq::new("direct"))
        .await
        .expect("direct create");
    assert_eq!(
        names(&svc.list(&id).await.expect("uncached list")),
        vec!["direct".to_string()]
    );
    assert!(svc.cached_topics(&id).is_none());
    assert!(topic_changes(&drain(&mut rx)).is_empty());

    // Same cluster, but with a short TTL: the entry goes stale on its own.
    let short = TopicService::new(svc.clusters().clone(), Duration::from_millis(20));
    assert_eq!(short.list(&id).await.expect("warm").len(), 1);
    assert!(short.cached_topics(&id).is_some());
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(short.cached_topics(&id).is_none());
    driver
        .create_topic(&CreateTopicReq::new("late"))
        .await
        .expect("direct create 2");
    assert_eq!(short.list(&id).await.expect("expired list").len(), 2);
}

#[tokio::test]
async fn delete_reports_per_topic_and_events_only_for_removed() {
    let (svc, id, mut rx) = connected(NO_CACHE).await;
    drain(&mut rx);
    svc.create(&id, &CreateTopicReq::new("orders"))
        .await
        .expect("create");
    drain(&mut rx);

    let results = svc
        .delete(
            &id,
            &DeleteTopicsReq {
                names: vec!["orders".into(), "ghost".into()],
            },
        )
        .await
        .expect("delete");

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].name, "orders");
    assert_eq!(results[0].error, None);
    assert_eq!(results[1].name, "ghost");
    assert!(results[1].error.as_deref().unwrap().contains("unknown topic"));

    assert_eq!(
        topic_changes(&drain(&mut rx)),
        vec![(format!("{id}:orders"), TopicChange::Deleted)]
    );
    assert!(svc.list(&id).await.expect("list").is_empty());
}

#[tokio::test]
async fn describe_and_offsets_pass_through_to_the_driver() {
    let (svc, id, mut rx) = connected(NO_CACHE).await;
    drain(&mut rx);
    svc.create(
        &id,
        &CreateTopicReq {
            name: "orders".into(),
            num_partitions: 2,
            replication_factor: 1,
            configs: BTreeMap::from([("cleanup.policy".to_string(), "compact".to_string())]),
            ..Default::default()
        },
    )
    .await
    .expect("create");
    // The create itself publishes exactly one topic.changed. Draining it here is what makes
    // the "reads publish nothing" assertion at the end of this test mean anything — and
    // asserting the exact vector also proves the driver did not double-publish it.
    assert_eq!(
        topic_changes(&drain(&mut rx)),
        vec![(format!("{id}:orders"), TopicChange::Created)]
    );

    let details = svc
        .describe(
            &id,
            &DescribeTopicsReq {
                names: vec!["orders".into()],
                include_internal: false,
            },
        )
        .await
        .expect("describe");
    assert_eq!(details.len(), 1);
    assert_eq!(details[0].partitions.len(), 2);
    assert_eq!(details[0].configs.get("cleanup.policy").map(String::as_str), Some("compact"));

    let ranges = svc
        .offsets(
            &id,
            &ListOffsetsReq {
                partitions: vec![TopicPartition::new("orders", 1)],
                spec: OffsetSpec::Latest,
            },
        )
        .await
        .expect("offsets");
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].partition, 1);
    assert_eq!(ranges[0].resolved_offset, Some(ranges[0].high));

    let all = svc
        .offsets(
            &id,
            &ListOffsetsReq {
                partitions: Vec::new(),
                spec: OffsetSpec::Earliest,
            },
        )
        .await
        .expect("offsets for all partitions");
    assert_eq!(all.len(), 2);
    assert!(
        all.iter()
            .all(|range| range.resolved_offset == Some(range.low))
    );

    // Reads never publish domain events.
    assert!(topic_changes(&drain(&mut rx)).is_empty());
}

#[tokio::test]
async fn offline_cluster_fails_with_not_connected_and_publishes_nothing() {
    let (svc, mut rx) = harness(NO_CACHE);
    let saved = svc
        .clusters()
        .save(&ClusterConfig::mock("offline"))
        .await
        .expect("save");
    let id = saved.id;

    for attempt in [
        "list",
        "describe",
        "offsets",
        "create",
        "delete",
        "update_config",
    ] {
        let err = match attempt {
            "list" => svc.list(&id).await.unwrap_err(),
            "describe" => svc.describe(&id, &DescribeTopicsReq::default()).await.unwrap_err(),
            "offsets" => svc.offsets(&id, &ListOffsetsReq::default()).await.unwrap_err(),
            "create" => svc.create(&id, &CreateTopicReq::new("orders")).await.unwrap_err(),
            "delete" => svc
                .delete(&id, &DeleteTopicsReq { names: vec!["orders".into()] })
                .await
                .unwrap_err(),
            _ => svc
                .update_config(&id, &UpdateTopicConfigReq::default())
                .await
                .unwrap_err(),
        };
        assert_eq!(err.kind(), ErrorKind::NotConnected, "{attempt}");
    }

    assert!(drain(&mut rx).is_empty());
}

#[tokio::test]
async fn failed_write_keeps_the_cache_and_publishes_nothing() {
    let (svc, id, mut rx) = connected(Duration::from_secs(60)).await;
    drain(&mut rx);
    svc.create(&id, &CreateTopicReq::new("orders"))
        .await
        .expect("create");
    assert!(svc.cached_topics(&id).is_none());
    assert!(svc.list(&id).await.expect("warm").len() == 1);
    drain(&mut rx);

    let err = svc
        .create(&id, &CreateTopicReq::new("bad name!"))
        .await
        .expect_err("invalid topic name");
    assert_eq!(err.kind(), ErrorKind::InvalidConfig);

    let err = svc
        .update_config(
            &id,
            &UpdateTopicConfigReq {
                topic: "ghost".into(),
                ..Default::default()
            },
        )
        .await
        .expect_err("unknown topic");
    assert_eq!(err.kind(), ErrorKind::NotFound);

    // Cache survived both failures, and nothing was published.
    assert_eq!(names(&svc.list(&id).await.expect("cached list")), vec!["orders".to_string()]);
    assert!(topic_changes(&drain(&mut rx)).is_empty());
}
