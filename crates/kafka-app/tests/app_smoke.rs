//! T030 smoke: save → connect → create/list → produce → consume → group reset
//! → stop → shutdown, all Mock, ending with zero residual runtime tasks.

use std::time::Duration;

use kafka_app::App;
use kafka_app::bus::{AppEvent, BusReceiver, EventBus, SubscriptionStatus};
use kafka_app::store::Store;
use kafka_core::cluster::{ClusterConfig, ConnectionState};
use kafka_core::group::{CommitOffsetsReq, ResetOffsetsReq, ResetSpec};
use kafka_core::message::{OffsetStart, ProduceMessage, ProduceReq, SubscribeReq};
use kafka_core::topic::{CreateTopicReq, TopicPartitionOffset};

const GROUP: &str = "smoke-group";
const TOPIC: &str = "orders";

fn alive_tasks() -> usize {
    tokio::runtime::Handle::current().metrics().num_alive_tasks()
}

/// Reads events until `predicate` matches; a hang trips the bounded timeout.
async fn wait_for<F>(rx: &mut BusReceiver, predicate: F) -> AppEvent
where
    F: Fn(&AppEvent) -> bool,
{
    loop {
        let event = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("timed out waiting for an event")
            .expect("bus closed");
        if predicate(&event) {
            return event;
        }
    }
}

fn drain(rx: &mut BusReceiver) -> Vec<AppEvent> {
    let mut out = Vec::new();
    while let Ok(event) = rx.try_recv() {
        out.push(event);
    }
    out
}

#[tokio::test(start_paused = true)]
async fn smoke_chain_save_connect_produce_consume_reset_stop_shutdown() {
    let idle_before = alive_tasks();
    let bus = EventBus::new(256);
    let mut rx = bus.subscribe();
    let app = App::new(Store::open_in_memory().expect("in-memory store"), bus);

    // 1. an in-memory store saves a mock=true cluster
    let saved = app.clusters().save(&ClusterConfig::mock("smoke")).await.expect("save");
    assert!(saved.mock, "smoke cluster must be mock");
    let id = saved.id;

    // 2. connect → topics.create() → topics.list() sees it
    app.clusters().connect(&id).await.expect("connect");
    let create = CreateTopicReq { name: TOPIC.to_string(), num_partitions: 1, ..Default::default() };
    app.topics().create(&id, &create).await.expect("create topic");
    let listed = app.topics().list(&id).await.expect("list topics");
    assert_eq!(listed.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(), [TOPIC]);

    // 3. produce.send_one() → consume.start() from earliest → the bus carries it
    let req = ProduceReq { topic: TOPIC.to_string(), messages: vec![ProduceMessage::text("hello")] };
    let report = app.produce().send_one(&id, req).await.expect("produce");
    assert_eq!((report.ok_count, report.error_count), (1, 0));

    let req = SubscribeReq { follow: true, start: OffsetStart::Earliest, ..SubscribeReq::new(TOPIC) };
    let sub_id = req.subscription_id.clone();
    let info = app.consume().start(&id, req).await.expect("start consume");
    assert_eq!(info.state, SubscriptionStatus::Running);
    tokio::task::yield_now().await;
    // The forwarding task must exist, or the residual check below is vacuous.
    assert!(alive_tasks() > idle_before, "consume task not spawned");

    let event = wait_for(&mut rx, |event| {
        matches!(event, AppEvent::MessageReceived { subscription_id, .. } if subscription_id == &sub_id)
    })
    .await;
    let AppEvent::MessageReceived { message, .. } = event else { unreachable!("matched above") };
    assert_eq!(message.value_utf8(), Some("hello"));

    // 4. groups.reset() → GroupChanged (a group only exists once an offset is
    // committed, so seed one through the connected driver first)
    let driver = app.clusters().driver(&id).expect("driver");
    let commit = CommitOffsetsReq {
        group_id: Some(GROUP.to_string()),
        offsets: vec![TopicPartitionOffset { topic: TOPIC.to_string(), partition: 0, offset: 1, ..Default::default() }],
    };
    driver.commit_offsets(commit).await.expect("commit offsets");
    let reset = ResetOffsetsReq {
        group_id: GROUP.to_string(),
        topics: Vec::new(),
        partitions: Vec::new(),
        spec: ResetSpec::Earliest,
        dry_run: false,
    };
    let applied = app.groups().reset(&id, &reset).await.expect("reset offsets");
    assert_eq!(applied.applied.len(), 1);
    let events = drain(&mut rx);
    assert!(events.iter().any(|e| matches!(e, AppEvent::GroupChanged { group_id, .. } if group_id == GROUP)),
        "GroupChanged missing from {events:?}");

    // 5. consume.stop() → SubscriptionDone
    assert!(app.consume().stop(&sub_id).await.expect("stop"));
    let event = wait_for(&mut rx, |event| {
        matches!(event, AppEvent::SubscriptionDone { subscription_id, .. } if subscription_id == &sub_id)
    })
    .await;
    let AppEvent::SubscriptionDone { delivered, reason, .. } = event else { unreachable!("matched above") };
    assert_eq!(delivered, 1);
    assert_eq!(reason.as_deref(), Some("stopped by client"));
    assert_eq!(app.consume().active_count(), 0);

    // 6. shutting the app down leaves no residual runtime task behind
    app.shutdown().await;
    drop(app);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while alive_tasks() > idle_before {
        assert!(tokio::time::Instant::now() < deadline, "residual tasks: {}", alive_tasks());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(start_paused = true)]
async fn builder_accepts_explicit_wiring_and_falls_back_to_defaults() {
    let bus = EventBus::new(64);
    let mut rx = bus.subscribe();
    let store = Store::open_in_memory().expect("in-memory store");
    let app = App::builder().store(store).bus(bus).build();

    let saved = app.clusters().save(&ClusterConfig::mock("explicit")).await.expect("save");
    app.clusters().connect(&saved.id).await.expect("connect");
    let connected = drain(&mut rx).iter().any(|event| {
        matches!(event, AppEvent::ConnectionStateChanged { state: ConnectionState::Connected, .. })
    });
    assert!(connected, "a builder-built app must publish on the supplied bus");
    app.shutdown().await;

    // No store/bus given: in-memory store and default bus.
    let app = App::builder().build();
    let saved = app.clusters().save(&ClusterConfig::mock("default")).await.expect("save");
    assert!(saved.mock, "the default store must round-trip the config");
    app.shutdown().await;
}
