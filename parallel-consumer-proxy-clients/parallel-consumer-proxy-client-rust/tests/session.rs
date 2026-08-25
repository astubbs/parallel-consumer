// Copyright (C) 2026 Antony Stubbs and contributors

//! Wave one's whole claim: one record, end to end, against the real wire.

mod harness;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use parallel_consumer_proxy_client::{capability, ClientOptions, InboundRecord, Outcome, ParallelConsumerClient};
use tokio::sync::Notify;

/// How long the test watches for a second delivery after reporting success. The harness's
/// redelivery path is fast (the scenario's retry delay is short), so this is a wait for an event
/// that should never come, not a race against one that should.
const REDELIVERY_SETTLE: Duration = Duration::from_secs(3);

/// The committed offset itself is engine state no client can see, and the harness has no verdict
/// channel - it exits 0 whatever happened. So the client-side assertion is the wire-observable
/// consequence: the record arrives once, the success report is followed by silence rather than a
/// redelivery, and the session closes cleanly.
///
/// The scenario name is the conformance suite's identity, so this test carries it verbatim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_processed_record_advances_the_committed_offset() {
    let scenario = harness::scenario::PROCESSED_RECORD_ADVANCES_OFFSET;
    let sidecar = harness::for_scenario(scenario).expect("locating the conformance harness");

    let client = ParallelConsumerClient::connect(ClientOptions {
        sidecar_path: sidecar.path,
        sidecar_args: sidecar.args,
        // THE SCENARIO NAME IS ALSO THE TOPIC NAME - the harness seeds its records on the topic it
        // is named after.
        topics: vec![scenario.to_owned()],
        // The mock harness builds mock Kafka clients and reads no properties. Real credentials
        // never belong in a conformance test.
        kafka_properties: HashMap::new(),
        instance_tag: Some("rust-client-wave-one".to_owned()),
        ..Default::default()
    })
    .await
    .expect("opening the session");

    let session = client.session().clone();
    assert!(
        session.max_concurrency >= 1,
        "effective max_concurrency was {}, want >= 1",
        session.max_concurrency
    );
    assert!(
        session.executor_count >= 1,
        "effective executor_count was {}, want >= 1",
        session.executor_count
    );
    assert!(
        session.negotiated(capability::DISPATCH),
        "dispatch was not negotiated; the session's capabilities were {:?}",
        session.capabilities
    );

    let seen: Arc<Mutex<Vec<InboundRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let first = Arc::new(Notify::new());

    let recorded = Arc::clone(&seen);
    let arrived = Arc::clone(&first);
    client
        .poll(move |record: InboundRecord| {
            let recorded = Arc::clone(&recorded);
            let arrived = Arc::clone(&arrived);
            async move {
                recorded.lock().expect("the record log is poisoned").push(record);
                arrived.notify_one();
                Ok(Outcome::success())
            }
        })
        .expect("starting the poll");

    tokio::time::timeout(Duration::from_secs(60), first.notified())
        .await
        .expect("no record was dispatched before the deadline");

    // A success is followed by silence. If the report had not landed, or had not been honoured,
    // the record would come back.
    tokio::time::sleep(REDELIVERY_SETTLE).await;

    let delivered = seen.lock().expect("the record log is poisoned").clone();
    assert_eq!(
        delivered.len(),
        1,
        "the record was delivered {} times, want exactly 1: {delivered:?}",
        delivered.len()
    );

    let record = &delivered[0];
    assert_eq!(record.topic, scenario, "record topic");
    assert_eq!(record.attempt, 1, "attempt on a first delivery");
    assert!(
        !record.has_failed_before(),
        "a first delivery reported a previous failure at {:?}, reason {:?}",
        record.last_failure_at,
        record.last_failure_reason
    );
    assert!(
        record.value.as_ref().is_some_and(|value| !value.is_empty()),
        "the seeded record carried no value: key={:?} value={:?}",
        record.key,
        record.value
    );

    client.shutdown().await.expect("closing the session");
}
