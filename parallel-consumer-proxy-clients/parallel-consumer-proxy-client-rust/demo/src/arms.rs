// Copyright (C) 2026 Antony Stubbs and contributors

//! The two arms, and what each of them measured.
//!
//! **Two arms is the whole contract outside Java.** The reference demo carries four more -
//! `pc-core`, `java-direct`, `java-grpc-uds` and `java-raw-grpc` - because one JVM can hold all of
//! them at once and each *pair* changes exactly one term. Rust has no in-process engine to compare
//! a wrapper against and no second client library, so there is nothing here for those pairs to
//! isolate; adding a hand-rolled-protocol arm would price the client library against itself in a
//! language where nobody would write the control.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use parallel_consumer_proxy_client::{
    blocking, ClientOptions, Outcome, ParallelConsumerClient, ProcessingOrder,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::Message;
use tokio::sync::watch;

use crate::broker::DemoBroker;
use crate::options::DemoOptions;
use crate::sidecar::SidecarCommand;

/// No arm may take longer than this before the demo calls it stalled rather than slow.
pub const ARM_BUDGET: Duration = Duration::from_secs(600);

/// The serial arm's name. Always **"AK core"**, never bare "core", which reads as
/// `parallel-consumer-core` (`CONCEPTS.md`).
pub const AK_CORE: &str = "AK core";

/// The sidecar arm's name, in the reference's `<language>-<transport>` shape.
pub const RUST_GRPC: &str = "pc-rust-grpc";

/// **The client the AK core arm actually ran.** "AK core" is a *category*, not a client, and the
/// answer differs in every language - `franz-go` in Go, `kafkajs` in TypeScript, `rdkafka` here. A
/// reader cannot judge the comparison without being told which one produced the number, so the
/// table prints both the role and the library.
pub const AK_CORE_CLIENT: &str = "rdkafka";

/// What the sidecar arm drives: this repository's Rust client library, not a hand-rolled protocol
/// client. Named for the same reason - the row says what the reader is being shown.
pub const RUST_GRPC_CLIENT: &str = "this client";

/// What one arm achieved: how long it took, over how many records, and across how many keys.
///
/// **There is no latency field, and that is the contract rather than an omission.** The backlog is
/// pre-produced, so the workload is closed-loop and a per-record timing would be flattered by
/// however far an arm had fallen behind. Throughput is the only honest number this shape produces.
///
/// `processed` and `unique_keys` are the two **deterministic** figures: every language replaying
/// the same backlog reports the same pair, which is what makes them comparable across languages
/// when elapsed and msg/s never can be. `bin/ci-demo-conformance.sh` relies on exactly that.
#[derive(Debug, Clone)]
pub struct ArmResult {
    /// The arm's role, and the identity every ratio and message keys off - `AK core`, `pc-rust-grpc`.
    pub arm: String,
    /// The Kafka client, or client library, the arm actually ran on.
    pub client: String,
    /// Wall clock from the first record being asked for to the last one being processed.
    pub elapsed: Duration,
    /// How many records it actually processed. Must equal the target: a short arm is a **failed**
    /// arm, not a fast one.
    pub processed: usize,
    /// How many distinct record keys it saw. Shows the backlog was really spread over the key
    /// space rather than being one key repeated.
    pub unique_keys: usize,
}

impl ArmResult {
    /// Throughput, the only figure this demo reports.
    pub fn rate_per_second(&self) -> f64 {
        let seconds = self.elapsed.as_secs_f64();
        if seconds > 0.0 {
            self.processed as f64 / seconds
        } else {
            0.0
        }
    }

    /// The arm as the tables name it: the role, and the client that produced the number.
    pub fn label(&self) -> String {
        format!("{} ({})", self.arm, self.client)
    }
}

/// **The AK core arm**: Rust's own Kafka client, one record at a time, in this process.
///
/// `BaseConsumer::poll` hands back a single message, which is exactly the shape this arm is meant
/// to have - no batch fan-out, no threads, no engine. The clock starts *after* the consumer is
/// built and stops before it is dropped, because this arm is the denominator of every ratio in
/// both tables and no other arm charges itself for client construction or teardown.
///
/// # Errors
///
/// If the consumer cannot be built or subscribed, if a fetch fails, or if the backlog does not
/// arrive within [`ARM_BUDGET`] - a demo that spun here forever would print nothing at all.
pub fn ak_core(
    broker: &DemoBroker,
    options: &DemoOptions,
    topic: &str,
    target: usize,
) -> Result<ArmResult, String> {
    println!("\n=== {AK_CORE} starting over {target} records ===");
    let mut config = ClientConfig::new();
    for (key, value) in broker.client_properties(&group_id("ak-core")) {
        config.set(key, value);
    }
    let consumer: BaseConsumer = config
        .create()
        .map_err(|e| format!("{AK_CORE}: could not build a consumer: {e}"))?;
    consumer
        .subscribe(&[topic])
        .map_err(|e| format!("{AK_CORE}: could not subscribe to {topic}: {e}"))?;

    let work = Duration::from_millis(options.delay_ms);
    let started_at = Instant::now();
    let mut processed = 0usize;
    let mut keys: HashSet<Vec<u8>> = HashSet::new();
    while processed < target {
        if started_at.elapsed() > ARM_BUDGET {
            return Err(format!("{AK_CORE} stalled at {processed} of {target}"));
        }
        match consumer.poll(Duration::from_millis(500)) {
            Some(Ok(message)) => {
                // The user's function, and the same sleep every other arm runs. The contract's
                // predicate is whether the CLIENT is thread-per-record, not what the language is
                // called - and this arm is: one thread, one record, nothing else to do with it. So
                // a blocking sleep occupies no slot the engine is counting, which is exactly what
                // it does NOT do on the sidecar arm below.
                let _ = message.payload();
                // A null key is not a key, so only a present one counts. Nothing this demo seeds
                // has one, and counting `None` as a distinct key would make the two arms disagree
                // the first time something did.
                if let Some(key) = message.key() {
                    keys.insert(key.to_vec());
                }
                std::thread::sleep(work);
                processed += 1;
            }
            Some(Err(e)) => return Err(format!("{AK_CORE}: fetching from {topic} failed: {e}")),
            None => {}
        }
    }
    Ok(finished(AK_CORE, AK_CORE_CLIENT, started_at, processed, keys.len()))
}

/// **The sidecar arm**: this application as a *foreign client*.
///
/// The client library spawns the sidecar, receives records over a socket, runs this function on
/// them and reports outcomes back. **The application does no Kafka I/O on this path**: the sidecar
/// owns the consumer, the producer, the group membership and the offsets. That is a claim about
/// the *path* rather than about this process - the same binary creates the topic, seeds the
/// backlog and runs the AK core arm with `rdkafka`, because a comparison needs both sides. A
/// genuinely foreign application carries no Kafka client at all, which is the property this arm
/// stands in for.
///
/// # Errors
///
/// If the session cannot be opened, if it ends before the target is reached, or if the arm exceeds
/// [`ARM_BUDGET`].
pub async fn rust_grpc(
    broker: &DemoBroker,
    options: &DemoOptions,
    sidecar: &SidecarCommand,
    topic: &str,
    target: usize,
) -> Result<ArmResult, String> {
    println!("\n=== {RUST_GRPC} starting over {target} records ===");

    let client = ParallelConsumerClient::connect(ClientOptions {
        sidecar_path: sidecar.path.clone(),
        sidecar_args: sidecar.args.clone(),
        topics: vec![topic.to_owned()],
        max_concurrency: Some(options.max_concurrency as i32),
        // Set EXPLICITLY, and leaving it out would not be a harmless omission: unspecified means
        // "take parallel-consumer-core's default", which is KEY - so this arm would run key-ordered
        // against an unordered AK core arm and the tables would compare two different workloads.
        ordering: Some(ProcessingOrder::Unordered),
        kafka_properties: broker.client_properties(&group_id("pc-rust-grpc")),
        ..Default::default()
    })
    .await
    .map_err(|e| format!("{RUST_GRPC}: opening the session: {e}"))?;

    let processed = Arc::new(AtomicUsize::new(0));
    // The distinct keys this arm saw, which is half of what the table uses to show the work
    // happened rather than assert it. A mutex in a path whose every invocation already sleeps for
    // `--delay-ms` costs nothing measurable; correctness across the executor pool is what matters
    // here, and an atomic cannot count distinct values.
    let seen_keys: Arc<Mutex<HashSet<Vec<u8>>>> = Arc::new(Mutex::new(HashSet::new()));
    let (counted, mut counts) = watch::channel(0usize);
    let work = Duration::from_millis(options.delay_ms);
    let counter = Arc::clone(&processed);
    let key_sink = Arc::clone(&seen_keys);

    // THE SLEEP GOES THROUGH `blocking(...)`, AND THAT IS RUST'S ONE DIVERGENCE WORTH READING.
    // The contract's rule is not about the language: it asks whether the CLIENT is
    // thread-per-record, and this one is not - its executors are tasks on an async runtime. A
    // blocking call made directly inside one occupies a runtime worker thread, of which there are
    // as many as the machine has cores, so the table would report the RUNTIME's ceiling while
    // appearing to report the engine's. `blocking(...)` is the library's own entry point for a
    // blocking user function: it runs each invocation on the runtime's blocking pool, so
    // concurrency is bounded by the ceiling the fingerprint printed. This is still a real
    // `std::thread::sleep` either way - only where it blocks changes. Measured rather than
    // reasoned about, and the contract now cites the figures: docs/inflight/clients/rust.md.
    client
        .poll(blocking(move |record| {
            std::thread::sleep(work);
            if let Some(key) = record.key.as_ref() {
                key_sink
                    .lock()
                    .expect("the key-set lock is never poisoned")
                    .insert(key.clone());
            }
            let done = counter.fetch_add(1, Ordering::Relaxed) + 1;
            let _ = counted.send(done);
            Ok(Outcome::success())
        }))
        .map_err(|e| format!("{RUST_GRPC}: starting the poll: {e}"))?;

    let started_at = Instant::now();
    let reached = async {
        while *counts.borrow_and_update() < target {
            if counts.changed().await.is_err() {
                return;
            }
        }
    };
    tokio::select! {
        () = reached => {}
        // Reaching the target is not the only thing that can end this wait: a failed or completed
        // session ends it too. Without this the arm would sit out the whole budget after a session
        // that died in its first second.
        () = client.closed() => {}
        () = tokio::time::sleep(ARM_BUDGET) => {}
    }
    let elapsed = started_at.elapsed();
    let count = processed.load(Ordering::Relaxed);
    let unique_keys = seen_keys.lock().expect("the key-set lock is never poisoned").len();

    let shutdown = client.shutdown().await;
    if count < target {
        // The session's own fault, when it had one, is the useful half of this message - a bare
        // "ended early" would send a reader looking at the wrong thing.
        let reason = match shutdown {
            Err(e) => format!(" - the session reported: {e}"),
            Ok(()) => String::new(),
        };
        return Err(format!("{RUST_GRPC} ended early at {count} of {target}{reason}"));
    }
    shutdown.map_err(|e| format!("{RUST_GRPC}: closing the session: {e}"))?;

    println!(
        "=== {RUST_GRPC} finished: {count} records over {unique_keys} keys in {}ms ===",
        elapsed.as_millis()
    );
    Ok(ArmResult {
        arm: RUST_GRPC.to_owned(),
        client: RUST_GRPC_CLIENT.to_owned(),
        elapsed,
        processed: count,
        unique_keys,
    })
}

fn finished(arm: &str, client: &str, started_at: Instant, processed: usize, unique_keys: usize) -> ArmResult {
    let elapsed = started_at.elapsed();
    println!(
        "=== {arm} finished: {processed} records over {unique_keys} keys in {}ms ===",
        elapsed.as_millis()
    );
    ArmResult {
        arm: arm.to_owned(),
        client: client.to_owned(),
        elapsed,
        processed,
        unique_keys,
    }
}

/// A fresh group per arm per replay, so every arm reads the same records from the beginning.
fn group_id(arm: &str) -> String {
    format!("pc-demo-{arm}-{}", unique())
}

/// A monotonically increasing number, for naming things that must not collide within a run.
pub fn unique() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or_default()
}
