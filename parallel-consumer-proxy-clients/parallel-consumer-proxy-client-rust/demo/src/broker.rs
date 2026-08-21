// Copyright (C) 2026 Antony Stubbs and contributors

//! The broker the demo reads from, however the reader got here, and the backlog every arm replays.
//!
//! # Two ways in, and the second one is a rule rather than a convenience
//!
//! - **Nothing supplied** - the demo starts a real broker in a container, because that is what a
//!   user actually runs.
//! - **An address supplied** - the demo uses it and starts nothing. This is how the demo runs
//!   *inside* its own container, and it is not optional there: **a demo container is never granted
//!   the host Docker socket** (plan unit U35), so it could not start a broker even if it wanted
//!   to. It reaches a compose sibling on the demo's own network instead. A documented socket mount
//!   is root-equivalent host access taught as the normal way to run the product, which is why the
//!   rule exists rather than the shortcut.
//!
//! The same door serves own-cluster mode, where the address is the user's real cluster - so
//! nothing here logs or echoes it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use rdkafka::ClientContext;

/// The key space the seeded records spread over. Ordering is unordered in both arms, so this
/// changes nothing today; it exists so that a key-ordered lane added later has more than one key
/// to shard across, rather than needing the seeding rewritten first. The same number as the
/// reference demo's, so the two seed identical shapes.
const KEY_SPACE: usize = 1_000;

/// The address a natively-started broker advertises to the host. The compose file gives the broker
/// a second listener for exactly this, and the port is high and unusual on purpose - 9092 is what
/// a developer's own broker is already on.
const HOST_ADDRESS: &str = "localhost:29092";

/// The demo's broker, and the only thing in this demo that knows its address.
pub struct DemoBroker {
    bootstrap: String,
    /// Set when this run started the broker, so the demo can say how to stop it again.
    started_here: Option<PathBuf>,
}

impl DemoBroker {
    /// Uses the supplied broker, or starts one when none was supplied.
    ///
    /// # Errors
    ///
    /// If no address was supplied and a broker cannot be started - including the containerised
    /// case, where starting one is forbidden rather than merely difficult.
    pub fn resolve(supplied: Option<&str>) -> Result<Self, String> {
        if let Some(address) = supplied.map(str::trim).filter(|a| !a.is_empty()) {
            // deliberately not printed: own-cluster mode puts a real address here
            println!("Using the broker supplied by the caller.");
            return Ok(Self {
                bootstrap: address.to_owned(),
                started_here: None,
            });
        }

        if containerised() {
            return Err(
                "No broker address was supplied, and this demo is running in a container, where it \
                 must never start one: a demo container is never granted the host Docker socket \
                 (plan unit U35). Its compose file supplies PC_DEMO_BOOTSTRAP pointing at the \
                 broker sibling - run it with `docker compose up`, or demo/run.sh --docker."
                    .to_owned(),
            );
        }

        println!("No broker supplied, starting one as a container from the demo's own compose file.");
        // Resolved HERE and not in the caller, because finding it means finding the repository -
        // and the demo's own container has no repository to find: .dockerignore excludes .git, so
        // the walk up the tree fails. Only this branch needs the file, so only this branch may ask
        // for it. Asking eagerly failed the container before it reached its first Kafka call.
        let compose_file = crate::sidecar::compose_file()?;
        start_compose_broker(&compose_file)?;
        Ok(Self {
            bootstrap: HOST_ADDRESS.to_owned(),
            started_here: Some(compose_file),
        })
    }

    /// Creates the demo's topic, tolerating one a previous run already left behind.
    ///
    /// # Errors
    ///
    /// If the topic cannot be created, or already exists with a **different** partition count.
    /// Reusing a topic silently is fine; reusing one whose shape does not match is not, because
    /// the fingerprint printed at the top of the run would name a `--partitions` value that never
    /// applied - and that block is the demo's whole reproducibility promise.
    pub async fn ensure_topic(&self, topic: &str, partitions: i32) -> Result<(), String> {
        let admin: AdminClient<DefaultClientContext> = self
            .config()
            .create()
            .map_err(|e| format!("could not build an admin client: {e}"))?;

        let created = admin
            .create_topics(
                &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await
            .map_err(|e| format!("could not create the demo topic {topic}: {e}"))?;

        for result in created {
            match result {
                Ok(_) => println!("Created topic {topic} with {partitions} partitions"),
                Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {
                    let existing = self.partitions_of(topic)?;
                    if existing != partitions {
                        return Err(format!(
                            "topic {topic} already exists with {existing} partitions, but this run \
                             asked for {partitions} - pass --topic to name a fresh one, or \
                             --partitions {existing}"
                        ));
                    }
                    println!("Topic {topic} already exists with the requested {partitions} partitions, reusing it");
                }
                Err((name, code)) => {
                    return Err(format!("could not create the demo topic {name}: {code}"));
                }
            }
        }
        Ok(())
    }

    fn partitions_of(&self, topic: &str) -> Result<i32, String> {
        let admin: AdminClient<DefaultClientContext> = self
            .config()
            .create()
            .map_err(|e| format!("could not build an admin client: {e}"))?;
        let metadata = admin
            .inner()
            .fetch_metadata(Some(topic), Duration::from_secs(30))
            .map_err(|e| format!("could not describe the existing topic {topic}: {e}"))?;
        metadata
            .topics()
            .iter()
            .find(|described| described.name() == topic)
            .map(|described| described.partitions().len() as i32)
            .ok_or_else(|| format!("the broker reported no metadata for topic {topic}"))
    }

    /// Produces the backlog every arm then replays, from `from` up to but not including `to`.
    ///
    /// **Pre-produced rather than produced alongside the arms**, and that is what makes the
    /// workload closed-loop - which is in turn why no arm reports latency. A per-record timing
    /// here would be flattered by however far an arm had fallen behind, so throughput is the only
    /// honest number this shape can produce.
    ///
    /// # Errors
    ///
    /// If any single record failed to be produced. A flush that returns without error says nothing
    /// about individual deliveries, so a discarded result would let the demo report a full backlog,
    /// run both arms against a short one, and print numbers for a workload that never existed.
    pub fn seed(&self, topic: &str, from: usize, to: usize) -> Result<(), String> {
        if to <= from {
            return Ok(());
        }
        let first_failure: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let producer: ThreadedProducer<FailureRecordingContext> = self
            .config()
            .set("linger.ms", "20")
            .create_with_context(FailureRecordingContext {
                first_failure: Arc::clone(&first_failure),
            })
            .map_err(|e| format!("could not build a producer: {e}"))?;

        println!("Producing records {from} to {to}...");
        for index in from..to {
            let key = format!("key-{}", index % KEY_SPACE);
            let value = format!("record-{index}");
            let mut pending = BaseRecord::to(topic).key(&key).payload(&value);
            // A full local queue is backpressure, not a failure: librdkafka hands the record back
            // and the producer's own thread drains as it goes, so this retries rather than
            // dropping the record on the floor - which would silently shorten the backlog.
            loop {
                match producer.send(pending) {
                    Ok(()) => break,
                    Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), returned)) => {
                        pending = returned;
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err((e, _)) => return Err(format!("could not queue a record for {topic}: {e}")),
                }
            }
        }
        producer
            .flush(Duration::from_secs(120))
            .map_err(|e| format!("the demo could not flush its backlog: {e}"))?;

        if let Some(problem) = first_failure.lock().expect("the failure lock is never poisoned").take() {
            return Err(format!("the demo could not seed its backlog: {problem}"));
        }
        println!("Produced {} records", to - from);
        Ok(())
    }

    /// The Kafka properties every arm's client needs to reach this broker.
    ///
    /// # Why `enable.auto.commit` is in here
    ///
    /// Parallel Consumer owns offset commits, so it refuses a consumer with auto-commit on, and
    /// the sidecar forces the setting itself whatever this map says. The AK core arm needs it for
    /// a different reason: it is measuring how fast one thread can process records, and an
    /// auto-committing consumer would be timing an offset commit loop as well. One map, so the two
    /// arms cannot drift on anything else.
    pub fn client_properties(&self, group_id: &str) -> HashMap<String, String> {
        HashMap::from([
            ("bootstrap.servers".to_owned(), self.bootstrap.clone()),
            ("group.id".to_owned(), group_id.to_owned()),
            ("auto.offset.reset".to_owned(), "earliest".to_owned()),
            ("enable.auto.commit".to_owned(), "false".to_owned()),
        ])
    }

    /// How to stop a broker this run started, or `None` when the caller brought their own.
    ///
    /// The reference demo stops its Testcontainers broker on the way out; this one cannot, because
    /// the container is a compose service that outlives the process by design - the same one the
    /// `--docker` path uses. Printing the command is the honest substitute for a teardown the demo
    /// does not own.
    pub fn teardown_hint(&self) -> Option<String> {
        self.started_here
            .as_ref()
            .map(|compose| format!("docker compose -f {} down", compose.display()))
    }

    fn config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.bootstrap);
        config
    }
}

/// Whether this process is running inside a container, asked of the filesystem rather than guessed.
fn containerised() -> bool {
    Path::new("/.dockerenv").exists()
}

/// Starts the broker service from the demo's own compose file and waits for it to be healthy.
///
/// **Only the `broker` service**, never the whole file: the `demo` service in there is the
/// containerised path, and starting it from the native path would run the demo twice.
fn start_compose_broker(compose_file: &Path) -> Result<(), String> {
    if !compose_file.is_file() {
        return Err(format!(
            "the demo's compose file is not at {} - pass --bootstrap to use a broker you already have",
            compose_file.display()
        ));
    }
    // `--wait` is what makes this a start rather than a request to start: it blocks until the
    // service's own healthcheck passes, so the first Kafka call the demo makes is not a race.
    let outcome = Command::new("docker")
        .args(["compose", "-f"])
        .arg(compose_file)
        .args(["up", "--detach", "--wait", "broker"])
        .output()
        .map_err(|e| format!("could not run docker compose - is Docker installed and running? {e}"))?;

    if !outcome.status.success() {
        return Err(format!(
            "starting the broker failed ({}):\n{}",
            outcome.status,
            String::from_utf8_lossy(&outcome.stderr).trim()
        ));
    }
    Ok(())
}

/// Records the FIRST delivery failure, which is the one worth reporting - the rest are usually the
/// same broker problem seen again.
struct FailureRecordingContext {
    first_failure: Arc<Mutex<Option<String>>>,
}

impl ClientContext for FailureRecordingContext {}

impl ProducerContext for FailureRecordingContext {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, _opaque: Self::DeliveryOpaque) {
        if let Err((error, _)) = result {
            let mut held = self.first_failure.lock().expect("the failure lock is never poisoned");
            if held.is_none() {
                *held = Some(error.to_string());
            }
        }
    }
}
