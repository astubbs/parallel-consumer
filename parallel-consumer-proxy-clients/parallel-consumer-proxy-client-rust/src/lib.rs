// Copyright (C) 2026 Antony Stubbs and contributors

//! A Rust client for the Parallel Consumer language proxy: key-ordered concurrent Kafka
//! processing, from Rust, with the engine running as a sidecar process.
//!
//! The shape, which is the same in every language:
//!
//! ```text
//! application process
//! ├── the user's function (an ordinary Rust closure or type - the proxy never learns what it is)
//! ├── this crate
//! │   ├── transport  - spawns the sidecar, holds the ONE gRPC stream, owns the dispatch queue
//! │   └── executors  - tokio tasks, each: take record -> run the function -> report the outcome
//! └── sidecar proxy (child process) - runs Parallel Consumer, owns Kafka entirely
//! ```
//!
//! # Failure is a value, not an exception
//!
//! Rust has no exceptions, so the outcome of the user's function *is* its return value:
//! [`RecordProcessor::process`] returns `Result<Outcome, ProcessingError>`, and the `Err` case is
//! the failure outcome the proxy retries - there is no `fail()` constructor to keep in step with
//! it, and no way to return a success and a failure at once. [`ProcessingError`] converts from any
//! [`std::error::Error`], so `?` inside the user's function is the whole failure path:
//!
//! ```no_run
//! # use parallel_consumer_proxy_client::{InboundRecord, Outcome, ProcessingError};
//! async fn process(record: InboundRecord) -> Result<Outcome, ProcessingError> {
//!     let text = String::from_utf8(record.value.unwrap_or_default())?; // `?` = a failure outcome
//!     println!("{text}");
//!     Ok(Outcome::success())
//! }
//! ```
//!
//! A panic inside the user's function is caught and reported as a failure too - a worker crash
//! must not tear down the session - but returning `Err` is the supported spelling.
//!
//! # The session
//!
//! ```no_run
//! # use std::collections::HashMap;
//! # use parallel_consumer_proxy_client::{ClientOptions, InboundRecord, Outcome, ParallelConsumerClient};
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = ParallelConsumerClient::connect(ClientOptions {
//!     sidecar_path: "/opt/parallel-consumer/proxy".into(),
//!     topics: vec!["orders".to_owned()],
//!     kafka_properties: HashMap::from([
//!         ("bootstrap.servers".to_owned(), "localhost:9092".to_owned()),
//!     ]),
//!     ..Default::default()
//! })
//! .await?;
//!
//! // Returns as soon as processing is running - see `ParallelConsumerClient::poll`.
//! // The parameter type is spelled out because `poll` is generic over `RecordProcessor` rather
//! // than over `Fn`, so nothing else pins it.
//! client.poll(|record: InboundRecord| async move {
//!     println!("{}-{}@{}", record.topic, record.partition, record.offset);
//!     Ok(Outcome::success())
//! })?;
//!
//! client.closed().await; // or carry on doing other work
//! client.shutdown().await?;
//! # Ok(()) }
//! ```
//!
//! # State this crate deliberately does not keep
//!
//! It is **stateless per record**. The fencing token rides from dispatch to report inside the
//! executing task and is echoed back byte-identically; there is no request map, no dedupe cache
//! and no completion registry, because a client that holds no per-record state cannot have a
//! per-record state bug. Fencing is the proxy's job.
//!
//! # Wave one
//!
//! Implemented: connect, [`Configure`](ClientOptions), one `Dispatch` wave, the user's function,
//! the report, and a clean client-initiated shutdown. Not implemented, and therefore **not
//! declared** in the handshake (see [`capability`]): heartbeats and the liveness lease, the
//! manifest reconnect, worker-death reporting, terminal outcomes, and the proxy-initiated shutdown
//! drain.

#![warn(missing_docs)]

// Generated from the FROZEN schema at build time by build.rs - see its header for why this is a
// build step rather than committed output. The lint allowances are for the generated code only;
// nothing hand-written lives in here.
#[allow(clippy::all, clippy::pedantic, missing_docs, rustdoc::all)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/parallelconsumer.proxy.v1.rs"));
}

mod client;
mod error;
mod options;
mod outcome;
mod record;
mod session;
mod sidecar;

pub use client::ParallelConsumerClient;
pub use error::ClientError;
pub use options::{capability, ClientOptions, ProcessingOrder, SidecarStderr};
pub use outcome::{blocking, Blocking, Outcome, ProcessingError, RecordProcessor};
pub use record::{InboundRecord, OutboundRecord};
pub use session::Session;
