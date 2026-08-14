// Copyright (C) 2026 Antony Stubbs and contributors

//! The library's own failure type - distinct from [`ProcessingError`](crate::ProcessingError),
//! which is the *user function's* failure and is a normal protocol outcome rather than a fault.

use thiserror::Error;

/// Everything that can go wrong with a session, as a value.
///
/// **No variant ever carries a Kafka property.** `kafka_properties` holds credentials, and the
/// natural rendering of a configuration error would put them in a log line, so this type names
/// property *keys* at most and the `Configure` message is never formatted into a message.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ClientError {
    /// The options could not be used to open a session - caught before the sidecar is spawned.
    #[error("the client options are not usable: {0}")]
    Options(String),

    /// The sidecar process could not be started, did not report its port, or could not be reaped.
    #[error("the sidecar process: {0}")]
    Sidecar(String),

    /// The gRPC connection or stream failed.
    #[error("the session transport: {0}")]
    Transport(String),

    /// The proxy did something the frozen protocol does not permit - including a `Dispatch` wave
    /// that overflows the client's queue past the ceiling the proxy itself declared.
    #[error("protocol violation: {0}")]
    Protocol(String),

    /// A step of connecting or shutting down did not complete inside its budget.
    #[error("timed out {0}")]
    Timeout(String),

    /// [`poll`](crate::ParallelConsumerClient::poll) was called a second time on one client. The
    /// poll-with-a-function shape is at most once per client, in every language.
    #[error("poll has already been called on this client")]
    AlreadyPolling,
}
