// Copyright (C) 2026 Antony Stubbs and contributors

//! The user's function, and what it returns.
//!
//! **This is where Rust's lack of exceptions is answered.** Every other client library in this
//! project translates a thrown exception into a failure outcome in exactly one place; Rust has
//! nothing to translate, so the outcome is the return value: `Ok(Outcome)` is a success and
//! `Err(ProcessingError)` *is* the failure outcome. There is no `Outcome::failure()` constructor
//! to keep in step with the error path, and no way to express both at once - the type system
//! enforces what other languages enforce by convention.

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use crate::proto;
use crate::record::{InboundRecord, OutboundRecord};

/// What a successful invocation produced: nothing, or records for the proxy to produce.
///
/// Failure is not a variant of this type, because `Result` already is one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Outcome {
    produce: Vec<OutboundRecord>,
}

impl Outcome {
    /// The record was processed, with no output.
    #[must_use]
    pub fn success() -> Self {
        Self::default()
    }

    /// The record was processed, and the proxy should produce these records with its own producer
    /// before the input record's offset may become eligible to commit. This is the only sanctioned
    /// route for worker output to Kafka.
    #[must_use]
    pub fn produce(records: impl IntoIterator<Item = OutboundRecord>) -> Self {
        Self {
            produce: records.into_iter().collect(),
        }
    }

    pub(crate) fn into_wire(self) -> Vec<proto::ProduceRecord> {
        self.produce.into_iter().map(OutboundRecord::into_wire).collect()
    }
}

/// The user function's failure: the reason that rides back with the record's redelivery.
///
/// It converts from any standard error, so `?` is the whole failure path inside a processor. It
/// deliberately does **not** implement [`std::error::Error`] itself - that is what keeps the
/// blanket conversion legal, and it is the same trade [`anyhow::Error`](https://docs.rs/anyhow)
/// makes.
///
/// The reason text reaches the proxy's logs and the next delivery: **do not put record payload or
/// credentials in it.**
#[derive(Clone, PartialEq, Eq)]
pub struct ProcessingError {
    reason: String,
}

impl ProcessingError {
    /// A failure with this reason.
    #[must_use]
    pub fn new(reason: impl Into<String>) -> Self {
        Self { reason: reason.into() }
    }

    /// The reason text, as it will travel on the wire.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub(crate) fn into_reason(self) -> String {
        self.reason
    }
}

impl fmt::Display for ProcessingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.reason)
    }
}

impl fmt::Debug for ProcessingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ProcessingError").field(&self.reason).finish()
    }
}

/// The blanket conversion that makes `?` the failure path.
///
/// There is deliberately no `From<String>` or `From<&str>` beside it: the compiler refuses both,
/// because a future release of the standard library could implement [`std::error::Error`] for
/// either. [`ProcessingError::new`] is the spelling for a bare message, and it reads better at the
/// call site than a conversion would.
impl<E: std::error::Error + Send + Sync + 'static> From<E> for ProcessingError {
    fn from(error: E) -> Self {
        Self::new(error.to_string())
    }
}

/// The user's function.
///
/// A trait rather than only a closure type, because the surface must work for a language with no
/// closures at all - and because a processor that owns state (a connection pool, a counter) is the
/// ordinary case. Closures get there for free: any `Fn(InboundRecord) -> impl Future` implements
/// this, so `client.poll(|record| async move { ... })` needs no adapter.
///
/// Implementations are shared across every executor, so they take `&self` and must be `Sync`. A
/// **blocking** function belongs in [`blocking`], not here: this method is polled on the async
/// runtime, and blocking it would stall the transport task that has to keep reading the stream.
pub trait RecordProcessor: Send + Sync + 'static {
    /// Processes one record. `Err` is the failure outcome, reported with its reason.
    fn process(&self, record: InboundRecord) -> impl Future<Output = Result<Outcome, ProcessingError>> + Send;
}

impl<F, Fut> RecordProcessor for F
where
    F: Fn(InboundRecord) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Outcome, ProcessingError>> + Send,
{
    fn process(&self, record: InboundRecord) -> impl Future<Output = Result<Outcome, ProcessingError>> + Send {
        self(record)
    }
}

/// A processor built from an ordinary **blocking** function - a database call, a synchronous HTTP
/// client, a CPU-bound transform.
///
/// It runs each invocation on the runtime's blocking pool, so user code that blocks cannot stall
/// the task that reads the session stream. Reaching for this is the right answer whenever the
/// function is not already `async`; wrapping blocking work in an `async` block is the Rust mistake
/// this exists to prevent.
///
/// ```no_run
/// # use parallel_consumer_proxy_client::{blocking, Outcome, ParallelConsumerClient};
/// # fn go(client: &ParallelConsumerClient) -> Result<(), Box<dyn std::error::Error>> {
/// client.poll(blocking(|record| {
///     std::fs::write("/tmp/last-record", record.value.unwrap_or_default())?;
///     Ok(Outcome::success())
/// }))?;
/// # Ok(()) }
/// ```
pub fn blocking<F>(function: F) -> Blocking<F>
where
    F: Fn(InboundRecord) -> Result<Outcome, ProcessingError> + Send + Sync + 'static,
{
    Blocking(Arc::new(function))
}

/// The processor [`blocking`] returns.
#[derive(Debug)]
pub struct Blocking<F>(Arc<F>);

impl<F> RecordProcessor for Blocking<F>
where
    F: Fn(InboundRecord) -> Result<Outcome, ProcessingError> + Send + Sync + 'static,
{
    fn process(&self, record: InboundRecord) -> impl Future<Output = Result<Outcome, ProcessingError>> + Send {
        let function = Arc::clone(&self.0);
        async move {
            match tokio::task::spawn_blocking(move || function(record)).await {
                Ok(outcome) => outcome,
                // A panic in the user's function is a failure report, never a torn-down session.
                Err(join) => Err(ProcessingError::new(crate::client::panic_reason(join))),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Unreachable;

    impl fmt::Display for Unreachable {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("the downstream service was unreachable")
        }
    }

    impl std::error::Error for Unreachable {}

    #[test]
    fn any_standard_error_becomes_a_failure_reason() {
        fn user_code() -> Result<Outcome, ProcessingError> {
            Err(Unreachable)?
        }

        assert_eq!(
            user_code().unwrap_err().reason(),
            "the downstream service was unreachable"
        );
    }

    /// The surface must work for a caller that cannot write a closure, so a named type has to be
    /// able to implement the trait alongside the blanket closure impl. If the two ever collide,
    /// this stops compiling - which is the finding, not a nuisance.
    #[test]
    fn a_named_type_can_be_a_processor() {
        struct Counter;

        impl RecordProcessor for Counter {
            async fn process(&self, _record: InboundRecord) -> Result<Outcome, ProcessingError> {
                Ok(Outcome::success())
            }
        }

        fn accepts<P: RecordProcessor>(_processor: P) {}

        accepts(Counter);
        accepts(|_record: InboundRecord| async { Ok(Outcome::success()) });
        accepts(blocking(|_record: InboundRecord| Ok(Outcome::success())));
    }
}
