// Copyright (C) 2026 Antony Stubbs and contributors

//! The session: one sidecar process, one gRPC stream, one dispatch queue, `executor_count`
//! executors.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_channel::{Receiver, Sender, TrySendError};
use tokio::sync::watch;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::timeout;
use tonic::codec::Streaming;
use tonic::Status;

use crate::error::ClientError;
use crate::options::ClientOptions;
use crate::outcome::{Outcome, ProcessingError, RecordProcessor};
use crate::proto::proxy_service_client::ProxyServiceClient;
use crate::proto::{client_message, proxy_message, report, ClientMessage, DispatchRecord, ProxyMessage, Report};
use crate::record::InboundRecord;
use crate::session::Session;
use crate::sidecar::Sidecar;

/// One session, from the handshake to the half-close.
///
/// [`connect`](Self::connect) opens it, [`poll`](Self::poll) starts processing,
/// [`shutdown`](Self::shutdown) ends it cleanly. Dropping the client without shutting it down
/// still stops the sidecar - closing the lifecycle pipe is the parent-death signal - but it skips
/// the drain, so the proxy recovers by rebalance rather than by a clean commit.
pub struct ParallelConsumerClient {
    shared: Arc<Shared>,
    sidecar: Option<Sidecar>,
    executors: Mutex<Vec<JoinHandle<()>>>,
    transport: Mutex<Option<JoinHandle<()>>>,
    polled: AtomicBool,
    stop_handout: watch::Sender<bool>,
    shutdown_grace: Duration,
}

/// What the transport task and every executor share. Nothing per record lives here - see the crate
/// documentation on why the client keeps no per-record state at all.
struct Shared {
    session: Session,
    /// The dispatch queue. Its depth is the proxy's own in-flight ceiling, so in a correct system
    /// it cannot overflow; an overflow is a protocol violation, not load.
    queue: Receiver<DispatchRecord>,
    /// Records dispatched and not yet reported - QUEUED PLUS EXECUTING. This, and not the
    /// channel's occupancy, is what `max_concurrency` bounds: the guide's worked example overflows
    /// on a fourth record while three are unresolved, two of them already out with executors. A
    /// count of queued records alone would let a record leaving the queue make room, so the
    /// overflow the guide describes could never be detected.
    unresolved: AtomicUsize,
    /// Every message this client sends. One channel, so the executors never contend for the
    /// stream, and closing it *is* the half-close.
    outbound: Sender<ClientMessage>,
    ended: watch::Sender<bool>,
    failure: Mutex<Option<ClientError>>,
}

impl ParallelConsumerClient {
    /// Spawns the sidecar, connects to it, and completes the fresh-session handshake. It returns
    /// once the proxy's effective configuration has arrived - only then is the session open.
    ///
    /// # Errors
    ///
    /// If the options are unusable, the sidecar cannot be started or does not report a port, the
    /// connection fails, or the handshake does not produce a usable `Configured`.
    pub async fn connect(options: ClientOptions) -> Result<Self, ClientError> {
        options.validate()?;

        let sidecar = Sidecar::spawn(&options).await?;
        match Self::handshake(&options, sidecar.port).await {
            Ok((session, stream, outbound)) => Ok(Self::start(&options, sidecar, session, stream, outbound)),
            Err(e) => {
                sidecar.stop(options.shutdown_grace).await.ok();
                Err(e)
            }
        }
    }

    async fn handshake(
        options: &ClientOptions,
        port: u16,
    ) -> Result<(Session, Streaming<ProxyMessage>, Sender<ClientMessage>), ClientError> {
        // The ordinary host:port authority the proxy's loopback allowlist expects; no TLS, no
        // interceptors, no load balancing - the deliberately narrow slice of gRPC the protocol
        // permits, so that every language's implementation suffices.
        let endpoint = format!("http://127.0.0.1:{port}");
        let mut grpc = timeout(options.connect_timeout, ProxyServiceClient::connect(endpoint))
            .await
            .map_err(|_| ClientError::Timeout(format!("connecting to the sidecar on port {port}")))?
            .map_err(|e| ClientError::Transport(format!("connecting to the sidecar on port {port}: {e}")))?;

        let (outbound, outbound_rx) = async_channel::unbounded::<ClientMessage>();
        // NOTE what is NOT in the error text below: the Configure message itself. It carries
        // kafka_properties, and the natural rendering of a send failure would put credentials in a
        // log line.
        outbound
            .try_send(ClientMessage {
                message: Some(client_message::Message::Configure(options.configure())),
            })
            .map_err(|_| ClientError::Transport("the outbound stream closed before Configure".to_owned()))?;

        let mut stream = timeout(options.connect_timeout, grpc.session(outbound_rx))
            .await
            .map_err(|_| ClientError::Timeout("opening the session stream".to_owned()))?
            .map_err(|status| ClientError::Transport(format!("opening the session stream: {status}")))?
            .into_inner();

        let first = timeout(options.connect_timeout, stream.message())
            .await
            .map_err(|_| ClientError::Timeout("awaiting Configured".to_owned()))?
            .map_err(|status| ClientError::Transport(format!("awaiting Configured: {status}")))?;

        match first.and_then(|message| message.message) {
            Some(proxy_message::Message::Configured(configured)) => {
                Ok((Session::from_wire(configured)?, stream, outbound))
            }
            Some(other) => Err(ClientError::Protocol(format!(
                "the handshake reply was {}, not Configured",
                message_kind(&other)
            ))),
            None => Err(ClientError::Protocol(
                "the stream ended before Configured arrived".to_owned(),
            )),
        }
    }

    fn start(
        options: &ClientOptions,
        sidecar: Sidecar,
        session: Session,
        stream: Streaming<ProxyMessage>,
        outbound: Sender<ClientMessage>,
    ) -> Self {
        // THE QUEUE'S DEPTH IS THE PROXY'S DECLARED CEILING. Any other depth turns a protocol
        // violation into either a silent buffer or a lost record. Admission is decided by the
        // unresolved count in `enqueue`, so this bound can never be the thing that fires - it is
        // the structural statement of the same invariant.
        let depth = ceiling(session.max_concurrency);
        let (queue_tx, queue) = async_channel::bounded::<DispatchRecord>(depth);
        let (ended, _) = watch::channel(false);
        let (stop_handout, _) = watch::channel(false);

        let shared = Arc::new(Shared {
            session,
            queue,
            unresolved: AtomicUsize::new(0),
            outbound,
            ended,
            failure: Mutex::new(None),
        });

        // The transport task starts NOW, not at poll: the proxy may dispatch the moment it is
        // configured, and the stream also carries the control plane, so an admin that is not
        // reading head-of-line-blocks itself.
        let transport = tokio::spawn(transport(Arc::clone(&shared), stream, queue_tx));

        Self {
            shared,
            sidecar: Some(sidecar),
            executors: Mutex::new(Vec::new()),
            transport: Mutex::new(Some(transport)),
            polled: AtomicBool::new(false),
            stop_handout,
            shutdown_grace: options.shutdown_grace,
        }
    }

    /// The effective configuration this session is running with - what the proxy replied,
    /// including the negotiated capability set. Assert on this, never on the options.
    #[must_use]
    pub fn session(&self) -> &Session {
        &self.shared.session
    }

    /// Starts processing with the user's function, and **returns immediately**. At most once per
    /// client.
    ///
    /// It returns rather than blocking because the session has no natural end and this crate is
    /// async: a caller that wants to wait for the session awaits [`closed`](Self::closed), and one
    /// that has other work to do simply does it. Blocking here would mean the only way to reach
    /// [`shutdown`](Self::shutdown) was from a second task.
    ///
    /// `executor_count` tokio tasks are spawned, so this must be called from within a runtime.
    ///
    /// # Errors
    ///
    /// [`ClientError::AlreadyPolling`] if this client is already processing.
    ///
    /// # Panics
    ///
    /// If a previous call to this method panicked while holding the executor list.
    pub fn poll<P: RecordProcessor>(&self, processor: P) -> Result<(), ClientError> {
        if self.polled.swap(true, Ordering::SeqCst) {
            return Err(ClientError::AlreadyPolling);
        }

        let processor = Arc::new(processor);
        let mut executors = self.executors.lock().expect("the executor list is poisoned");
        for _ in 0..self.shared.session.executor_count {
            executors.push(tokio::spawn(execute(
                Arc::clone(&self.shared),
                Arc::clone(&processor),
                self.stop_handout.subscribe(),
            )));
        }
        Ok(())
    }

    /// Resolves when the session's stream has ended - because the proxy completed it, because it
    /// failed, or because this client shut it down. The reason, if it was a fault, comes back from
    /// [`shutdown`](Self::shutdown).
    pub async fn closed(&self) {
        let mut ended = self.shared.ended.subscribe();
        // Returns immediately when the stream has already ended.
        ended.wait_for(|ended| *ended).await.ok();
    }

    /// The client-initiated shutdown: stop handing records out, let executing records finish and
    /// report, then half-close the stream and reap the sidecar.
    ///
    /// **The half-close IS the shutdown signal** - there is no shutdown-request message, because a
    /// client that has reported everything it ran has nothing left to say.
    ///
    /// # Errors
    ///
    /// The session's first fault, if it had one - including a fault the transport task recorded
    /// while the application was doing something else.
    ///
    /// # Panics
    ///
    /// If a task panicked while holding this client's executor or transport handles.
    pub async fn shutdown(mut self) -> Result<(), ClientError> {
        self.stop_handout.send_replace(true);

        let executors: Vec<_> = self
            .executors
            .lock()
            .expect("the executor list is poisoned")
            .drain(..)
            .collect();
        for executor in executors {
            executor.await.ok();
        }

        // QUEUED RECORDS ARE DROPPED, and that is the specification's own consequence rather than
        // a shortcut. The guide says to report them `Released`, but `Released` is gated by the
        // `shutdown` capability, which this client does not implement and therefore does not
        // declare - and sending an outcome outside the negotiated set is itself a violation. So
        // the records are dropped and the proxy returns them to scheduling by the same path it
        // uses for a lost connection, attempt counts unchanged. The wave that implements the
        // shutdown drain sends `Released` here, under a `session.negotiated(capability::SHUTDOWN)`
        // test.
        self.shared.queue.close();
        while self.shared.queue.try_recv().is_ok() {
            // Discarded, so no longer unresolved - the same accounting a report does, for records
            // that will never get one.
            self.shared.settle();
        }

        // Half-close: no more sends, ever. Everything run has been reported.
        self.shared.outbound.close();

        // Taken in its own statement so the lock is released before the await below - a std Mutex
        // guard must never be held across one.
        let transport = self.transport.lock().expect("the transport handle is poisoned").take();
        if let Some(mut transport) = transport {
            // Give the proxy its drain: it commits, completes the stream, and the transport task
            // ends on its own.
            if timeout(self.shutdown_grace, &mut transport).await.is_err() {
                transport.abort();
                transport.await.ok();
            }
        }

        if let Some(sidecar) = self.sidecar.take() {
            // Closing the lifecycle pipe is the reap. Never kill a sidecar with the stream still
            // open - that turns a clean drain into a reconnect-window recovery for the next group
            // member.
            if let Err(e) = sidecar.stop(self.shutdown_grace).await {
                self.shared.fail(e);
            }
        }

        self.shared.take_failure().map_or(Ok(()), Err)
    }
}

impl Drop for ParallelConsumerClient {
    /// Best effort, because a destructor cannot await: stop the tasks, and let the sidecar's
    /// lifecycle pipe close with this client. [`shutdown`](ParallelConsumerClient::shutdown) is
    /// the supported route, and the only one that drains.
    fn drop(&mut self) {
        self.stop_handout.send_replace(true);
        if let Ok(mut executors) = self.executors.lock() {
            for executor in executors.drain(..) {
                executor.abort();
            }
        }
        if let Ok(mut transport) = self.transport.lock() {
            if let Some(handle) = transport.take() {
                handle.abort();
            }
        }
    }
}

impl Shared {
    /// Records the session's FIRST fault. Later ones are consequences of it far more often than
    /// they are new information.
    fn fail(&self, error: ClientError) {
        if let Ok(mut failure) = self.failure.lock() {
            failure.get_or_insert(error);
        }
    }

    fn take_failure(&self) -> Option<ClientError> {
        self.failure.lock().ok().and_then(|mut failure| failure.take())
    }

    /// Queues one wave in record order. Hand-out is FIFO - by arrival, and within a wave by the
    /// wave's own order - which a bounded MPMC queue gives directly.
    ///
    /// ADMISSION IS DECIDED BY THE UNRESOLVED COUNT, not by whether the channel has a free slot: a
    /// record handed to an executor has left the channel but is still in flight against the
    /// proxy's ceiling, and only its report frees the slot ([`Shared::settle`]).
    fn enqueue(&self, records: Vec<DispatchRecord>, queue: &Sender<DispatchRecord>) -> Result<(), ClientError> {
        for record in records {
            // Read then add, rather than add then check: `enqueue` has exactly one caller, the
            // transport task, so this is single-producer, and a concurrent `settle` can only free
            // capacity - never take it. It also leaves the count honest on the violation path.
            let already = self.unresolved.load(Ordering::SeqCst);
            if already >= ceiling(self.session.max_concurrency) {
                return Err(self.overflow(already));
            }
            self.unresolved.fetch_add(1, Ordering::SeqCst);
            match queue.try_send(record) {
                Ok(()) => {}
                // The receiving end is gone: the session is shutting down, not misbehaving.
                Err(TrySendError::Closed(_)) => {
                    self.settle();
                    return Ok(());
                }
                // Unreachable while the count above is right - queued records are a subset of the
                // unresolved ones, so the channel fills only once the ceiling is already reached.
                // Kept as the structural backstop, answering exactly as the counted check does.
                Err(TrySendError::Full(_)) => {
                    self.settle();
                    return Err(self.overflow(already));
                }
            }
        }
        Ok(())
    }

    /// The counted protocol violation: the proxy exceeded the in-flight ceiling it declared itself.
    fn overflow(&self, already_unresolved: usize) -> ClientError {
        ClientError::Protocol(format!(
            "the proxy dispatched a record while {} were already unresolved - queued plus \
             executing - past the max_concurrency of {} it declared itself, so this is a protocol \
             violation and not load; the call is cancelled rather than answered with \
             FAILED_PRECONDITION, because a gRPC client cannot set a status",
            already_unresolved, self.session.max_concurrency
        ))
    }

    /// One record reached a verdict and was reported, released or discarded: it no longer counts
    /// against the ceiling. Saturating, so a double settle can never wrap the count into a ceiling
    /// nothing can overflow.
    fn settle(&self) {
        self.unresolved
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |unresolved| {
                Some(unresolved.saturating_sub(1))
            })
            .ok();
    }

    async fn run_one<P: RecordProcessor>(&self, processor: &Arc<P>, dispatched: DispatchRecord) {
        // THE TOKEN IS ECHOED VERBATIM - the message the proxy sent, never one rebuilt from parsed
        // parts. It is opaque: nothing here reads record_id or compares epochs.
        let token = dispatched.token.clone();
        let record = InboundRecord::from_wire(&dispatched);

        let outcome = match invoke(processor, record).await {
            Ok(outcome) => report::Outcome::Success(report::Success {
                produce: outcome.into_wire(),
            }),
            Err(failure) => report::Outcome::Failure(report::Failure {
                reason: Some(failure.into_reason()),
            }),
        };

        // A closed outbound channel means the session ended before this outcome could be reported.
        // The engine's own paths return the record to scheduling; there is nothing to report to.
        let _ = self
            .outbound
            .send(ClientMessage {
                message: Some(client_message::Message::Report(Report {
                    token,
                    outcome: Some(outcome),
                })),
            })
            .await;

        // Reported: the record stops counting against the proxy's in-flight ceiling. This is the
        // only place a record executing frees its slot - leaving the queue never did.
        self.settle();
    }
}

/// The transport task. **It always reads.** Backpressure is never applied by not reading: the
/// stream also carries the control plane, so a client that stops reading to slow the proxy down
/// head-of-line-blocks itself.
async fn transport(shared: Arc<Shared>, mut stream: Streaming<ProxyMessage>, queue: Sender<DispatchRecord>) {
    loop {
        match stream.message().await {
            Ok(Some(message)) => match message.message {
                Some(proxy_message::Message::Dispatch(wave)) => {
                    if let Err(violation) = shared.enqueue(wave.records, &queue) {
                        shared.fail(violation);
                        // Returning drops the stream, which CANCELS the call. That is the whole of
                        // a gRPC client's vocabulary for "I am ending this": only a server can
                        // answer with a status, so the count travels in the error above instead.
                        break;
                    }
                }
                other => {
                    // Every remaining proxy message is gated by a capability this client does not
                    // declare, and the rule for an un-negotiated message is that the receiver
                    // never acts on it. Recording it keeps the violation visible without failing
                    // an otherwise healthy stream.
                    shared.fail(ClientError::Protocol(format!(
                        "the proxy sent {} outside the negotiated capability set {:?} - ignored",
                        other.as_ref().map_or("an empty message", message_kind),
                        shared.session.capabilities
                    )));
                }
            },
            // The proxy completed the stream: the ordinary end of a session.
            Ok(None) => break,
            Err(status) => {
                if !is_session_end(&status) {
                    shared.fail(ClientError::Transport(format!("the session stream: {status}")));
                }
                break;
            }
        }
    }

    shared.ended.send_replace(true);
    // Nothing more will arrive, so executors waiting on the queue stop waiting.
    queue.close();
}

/// One executor: take a record, run the user's function, report the outcome.
async fn execute<P: RecordProcessor>(shared: Arc<Shared>, processor: Arc<P>, mut stop_handout: watch::Receiver<bool>) {
    loop {
        if *stop_handout.borrow() {
            break;
        }
        tokio::select! {
            biased;
            // Hand-out stops immediately; a record already running is not interrupted, because it
            // is only this loop that stops.
            changed = stop_handout.changed() => {
                if changed.is_err() {
                    break;
                }
            }
            record = shared.queue.recv() => match record {
                Ok(record) => shared.run_one(&processor, record).await,
                Err(_closed) => break,
            }
        }
    }
}

/// Runs the user's function on its own task so that a panic is reported as a failure instead of
/// killing an executor. A crashing worker must produce a failure report, not tear down the stream.
async fn invoke<P: RecordProcessor>(processor: &Arc<P>, record: InboundRecord) -> Result<Outcome, ProcessingError> {
    let processor = Arc::clone(processor);
    match tokio::spawn(async move { processor.process(record).await }).await {
        Ok(outcome) => outcome,
        Err(join) => Err(ProcessingError::new(panic_reason(join))),
    }
}

/// A failure reason for a processor that panicked or was cancelled, with the panic's own message
/// when it had one.
pub(crate) fn panic_reason(join: JoinError) -> String {
    if join.is_cancelled() {
        return "the processor was cancelled before it finished".to_owned();
    }
    match join.try_into_panic() {
        Ok(payload) => {
            let message = payload
                .downcast_ref::<&str>()
                .map(|text| (*text).to_owned())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "no message".to_owned());
            format!("the processor panicked: {message}")
        }
        Err(join) => format!("the processor ended abnormally: {join}"),
    }
}

/// The in-flight ceiling as a count of records: `Configured.max_concurrency`, which is both the
/// queue's depth and the number of UNRESOLVED records this client will admit. One function so the
/// two can never be computed differently.
fn ceiling(max_concurrency: i32) -> usize {
    usize::try_from(max_concurrency).unwrap_or(1).max(1)
}

/// Whether a stream error is the ordinary end of a session rather than a fault: the call being
/// cancelled by this client's own shutdown, or the peer going away once it has drained.
fn is_session_end(status: &Status) -> bool {
    matches!(status.code(), tonic::Code::Cancelled | tonic::Code::Unavailable)
}

/// The NAME of a proxy message, never its content - a dispatch's records carry payload, and an
/// error message is not the place for it.
fn message_kind(message: &proxy_message::Message) -> &'static str {
    match message {
        proxy_message::Message::Configured(_) => "Configured",
        proxy_message::Message::Dispatch(_) => "Dispatch",
        proxy_message::Message::Drop(_) => "Drop",
        proxy_message::Message::Shutdown(_) => "Shutdown",
        proxy_message::Message::SetExecutorCount(_) => "SetExecutorCount",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{Dispatch, Token};

    fn shared_with_ceiling(max_concurrency: i32) -> (Arc<Shared>, Sender<DispatchRecord>, Receiver<DispatchRecord>) {
        let (queue_tx, queue) = async_channel::bounded::<DispatchRecord>(ceiling(max_concurrency));
        let (outbound, _outbound_rx) = async_channel::unbounded::<ClientMessage>();
        let (ended, _) = watch::channel(false);
        let shared = Arc::new(Shared {
            session: Session::from_wire(crate::proto::Configured {
                max_concurrency: Some(max_concurrency),
                executor_count: Some(1),
                capabilities: vec!["dispatch".to_owned()],
                ..Default::default()
            })
            .unwrap(),
            queue: queue.clone(),
            unresolved: AtomicUsize::new(0),
            outbound,
            ended,
            failure: Mutex::new(None),
        });
        (shared, queue_tx, queue)
    }

    fn dispatch_of(count: usize) -> Dispatch {
        Dispatch {
            records: (0..count)
                .map(|index| DispatchRecord {
                    token: Some(Token {
                        record_id: Some(format!("record-{index}")),
                        epoch: Some(1),
                    }),
                    record: Some(crate::proto::Record {
                        offset: Some(index as i64),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .collect(),
        }
    }

    #[test]
    fn a_wave_is_queued_in_record_order() {
        let (shared, queue_tx, queue) = shared_with_ceiling(3);

        shared.enqueue(dispatch_of(3).records, &queue_tx).unwrap();

        let offsets: Vec<i64> = std::iter::from_fn(|| queue.try_recv().ok())
            .map(|record| record.record.unwrap().offset.unwrap())
            .collect();
        assert_eq!(offsets, vec![0, 1, 2], "hand-out is FIFO by the wave's own order");
    }

    #[test]
    fn overflowing_the_ceiling_is_a_protocol_violation_naming_the_count() {
        let (shared, queue_tx, _queue) = shared_with_ceiling(2);

        let violation = shared
            .enqueue(dispatch_of(3).records, &queue_tx)
            .expect_err("a wave larger than max_concurrency must not be absorbed");

        let message = violation.to_string();
        assert!(message.contains("max_concurrency of 2"), "{message}");
        assert!(message.contains("cancelled"), "{message}");
    }

    /// THE GUIDE'S OWN WORKED EXAMPLE, which the wave-sized test above cannot reach: ceiling 3,
    /// two executors holding A and B, C still queued, and a fourth record arriving. Counting
    /// queued records alone, D fits - the channel has two free slots - and the overflow the guide
    /// describes at length is undetectable.
    #[test]
    fn a_record_out_with_an_executor_still_occupies_the_ceiling() {
        let (shared, queue_tx, queue) = shared_with_ceiling(3);
        shared.enqueue(dispatch_of(3).records, &queue_tx).unwrap();

        queue.try_recv().expect("executor-1 takes A");
        queue.try_recv().expect("executor-2 takes B");
        assert_eq!(
            shared.unresolved.load(Ordering::SeqCst),
            3,
            "leaving the queue for an executor does not resolve a record"
        );

        let violation = shared
            .enqueue(dispatch_of(1).records, &queue_tx)
            .expect_err("a fourth record while three are unresolved overflows the ceiling");

        let message = violation.to_string();
        assert!(message.contains("3 were already unresolved"), "{message}");
        assert!(message.contains("max_concurrency of 3"), "{message}");
    }

    /// The other half of the same rule: only a verdict frees a slot.
    #[test]
    fn reporting_a_record_is_what_makes_room_for_the_next() {
        let (shared, queue_tx, queue) = shared_with_ceiling(1);
        shared.enqueue(dispatch_of(1).records, &queue_tx).unwrap();
        queue.try_recv().expect("the only executor takes it");

        shared
            .enqueue(dispatch_of(1).records, &queue_tx)
            .expect_err("the record is executing, so the ceiling is still full");

        shared.settle();
        shared
            .enqueue(dispatch_of(1).records, &queue_tx)
            .expect("the report freed the slot");
    }

    #[tokio::test]
    async fn a_panicking_processor_becomes_a_failure_outcome() {
        let processor = Arc::new(|_record: InboundRecord| async {
            panic!("the user's function fell over");
        });

        let failure = invoke(&processor, InboundRecord::from_wire(&DispatchRecord::default()))
            .await
            .expect_err("a panic must be reported as a failure, not lost");

        assert!(failure.reason().contains("the user's function fell over"), "{failure}");
    }
}
