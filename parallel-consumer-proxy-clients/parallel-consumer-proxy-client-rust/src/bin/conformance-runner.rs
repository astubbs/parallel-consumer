// Copyright (C) 2026 Antony Stubbs and contributors

//! Rust's half of the shared cross-language conformance suite (astubbs#242, confluentinc#154).
//!
//! IT ASSERTS NOTHING, DELIBERATELY. The suite that knows what correct looks like - offset
//! frontiers, ordering, redelivery, attempt counts - is the Java module
//! `parallel-consumer-proxy-clients/parallel-consumer-proxy-conformance`, and it keeps owning that
//! knowledge for every language. This binary's whole job is to DO WHAT THE SCENARIO SAYS and then
//! exit; if it were free to decide what "correct" means, ten languages would each decide it
//! slightly differently and the agreement between them would prove nothing.
//!
//! Its contract - flags, exit codes, the stdout line, the behaviour tokens - is documented once, in
//! that module's `README.md`, and is identical in every language.
//!
//! THIS DOES NOT REPLACE THE CRATE'S OWN TESTS. The shared suite proves every client behaves
//! identically on the protocol; `tests/` catches what is invisible from outside the process - a
//! blocking call inside an executor, a task that outlives its session, a child that is never
//! reaped. Both layers are load-bearing.
//!
//! A CURRENT-THREAD RUNTIME, ON PURPOSE. A bin target sees only the crate's own dependencies, and
//! `rt-multi-thread` is a dev-dependency here - but the stricter runtime is the better test anyway:
//! every executor, the transport task and this runner share one thread, so a client that blocked
//! instead of awaiting would deadlock the concurrency scenario rather than getting away with it on
//! a spare core.

use std::collections::HashMap;
use std::path::Path;
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use parallel_consumer_proxy_client::{
    ClientOptions, InboundRecord, Outcome, ParallelConsumerClient, ProcessingError,
};
use tokio::sync::watch;

/// Exit statuses ARE the verdict channel. There is no results file and no report message: a
/// scenario passed if this process exited 0 and the Java suite's own assertions about engine state
/// held.
const EXIT_OK: u8 = 0;
const EXIT_BEHAVIOUR_FAILED: u8 = 1;
const EXIT_USAGE: u8 = 2;

const BEHAVIOUR_SUCCEED: &str = "succeed";
const BEHAVIOUR_REPORT_NOTHING: &str = "report-nothing";
const BEHAVIOUR_FAIL_THEN_SUCCEED: &str = "fail-then-succeed";
const BEHAVIOUR_HOLD_FIRST_UNTIL_SECOND: &str = "hold-first-until-second";

/// The exact text a fail-then-succeed run reports. The Java suite asserts the redelivery carries it
/// back VERBATIM, so it is a fixed literal of the contract in every language, never composed here.
const PRESCRIBED_FAILURE_REASON: &str = "conformance-prescribed-failure";

// Fixed session tunables, contract rather than this runner's judgement: they exist only so
// scenarios converge at unit-test speed against the engine's production defaults (a 5s commit
// interval, a 1s retry delay). Every language sets the same two values.
const COMMIT_INTERVAL: Duration = Duration::from_millis(100);
const RETRY_DELAY: Duration = Duration::from_millis(50);

/// How long a report-nothing run keeps its session OPEN after its last observation.
///
/// IT IS WHAT MAKES THE NEGATIVE CONTROL A CONTROL. Without it the runner exits the instant the
/// record arrives, and a sabotaged runner that DID report success has its report killed in flight
/// by the process exit - so the suite sees an unadvanced offset either way and the scenario passes
/// for a broken client. Measured in the Go wave, not reasoned about: reporting success from this
/// behaviour left the suite green until the hold existed.
const REPORT_NOTHING_HOLD: Duration = Duration::from_secs(3);

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let arguments = match Arguments::parse(std::env::args().skip(1).collect()) {
        Ok(arguments) => arguments,
        Err(problem) => {
            eprintln!("conformance-runner: {problem}");
            return ExitCode::from(EXIT_USAGE);
        }
    };
    ExitCode::from(run(arguments).await)
}

struct Arguments {
    scenario: String,
    behaviour: String,
    sidecar: String,
    expect_dispatches: usize,
    timeout_seconds: u64,
}

impl Arguments {
    /// The five flags, spelled identically in every language - including the British `--behaviour`.
    fn parse(argv: Vec<String>) -> Result<Self, String> {
        let mut values: HashMap<String, String> = HashMap::new();
        let mut index = 0;
        while index < argv.len() {
            let flag = &argv[index];
            let value = argv
                .get(index + 1)
                .ok_or_else(|| format!("{flag} takes a value"))?;
            if !flag.starts_with("--") {
                return Err(format!("expected --flag value pairs, got {flag}"));
            }
            values.insert(flag.clone(), value.clone());
            index += 2;
        }

        let take = |name: &str| -> Result<String, String> {
            values
                .get(name)
                .filter(|value| !value.is_empty())
                .cloned()
                .ok_or_else(|| format!("{name} is required"))
        };

        let scenario = take("--scenario")?;
        let behaviour = take("--behaviour")?;
        let sidecar = take("--sidecar")?;
        if !matches!(
            behaviour.as_str(),
            BEHAVIOUR_SUCCEED
                | BEHAVIOUR_REPORT_NOTHING
                | BEHAVIOUR_FAIL_THEN_SUCCEED
                | BEHAVIOUR_HOLD_FIRST_UNTIL_SECOND
        ) {
            return Err(format!("unknown behaviour {behaviour:?}"));
        }
        if !Path::new(&sidecar).is_absolute() {
            return Err(format!("--sidecar must be absolute, got {sidecar:?}"));
        }
        let expect_dispatches: usize = take("--expect-dispatches")?
            .parse()
            .map_err(|_| "--expect-dispatches must be a positive integer".to_owned())?;
        let timeout_seconds: u64 = take("--timeout-seconds")?
            .parse()
            .map_err(|_| "--timeout-seconds must be a positive integer".to_owned())?;
        if expect_dispatches < 1 {
            return Err("--expect-dispatches must be at least 1".to_owned());
        }
        if timeout_seconds < 1 {
            return Err("--timeout-seconds must be at least 1".to_owned());
        }

        Ok(Self { scenario, behaviour, sidecar, expect_dispatches, timeout_seconds })
    }
}

async fn run(arguments: Arguments) -> u8 {
    let budget = Duration::from_secs(arguments.timeout_seconds);
    let tracker = Arc::new(Tracker::new(arguments.expect_dispatches));

    let client = match ParallelConsumerClient::connect(ClientOptions {
        sidecar_path: arguments.sidecar.clone().into(),
        // THE SCENARIO NAME IS ALSO THE TOPIC NAME.
        topics: vec![arguments.scenario.clone()],
        // Enough executors for every dispatch the scenario prescribes, so a scenario that holds a
        // record cannot deadlock on an executor count smaller than its own shape.
        max_concurrency: Some(arguments.expect_dispatches as i32),
        commit_interval: Some(COMMIT_INTERVAL),
        default_message_retry_delay: Some(RETRY_DELAY),
        // The mock lane builds mock Kafka clients and reads no properties. Real credentials never
        // belong in a conformance test.
        kafka_properties: HashMap::new(),
        instance_tag: Some("conformance-runner-rust".to_owned()),
        ..Default::default()
    })
    .await
    {
        Ok(client) => client,
        Err(error) => {
            eprintln!("conformance-runner: opening the session: {error}");
            return EXIT_BEHAVIOUR_FAILED;
        }
    };

    let behaviour = arguments.behaviour.clone();
    let processor_tracker = Arc::clone(&tracker);
    if let Err(error) = client.poll(move |record: InboundRecord| {
        let tracker = Arc::clone(&processor_tracker);
        let behaviour = behaviour.clone();
        async move { process(&behaviour, &tracker, record).await }
    }) {
        eprintln!("conformance-runner: starting the poll: {error}");
        let _ = client.shutdown().await;
        return EXIT_BEHAVIOUR_FAILED;
    }

    // report-nothing completes at OBSERVATION, because by prescription its records are never
    // reported and so can never complete. Every other behaviour completes when the last record it
    // was handed has had its outcome decided.
    let report_nothing = arguments.behaviour == BEHAVIOUR_REPORT_NOTHING;
    let finished = tracker.await_prescribed_behaviour(report_nothing, budget).await;
    if !finished {
        eprintln!(
            "conformance-runner: scenario {:?} behaviour {:?} did not complete within {}s - \
             observed {} of {}, completed {}",
            arguments.scenario,
            arguments.behaviour,
            arguments.timeout_seconds,
            tracker.observed(),
            arguments.expect_dispatches,
            tracker.completed()
        );
        let _ = client.shutdown().await;
        return EXIT_BEHAVIOUR_FAILED;
    }

    if report_nothing {
        // Hold the session open, reporting nothing, so the suite is watching a LIVE client rather
        // than the wreckage of one - see REPORT_NOTHING_HOLD.
        tokio::time::sleep(REPORT_NOTHING_HOLD).await;
        // PRESCRIBED: the record is never reported and the session is abandoned rather than
        // drained - a worker that vanished mid-record is exactly what this scenario models. Exiting
        // closes the sidecar's lifecycle pipe, which reaps it, so nothing is leaked by not closing.
        return EXIT_OK;
    }

    if let Err(error) = client.shutdown().await {
        eprintln!("conformance-runner: closing the session: {error}");
        return EXIT_BEHAVIOUR_FAILED;
    }
    EXIT_OK
}

async fn process(
    behaviour: &str,
    tracker: &Tracker,
    record: InboundRecord,
) -> Result<Outcome, ProcessingError> {
    let attempt = record.attempt;
    let ordinal = tracker.observe(&record);

    match behaviour {
        BEHAVIOUR_SUCCEED => {
            tracker.complete();
            Ok(Outcome::success())
        }

        BEHAVIOUR_REPORT_NOTHING => {
            // Never report. A future that never resolves is how a Rust worker says "this record's
            // function has not returned"; the process exits with the record still in flight.
            std::future::pending::<()>().await;
            unreachable!("a pending future never resolves")
        }

        BEHAVIOUR_FAIL_THEN_SUCCEED => {
            tracker.complete();
            if attempt == 1 {
                // `Err` IS the language's failure idiom, and its reason travels verbatim.
                return Err(ProcessingError::new(PRESCRIBED_FAILURE_REASON));
            }
            Ok(Outcome::success())
        }

        BEHAVIOUR_HOLD_FIRST_UNTIL_SECOND => {
            if ordinal == 1 {
                // Hold the first record until a SECOND is dispatched. Whether one arrives at all,
                // and which key it carries, is the whole of what the scenario is asking - and it is
                // the Java suite that decides what the answer means.
                tracker.second_arrived().await;
            }
            tracker.complete();
            Ok(Outcome::success())
        }

        // unreachable: Arguments::parse rejects an unknown behaviour before the session opens
        other => Err(ProcessingError::new(format!("conformance: unknown behaviour {other:?}"))),
    }
}

/// Counts deliveries and outcomes, and prints the observation line. It holds no per-record state -
/// only counts - because the client library holds none either, and this runner must not become the
/// place where a client's missing bookkeeping is quietly supplied.
struct Tracker {
    expected: usize,
    observed: AtomicUsize,
    completed: AtomicUsize,
    /// Taken for the increment AND the print together, so the transcript's order is the order the
    /// ordinals were handed out in.
    printing: Mutex<()>,
    counts: watch::Sender<(usize, usize)>,
}

impl Tracker {
    fn new(expected: usize) -> Self {
        Self {
            expected,
            observed: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            printing: Mutex::new(()),
            counts: watch::channel((0, 0)).0,
        }
    }

    /// Prints the delivery and returns its 1-based ordinal in arrival order.
    fn observe(&self, record: &InboundRecord) -> usize {
        let ordinal = {
            let _printing = self.printing.lock().expect("the print lock is never poisoned");
            let ordinal = self.observed.fetch_add(1, Ordering::SeqCst) + 1;
            // Printed at the moment of delivery, before the behaviour acts on it. reason comes last
            // because it is worker-supplied text that may contain spaces.
            println!(
                "dispatch key={} offset={} attempt={} reason={}",
                record.key.as_deref().map(String::from_utf8_lossy).unwrap_or_default(),
                record.offset,
                record.attempt,
                record.last_failure_reason.as_deref().unwrap_or("")
            );
            ordinal
        };
        self.publish();
        ordinal
    }

    fn complete(&self) {
        self.completed.fetch_add(1, Ordering::SeqCst);
        self.publish();
    }

    fn observed(&self) -> usize {
        self.observed.load(Ordering::SeqCst)
    }

    fn completed(&self) -> usize {
        self.completed.load(Ordering::SeqCst)
    }

    fn publish(&self) {
        let _ = self.counts.send((self.observed(), self.completed()));
    }

    /// Resolves once a second delivery has been observed - the instrument the ordering scenario is.
    async fn second_arrived(&self) {
        let mut counts = self.counts.subscribe();
        while counts.borrow().0 < 2 {
            if counts.changed().await.is_err() {
                return;
            }
        }
    }

    /// Whether the prescription finished inside the budget.
    async fn await_prescribed_behaviour(&self, at_observation: bool, budget: Duration) -> bool {
        let expected = self.expected;
        let mut counts = self.counts.subscribe();
        let enough = |(observed, completed): (usize, usize)| {
            if at_observation { observed >= expected } else { completed >= expected }
        };
        tokio::time::timeout(budget, async {
            while !enough(*counts.borrow()) {
                if counts.changed().await.is_err() {
                    return;
                }
            }
        })
        .await
        .is_ok()
            && enough((self.observed(), self.completed()))
    }
}
