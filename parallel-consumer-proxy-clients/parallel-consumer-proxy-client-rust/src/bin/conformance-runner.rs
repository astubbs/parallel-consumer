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
//! Its contract - flags, exit codes, the two stdout lines per record, the behaviour tokens - is
//! documented once, in that module's `README.md`, and is identical in every language.
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
use tokio::time::Instant;

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
const BEHAVIOUR_HOLD_UNTIL_CEILING_FULL: &str = "hold-until-ceiling-full";

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

/// How long `hold-until-ceiling-full` keeps a FULL group held before releasing it.
///
/// IT IS WHAT TURNS "THE CEILING WAS NEVER EXCEEDED" FROM A RACE INTO A MEASUREMENT. Release the
/// group the instant it fills and a client that declared a larger ceiling still passes - its extra
/// records arrive a few milliseconds later, by which time the outstanding count has already fallen
/// back. Holding the full ceiling still means the extra dispatch arrives INSIDE the window and
/// prints its line while every other record is unresolved. A correct engine cannot dispatch
/// anything during the window at all, so the wait costs a conforming client nothing but time.
const CEILING_SETTLE: Duration = Duration::from_millis(250);

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
    max_concurrency: usize,
    timeout_seconds: u64,
}

impl Arguments {
    /// The six flags, spelled identically in every language - including the British `--behaviour`.
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
                | BEHAVIOUR_HOLD_UNTIL_CEILING_FULL
        ) {
            return Err(format!("unknown behaviour {behaviour:?}"));
        }
        if !Path::new(&sidecar).is_absolute() {
            return Err(format!("--sidecar must be absolute, got {sidecar:?}"));
        }
        let expect_dispatches: usize = take("--expect-dispatches")?
            .parse()
            .map_err(|_| "--expect-dispatches must be a positive integer".to_owned())?;
        let max_concurrency: usize = take("--max-concurrency")?
            .parse()
            .map_err(|_| "--max-concurrency must be a positive integer".to_owned())?;
        let timeout_seconds: u64 = take("--timeout-seconds")?
            .parse()
            .map_err(|_| "--timeout-seconds must be a positive integer".to_owned())?;
        if expect_dispatches < 1 {
            return Err("--expect-dispatches must be at least 1".to_owned());
        }
        if max_concurrency < 1 {
            return Err("--max-concurrency must be at least 1".to_owned());
        }
        if timeout_seconds < 1 {
            return Err("--timeout-seconds must be at least 1".to_owned());
        }

        Ok(Self { scenario, behaviour, sidecar, expect_dispatches, max_concurrency, timeout_seconds })
    }
}

async fn run(arguments: Arguments) -> u8 {
    let budget = Duration::from_secs(arguments.timeout_seconds);
    let tracker = Arc::new(Tracker::new(arguments.expect_dispatches, arguments.max_concurrency));

    let client = match ParallelConsumerClient::connect(ClientOptions {
        sidecar_path: arguments.sidecar.clone().into(),
        // THE SCENARIO NAME IS ALSO THE TOPIC NAME.
        topics: vec![arguments.scenario.clone()],
        // THE CEILING IS THE SCENARIO'S TO CHOOSE, and this runner never derives one: it is set
        // from --max-concurrency and from nothing else. Deriving it from --expect-dispatches, which
        // is what this line used to do, is by construction a ceiling no scenario can reach.
        max_concurrency: Some(arguments.max_concurrency as i32),
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
        async move { process(&behaviour, &tracker, budget, record).await }
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
    budget: Duration,
    record: InboundRecord,
) -> Result<Outcome, ProcessingError> {
    let attempt = record.attempt;
    let ordinal = tracker.observe(&record);

    match behaviour {
        BEHAVIOUR_SUCCEED => {
            tracker.complete();
            tracker.settle(&record, None)
        }

        BEHAVIOUR_REPORT_NOTHING => {
            // Never report, and PRINT NO settled LINE - by prescription this record is never
            // resolved, and the absence of the line is the observation. A future that never
            // resolves is how a Rust worker says "this record's function has not returned"; the
            // process exits with the record still in flight.
            std::future::pending::<()>().await;
            unreachable!("a pending future never resolves")
        }

        BEHAVIOUR_FAIL_THEN_SUCCEED => {
            tracker.complete();
            if attempt == 1 {
                // `Err` IS the language's failure idiom, and its reason travels verbatim.
                return tracker.settle(&record, Some(PRESCRIBED_FAILURE_REASON));
            }
            tracker.settle(&record, None)
        }

        BEHAVIOUR_HOLD_FIRST_UNTIL_SECOND => {
            if ordinal == 1 {
                // Hold the first record until a SECOND is dispatched. Whether one arrives at all,
                // and which key it carries, is the whole of what the scenario is asking - and it is
                // the Java suite that decides what the answer means.
                tracker.second_arrived().await;
            }
            tracker.complete();
            tracker.settle(&record, None)
        }

        BEHAVIOUR_HOLD_UNTIL_CEILING_FULL => {
            // Hold until --max-concurrency records are held AT ONCE, keep the full group still for
            // CEILING_SETTLE, then release the whole group and start the next one.
            if !tracker.enter_ceiling_group(budget).await {
                // The prescription could not be carried out, so the run failed: the reason is
                // reported and printed, and the top-level budget in await_prescribed_behaviour -
                // this runner's existing route to exit 1 - is left to expire, because this record
                // is deliberately never counted as completed.
                let never_filled =
                    format!("conformance: the ceiling group of {} never filled", tracker.max_concurrency);
                return tracker.settle(&record, Some(&never_filled));
            }
            tracker.complete();
            tracker.settle(&record, None)
        }

        // unreachable: Arguments::parse rejects an unknown behaviour before the session opens
        other => Err(ProcessingError::new(format!("conformance: unknown behaviour {other:?}"))),
    }
}

/// Counts deliveries and outcomes, prints both observation lines, and holds the ceiling group's
/// barrier - the only three things a runner has to keep. It holds no per-record state -
/// only counts - because the client library holds none either, and this runner must not become the
/// place where a client's missing bookkeeping is quietly supplied.
struct Tracker {
    expected: usize,
    max_concurrency: usize,
    observed: AtomicUsize,
    completed: AtomicUsize,
    /// Taken for the increment AND the print together, and for the `settled` print as well, so the
    /// transcript's order is the order the events happened in - which is the whole of what the
    /// suite reads overlap from.
    printing: Mutex<()>,
    counts: watch::Sender<(usize, usize)>,
    /// How many records the `hold-until-ceiling-full` barrier is holding right now. It guards the
    /// generation below too: every read and every bump of the generation happens under this lock.
    held: Mutex<usize>,
    /// Which generation of the ceiling group a held record belongs to - the barrier's release
    /// signal, bumped once per group.
    ///
    /// A WATCH CHANNEL RATHER THAN A `Condvar`, because every wait here happens inside an executor
    /// task on a current-thread runtime: a `Condvar` would park the one thread that still has to
    /// dispatch the rest of the group, and the barrier could never fill. The same reasoning makes
    /// the settle window a `tokio::time::sleep` rather than a `thread::sleep`.
    generation: watch::Sender<u64>,
}

impl Tracker {
    fn new(expected: usize, max_concurrency: usize) -> Self {
        Self {
            expected,
            max_concurrency,
            observed: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            printing: Mutex::new(()),
            counts: watch::channel((0, 0)).0,
            held: Mutex::new(0),
            generation: watch::channel(0).0,
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

    /// Prints the `settled` line the moment this record's outcome has been DECIDED, and returns
    /// that outcome so the caller reports exactly what it printed.
    ///
    /// A dispatch line opens a record's unresolved window and this one closes it, so the running
    /// difference between the two - read in line order - is how many records this client was
    /// holding at that instant. That is why it prints under the same lock the dispatch side takes.
    fn settle(&self, record: &InboundRecord, failure: Option<&str>) -> Result<Outcome, ProcessingError> {
        {
            let _printing = self.printing.lock().expect("the print lock is never poisoned");
            println!(
                "settled key={} offset={} attempt={} reason={}",
                record.key.as_deref().map(String::from_utf8_lossy).unwrap_or_default(),
                record.offset,
                record.attempt,
                failure.unwrap_or("")
            );
        }
        match failure {
            // `Err` IS the language's failure idiom, and its reason travels verbatim.
            Some(reason) => Err(ProcessingError::new(reason)),
            None => Ok(Outcome::success()),
        }
    }

    /// The cyclic barrier at the heart of `hold-until-ceiling-full`: hold until this record is one
    /// of `max_concurrency` held at once, keep the full group still for [`CEILING_SETTLE`], then
    /// release it.
    ///
    /// A group also releases once every prescribed delivery has been observed, so a scenario whose
    /// record count is not a multiple of its ceiling cannot strand its last, short group.
    ///
    /// AWAITING IS HOW A RUST WORKER SAYS THE RECORD IS STILL OUT: the future has not resolved, so
    /// the record is unresolved, while the executor task is parked rather than the runtime blocked.
    ///
    /// Returns false if the group never filled inside the budget, which is this runner failing
    /// rather than the client being wrong about anything.
    async fn enter_ceiling_group(&self, budget: Duration) -> bool {
        let deadline = Instant::now() + budget;
        let mut generations = self.generation.subscribe();

        let my_generation = {
            let mut held = self.held.lock().expect("the ceiling lock is never poisoned");
            *held += 1;
            let releasing = *held >= self.max_concurrency || self.observed() >= self.expected;
            if releasing {
                None
            } else {
                Some(*self.generation.borrow())
            }
        };

        let Some(my_generation) = my_generation else {
            // THE SETTLE WINDOW, HELD OUTSIDE THE LOCK so a record the engine should not be
            // dispatching can still print its arrival if it turns up. A correct engine can dispatch
            // nothing here - the ceiling is full - so an extra line inside this window IS the
            // excess this scenario looks for.
            tokio::time::sleep(CEILING_SETTLE).await;
            let mut held = self.held.lock().expect("the ceiling lock is never poisoned");
            *held = 0;
            self.generation.send_modify(|generation| *generation += 1);
            return true;
        };

        loop {
            // The generation's VALUE, not merely a wakeup: a bump that landed between the unlock
            // and the first look is already visible here, so it cannot be missed.
            if *generations.borrow_and_update() != my_generation {
                return true;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            match tokio::time::timeout(remaining, generations.changed()).await {
                Ok(Ok(())) => {}
                // The sender lives in this tracker and outlives every executor, so a closed channel
                // cannot happen; the budget running out is the real case, and either way this group
                // did not fill.
                Ok(Err(_)) | Err(_) => return false,
            }
        }
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
