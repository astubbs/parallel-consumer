// Copyright (C) 2026 Antony Stubbs and contributors
//
// Swift's half of the shared cross-language conformance suite (astubbs#242, confluentinc#154).
//
// IT ASSERTS NOTHING, DELIBERATELY. The suite that knows what correct looks like - offset frontiers,
// ordering, redelivery, attempt counts - is the Java module
// parallel-consumer-proxy-clients/parallel-consumer-proxy-conformance, and it keeps owning that
// knowledge for every language. This binary's whole job is to DO WHAT THE SCENARIO SAYS and then
// exit; if it were free to decide what "correct" means, eleven languages would each decide it
// slightly differently and the agreement between them would prove nothing.
//
// Its contract - the six flags, the three exit codes, the two stdout observation lines, the five
// behaviour tokens, the fixed literals - is documented once, in that module's README.md, and is
// identical in every language.
//
// THIS DOES NOT REPLACE THE MODULE'S OWN TESTS. The shared suite proves every client behaves
// identically on the protocol; Tests/ catches what is invisible from outside the process - the
// ceiling counting the wrong thing, a credential in a rendering, a port line missed among log lines.
// Both layers are load-bearing.

import Foundation
import Logging
import ParallelConsumerProxyClient

/// Exit statuses ARE the verdict channel. There is no results file and no report message: a scenario
/// passed if this process exited 0 and the Java suite's own assertions about engine state held.
private enum ExitStatus {
    static let ok: Int32 = 0
    static let behaviourFailed: Int32 = 1
    static let usage: Int32 = 2
}

private enum Behaviour: String {
    case succeed
    case reportNothing = "report-nothing"
    case failThenSucceed = "fail-then-succeed"
    case holdFirstUntilSecond = "hold-first-until-second"
    case holdUntilCeilingFull = "hold-until-ceiling-full"
}

/// The exact text a `fail-then-succeed` run reports. The Java suite asserts the redelivery carries it
/// back VERBATIM, so it is a fixed literal of the contract in every language, never composed here.
private let prescribedFailureReason = "conformance-prescribed-failure"

// Fixed session tunables, contract rather than this runner's judgement: they exist only so scenarios
// converge at unit-test speed against the engine's production defaults (a 5s commit interval, a 1s
// retry delay). Every language sets the same two values.
private let commitInterval = Duration.milliseconds(100)
private let retryDelay = Duration.milliseconds(50)

/// How long a `report-nothing` run keeps its session OPEN after its last observation.
///
/// IT IS WHAT MAKES THE NEGATIVE CONTROL A CONTROL. Without it the runner exits the instant the
/// record arrives, and a sabotaged runner that DID report success has its report killed in flight by
/// the process exit - so the suite sees an unadvanced offset either way and the scenario passes for a
/// broken client. Measured in the Go wave, not reasoned about.
private let reportNothingHold = Duration.seconds(3)

/// How long `hold-until-ceiling-full` keeps a FULL group held before releasing it.
///
/// IT IS WHAT TURNS "the ceiling was never exceeded" FROM A RACE INTO A MEASUREMENT. Release the
/// group the instant it fills and a client that declared a larger ceiling still passes - its extra
/// records arrive a few milliseconds later, by which time the outstanding count has already fallen
/// back. Holding the full ceiling still means the extra dispatch arrives INSIDE the window and
/// prints its line while every other record is unresolved. A correct engine cannot dispatch anything
/// during the window at all, so the wait costs a conforming client nothing but time.
private let ceilingSettle = Duration.milliseconds(250)

private struct Arguments {
    let scenario: String
    let behaviour: Behaviour
    let sidecar: String
    let expectDispatches: Int
    let maxConcurrency: Int
    let timeoutSeconds: Int
}

/// Counts deliveries and outcomes, and prints the two observation lines.
///
/// It holds no per-record state - only counts - because the client library holds none either, and
/// this runner must not become the place where a client's missing bookkeeping is quietly supplied.
///
/// BOTH LINES ARE PRINTED UNDER THE ONE LOCK THAT COUNTS THEM. The suite reads overlap purely from
/// the ORDER of the lines - a dispatch opens a record's unresolved window and its settled line
/// closes it - so the order they reach stdout in has to be the order the events happened in, with
/// several executors printing at once.
private final class Tracker: @unchecked Sendable {
    private let lock = NSLock()
    private var observedCount = 0
    private var completedCount = 0

    /// Prints the delivery and returns its 1-based ordinal in arrival order. The lock covers the
    /// increment AND the print together, so the transcript's order is the order the ordinals were
    /// handed out in.
    func observe(_ record: InboundRecord) -> Int {
        lock.withLock {
            observedCount += 1
            // Printed at the moment of delivery, before the behaviour acts on it. `reason` is the
            // history the record ARRIVED with, empty on a first delivery.
            emit("dispatch", record, reason: record.lastFailureReason ?? "")
            return observedCount
        }
    }

    /// Prints the settled line and counts the record as done - the moment the prescribed behaviour
    /// has DECIDED this record's outcome, which is when it stops being unresolved.
    ///
    /// `reason` is the failure this runner is REPORTING, empty for a success, and never the reason
    /// the record arrived with. `report-nothing` never calls this: by prescription its record is
    /// never resolved, and the absence of the line is the observation.
    func settle(_ record: InboundRecord, reason: String) {
        lock.withLock {
            completedCount += 1
            emit("settled", record, reason: reason)
        }
    }

    /// One observation line. THE LOCK IS ALREADY HELD - it is what serializes the transcript.
    ///
    /// `reason` comes last because it is worker-supplied text that may contain spaces.
    private func emit(_ kind: String, _ record: InboundRecord, reason: String) {
        print(
            "\(kind) key=\(record.keyText ?? "") offset=\(record.offset) "
                + "attempt=\(record.attempt) reason=\(reason)")
        // The suite reads this through a PIPE, where stdout is fully buffered rather than line
        // buffered, so an unflushed transcript arrives only if the process happens to exit
        // cleanly - and report-nothing deliberately does not.
        //
        // fflush(nil) rather than fflush(stdout): the Swift 6 language mode refuses a reference
        // to Glibc's `stdout`, which is a mutable global and therefore not concurrency-safe.
        // Passing nil flushes every open output stream, which is what is wanted anyway.
        fflush(nil)
    }

    var observed: Int { lock.withLock { observedCount } }
    var completed: Int { lock.withLock { completedCount } }

    /// Resolves once a second delivery has been observed - the instrument the ordering scenario is.
    func awaitSecond(within budget: Duration) async -> Bool {
        await poll(within: budget) { self.observed >= 2 }
    }

    /// Whether the prescription finished inside the budget. `report-nothing` completes at
    /// OBSERVATION, because by prescription its records are never reported and so can never complete.
    func awaitPrescribed(atObservation: Bool, expected: Int, within budget: Duration) async -> Bool {
        await poll(within: budget) {
            (atObservation ? self.observed : self.completed) >= expected
        }
    }

    private func poll(within budget: Duration, until satisfied: @Sendable () -> Bool) async -> Bool {
        let deadline = ContinuousClock.now + budget
        while ContinuousClock.now < deadline {
            if satisfied() { return true }
            try? await Task.sleep(for: .milliseconds(5))
        }
        return satisfied()
    }
}

/// The cyclic barrier of width `--max-concurrency` at the heart of `hold-until-ceiling-full`: hold
/// every delivery until that many are held AT ONCE, keep the full group still for ``ceilingSettle``,
/// then release the whole group and start the next one.
///
/// IT IS AN ACTOR WITH A CONTINUATION PER WAITER RATHER THAN A LOCK WITH A CONDITION VARIABLE,
/// because the records arrive on the client's executors, which are Swift concurrency tasks sharing
/// the cooperative thread pool. A waiter that blocked its thread would take one of those threads out
/// of the pool for the whole hold, and a barrier of the pool's own width would then deadlock waiting
/// for records that no thread is left to deliver. Suspending on a continuation frees the thread
/// while leaving the record's function un-returned, which is the whole property the scenario
/// measures - so this file blocks nothing and sleeps only with `Task.sleep`.
///
/// A group also releases once every prescribed delivery has been observed, so a scenario whose
/// record count is not a multiple of its ceiling cannot strand its last, short group.
private actor CeilingBarrier {
    private let width: Int
    private let expectDispatches: Int
    private let observedCount: @Sendable () -> Int

    private var held = 0
    private var generation = 0
    private var nextWaiterID = 0

    /// The waiters, per generation: `generation -> waiter id -> its continuation`. A release resumes
    /// exactly the generation it closes, which is this actor's spelling of "wait until the
    /// generation is no longer mine" - a waiter cannot be woken by the group it does not belong to.
    private var waiters: [Int: [Int: CheckedContinuation<Bool, Never>]] = [:]

    private enum Entry {
        case release
        case waited(Bool)
    }

    init(width: Int, expectDispatches: Int, observedCount: @escaping @Sendable () -> Int) {
        self.width = width
        self.expectDispatches = expectDispatches
        self.observedCount = observedCount
    }

    /// Holds this record until its group is full and settled.
    ///
    /// NONISOLATED SO THE SETTLE SLEEP IS OUTSIDE THE BARRIER'S STATE, exactly as the contract's
    /// pseudocode holds it outside the lock: a record the engine should not be dispatching still has
    /// to be able to enter and print its arrival during the window, and that arrival is the whole
    /// thing the scenario looks for.
    ///
    /// - Returns: false if the group never filled inside the budget, which is this runner failing
    ///   rather than the client being wrong about anything.
    nonisolated func hold(within budget: Duration) async -> Bool {
        switch await enter(within: budget) {
        case .waited(let released):
            return released
        case .release:
            try? await Task.sleep(for: ceilingSettle)
            await release()
            return true
        }
    }

    /// Joins the current group, and either becomes its releaser or suspends until it is released.
    private func enter(within budget: Duration) async -> Entry {
        held += 1
        if held >= width || observedCount() >= expectDispatches {
            return .release
        }

        let myGeneration = generation
        let id = nextWaiterID
        nextWaiterID += 1
        // The budget, armed as a task rather than as a deadline on the wait itself: a continuation
        // has no timeout of its own, and racing one against a sleep risks a waiter left suspended
        // forever. Expiry goes through the actor, so the continuation is resumed exactly once
        // whichever arrives first - and a cancelled sleep just finds the waiter already gone.
        let expiry = Task.detached { [self] in
            try? await Task.sleep(for: budget)
            await expire(id, in: myGeneration)
        }
        let released = await withCheckedContinuation { (continuation: CheckedContinuation<Bool, Never>) in
            waiters[myGeneration, default: [:]][id] = continuation
        }
        expiry.cancel()
        return .waited(released)
    }

    /// Closes the full group: the settle window has passed, so every waiter succeeds and the next
    /// group starts empty.
    private func release() {
        let closing = generation
        held = 0
        generation += 1
        for continuation in (waiters.removeValue(forKey: closing) ?? [:]).values {
            continuation.resume(returning: true)
        }
    }

    /// The budget ran out with the group still short. Waking the waiter with false is what turns
    /// that into an exit 1 rather than a run that hangs until the harness kills it.
    private func expire(_ id: Int, in generation: Int) {
        guard let continuation = waiters[generation]?.removeValue(forKey: id) else { return }
        continuation.resume(returning: false)
    }
}

@main
struct ConformanceRunner {
    static func main() async {
        let arguments: Arguments
        do {
            arguments = try parse(Array(CommandLine.arguments.dropFirst()))
        } catch {
            note("\(error)")
            exit(ExitStatus.usage)
        }

        let budget = Duration.seconds(arguments.timeoutSeconds)
        let tracker = Tracker()
        let failure = PrescriptionFailure()
        let ceiling = CeilingBarrier(
            width: arguments.maxConcurrency, expectDispatches: arguments.expectDispatches,
            observedCount: { tracker.observed })

        var options = ClientOptions(sidecarPath: arguments.sidecar)
        // THE SCENARIO NAME IS ALSO THE TOPIC NAME.
        options.topics = [arguments.scenario]
        // The ceiling is the SCENARIO's to choose and this runner never derives one: a ceiling
        // computed from the record count is one no scenario can reach, and a ceiling nothing reaches
        // cannot be tested.
        options.maxConcurrency = Int32(arguments.maxConcurrency)
        options.commitInterval = commitInterval
        options.defaultMessageRetryDelay = retryDelay
        // The mock lane builds mock Kafka clients and reads no properties. Real credentials never
        // belong in a conformance test.
        options.kafkaProperties = [:]
        options.instanceTag = "conformance-runner-swift"
        // Diagnostics go to stderr, which the suite captures and attaches to any failure message.
        options.logger = Logger(label: "conformance-runner") { label in
            StreamLogHandler.standardError(label: label)
        }

        let client: ParallelConsumerClient
        do {
            client = try await ParallelConsumerClient.connect(options: options)
        } catch {
            note("opening the session: \(error)")
            exit(ExitStatus.behaviourFailed)
        }

        let behaviour = arguments.behaviour
        let ceilingWidth = arguments.maxConcurrency
        do {
            try client.poll { record in
                let ordinal = tracker.observe(record)
                switch behaviour {
                case .succeed:
                    tracker.settle(record, reason: "")
                    return .success

                case .reportNothing:
                    // PRESCRIBED: never report. Suspending here for longer than the whole run is how
                    // a Swift worker says "this record's function has not returned"; the process
                    // exits with the record still in flight, which is a worker that vanished
                    // mid-record.
                    while true {
                        try await Task.sleep(for: .seconds(3600))
                    }

                case .failThenSucceed:
                    // The reason is the contract's fixed literal on the first attempt, never a
                    // message this runner composes: the suite asserts the redelivery carries it
                    // back verbatim.
                    let reason = record.attempt == 1 ? prescribedFailureReason : ""
                    tracker.settle(record, reason: reason)
                    return reason.isEmpty ? .success : .failure(reason: reason)

                case .holdFirstUntilSecond:
                    // Hold the FIRST record until a SECOND is dispatched. Whether one arrives at
                    // all, and which key it carries, is the whole of what the scenario is asking -
                    // and it is the Java suite that decides what the answer means.
                    if ordinal == 1 {
                        let sawSecond = await tracker.awaitSecond(within: budget)
                        if !sawSecond {
                            let reason = "no second delivery arrived within the budget"
                            failure.raise(reason)
                            tracker.settle(record, reason: reason)
                            return .failure(reason: reason)
                        }
                    }
                    tracker.settle(record, reason: "")
                    return .success

                case .holdUntilCeilingFull:
                    // Hold EVERY record until `--max-concurrency` of them are held at once, keep
                    // the full group held for the settle window, then succeed all of them. Not
                    // returning from this function is how a Swift worker says the record is still
                    // unresolved, which is exactly what the ceiling bounds.
                    let filled = await ceiling.hold(within: budget)
                    if !filled {
                        let reason = "the ceiling group of \(ceilingWidth) never filled"
                        failure.raise(reason)
                        tracker.settle(record, reason: reason)
                        return .failure(reason: reason)
                    }
                    tracker.settle(record, reason: "")
                    return .success
                }
            }
        } catch {
            note("starting the processor: \(error)")
            exit(ExitStatus.behaviourFailed)
        }

        let reportNothing = behaviour == .reportNothing
        let finished = await tracker.awaitPrescribed(
            atObservation: reportNothing, expected: arguments.expectDispatches, within: budget)
        // The raised reason first, because it names WHAT could not be carried out; the count below
        // only says that something did not finish in time, which is the same failure seen later.
        if let reason = failure.reason {
            note("the prescribed behaviour could not be carried out: \(reason)")
            exit(ExitStatus.behaviourFailed)
        }
        if !finished {
            note(
                "scenario '\(arguments.scenario)' behaviour '\(behaviour.rawValue)' did not complete "
                    + "within \(arguments.timeoutSeconds)s - observed \(tracker.observed) of "
                    + "\(arguments.expectDispatches), completed \(tracker.completed)")
            exit(ExitStatus.behaviourFailed)
        }

        if reportNothing {
            // Hold the session open, reporting nothing, so the suite is watching a LIVE client
            // rather than the wreckage of one - see `reportNothingHold`.
            try? await Task.sleep(for: reportNothingHold)
            // PRESCRIBED: the record is never reported and the session is abandoned rather than
            // drained. `exit` rather than an orderly return, because the executor holding the record
            // never returns and any orderly path would wait for it. Flushed first, or the transcript
            // the suite reads dies with the process; the sidecar is reaped anyway, because exiting
            // closes its lifecycle pipe.
            fflush(nil)
            exit(ExitStatus.ok)
        }

        do {
            try await client.shutdown()
        } catch {
            note("closing the session: \(error)")
            exit(ExitStatus.behaviourFailed)
        }
        exit(ExitStatus.ok)
    }

    /// The six flags, spelled identically in every language - including the British `--behaviour`:
    /// `--scenario`, `--behaviour`, `--sidecar`, `--expect-dispatches`, `--max-concurrency` and
    /// `--timeout-seconds`. All are required, and anything else is a usage error.
    private static func parse(_ argv: [String]) throws -> Arguments {
        var values: [String: String] = [:]
        var index = 0
        while index < argv.count {
            let flag = argv[index]
            guard flag.hasPrefix("--") else {
                throw UsageError("expected --flag value pairs, got \(flag)")
            }
            guard index + 1 < argv.count else { throw UsageError("\(flag) takes a value") }
            values[flag] = argv[index + 1]
            index += 2
        }

        func required(_ name: String) throws -> String {
            guard let value = values[name], !value.isEmpty else {
                throw UsageError("\(name) is required")
            }
            return value
        }

        let scenario = try required("--scenario")
        let behaviourToken = try required("--behaviour")
        let sidecar = try required("--sidecar")
        let expect = try required("--expect-dispatches")
        let concurrency = try required("--max-concurrency")
        let timeout = try required("--timeout-seconds")

        guard let behaviour = Behaviour(rawValue: behaviourToken) else {
            throw UsageError("unknown behaviour '\(behaviourToken)'")
        }
        guard sidecar.hasPrefix("/") else {
            throw UsageError("--sidecar must be absolute, got '\(sidecar)'")
        }
        guard let expectDispatches = Int(expect), let maxConcurrency = Int(concurrency),
            let timeoutSeconds = Int(timeout)
        else {
            throw UsageError(
                "--expect-dispatches, --max-concurrency and --timeout-seconds must be positive "
                    + "integers")
        }
        guard expectDispatches >= 1, maxConcurrency >= 1, timeoutSeconds >= 1 else {
            throw UsageError(
                "--expect-dispatches, --max-concurrency and --timeout-seconds must be at least 1")
        }

        return Arguments(
            scenario: scenario, behaviour: behaviour, sidecar: sidecar,
            expectDispatches: expectDispatches, maxConcurrency: maxConcurrency,
            timeoutSeconds: timeoutSeconds)
    }

    private static func note(_ message: String) {
        FileHandle.standardError.write(Data("conformance-runner: \(message)\n".utf8))
    }
}

private struct UsageError: Error, CustomStringConvertible {
    let description: String
    init(_ description: String) { self.description = description }
}

/// A one-way flag the `@Sendable` processor closure can raise and `main` can read, carrying the
/// reason the prescription could not be carried out. The FIRST reason wins: whichever record gave
/// up first is the one that explains the run.
private final class PrescriptionFailure: @unchecked Sendable {
    private let lock = NSLock()
    private var value: String?
    func raise(_ reason: String) {
        lock.withLock {
            if value == nil { value = reason }
        }
    }
    var reason: String? { lock.withLock { value } }
}
