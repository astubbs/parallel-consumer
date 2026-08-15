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
// Its contract - the five flags, the three exit codes, the stdout observation line, the four
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

private struct Arguments {
    let scenario: String
    let behaviour: Behaviour
    let sidecar: String
    let expectDispatches: Int
    let timeoutSeconds: Int
}

/// Counts deliveries and outcomes, and prints the observation line.
///
/// It holds no per-record state - only counts - because the client library holds none either, and
/// this runner must not become the place where a client's missing bookkeeping is quietly supplied.
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
            // Printed at the moment of delivery, before the behaviour acts on it. `reason` comes
            // last because it is worker-supplied text that may contain spaces.
            print(
                "dispatch key=\(record.keyText ?? "") offset=\(record.offset) "
                    + "attempt=\(record.attempt) reason=\(record.lastFailureReason ?? "")")
            // The suite reads this through a PIPE, where stdout is fully buffered rather than line
            // buffered, so an unflushed transcript arrives only if the process happens to exit
            // cleanly - and report-nothing deliberately does not.
            //
            // fflush(nil) rather than fflush(stdout): the Swift 6 language mode refuses a reference
            // to Glibc's `stdout`, which is a mutable global and therefore not concurrency-safe.
            // Passing nil flushes every open output stream, which is what is wanted anyway.
            fflush(nil)
            return observedCount
        }
    }

    func complete() {
        lock.withLock { completedCount += 1 }
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
        let holdExpired = Flag()

        var options = ClientOptions(sidecarPath: arguments.sidecar)
        // THE SCENARIO NAME IS ALSO THE TOPIC NAME.
        options.topics = [arguments.scenario]
        // Enough capacity for every dispatch the scenario prescribes, so a scenario that holds a
        // record cannot deadlock on an executor count smaller than its own shape.
        options.maxConcurrency = Int32(arguments.expectDispatches)
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
        do {
            try client.poll { record in
                let ordinal = tracker.observe(record)
                switch behaviour {
                case .succeed:
                    tracker.complete()
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
                    tracker.complete()
                    return record.attempt == 1
                        ? .failure(reason: prescribedFailureReason) : .success

                case .holdFirstUntilSecond:
                    // Hold the FIRST record until a SECOND is dispatched. Whether one arrives at
                    // all, and which key it carries, is the whole of what the scenario is asking -
                    // and it is the Java suite that decides what the answer means.
                    if ordinal == 1 {
                        let sawSecond = await tracker.awaitSecond(within: budget)
                        if !sawSecond {
                            holdExpired.raise()
                            tracker.complete()
                            return .failure(reason: "no second delivery arrived within the budget")
                        }
                    }
                    tracker.complete()
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
        if !finished {
            note(
                "scenario '\(arguments.scenario)' behaviour '\(behaviour.rawValue)' did not complete "
                    + "within \(arguments.timeoutSeconds)s - observed \(tracker.observed) of "
                    + "\(arguments.expectDispatches), completed \(tracker.completed)")
            exit(ExitStatus.behaviourFailed)
        }
        if holdExpired.raised {
            note("the held record never saw a second delivery")
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

    /// The five flags, spelled identically in every language - including the British `--behaviour`.
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
        let timeout = try required("--timeout-seconds")

        guard let behaviour = Behaviour(rawValue: behaviourToken) else {
            throw UsageError("unknown behaviour '\(behaviourToken)'")
        }
        guard sidecar.hasPrefix("/") else {
            throw UsageError("--sidecar must be absolute, got '\(sidecar)'")
        }
        guard let expectDispatches = Int(expect), let timeoutSeconds = Int(timeout) else {
            throw UsageError("--expect-dispatches and --timeout-seconds must be positive integers")
        }
        guard expectDispatches >= 1, timeoutSeconds >= 1 else {
            throw UsageError("--expect-dispatches and --timeout-seconds must be at least 1")
        }

        return Arguments(
            scenario: scenario, behaviour: behaviour, sidecar: sidecar,
            expectDispatches: expectDispatches, timeoutSeconds: timeoutSeconds)
    }

    private static func note(_ message: String) {
        FileHandle.standardError.write(Data("conformance-runner: \(message)\n".utf8))
    }
}

private struct UsageError: Error, CustomStringConvertible {
    let description: String
    init(_ description: String) { self.description = description }
}

/// A one-way flag the `@Sendable` processor closure can raise and `main` can read.
private final class Flag: @unchecked Sendable {
    private let lock = NSLock()
    private var value = false
    func raise() { lock.withLock { value = true } }
    var raised: Bool { lock.withLock { value } }
}
