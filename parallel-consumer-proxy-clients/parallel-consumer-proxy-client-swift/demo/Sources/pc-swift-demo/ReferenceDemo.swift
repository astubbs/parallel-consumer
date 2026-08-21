// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE SWIFT DEMO: the same records through Swift's own Kafka client, and through Swift over the
// sidecar. The contract it keeps is parallel-consumer-proxy/demo/README.md, and the artifact it is
// transcribed from is the Java seed at
// parallel-consumer-proxy-clients/parallel-consumer-proxy-client-java/demo (plan unit U35, KTD40).
//
// TWO ARMS IS THE WHOLE CONTRACT HERE. Java carries four more - pc-core, java-direct,
// java-grpc-uds and java-raw-grpc - because one JVM can hold every engine at once and each pair
// changes exactly one term. Swift's only Kafka client is its own, so there is nothing to compare a
// wrapper or a raw wire against, and mirroring those arms would produce rows that mean nothing.
//
// THE SIDECAR ARM GOES THROUGH THE CLIENT LIBRARY IN THIS MODULE, never the protocol by hand. The
// seed demo was once only a hand-written arm: it proved the ENGINE worked and said nothing about
// the client library, which is the artifact users actually touch. That is why this file imports
// ParallelConsumerProxyClient and nothing from the protocol module.
//
// THE SIMULATED WORK IS `Task.sleep`, NOT A BLOCKING SLEEP, and the contract now agrees: its
// predicate is whether the CLIENT is thread-per-record, and this one is not - `poll` runs its
// executors as Swift concurrency tasks on the cooperative pool. A blocking sleep there would cap
// in-flight work at the machine's core count while the table appeared to report the engine.
// demo/README.md carries the mechanism in full.
//
// NIOCore is imported for one reason: `KafkaConsumerMessage.key` is a `ByteBuffer`, and the AK core
// arm has to read it to count the keys it saw.

import Foundation
import Kafka
import Logging
import NIOCore
import ParallelConsumerProxyClient
import ServiceLifecycle

@main
struct ReferenceDemo {

    /// No arm may take longer than this before the demo calls it stalled rather than slow.
    static let armBudget = Duration.seconds(600)

    /// THE FIRST THING THE DEMO PRINTS, and it is contract rather than decoration: a reader who
    /// starts this and is met with a configuration line has been told nothing about what they are
    /// looking at. Every language prints this same banner, differing only in its own name.
    static let banner = """

        ================================================================
          PARALLEL CONSUMER  -  Swift demo
          The same records, twice: one at a time, then all at once.
        ================================================================
        """

    /// Where the sidecar binary lives inside the demo's image. The client library refuses a
    /// relative or PATH-resolved sidecar - which binary receives the Kafka credentials is not a
    /// decision for the working directory - so this is absolute, and overridable for a developer
    /// running the binary outside the image.
    static let defaultSidecarPath = "/opt/parallel-consumer/sidecar"

    private let options: DemoOptions
    private let broker: DemoBroker
    private let topic: String
    private let sidecarPath: String
    private let kafkaLogger: Logger
    private let libraryLogger: Logger

    static func main() async {
        // swift-log's own default handler writes to STANDARD OUTPUT at `info`, and this program's
        // stdout is its two tables. Everything diagnostic goes to stderr instead, so a caller can
        // pipe the tables somewhere without librdkafka's chatter arriving in the middle of them.
        LoggingSystem.bootstrap { label in StreamLogHandler.standardError(label: label) }

        let argv = Array(CommandLine.arguments.dropFirst())
        if DemoOptions.isHelpRequested(argv) {
            Note.say(DemoOptions.usage)
            return
        }

        // BEFORE THE PARSE, not after it, so the banner is the first thing printed on every path
        // that is a run - including one that dies in `init` for want of a sidecar. `--help` is
        // exempt above: usage text asked for by name is not a run.
        Note.say(banner)

        let options: DemoOptions
        do {
            options = try DemoOptions.parse(
                argv, environment: ProcessInfo.processInfo.environment)
        } catch {
            // A misspelled flag must not be reported as a result for settings nobody asked for.
            Note.fail("\(error)")
            Note.say(DemoOptions.usage)
            exit(2)
        }

        do {
            try await ReferenceDemo(options: options).run()
        } catch {
            Note.fail("\(error)")
            exit(1)
        }
    }

    init(options: DemoOptions) throws {
        self.options = options
        self.broker = try DemoBroker.resolve(options.bootstrap)
        self.topic =
            options.topic
            ?? "pc-demo-swift-\(Int(Date().timeIntervalSince1970))-\(UInt32.random(in: 0..<100_000))"
        self.sidecarPath =
            ProcessInfo.processInfo.environment["PC_DEMO_SIDECAR"] ?? Self.defaultSidecarPath
        guard sidecarPath.hasPrefix("/"), FileManager.default.isExecutableFile(atPath: sidecarPath)
        else {
            throw DemoError(
                "no executable sidecar at '\(sidecarPath)'. The demo's image puts one there; "
                    + "outside the image, point PC_DEMO_SIDECAR at an absolute path that launches "
                    + "bz.stub.parallelconsumer.proxy.Main.")
        }

        var kafka = Logger(label: "pc-swift-demo.kafka")
        // librdkafka is talkative at `info` and every line of it would land between the tables.
        kafka.logLevel = .notice
        self.kafkaLogger = kafka

        var library = Logger(label: "pc-swift-demo.proxy-client")
        library.logLevel = .notice
        self.libraryLogger = library
    }

    private func run() async throws {
        // THE FINGERPRINT COMES FIRST, and it never carries the bootstrap address.
        Note.say("\nEffective configuration:\n  \(options.describe())\n  topic = \(topic)")

        try await broker.seed(topic: topic, from: 0, to: options.records, logger: kafkaLogger)

        var small: [ArmResult] = []
        small.append(try await akCore(target: options.records))
        small.append(try await swiftGrpc(target: options.records))
        let baseline = small.first { $0.arm == ArmTable.akCore }
        Note.say(
            ArmTable.render(
                title:
                    "Small replay - every arm over the same \(options.records) records (the comparison)",
                results: small, baseline: baseline, acrossReplays: false))

        guard options.bigReplayWanted else {
            Note.say("\nBig replay skipped (--replay-factor \(options.replayFactor)).")
            return
        }

        let total = options.bigReplayRecords
        try await broker.seed(topic: topic, from: options.records, to: total, logger: kafkaLogger)

        // AK CORE IS EXCLUDED HERE because it does not go parallel: it would need
        // total * delayMs milliseconds to finish a backlog the sidecar arm clears in seconds, and
        // waiting that long to learn nothing new is not worth the wall clock.
        let big = [try await swiftGrpc(target: total)]
        Note.say(
            ArmTable.render(
                title: "Big replay - \(total) records, parallel arms only (AK core is serial and "
                    + "would take \(total * options.delayMs / 1000)s+)",
                results: big, baseline: baseline, acrossReplays: true))
    }

    // MARK: - the arms

    /// **AK core** - Swift's own Kafka client, one record at a time.
    ///
    /// `swift-kafka-client` delivers messages as an `AsyncSequence`, so "serial" here is a loop
    /// that does not return to the sequence until the record's work is done. There is no engine, no
    /// client library and no sidecar on this path - it is the denominator of every ratio in both
    /// tables.
    private func akCore(target: Int) async throws -> ArmResult {
        Note.say("\n=== \(ArmTable.akCore) starting over \(target) records ===")
        let configuration = broker.consumerConfiguration(
            groupID: groupID("ak-core"), topic: topic)
        let consumer = try KafkaConsumer(configuration: configuration, logger: kafkaLogger)
        let serviceGroup = ServiceGroup(services: [consumer], logger: kafkaLogger)

        let counter = RecordCounter(target: target)
        let delay = Duration.milliseconds(options.delayMs)
        // The clock starts AFTER the consumer is built, because this arm is the denominator of
        // every ratio in both tables and no other arm charges itself for client construction.
        let startedAt = ContinuousClock.now

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask { try await serviceGroup.run() }
            group.addTask {
                for try await message in consumer.messages {
                    try await Task.sleep(for: delay, tolerance: .zero)
                    // The key is read from the record this arm actually received, exactly as the
                    // sidecar arm reads it from its own. Counting is what the demo OBSERVES, not
                    // what the broker knows - swift-kafka-client has no admin or metadata API to
                    // ask with, and a broker-side answer would describe the topic rather than the
                    // run, so it could not show that this arm saw the backlog spread.
                    if counter.count(key: message.key.map { String(buffer: $0) }) >= target {
                        break
                    }
                }
                await serviceGroup.triggerGracefulShutdown()
            }
            group.addTask {
                // The budget, as a task rather than a deadline inside the loop: the loop is
                // suspended on the message sequence, so a backlog shorter than the target would
                // wait there for ever with no output. Reaching the target ends this task at once.
                if await counter.awaitTarget(within: Self.armBudget) { return }
                await serviceGroup.triggerGracefulShutdown()
            }
            try await group.waitForAll()
        }

        return try finished(
            ArmTable.akCore, startedAt: startedAt, counter: counter, target: target)
    }

    /// **swift-grpc** - the application as a foreign client, through the client library in this
    /// module.
    ///
    /// On this path the application does no Kafka I/O at all: the library spawns the sidecar,
    /// records arrive over a socket, this function runs on them, and outcomes go back. The sidecar
    /// owns the consumer, the producer, the group membership and the offsets. That the same process
    /// also seeds the topic and runs the AK core arm with an ordinary Kafka client is a statement
    /// about the PATH, not about the process - a genuinely foreign application carries no Kafka
    /// client library at all.
    private func swiftGrpc(target: Int) async throws -> ArmResult {
        Note.say("\n=== \(ArmTable.swiftGrpc) starting over \(target) records ===")

        var clientOptions = ClientOptions(sidecarPath: sidecarPath)
        clientOptions.topics = [topic]
        clientOptions.maxConcurrency = Int32(options.maxConcurrency)
        // Set EXPLICITLY. Leaving it out is not a harmless omission: the field is optional,
        // unspecified means "take parallel-consumer-core's default", and that default is KEY - so
        // this arm would run key-ordered against an unordered comparator and nothing would say so.
        clientOptions.ordering = .unordered
        clientOptions.kafkaProperties = broker.proxyProperties(groupID: groupID("swift-grpc"))
        clientOptions.instanceTag = "pc-swift-demo"
        clientOptions.logger = libraryLogger

        let client = try await ParallelConsumerClient.connect(options: clientOptions)

        let counter = RecordCounter(target: target)
        let delay = Duration.milliseconds(options.delayMs)
        let startedAt = ContinuousClock.now
        try client.poll { record in
            // THE SAME WAIT THE AK CORE ARM RUNS, so the two arms differ by transport and nothing
            // else. See this file's header for why it is `Task.sleep` in both rather than a
            // blocking one.
            try await Task.sleep(for: delay, tolerance: .zero)
            // Decoded the same lossy way the AK core arm decodes its `ByteBuffer`, so a key that
            // is not valid UTF-8 counts as the same key on both arms rather than vanishing from
            // one of them. `keyText` is the library's stricter reading and would not.
            counter.count(key: record.key.map { String(decoding: $0, as: UTF8.self) })
            return .success
        }
        let reached = await counter.awaitTarget(within: Self.armBudget)

        // Shut the session down BEFORE deciding the arm's verdict: shutdown carries the session's
        // first fault, and "stalled at 0 of 20" with the reason attached is a usable message where
        // the count alone is not. The clock has already stopped - the counter recorded the instant
        // the last record was processed - so the drain is not charged to the arm.
        var shutdownFailure: (any Error)?
        do {
            try await client.shutdown()
        } catch {
            shutdownFailure = error
        }

        guard reached else {
            let cause = shutdownFailure.map { "; the session reported: \($0)" } ?? ""
            throw DemoError(
                "\(ArmTable.swiftGrpc) stalled at \(counter.value) of \(target)\(cause)")
        }
        if let shutdownFailure {
            Note.fail("the session reported a fault at shutdown: \(shutdownFailure)")
        }

        return try finished(
            ArmTable.swiftGrpc, startedAt: startedAt, counter: counter, target: target)
    }

    // MARK: - plumbing

    private func finished(
        _ arm: String, startedAt: ContinuousClock.Instant, counter: RecordCounter, target: Int
    ) throws -> ArmResult {
        let processed = counter.value
        // Reaching the target is not the only way an arm's wait ends: a failed or completed session
        // ends it too. Without this check a broken run prints a plausible row at a plausible rate
        // and exits 0, which is the worst thing a demo whose shape ten languages copy can do.
        guard processed >= target, let finishedAt = counter.finishedAt else {
            throw DemoError("\(arm) ended early at \(processed) of \(target)")
        }
        let elapsed = startedAt.duration(to: finishedAt)
        let result = ArmResult(
            arm: arm, elapsed: elapsed, processed: processed, uniqueKeys: counter.uniqueKeys)
        Note.say(
            "=== \(arm) finished: \(processed) records over \(result.uniqueKeys) keys in "
                + "\(Int((result.seconds * 1000).rounded()))ms ===")
        return result
    }

    /// A fresh group per arm per replay, so every arm reads the same records from the beginning.
    private func groupID(_ arm: String) -> String {
        "pc-demo-swift-\(arm)-\(Int(Date().timeIntervalSince1970))-\(UInt32.random(in: 0..<100_000))"
    }
}

/// Counts processed records, the distinct keys among them, and remembers WHEN the last one landed.
///
/// THE KEY SET IS WHAT THE ARM SAW, and there is no other way for this demo to get it: Swift's
/// Kafka client exposes no admin client and no metadata API, so nothing here can ask the broker
/// what keys the topic holds. Counting what arrives is the better answer anyway - it demonstrates
/// that the arm really spread over the backlog rather than replaying one key.
///
/// A locked class rather than an actor, for the reason this module's client library gives for its
/// own dispatch queue: everything here is called from the user function, which runs on the client's
/// executors, and an actor would make each of these calls a suspension point in the middle of the
/// work being measured.
final class RecordCounter: @unchecked Sendable {
    private let lock = NSLock()
    private let target: Int
    private var processed = 0
    private var keys: Set<String> = []
    private var finished: ContinuousClock.Instant?

    /// The target is fixed at construction rather than supplied by whoever waits, because the
    /// finish instant is stamped by the task that processes the last record. A counter that only
    /// learned its target once someone started waiting could miss the stamp on a fast arm, and the
    /// arm would then report "ended early" for a run that completed.
    init(target: Int) {
        self.target = target
    }

    /// Counts one record and its key, and returns the record's 1-based ordinal.
    ///
    /// - Parameter key: the key as this arm received it, or `nil` for a null key. A null key is not
    ///   a key and is counted as none: Kafka distinguishes an absent key from an empty one, and
    ///   folding the two together would report a distinct-key figure that no other language's demo
    ///   could be expected to match.
    @discardableResult
    func count(key: String?) -> Int {
        lock.withLock {
            processed += 1
            if let key { keys.insert(key) }
            // Stamped HERE, by the task that processed the last record, rather than by whoever
            // notices afterwards: a poller's granularity would otherwise be charged to the arm.
            if processed >= target && finished == nil { finished = .now }
            return processed
        }
    }

    var value: Int { lock.withLock { processed } }
    var uniqueKeys: Int { lock.withLock { keys.count } }
    var finishedAt: ContinuousClock.Instant? { lock.withLock { finished } }

    /// Resolves when `target` records have been counted, or when the budget runs out.
    ///
    /// - Returns: whether the target was reached.
    func awaitTarget(within budget: Duration) async -> Bool {
        let deadline = ContinuousClock.now + budget
        while ContinuousClock.now < deadline {
            if value >= target { return true }
            try? await Task.sleep(for: .milliseconds(1), tolerance: .zero)
        }
        return value >= target
    }
}

/// Where the demo's own prose goes. The tables are stdout; everything else is stderr.
enum Note {
    static func say(_ message: String) {
        print(message)
        // The demo is usually read through `docker compose up`, where stdout is a pipe and
        // therefore fully buffered - so an unflushed table arrives only when the process exits.
        fflush(nil)
    }

    static func fail(_ message: String) {
        FileHandle.standardError.write(Data("pc-swift-demo: \(message)\n".utf8))
    }
}
