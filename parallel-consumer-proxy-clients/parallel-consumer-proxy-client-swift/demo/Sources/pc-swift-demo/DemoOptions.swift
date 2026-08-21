// Copyright (C) 2026 Antony Stubbs and contributors
//
// The demo's dials. THIS SURFACE IS NOT SWIFT'S TO CHOOSE: the flags, their defaults, their
// environment variables and the precedence between them are the contract every language's demo
// keeps (parallel-consumer-proxy/demo/README.md), transcribed from the Java seed's DemoOptions.
// A reader who has run one demo has run them all, and that only holds if this file is a mirror.
//
// R39 does not govern a demo. R39 constrains how configuration reaches the PROXY; a demo is an
// application, so `--records` is not a violation of it. The Java seed carries the same note,
// because without it someone reads the flags as breaking the plan's own rule and deletes them.

import Foundation

/// Anything the demo refuses to start on. Distinct from a run that failed: this is bad input.
struct DemoError: Error, CustomStringConvertible {
    let description: String
    init(_ description: String) { self.description = description }
}

/// The whole of what the demo is configured with.
struct DemoOptions: Sendable {

    /// Prefix for every environment variable this demo reads, so a reader can grep one string.
    static let environmentPrefix = "PC_DEMO_"

    var records = 2000
    var delayMs = 2
    var maxConcurrency = 100
    var partitions = 10
    var replayFactor = 20
    var bootstrap: String?
    var topic: String?

    /// The records the big replay consumes in total, including the small replay's own.
    var bigReplayRecords: Int { records * max(1, replayFactor) }

    /// True when the big replay is worth running at all; a factor of 1 or less skips it.
    var bigReplayWanted: Bool { replayFactor > 1 }

    /// Whether the caller asked for the usage text rather than a run.
    ///
    /// Answered here and not only in `run.sh`, because the script is not the only way in:
    /// `docker compose run demo --help` reaches this parser directly, and answering that with
    /// "unknown option" would be a poor first impression of a demo ten languages copy.
    static func isHelpRequested(_ argv: [String]) -> Bool {
        argv.contains("-h") || argv.contains("--help")
    }

    /// Parses the command line over the environment over the defaults - the ordinary convention,
    /// and the one the contract names: a container passes configuration by environment while a
    /// person at a terminal passes flags, and each must be able to override the other's layer.
    ///
    /// - Throws: ``DemoError`` on an unknown flag, a missing value, or a value that is not a
    ///   number in range. A demo that silently ignored a misspelled flag would report numbers for
    ///   settings nobody asked for, which is the one thing a demo must never do.
    static func parse(_ argv: [String], environment: [String: String]) throws -> DemoOptions {
        var options = DemoOptions()
        try options.applyEnvironment(environment)

        var index = 0
        while index < argv.count {
            let flag = argv[index]
            func value() throws -> String {
                index += 1
                guard index < argv.count else { throw DemoError("\(flag) needs a value") }
                return argv[index]
            }
            switch flag {
            case "--records":
                options.records = try positive(flag, value())
            case "--delay-ms":
                options.delayMs = try nonNegative(flag, value())
            case "--concurrency":
                options.maxConcurrency = try positive(flag, value())
            case "--partitions":
                options.partitions = try positive(flag, value())
            case "--replay-factor":
                // 1 or less skips the big replay, so this one is allowed to be zero
                options.replayFactor = try nonNegative(flag, value())
            case "--bootstrap":
                options.bootstrap = try value()
            case "--topic":
                options.topic = try value()
            default:
                throw DemoError("unknown option: \(flag)")
            }
            index += 1
        }

        try options.validate()
        return options
    }

    private mutating func applyEnvironment(_ environment: [String: String]) throws {
        func lookup(_ suffix: String) -> String? {
            // An EMPTY variable is an absent one, deliberately: compose passes every variable it
            // declares whether or not the caller set it, so `PC_DEMO_RECORDS: ${PC_DEMO_RECORDS:-}`
            // arrives as "" on an ordinary run and must not be read as a number.
            guard let raw = environment[DemoOptions.environmentPrefix + suffix] else { return nil }
            let trimmed = raw.trimmingCharacters(in: .whitespaces)
            return trimmed.isEmpty ? nil : trimmed
        }
        if let raw = lookup("RECORDS") { records = try Self.positive("PC_DEMO_RECORDS", raw) }
        if let raw = lookup("DELAY_MS") { delayMs = try Self.nonNegative("PC_DEMO_DELAY_MS", raw) }
        if let raw = lookup("CONCURRENCY") {
            maxConcurrency = try Self.positive("PC_DEMO_CONCURRENCY", raw)
        }
        if let raw = lookup("PARTITIONS") { partitions = try Self.positive("PC_DEMO_PARTITIONS", raw) }
        if let raw = lookup("REPLAY_FACTOR") {
            replayFactor = try Self.nonNegative("PC_DEMO_REPLAY_FACTOR", raw)
        }
        if let raw = lookup("BOOTSTRAP") { bootstrap = raw }
        if let raw = lookup("TOPIC") { topic = raw }
    }

    private func validate() throws {
        // Checked rather than trusted, and in Swift the consequence of not checking is worse than
        // Java's: `records * replayFactor` TRAPS on overflow rather than wrapping, so an unchecked
        // multiplication would abort the process with no message a reader could act on.
        let (_, overflowed) = records.multipliedReportingOverflow(by: max(1, replayFactor))
        if overflowed || bigReplayRecords > Int(Int32.max) {
            throw DemoError(
                "--records times --replay-factor is more records than the demo can count; "
                    + "lower one of them")
        }
    }

    private static func positive(_ name: String, _ raw: String) throws -> Int {
        let parsed = try number(name, raw)
        guard parsed >= 1 else { throw DemoError("\(name) must be at least 1, got \(parsed)") }
        return parsed
    }

    private static func nonNegative(_ name: String, _ raw: String) throws -> Int {
        let parsed = try number(name, raw)
        guard parsed >= 0 else { throw DemoError("\(name) must not be negative, got \(parsed)") }
        return parsed
    }

    private static func number(_ name: String, _ raw: String) throws -> Int {
        guard let parsed = Int(raw.trimmingCharacters(in: .whitespaces)) else {
            throw DemoError("\(name) needs a whole number, got '\(raw)'")
        }
        return parsed
    }

    /// The effective configuration, printed before the run.
    ///
    /// A number without its settings is not reproducible, so this is contract rather than a
    /// debugging aid. **The bootstrap address is deliberately absent**: own-cluster mode puts a
    /// user's real broker there, and the credential-hygiene rule that binds the proxy binds a demo
    /// too - nothing logged, nothing echoed.
    func describe() -> String {
        "records = \(records)"
            + "\n  delayMs = \(delayMs)"
            + "\n  maxConcurrency = \(maxConcurrency)"
            + "\n  partitions = \(partitions)"
            + "\n  replayFactor = \(replayFactor)"
    }

    /// The usage text, in the same order and with the same defaults as every other language's.
    static let usage = """

        usage: demo/run.sh [options]

          --records N        records in the comparison replay   (default 2000)
          --delay-ms N       simulated work per record, ms      (default 2)
          --concurrency N    max in-flight records              (default 100)
          --partitions N     partitions on the demo topic       (default 10)
          --replay-factor N  big replay = records x N; 1 skips  (default 20)
          --bootstrap ADDR   an existing broker
          --topic NAME       an existing topic; omit to name one

        Every flag has an environment variable: --delay-ms is PC_DEMO_DELAY_MS.
        Flags beat the environment beats the defaults.
        """
}
