// Copyright (C) 2026 Antony Stubbs and contributors
//
// What one arm achieved, and the two tables the contract asks for.
//
// THERE IS NO LATENCY FIELD HERE, and its absence is the contract rather than an omission. The
// backlog is pre-produced, so the workload is closed-loop: a per-record timing would be flattered
// by however far an arm had fallen behind, and would read as a number about the engine when it is
// really a number about the queue. Throughput is the only honest figure this shape can produce.
//
// THE TWO FIGURES THAT ARE NOT ABOUT SPEED - records and keys - ARE THE ONLY ONES COMPARABLE
// ACROSS LANGUAGES. Elapsed and msg/s are properties of the machine that ran them; `records` and
// `keys` are properties of the WORK, so every language processing the same backlog must report the
// same two numbers. That is what lets bin/ci-demo-conformance.sh compare eleven demos at all, and
// it is why they are contract rather than decoration: throughput alone cannot show the work
// happened, and a short arm is a failed arm rather than a fast one.

import Foundation

/// How long one arm took, over how many records, and across how many distinct keys.
struct ArmResult: Sendable {
    let arm: String
    let elapsed: Duration
    let processed: Int
    /// Distinct record keys this arm OBSERVED as it processed - counted from the records that
    /// reached the user function, never asked of the broker. Swift's Kafka client has no admin or
    /// metadata API to ask with, and a figure taken from the broker would describe the topic rather
    /// than the run.
    let uniqueKeys: Int

    var seconds: Double {
        let (whole, attoseconds) = elapsed.components
        return Double(whole) + Double(attoseconds) / 1e18
    }

    /// Throughput, which is the only SPEED figure this demo reports.
    var ratePerSecond: Double {
        seconds > 0 ? Double(processed) / seconds : 0
    }
}

enum ArmTable {

    /// The arm every language has: that language's own Kafka client, one record at a time.
    ///
    /// Always spelled "AK core" - bare "core" reads as `parallel-consumer-core` (CONCEPTS.md) - and
    /// always with the client NAMED beside it. "AK core" is a category, and the answer differs in
    /// every language; a reader cannot judge the comparison without knowing that this row was
    /// produced by `swift-kafka-client` rather than by some other binding.
    static let akCore = "AK core (swift-kafka-client)"

    /// This language over the sidecar: the client library in this module, spawning and driving a
    /// proxy the application never installs. "(this client)" is what it drives - the library beside
    /// this demo, not a hand-written wire.
    static let swiftGrpc = "swift-grpc (this client)"

    /// Renders one replay's table: same columns, same order, same widths as every other language.
    ///
    /// THE COLUMNS ARE PADDED BY HAND rather than by `String(format:)`, and that is a bug avoided
    /// rather than a style. Swift's `%s` takes a C string, so passing a `String` to it compiles and
    /// prints rubbish; and a `NumberFormatter` would put a French host's separators into a table
    /// the Java seed writes with `Locale.ROOT`. Neither failure is visible until someone reads the
    /// output on a machine that is not this one.
    ///
    /// - Parameter acrossReplays: true for the big replay, whose ratio column is measured against
    ///   the SMALL replay's AK core arm because the big replay has no serial arm of its own. It is
    ///   marked in the header and footnoted, rather than quietly compared.
    static func render(
        title: String, results: [ArmResult], baseline: ArmResult?, acrossReplays: Bool
    ) -> String {
        var table = "\n\n\(title)\n"
        table += row(
            "arm", "records", "keys", "elapsed", "msg/s",
            acrossReplays ? "vs AK core*" : "vs AK core")
        for result in results {
            let ratio: String
            if let baseline, baseline.ratePerSecond > 0 {
                ratio = decimal(result.ratePerSecond / baseline.ratePerSecond) + "x"
            } else {
                ratio = "-"
            }
            table += row(
                result.arm, grouped(result.processed), grouped(result.uniqueKeys),
                decimal(result.seconds) + "s", grouped(Int(result.ratePerSecond)), ratio)
        }
        if acrossReplays {
            table +=
                "\n  * against the SMALL replay's AK core arm. Across replays, so not "
                + "like-for-like.\n"
        }
        return table
    }

    /// One line of the table: the arm left-aligned, then five right-aligned columns.
    ///
    /// The arm column is wide enough for the client's name because the name is contract - see
    /// `akCore` above. Column WIDTH is not contract in either direction: the conformance harness
    /// discards padding and keeps identity and order, precisely so a language with a long client
    /// name is not in permanent violation of an alignment rule.
    private static func row(
        _ arm: String, _ records: String, _ keys: String, _ elapsed: String, _ rate: String,
        _ ratio: String
    ) -> String {
        "  " + padRight(arm, 30) + " " + padLeft(records, 9) + " " + padLeft(keys, 7) + " "
            + padLeft(elapsed, 10) + " " + padLeft(rate, 12) + " " + padLeft(ratio, 12) + "\n"
    }

    private static func padRight(_ value: String, _ width: Int) -> String {
        value.count >= width ? value : value + String(repeating: " ", count: width - value.count)
    }

    private static func padLeft(_ value: String, _ width: Int) -> String {
        value.count >= width ? value : String(repeating: " ", count: width - value.count) + value
    }

    /// One decimal place, with a full stop, on every machine.
    private static func decimal(_ value: Double) -> String {
        let scaled = (value * 10).rounded()
        let whole = Int(scaled / 10)
        let fraction = abs(Int(scaled) % 10)
        return "\(whole).\(fraction)"
    }

    /// Thousands separators, hand-rolled for the locale reason above.
    static func grouped(_ value: Int) -> String {
        let negative = value < 0
        var digits = Array(String(value.magnitude))
        var out: [Character] = []
        while digits.count > 3 {
            out.insert(contentsOf: [","] + digits.suffix(3), at: 0)
            digits.removeLast(3)
        }
        out.insert(contentsOf: digits, at: 0)
        return (negative ? "-" : "") + String(out)
    }
}
