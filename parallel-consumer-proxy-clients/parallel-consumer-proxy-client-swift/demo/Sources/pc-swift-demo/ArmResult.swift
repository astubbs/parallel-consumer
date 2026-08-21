// Copyright (C) 2026 Antony Stubbs and contributors
//
// What one arm achieved, and the two tables the contract asks for.
//
// THERE IS NO LATENCY FIELD HERE, and its absence is the contract rather than an omission. The
// backlog is pre-produced, so the workload is closed-loop: a per-record timing would be flattered
// by however far an arm had fallen behind, and would read as a number about the engine when it is
// really a number about the queue. Throughput is the only honest figure this shape can produce.

import Foundation

/// How long one arm took, and over how many records.
struct ArmResult: Sendable {
    let arm: String
    let elapsed: Duration
    let processed: Int

    var seconds: Double {
        let (whole, attoseconds) = elapsed.components
        return Double(whole) + Double(attoseconds) / 1e18
    }

    /// Throughput, which is the only figure this demo reports.
    var ratePerSecond: Double {
        seconds > 0 ? Double(processed) / seconds : 0
    }
}

enum ArmTable {

    /// The arm every language has: that language's own Kafka client, one record at a time. Always
    /// spelled "AK core" - bare "core" reads as `parallel-consumer-core` (CONCEPTS.md).
    static let akCore = "AK core"

    /// This language over the sidecar: the client library in this module, spawning and driving a
    /// proxy the application never installs.
    static let swiftGrpc = "swift-grpc"

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
            "arm", "elapsed", "msg/s", acrossReplays ? "vs AK core*" : "vs AK core")
        for result in results {
            let ratio: String
            if let baseline, baseline.ratePerSecond > 0 {
                ratio = decimal(result.ratePerSecond / baseline.ratePerSecond) + "x"
            } else {
                ratio = "-"
            }
            table += row(
                result.arm, decimal(result.seconds) + "s", grouped(Int(result.ratePerSecond)), ratio)
        }
        if acrossReplays {
            table +=
                "\n  * against the SMALL replay's AK core arm. Across replays, so not "
                + "like-for-like.\n"
        }
        return table
    }

    /// One line of the table: the arm left-aligned in 14, then three right-aligned columns.
    private static func row(_ arm: String, _ elapsed: String, _ rate: String, _ ratio: String)
        -> String
    {
        "  " + padRight(arm, 14) + " " + padLeft(elapsed, 10) + " " + padLeft(rate, 14) + " "
            + padLeft(ratio, 14) + "\n"
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
