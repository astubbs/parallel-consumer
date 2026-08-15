// Copyright (C) 2026 Antony Stubbs and contributors
//
// The sidecar child process and the lifecycle pipe that keeps it alive.

import Foundation

/// The proxy child process.
///
/// THE STDIN PIPE IS THE PARENT-DEATH SIGNAL: this process holds the write end and never writes to
/// it, so EOF on the child's stdin is proof the parent is gone. That is why `Process` is given an
/// `executableURL` and launched DIRECTLY, never through a shell - a shell wrapper would hold the
/// write end open and leak a JVM that still holds group membership.
final class Sidecar: @unchecked Sendable {
    /// The lifecycle channel's whole vocabulary: the proxy prints `port: <n>` and connects nothing
    /// else to it.
    private static let portLinePrefix = "port: "

    /// How many of the sidecar's own output lines are kept for the diagnostic. Bounded on purpose -
    /// see ``recentOutput()``.
    private static let tailLines = 40

    private let path: String
    private let process: Process
    private let lifecyclePipe: Pipe  // the child's stdin write end: held open, never written to
    private let stdoutPipe: Pipe

    private let lock = NSLock()
    private var tail: [String] = []
    private var announcedPort: Int?
    private var stdoutEnded = false
    private var stopped = false

    /// The loopback port the proxy announced.
    private(set) var port: Int = 0

    /// Spawns the sidecar. The caller then awaits ``resolvePort(within:)``.
    ///
    /// - Throws: ``ProxyClientError/sidecar(_:)`` if it cannot be started.
    init(options: ClientOptions) throws {
        path = options.sidecarPath
        process = Process()
        process.executableURL = URL(fileURLWithPath: options.sidecarPath)
        process.arguments = options.sidecarArguments

        lifecyclePipe = Pipe()
        stdoutPipe = Pipe()
        process.standardInput = lifecyclePipe
        process.standardOutput = stdoutPipe
        switch options.sidecarStandardError {
        case .inherit:
            // NOT a pipe, and that is the point: an inherited descriptor has no buffer this process
            // must drain, so the sidecar's diagnostics reach the application by default without any
            // risk of the child blocking on a full pipe nobody reads.
            process.standardError = FileHandle.standardError
        case .discard:
            process.standardError = FileHandle.nullDevice
        }

        do {
            try process.run()
        } catch {
            throw ProxyClientError.sidecar("\(path) could not be started: \(error)")
        }

        // THE DRAIN RUNS FOR THE CHILD'S WHOLE LIFE, not just until the port line. A pipe nobody
        // reads fills up - 64 KiB on Linux, which a JVM at INFO reaches in seconds under load - and
        // the sidecar then stops mid-log-line and never returns, which reaches the application as a
        // stalled consumer with no error and nothing in any log.
        //
        // A dedicated Thread rather than a Task: the read is a blocking syscall, and blocking a
        // cooperative-pool thread for the child's entire lifetime is how a Swift concurrency program
        // starves itself of executors.
        let drain = Thread { [weak self] in self?.drainStandardOutput() }
        drain.name = "pc-proxy-sidecar-stdout"
        drain.start()
    }

    deinit {
        // Best effort, because `deinit` cannot report anything and cannot await: closing the
        // lifecycle pipe is the parent-death signal, so a client that was never shut down still
        // stops its sidecar. `ParallelConsumerClient.shutdown()` is the supported route, and the
        // only one that drains.
        try? lifecyclePipe.fileHandleForWriting.close()
    }

    /// Waits for the sidecar's port line and records it.
    ///
    /// A bounded poll rather than a continuation woken by the drain thread. The wait happens exactly
    /// once per session against a budget measured in seconds, so a 2 ms granularity is invisible;
    /// what it buys is that there is no continuation to leak if the deadline wins the race, which is
    /// the failure mode a hand-rolled timeout usually ships with.
    ///
    /// - Throws: ``ProxyClientError/sidecar(_:)`` if stdout ends without a port line, or
    ///   ``ProxyClientError/timedOut(_:)`` if it never arrives.
    func resolvePort(within budget: Duration) async throws -> Int {
        let deadline = ContinuousClock.now + budget
        while true {
            let (announced, ended) = lock.withLock { (announcedPort, stdoutEnded) }
            if let announced {
                port = announced
                return announced
            }
            if ended {
                let output = recentOutput()
                await stop(grace: .seconds(5))
                throw ProxyClientError.sidecar(
                    "stdout ended before a 'port: <n>' line. Its last output was:\n\(output)")
            }
            if ContinuousClock.now >= deadline {
                let output = recentOutput()
                await stop(grace: .seconds(5))
                throw ProxyClientError.timedOut(
                    "waiting \(budget) for the sidecar's port line. Its last output was:\n\(output)")
            }
            try? await Task.sleep(for: .milliseconds(2))
        }
    }

    /// Closes the lifecycle pipe and waits up to `grace` for the child to exit.
    ///
    /// CLOSING STDIN IS THE REAP: it is the parent-death signal the proxy watches, and it is also the
    /// only thing that ends the conformance harness, which serves until stdin EOF and does not exit
    /// after a clean drain. Killing is the backstop for a child that honours neither.
    ///
    /// - Returns: `nil` when the child exited on its own; otherwise what went wrong, for the caller
    ///   to report.
    @discardableResult
    func stop(grace: Duration) async -> String? {
        let alreadyStopped = lock.withLock {
            let was = stopped
            stopped = true
            return was
        }
        if alreadyStopped { return nil }

        try? lifecyclePipe.fileHandleForWriting.close()  // the parent-death signal, and the reap

        let deadline = ContinuousClock.now + grace
        while process.isRunning && ContinuousClock.now < deadline {
            try? await Task.sleep(for: .milliseconds(5))
        }
        var problem: String?
        if process.isRunning {
            process.terminate()
            // SIGTERM first, then wait out the same grace again before SIGKILL: a JVM asked to stop
            // politely still has shutdown hooks to run, and killing it here is what turns a clean
            // exit into an unclean one.
            let killDeadline = ContinuousClock.now + grace
            while process.isRunning && ContinuousClock.now < killDeadline {
                try? await Task.sleep(for: .milliseconds(5))
            }
            if process.isRunning {
                kill(process.processIdentifier, SIGKILL)
            }
            problem =
                "did not exit within \(grace) of its lifecycle pipe closing, so it was signalled"
        }
        return problem
    }

    /// The last lines the sidecar wrote, most recent last.
    ///
    /// Bounded, because an unbounded buffer of a chatty child's output is a leak of its own; kept at
    /// all, because the last lines before a crash are the whole explanation and a spawn that fails
    /// without them costs an afternoon.
    func recentOutput() -> String {
        lock.withLock { tail.map { "    \($0)" }.joined(separator: "\n") }
    }

    /// The port from a lifecycle line, if this line is one.
    ///
    /// The specification's contract is that the port is stdout's FIRST line. The conformance harness
    /// diverges - it logs before it - and the authoring guide says a test absorbs that rather than
    /// asserting the position, so this SCANS rather than reading exactly one line. Scanning satisfies
    /// both.
    static func parsePortLine(_ line: String) -> Int? {
        guard line.hasPrefix(portLinePrefix) else { return nil }
        let digits = line.dropFirst(portLinePrefix.count).trimmingCharacters(in: .whitespaces)
        guard !digits.isEmpty, digits.allSatisfy(\.isNumber), let parsed = Int(digits) else {
            return nil
        }
        guard parsed > 0, parsed <= 65535 else { return nil }
        return parsed
    }

    // MARK: - private

    private func drainStandardOutput() {
        let handle = stdoutPipe.fileHandleForReading
        var buffer = Data()
        while true {
            let chunk = handle.availableData
            if chunk.isEmpty { break }  // EOF: the child is gone and its write end is closed
            buffer.append(chunk)
            while let newline = buffer.firstIndex(of: UInt8(ascii: "\n")) {
                let line = String(decoding: buffer[buffer.startIndex..<newline], as: UTF8.self)
                record(line: line)
                buffer = buffer[buffer.index(after: newline)...]
            }
        }
        if !buffer.isEmpty {
            record(line: String(decoding: buffer, as: UTF8.self))
        }
        lock.withLock { stdoutEnded = true }
    }

    private func record(line: String) {
        lock.withLock {
            tail.append(line)
            if tail.count > Self.tailLines { tail.removeFirst(tail.count - Self.tailLines) }
            if announcedPort == nil, let announced = Self.parsePortLine(line) {
                announcedPort = announced
            }
        }
    }
}
