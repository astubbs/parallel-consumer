// Copyright (C) 2026 Antony Stubbs and contributors
//
// The session: one sidecar process, one gRPC stream, one dispatch queue, `executorCount` executors.

import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2
import Logging
import ParallelConsumerProxyProtocol

/// One session, from the handshake to the half-close.
///
/// ``connect(options:)`` opens it, ``poll(_:)`` starts processing, ``shutdown()`` ends it cleanly.
/// Letting the client go without shutting it down still stops the sidecar - closing the lifecycle
/// pipe is the parent-death signal - but it skips the drain, so the proxy recovers by rebalance
/// rather than by a clean commit.
///
/// DOES `poll` BLOCK? No: it starts the executors and returns, and the caller observes the session's
/// end through ``sessionEnd()``. The authoring guide settled that the SHAPE is each language's own
/// and the PROPERTY is not - the caller must be able to learn the session ended, and why, without
/// ending the client to find out. `try await client.sessionEnd()` is the Swift spelling of the JVM's
/// `CompletionStage<Void> sessionEnd()`: it returns when the session ended cleanly and throws the
/// cause when it did not. It is a method rather than `poll`'s return value because a session can die
/// before or without a poll - a client that only connected still has an end to observe - and it can
/// be awaited from as many places as the application likes.
public final class ParallelConsumerClient: @unchecked Sendable {
    private let options: ClientOptions
    private let logger: Logger
    private let sidecar: Sidecar
    private let queue = DispatchRecordQueue()

    private let outbound: AsyncStream<PCPClientMessage>
    private let outboundContinuation: AsyncStream<PCPClientMessage>.Continuation

    private let lock = NSLock()
    private var storedSession: Session?
    private var configuredSeen = false
    private var firstFailure: (any Error)?
    private var outboundClosed = false
    private var polled = false
    private var sessionOver = false
    private var sessionTask: Task<Void, any Error>?
    private var executorsTask: Task<Void, Never>?
    private var endpoint = ""

    private init(options: ClientOptions, sidecar: Sidecar) {
        self.options = options
        self.logger = options.logger
        self.sidecar = sidecar
        (outbound, outboundContinuation) = AsyncStream<PCPClientMessage>.makeStream(
            // Unbounded, deliberately. This is the client's OUTBOUND side - reports and nothing
            // else - and a bounded buffer here would make an executor's report wait on the network,
            // which is the same head-of-line block the inbound rule forbids. What bounds outstanding
            // work is the in-flight ceiling in DispatchRecordQueue, not this buffer.
            bufferingPolicy: .unbounded)
    }

    /// Spawns the sidecar, connects to it, and completes the fresh-session handshake.
    ///
    /// It returns once the proxy's effective configuration has arrived - only then is the session
    /// open.
    public static func connect(options: ClientOptions) async throws -> ParallelConsumerClient {
        try options.validate()

        let sidecar = try Sidecar(options: options)
        let port = try await sidecar.resolvePort(within: options.connectTimeout)
        options.logger.info("sidecar \(options.sidecarPath) announced port \(port)")

        let client = ParallelConsumerClient(options: options, sidecar: sidecar)
        client.endpoint = "127.0.0.1:\(port)"

        // Configure travels through the ordinary outbound stream, so there is exactly one writer of
        // this stream for the whole session and no special case at the handshake.
        //
        // NOTE what is NOT logged or put into an error anywhere on this path: the Configure message
        // itself. It carries kafka_properties, and its natural rendering would put credentials into
        // a log line.
        var configure = PCPClientMessage()
        configure.configure = options.makeConfigure()
        client.send(configure)

        client.startSession(port: port)
        try await client.awaitConfigured(within: options.connectTimeout)

        options.logger.info("session open on \(client.endpoint): \(client.session.describe())")
        return client
    }

    /// The effective configuration this session is running with - what the proxy replied, including
    /// the negotiated capability set. Assert on this, never on the options.
    ///
    /// It is only reachable on an open session, which ``connect(options:)`` is what establishes.
    public var session: Session {
        guard let stored = lock.withLock({ storedSession }) else {
            preconditionFailure("the session's configuration is only readable on an open session")
        }
        return stored
    }

    /// Starts processing with the user's function and RETURNS IMMEDIATELY. At most once per client.
    public func poll(_ processor: @escaping RecordProcessor) throws {
        let count = Int(session.executorCount)
        try lock.withLock {
            guard !polled else { throw ProxyClientError.alreadyPolling }
            polled = true
        }
        let task = Task { [self] in
            await withTaskGroup(of: Void.self) { group in
                for _ in 0..<count {
                    group.addTask { await self.executorLoop(processor) }
                }
            }
        }
        lock.withLock { executorsTask = task }
        logger.info("processing with \(count) executors")
    }

    /// Returns when the session's stream has ended - because the proxy completed it, because it
    /// failed, or because this client shut it down - and throws the cause if it was a fault.
    public func sessionEnd() async throws {
        guard let task = lock.withLock({ sessionTask }) else { return }
        try await task.value
    }

    /// The client-initiated shutdown: stop handing records out, let executing records finish and
    /// report, then half-close the stream and reap the sidecar.
    ///
    /// THE HALF-CLOSE IS THE SHUTDOWN SIGNAL - there is no shutdown-request message, because a
    /// client that has reported everything it ran has nothing left to say.
    ///
    /// - Throws: the session's FIRST fault, if it had one - including one the transport task
    ///   recorded while the application was doing something else.
    public func shutdown() async throws {
        queue.stopHandout()
        if let executors = lock.withLock({ executorsTask }) {
            await executors.value
        }
        lock.withLock { executorsTask = nil }

        // QUEUED RECORDS ARE DISCARDED, and that is the specification's own consequence rather than
        // a shortcut. The guide says to report them Released - but Released is gated by the
        // `shutdown` capability, which this client does not implement and therefore does not
        // declare, and sending an outcome outside the negotiated set is itself a violation. So they
        // are dropped and the proxy returns them to scheduling by the same path it uses for a lost
        // connection, attempt counts unchanged, because it never committed their offsets. The wave
        // that implements the drain sends Released here, under a
        // `session.negotiated(Capability.shutdown)` test.
        let dropped = queue.discardQueued()
        if dropped > 0 {
            let note =
                "dropped \(dropped) queued records at shutdown: this session did not negotiate "
                + "'shutdown', so Released is not on it and the proxy reclaims them"
            logger.warning("\(note)")
        }

        closeOutbound()  // half-close: no more sends, ever. Everything run has been reported.

        // Give the proxy its drain: it commits, completes the stream, and the session task ends on
        // its own. A stream that will not end is cancelled rather than waited on forever.
        if let task = lock.withLock({ sessionTask }) {
            let deadline = ContinuousClock.now + options.shutdownGrace
            while !lock.withLock({ sessionOver }) && ContinuousClock.now < deadline {
                try? await Task.sleep(for: .milliseconds(5))
            }
            if !lock.withLock({ sessionOver }) {
                let note =
                    "the proxy did not complete the stream within \(options.shutdownGrace) of the "
                    + "half-close"
                logger.warning("\(note)")
                task.cancel()
            }
            _ = await task.result
        }

        // Closing the lifecycle pipe is the reap. Never kill a sidecar with the stream still open -
        // that turns a clean drain into a reconnect-window recovery for the next group member.
        if let problem = await sidecar.stop(grace: options.shutdownGrace) {
            recordFailure(ProxyClientError.sidecar(problem))
        }

        if let failure = lock.withLock({ firstFailure }) {
            throw failure
        }
    }

    // MARK: - the session

    private func startSession(port: Int) {
        let task = Task { [self] in
            do {
                try await runSession(port: port)
            } catch {
                recordFailure(error)
            }
            // Nothing more will arrive, so executors waiting on the queue stop waiting, and an
            // outcome produced after this point has nowhere to go.
            queue.close()
            closeOutbound()
            lock.withLock { sessionOver = true }

            if let failure = lock.withLock({ firstFailure }) {
                // The diagnostic floor: even though sessionEnd() carries the cause, a session that
                // died must not be discoverable only by someone who thought to ask.
                logger.error("the session failed: \(String(describing: failure))")
                throw failure
            }
            logger.info("the session ended cleanly")
        }
        lock.withLock { sessionTask = task }
    }

    private func runSession(port: Int) async throws {
        // The ordinary loopback authority the proxy's allowlist expects; no TLS, no interceptors, no
        // load balancing - the deliberately narrow slice of gRPC the protocol permits, so that every
        // language's implementation suffices.
        let transport = try HTTP2ClientTransport.Posix(
            target: .ipv4(address: "127.0.0.1", port: port),
            transportSecurity: .plaintext)

        let outboundMessages = outbound
        try await withGRPCClient(transport: transport) { grpc in
            let service = PCPProxyService.Client(wrapping: grpc)
            // The producer drains the outbound stream and returns when it finishes - and ITS RETURN
            // IS THE HALF-CLOSE, which is why closeOutbound() is the whole of "stop talking".
            let request = StreamingClientRequest(of: PCPClientMessage.self) { writer in
                for await message in outboundMessages {
                    try await writer.write(message)
                }
            }
            try await service.session(request: request) { response in
                try await self.consume(response)
            }
        }
    }

    private func consume(_ response: StreamingClientResponse<PCPProxyMessage>) async throws {
        let contents: StreamingClientResponse<PCPProxyMessage>.Contents
        do {
            contents = try response.accepted.get()
        } catch {
            throw ProxyClientError.transport("the session stream was refused: \(error)")
        }
        for try await part in contents.bodyParts {
            guard case .message(let message) = part else { continue }
            // THE ADMIN ALWAYS READS THE STREAM. Everything below is non-suspending, so nothing this
            // client does can stop the reads: the stream also carries the control plane, and an
            // admin that stops reading to slow the proxy down head-of-line-blocks itself.
            try handle(message)
        }
    }

    private func handle(_ message: PCPProxyMessage) throws {
        switch message.message {
        case .configured(let configured):
            let alreadySeen = lock.withLock {
                let seen = configuredSeen
                configuredSeen = true
                return seen
            }
            if alreadySeen {
                throw ProxyClientError.protocolViolation("a second Configured arrived on one session")
            }
            let session = try Session.from(wire: configured)
            queue.configure(maxConcurrency: session.maxConcurrency)
            lock.withLock { storedSession = session }

        case .dispatch(let dispatch):
            guard lock.withLock({ configuredSeen }) else {
                throw ProxyClientError.protocolViolation("a Dispatch arrived before Configured")
            }
            // Queued in record order; hand-out is FIFO by arrival and, within a wave, by the wave's
            // own order. An overflow throws out of here, out of `consume`, and out of the response
            // handler - WHICH CANCELS THE CALL, because returning from that handler is the whole of
            // a gRPC client's vocabulary for "I am ending this": only a server sets a status, so
            // FAILED_PRECONDITION is not available and the counts travel in the local error instead.
            for record in dispatch.records {
                try queue.admit(record)
            }

        case .drop, .shutdown, .setExecutorCount, .none:
            // Every remaining proxy message is gated by a capability this client does not declare,
            // and the rule for an un-negotiated message is that the receiver never acts on it.
            // Recording it keeps the violation visible - it surfaces from sessionEnd() and
            // shutdown() - without failing an otherwise healthy stream.
            let kind = Self.kind(of: message)
            recordFailure(
                ProxyClientError.protocolViolation(
                    "the proxy sent \(kind) outside the negotiated capability set - ignored"))
            logger.warning("dropped an un-negotiated \(kind)")
        }
    }

    /// The NAME of a proxy message, never its content - a dispatch's records carry payload, and an
    /// error message is not the place for it.
    private static func kind(of message: PCPProxyMessage) -> String {
        switch message.message {
        case .configured: return "Configured"
        case .dispatch: return "Dispatch"
        case .drop: return "Drop"
        case .shutdown: return "Shutdown"
        case .setExecutorCount: return "SetExecutorCount"
        case .none: return "an empty message"
        }
    }

    // MARK: - the executors

    private func executorLoop(_ processor: @escaping RecordProcessor) async {
        while let record = await queue.take() {
            await run(processor, on: record)
        }
    }

    private func run(_ processor: RecordProcessor, on dispatched: PCPDispatchRecord) async {
        // Frees the record's slot against the in-flight ceiling when the scope ends, however it
        // ends. `defer` rather than a call at the bottom, because rule 2 requires the decrement
        // where an executor dying mid-record cannot skip it. Skip it once and the ceiling shrinks
        // permanently, one slot per crash, and the client eventually declares a protocol violation
        // against a correct proxy. It runs AFTER the send below, which is the other half of the
        // rule: a report frees the slot, not an executor picking the record up.
        defer { queue.settle() }

        var report = PCPReport()
        // THE TOKEN IS ECHOED VERBATIM - the message the proxy sent, never one rebuilt from parsed
        // parts. It is opaque: nothing here reads record_id or compares epochs.
        report.token = dispatched.token

        do {
            switch try await processor(Self.inbound(from: dispatched)) {
            case .success(let produce):
                var success = PCPReport.Success()
                success.produce = produce.map { outbound in
                    var wire = PCPProduceRecord()
                    if let topic = outbound.topic { wire.topic = topic }
                    if let key = outbound.key { wire.key = key }
                    if let value = outbound.value { wire.value = value }
                    return wire
                }
                report.success = success
            case .failure(let reason):
                var failure = PCPReport.Failure()
                failure.reason = reason
                report.failure = failure
            }
        } catch {
            // THE ONE PLACE a thrown error becomes a failure outcome. A worker that falls over must
            // produce a failure report, not tear down the session.
            var failure = PCPReport.Failure()
            failure.reason = String(describing: error)
            report.failure = failure
        }

        var message = PCPClientMessage()
        message.report = report
        send(message)
    }

    private static func inbound(from dispatched: PCPDispatchRecord) -> InboundRecord {
        let record = dispatched.record
        // Absence and emptiness are different in both byte fields: a null key is not an empty key,
        // and a tombstone is not an empty value.
        return InboundRecord(
            topic: record.topic,
            partition: record.partition,
            offset: record.offset,
            key: record.hasKey ? record.key : nil,
            value: record.hasValue ? record.value : nil,
            attempt: dispatched.attempt,
            hasFailedBefore: dispatched.hasLastFailureAt,
            lastFailureReason: dispatched.hasLastFailureReason ? dispatched.lastFailureReason : nil)
    }

    // MARK: - plumbing

    private func send(_ message: PCPClientMessage) {
        guard !lock.withLock({ outboundClosed }) else {
            // The session ended before this outcome could be reported. The engine's own paths return
            // the record to scheduling; there is nothing to report to.
            return
        }
        outboundContinuation.yield(message)
    }

    private func closeOutbound() {
        let already = lock.withLock {
            let was = outboundClosed
            outboundClosed = true
            return was
        }
        if !already { outboundContinuation.finish() }
    }

    private func recordFailure(_ problem: any Error) {
        lock.withLock {
            // The session's FIRST fault. Later ones are consequences of it far more often than they
            // are new information.
            if firstFailure == nil { firstFailure = problem }
        }
    }

    /// Waits for `Configured`, the session dying first, or the budget running out.
    ///
    /// The same bounded poll ``Sidecar/resolvePort(within:)`` uses, and for the same reason: it runs
    /// once per session against a budget measured in seconds, and it cannot leak a continuation when
    /// the deadline wins.
    private func awaitConfigured(within budget: Duration) async throws {
        let deadline = ContinuousClock.now + budget
        while true {
            if lock.withLock({ storedSession }) != nil { return }
            if lock.withLock({ sessionOver }) {
                let failure: any Error =
                    lock.withLock { firstFailure }
                    ?? ProxyClientError.protocolViolation(
                        "the stream ended before Configured arrived")
                await sidecar.stop(grace: options.shutdownGrace)
                throw failure
            }
            if ContinuousClock.now >= deadline {
                lock.withLock { sessionTask }?.cancel()
                await sidecar.stop(grace: options.shutdownGrace)
                throw ProxyClientError.timedOut(
                    "awaiting Configured from the sidecar on \(endpoint)")
            }
            try? await Task.sleep(for: .milliseconds(2))
        }
    }
}
