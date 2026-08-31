// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE DISPATCH QUEUE (authoring guide section 3, KTD39). The gap between the proxy's in-flight
// ceiling and the client's executor count is a queue inside the client library, and every rule about
// it is an ordering-or-liveness decision specified once for all eleven languages.
//
// It is its own type for one reason: rule 2 requires the unresolved count to live in ONE place, as a
// number the queue owns - `DispatchQueue.inFlight` in TypeScript, `_outstanding` in Python,
// `Shared::unresolved` in Rust, `@unresolved` in Ruby. A count spread across the transport task and
// the executors is a count two of them can disagree about, and it is also a count no test can reach
// without a live session.
//
// THE RULE THIS FILE EXISTS TO GET RIGHT: max_concurrency bounds the records this client has been
// DISPATCHED AND NOT YET REPORTED - queued PLUS executing - and never the deque's own length.
// Handing a record to an executor moves it; it does not free its slot. Three of the first five
// clients in this fan-out bounded the queue instead, and a client that bounds the queue cannot
// detect overflow AT ALL: a record leaving the queue always makes room, so the condition never
// arises. Tests/ carries the guide's own worked example as the control.
//
// Only a verdict frees a slot, and there are exactly four: a report is sent; a record is reported
// `Released` at shutdown on a session that negotiated `shutdown`; a record is discarded; a worker's
// death is reported by `WorkerDied`. This client implements the first and the third - the other two
// are gated by capabilities it does not declare.
//
// IT IS A LOCKED CLASS RATHER THAN AN ACTOR, AND THAT IS THE `defer` REQUIREMENT SPEAKING. Rule 2
// puts the decrement where an executor dying mid-record cannot skip it - the language's
// finally/ensure/defer - and Swift's `defer` body is synchronous, so it cannot `await`. An actor's
// `settle()` would be `async` at every call site and therefore unusable from the one place the rule
// names. Everything that must be callable from `defer` is synchronous here; only `take()`, which
// genuinely waits, is `async`.
//
// The name is `DispatchRecordQueue` and not `DispatchQueue` because the latter is libdispatch's, and
// a client library that shadows a standard type in its own module makes every reader stop and check.

import Foundation
import ParallelConsumerProxyProtocol
import SwiftProtobuf

final class DispatchRecordQueue: @unchecked Sendable {
    private let lock = NSLock()
    private var queued: [PCPDispatchRecord] = []
    private var unresolvedCount: Int32 = 0
    private var ceiling: Int32 = 0
    private var closed = false
    private var stopped = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    /// Sets the ceiling from `Configured`.
    ///
    /// Until this is called the queue admits nothing, which is what makes a `Dispatch` before
    /// `Configured` impossible to absorb by accident.
    func configure(maxConcurrency: Int32) {
        lock.withLock { ceiling = maxConcurrency }
    }

    /// Admits one dispatched record.
    ///
    /// Admission is decided by the UNRESOLVED count, not by whether the deque has room. It never
    /// waits: the admin task also carries the control plane, so an admission that applied
    /// backpressure by suspending would head-of-line-block the session's own control messages.
    ///
    /// - Throws: ``ProxyClientError/protocolViolation(_:)`` when the proxy has exceeded the ceiling
    ///   it declared itself. Overflow is a protocol violation, not a load condition, so records are
    ///   never dropped to make room and the queue never grows unbounded.
    func admit(_ record: PCPDispatchRecord) throws {
        try lock.withLock {
            guard unresolvedCount < ceiling else {
                // The STATIC renderer, taking the ceiling it was handed. NSLock is not recursive, so
                // calling the instance method from inside this critical section deadlocks the
                // transport task against itself - which is what the overflow unit tests caught, and
                // is why they exercise the message rather than only the throw.
                throw ProxyClientError.protocolViolation(
                    Self.overflowMessage(
                        alreadyUnresolved: unresolvedCount, ceiling: ceiling, token: record.token))
            }
            unresolvedCount += 1
            queued.append(record)
        }
        wakeWaiters()
    }

    /// Takes the next record, FIFO - by arrival, and within one `Dispatch` by record order.
    ///
    /// - Returns: `nil` when hand-out has stopped, or the queue is closed and empty.
    func take() async -> PCPDispatchRecord? {
        while true {
            if let outcome = tryTake() { return outcome.record }
            // Registered under the lock and re-checked inside the continuation, because the state
            // can change between releasing the lock above and installing the continuation.
            await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                lock.lock()
                if stopped || closed || !queued.isEmpty {
                    lock.unlock()
                    continuation.resume()
                    return
                }
                waiters.append(continuation)
                lock.unlock()
            }
        }
    }

    /// One record reached a verdict: it no longer counts against the ceiling.
    func settle() {
        lock.withLock {
            if unresolvedCount > 0 { unresolvedCount -= 1 }
        }
    }

    /// Stops hand-out.
    ///
    /// Records already out with executors are not interrupted - it is only the hand-out that stops.
    func stopHandout() {
        lock.withLock { stopped = true }
        wakeWaiters()
    }

    /// Wakes every waiter; nothing further will arrive.
    func close() {
        lock.withLock { closed = true }
        wakeWaiters()
    }

    /// Drops the queued records and settles each, for a shutdown on a session that did NOT negotiate
    /// `shutdown` - where reporting them `Released` would itself be the un-negotiated-message
    /// violation. Their offsets were never committed, so the proxy returns them to scheduling.
    ///
    /// - Returns: how many were dropped.
    func discardQueued() -> Int {
        let dropped: Int = lock.withLock {
            let count = queued.count
            queued.removeAll()
            unresolvedCount = max(0, unresolvedCount - Int32(count))
            closed = true
            return count
        }
        wakeWaiters()
        return dropped
    }

    /// Records dispatched and not yet resolved: queued PLUS executing.
    var unresolved: Int32 {
        lock.withLock { unresolvedCount }
    }

    /// How many records are waiting for an executor.
    ///
    /// A DIAGNOSTIC, never an admission input - see the file comment.
    var depth: Int {
        lock.withLock { queued.count }
    }

    /// The counted protocol violation, exposed so a test can assert the text a session cannot reach.
    func overflowMessage(alreadyUnresolved: Int32, token: PCPToken) -> String {
        let ceilingNow = lock.withLock { ceiling }
        return Self.overflowMessage(
            alreadyUnresolved: alreadyUnresolved, ceiling: ceilingNow, token: token)
    }

    /// The same text, from values the caller already holds - so it is callable from inside the lock.
    private static func overflowMessage(
        alreadyUnresolved: Int32, ceiling: Int32, token: PCPToken
    ) -> String {
        // RENDERING THE TOKEN IS NOT A BREACH OF ITS OPACITY, and the specification requires it
        // here. Opacity forbids DERIVING - parsing record_id, comparing epochs, branching on either -
        // and says nothing about printing. So the token's fields are printed as they arrived, by the
        // generated message's own text renderer, never assembled from parts this client interpreted.
        // A Token carries no credentials, unlike the Configure message section 10.4 forbids
        // rendering whole.
        let rendered = token.textFormatString().trimmingCharacters(in: .whitespacesAndNewlines)
        return "the proxy dispatched record {\(rendered)} while \(alreadyUnresolved) were already "
            + "unresolved - queued plus executing - past the max_concurrency of \(ceiling) it "
            + "declared itself, so this is a protocol violation and not load; the call is cancelled "
            + "rather than answered with FAILED_PRECONDITION, because a gRPC client cannot set a status"
    }

    // MARK: - private

    private struct Taken {
        let record: PCPDispatchRecord?
    }

    /// One attempt at the queue: a record, a definitive "no more" (`Taken(record: nil)`), or `nil`
    /// meaning "wait and try again".
    private func tryTake() -> Taken? {
        lock.withLock {
            if stopped { return Taken(record: nil) }
            if !queued.isEmpty { return Taken(record: queued.removeFirst()) }
            if closed { return Taken(record: nil) }
            return nil
        }
    }

    /// Wakes every waiter rather than one.
    ///
    /// A woken executor re-checks and waits again if it lost the race, so an extra wakeup costs a
    /// loop iteration; waking exactly one and getting the bookkeeping wrong costs a stall. FIFO is
    /// unaffected either way - it is a property of the deque, not of which task wins.
    private func wakeWaiters() {
        let woken: [CheckedContinuation<Void, Never>] = lock.withLock {
            let current = waiters
            waiters.removeAll()
            return current
        }
        for continuation in woken { continuation.resume() }
    }
}
