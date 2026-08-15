// Copyright (C) 2026 Antony Stubbs and contributors
//
// The in-flight ceiling, which the authoring guide names as the rule of its section 3 most often
// implemented wrongly - three of the first five clients in this fan-out bounded the queue's own
// length instead of counting unresolved records, and a client that does that cannot detect overflow
// AT ALL, because a record leaving the queue always makes room.
//
// The first test below is the guide's own worked example, and it is written so that it FAILS against
// that defect: at the moment it admits the fourth record, two of the three unresolved records are
// executing and only one is queued, so a queue-length bound has room and admits it silently.

import XCTest

@testable import ParallelConsumerProxyClient
@testable import ParallelConsumerProxyProtocol

final class DispatchRecordQueueTests: XCTestCase {

    func testTheCeilingCountsUnresolvedRecordsNotQueuedOnes() async throws {
        // The guide's worked example: max_concurrency = 3, executor_count = 2, one wave A, B, C.
        let queue = DispatchRecordQueue()
        queue.configure(maxConcurrency: 3)

        try queue.admit(record(offset: 10, recordId: "A"))
        try queue.admit(record(offset: 11, recordId: "B"))
        try queue.admit(record(offset: 12, recordId: "C"))
        XCTAssertEqual(queue.unresolved, 3)
        XCTAssertEqual(queue.depth, 3)

        // Two executors take their records. HANDING A RECORD OUT MOVES IT; IT DOES NOT FREE ITS SLOT.
        let first = await queue.take()
        let second = await queue.take()
        XCTAssertEqual(first?.token.recordID, "A")
        XCTAssertEqual(second?.token.recordID, "B")
        XCTAssertEqual(queue.unresolved, 3, "taking a record must not free its slot")
        XCTAssertEqual(queue.depth, 1, "only C is still queued - a queue-length bound has room here")

        // A fourth record now is the proxy exceeding its own declared ceiling. THIS is the assertion
        // a client bounding the queue fails: it has two free slots by its own reckoning.
        XCTAssertThrowsError(try queue.admit(record(offset: 13, recordId: "D"))) { thrown in
            guard case ProxyClientError.protocolViolation = thrown else {
                return XCTFail("expected a protocol violation, got \(thrown)")
            }
        }

        // Only a verdict frees a slot - B's report, not B's executor picking it up.
        queue.settle()
        XCTAssertEqual(queue.unresolved, 2)
        XCTAssertNoThrow(try queue.admit(record(offset: 13, recordId: "D")))
        XCTAssertEqual(queue.unresolved, 3)
    }

    func testHandOutIsFifoWithinAWave() async throws {
        let queue = DispatchRecordQueue()
        queue.configure(maxConcurrency: 4)
        for (index, name) in ["A", "B", "C", "D"].enumerated() {
            try queue.admit(record(offset: Int64(index), recordId: name))
        }

        var handed: [String] = []
        for _ in 0..<4 {
            guard let taken = await queue.take() else { return XCTFail("the queue ran dry early") }
            handed.append(taken.token.recordID)
        }
        XCTAssertEqual(handed, ["A", "B", "C", "D"])
    }

    func testTheOverflowMessageNamesBothCountsTheCeilingAndTheToken() throws {
        let queue = DispatchRecordQueue()
        queue.configure(maxConcurrency: 1)
        try queue.admit(record(offset: 1, recordId: "held"))

        var thrown: (any Error)?
        XCTAssertThrowsError(try queue.admit(record(offset: 2, recordId: "overflowing", epoch: 7))) {
            thrown = $0
        }
        guard case .protocolViolation(let message)? = thrown as? ProxyClientError else {
            return XCTFail("expected a protocol violation, got \(String(describing: thrown))")
        }
        // The counts the application needs to act on...
        XCTAssertTrue(message.contains("1 were already unresolved"), message)
        XCTAssertTrue(message.contains("max_concurrency of 1"), message)
        // ...and the token, rendered as it arrived. Opacity forbids DERIVING, not printing, and the
        // specification's overflow contract requires the token. This client is the second in the
        // fan-out to do it.
        XCTAssertTrue(message.contains("overflowing"), message)
        XCTAssertTrue(message.contains("7"), message)
        // The reason it cancels rather than answering FAILED_PRECONDITION belongs in the message
        // too: it is the first thing a reader of this error asks.
        XCTAssertTrue(message.contains("cancelled"), message)
    }

    func testDiscardingAtShutdownSettlesWhatItDrops() throws {
        let queue = DispatchRecordQueue()
        queue.configure(maxConcurrency: 3)
        try queue.admit(record(offset: 1, recordId: "A"))
        try queue.admit(record(offset: 2, recordId: "B"))
        XCTAssertEqual(queue.unresolved, 2)

        XCTAssertEqual(queue.discardQueued(), 2)
        // A discard is one of the four verdicts, so the slots come back - otherwise the ceiling
        // shrinks permanently and the client eventually declares a violation against a correct proxy.
        XCTAssertEqual(queue.unresolved, 0)
        XCTAssertEqual(queue.depth, 0)
    }

    func testDiscardingLeavesExecutingRecordsCounted() async throws {
        let queue = DispatchRecordQueue()
        queue.configure(maxConcurrency: 3)
        try queue.admit(record(offset: 1, recordId: "executing"))
        try queue.admit(record(offset: 2, recordId: "queued"))
        _ = await queue.take()  // "executing" is now with an executor

        XCTAssertEqual(queue.discardQueued(), 1, "only the QUEUED record is dropped")
        XCTAssertEqual(
            queue.unresolved, 1,
            "the executing record still counts: it is still going to report, and dropping its slot "
                + "would let the proxy over-dispatch")
    }

    func testStoppingHandOutIsNotAVerdict() async throws {
        let queue = DispatchRecordQueue()
        queue.configure(maxConcurrency: 2)
        try queue.admit(record(offset: 1, recordId: "A"))

        queue.stopHandout()
        let taken = await queue.take()
        XCTAssertNil(taken, "hand-out has stopped")
        XCTAssertEqual(
            queue.unresolved, 1,
            "stopping hand-out resolves nothing - the record is still the proxy's to reclaim")
    }

    func testNothingIsAdmittedBeforeConfigured() {
        // The ceiling starts at zero, so a Dispatch arriving before Configured cannot be absorbed by
        // accident: there is no slot for it.
        let queue = DispatchRecordQueue()
        XCTAssertThrowsError(try queue.admit(record(offset: 1, recordId: "early")))
    }

    func testTakeReturnsNilOnceClosedAndEmpty() async {
        let queue = DispatchRecordQueue()
        queue.configure(maxConcurrency: 1)
        queue.close()
        let taken = await queue.take()
        XCTAssertNil(taken)
    }

    func testAWaitingExecutorIsWokenByAnAdmission() async throws {
        let queue = DispatchRecordQueue()
        queue.configure(maxConcurrency: 1)

        // The executor blocks first, then the record arrives - which is the ordinary case for every
        // wave after the first, and the one a missing wakeup breaks.
        async let taken = queue.take()
        try await Task.sleep(for: .milliseconds(20))
        try queue.admit(record(offset: 99, recordId: "late"))
        let awaited = await taken
        XCTAssertEqual(awaited?.token.recordID, "late")
    }

    // MARK: - helpers

    private func record(offset: Int64, recordId: String, epoch: Int64 = 1) -> PCPDispatchRecord {
        var token = PCPToken()
        token.recordID = recordId
        token.epoch = epoch
        var inner = PCPRecord()
        inner.topic = "a-topic"
        inner.partition = 0
        inner.offset = offset
        var dispatched = PCPDispatchRecord()
        dispatched.token = token
        dispatched.record = inner
        dispatched.attempt = 1
        return dispatched
    }
}
