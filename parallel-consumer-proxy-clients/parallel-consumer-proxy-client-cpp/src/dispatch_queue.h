// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE DISPATCH QUEUE (authoring guide §3, KTD39). The gap between the proxy's in-flight ceiling and
// the client's executor count is a queue inside the client library, and every rule about it is an
// ordering-or-liveness decision specified once for all eleven languages.
//
// It is its own class for one reason: rule 2 requires the unresolved count to live in ONE place, as
// a number the queue owns - `DispatchQueue.inFlight` in TypeScript, `_outstanding` in Python,
// `Shared::unresolved` in Rust, `@unresolved` in Ruby. A count spread across the transport thread
// and the executors is a count two of them can disagree about, and it is also a count no test can
// reach without a live session.
//
// THE RULE THIS FILE EXISTS TO GET RIGHT: max_concurrency bounds the records this client has been
// DISPATCHED AND NOT YET REPORTED - queued PLUS executing - and never the deque's own length.
// Handing a record to an executor moves it; it does not free its slot. Three of the first five
// clients in this fan-out bounded the queue instead, and a client that bounds the queue cannot
// detect overflow AT ALL: a record leaving the queue always makes room, so the condition never
// arises. tests/dispatch_queue_test.cpp carries the guide's own worked example as the control.
//
// Only a verdict frees a slot, and there are exactly four: a report is sent; a record is reported
// Released at shutdown on a session that negotiated `shutdown`; a record is discarded; a worker's
// death is reported by WorkerDied. This client implements the first and the third - the other two
// are gated by capabilities it does not declare.

#ifndef PARALLELCONSUMER_PROXY_DISPATCH_QUEUE_H
#define PARALLELCONSUMER_PROXY_DISPATCH_QUEUE_H

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>

#include "parallelconsumer/proxy/v1/proxy.pb.h"

namespace parallelconsumer::proxy {

class DispatchQueue {
public:
    /// Sets the ceiling from Configured. Until this is called the queue admits nothing, which is
    /// what makes a Dispatch before Configured impossible to absorb by accident.
    void configure(std::int32_t max_concurrency);

    /// Admits one dispatched record.
    ///
    /// Admission is decided by the UNRESOLVED count, not by whether the deque has room.
    ///
    /// @throws ProtocolError when the proxy has exceeded the ceiling it declared itself. Overflow is
    ///         a protocol violation, not a load condition, so records are never dropped to make room
    ///         and the queue never grows unbounded.
    void admit(const v1::DispatchRecord& record);

    /// Takes the next record, FIFO - by arrival, and within one Dispatch by record order.
    ///
    /// @return false when hand-out has stopped, or the queue is closed and empty
    bool take(v1::DispatchRecord& record);

    /// One record reached a verdict: it no longer counts against the ceiling.
    void settle();

    /// Stops hand-out. Records already out with executors are not interrupted - it is only the
    /// hand-out that stops.
    void stop_handout();

    /// Wakes every waiter; nothing further will arrive.
    void close();

    /// Drops the queued records and settles each, for a shutdown on a session that did NOT negotiate
    /// `shutdown` - where reporting them Released would itself be the un-negotiated-message
    /// violation. Their offsets were never committed, so the proxy returns them to scheduling.
    ///
    /// @return how many were dropped
    std::int32_t discard_queued();

    /// Records dispatched and not yet resolved: queued PLUS executing.
    [[nodiscard]] std::int32_t unresolved() const;

    /// How many records are waiting for an executor. A DIAGNOSTIC, never an admission input - see
    /// the file comment.
    [[nodiscard]] std::size_t depth() const;

    /// The counted protocol violation, exposed so a test can assert the text a session cannot reach.
    [[nodiscard]] std::string overflow_message(std::int32_t already_unresolved, const v1::Token& token) const;

private:
    mutable std::mutex mutex_;
    std::condition_variable changed_;
    std::deque<v1::DispatchRecord> queued_;
    std::int32_t unresolved_ = 0;
    std::int32_t ceiling_ = 0;
    bool closed_ = false;
    bool stopped_ = false;
};

}  // namespace parallelconsumer::proxy

#endif  // PARALLELCONSUMER_PROXY_DISPATCH_QUEUE_H
