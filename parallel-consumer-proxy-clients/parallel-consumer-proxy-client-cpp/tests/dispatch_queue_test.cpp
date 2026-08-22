// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE IN-FLIGHT CEILING, which is the rule three of the first five clients in this fan-out
// implemented wrongly from the same text. Every test here is written against the authoring guide's
// §3 rule 2 - "max_concurrency bounds the records this client has been dispatched and has not yet
// reported, QUEUED PLUS EXECUTING" - and the third one is the guide's own worked example, which is
// the only shape that discriminates a correct client from the defect.
//
// PROVEN RED BEFORE GREEN: with DispatchQueue::admit changed to compare `queued_.size()` against the
// ceiling instead of `unresolved_` - the exact defect the guide describes - "a record out with an
// executor still occupies the ceiling" and "only a verdict frees a slot" both fail, while
// "overflowing the ceiling ..." (one wave larger than the ceiling) still PASSES. That is the whole
// argument for why the wave-sized test is not enough on its own.

#include "dispatch_queue.h"
#include "error.h"
#include "test_support.h"

namespace {

namespace pcp = parallelconsumer::proxy;
namespace v1 = parallelconsumer::proxy::v1;

v1::DispatchRecord record_at(std::int64_t offset) {
    v1::DispatchRecord dispatched;
    dispatched.mutable_token()->set_record_id("record-" + std::to_string(offset));
    dispatched.mutable_token()->set_epoch(1);
    dispatched.mutable_record()->set_offset(offset);
    dispatched.set_attempt(1);
    return dispatched;
}

PCP_TEST(fifo_within_a_wave, "a wave is handed out in record order") {
    pcp::DispatchQueue queue;
    queue.configure(3);
    for (std::int64_t offset = 0; offset < 3; ++offset) {
        queue.admit(record_at(offset));
    }

    std::vector<std::int64_t> handed;
    v1::DispatchRecord taken;
    queue.close();  // so take() returns false rather than blocking once drained
    while (queue.take(taken)) {
        handed.push_back(taken.record().offset());
    }

    PCP_CHECK_EQ(handed.size(), std::size_t{3});
    PCP_CHECK_EQ(handed[0], std::int64_t{0});
    PCP_CHECK_EQ(handed[1], std::int64_t{1});
    PCP_CHECK_EQ(handed[2], std::int64_t{2});
}

PCP_TEST(a_wave_larger_than_the_ceiling_overflows,
         "a wave larger than the ceiling is a protocol violation naming the counts and the token") {
    pcp::DispatchQueue queue;
    queue.configure(2);
    queue.admit(record_at(0));
    queue.admit(record_at(1));

    bool refused = false;
    try {
        queue.admit(record_at(2));
    } catch (const pcp::ProtocolError& violation) {
        refused = true;
        const std::string message = violation.what();
        PCP_CHECK_CONTAINS(message, "2 were already unresolved");
        PCP_CHECK_CONTAINS(message, "max_concurrency of 2");
        PCP_CHECK_CONTAINS(message, "cancelled");
        // The token, rendered as it arrived. Opacity forbids DERIVING from it, not printing it, and
        // the specification's overflow contract requires it - no other client in this fan-out does
        // this yet.
        PCP_CHECK_CONTAINS(message, "record-2");
        PCP_CHECK_CONTAINS(message, "epoch: 1");
    }
    PCP_CHECK(refused);
}

/// THE GUIDE'S OWN WORKED EXAMPLE, and the only shape the wave-sized test above cannot reach:
/// ceiling 3, two executors holding A and B, C still queued, and a fourth record arriving. Counting
/// queued records alone, D fits - the deque holds one and has two slots free - and the overflow the
/// guide describes at length is undetectable.
PCP_TEST(executing_records_still_occupy_the_ceiling,
         "a record out with an executor still occupies the ceiling") {
    pcp::DispatchQueue queue;
    queue.configure(3);
    queue.admit(record_at(0));
    queue.admit(record_at(1));
    queue.admit(record_at(2));

    v1::DispatchRecord taken;
    PCP_CHECK(queue.take(taken));  // executor-1 takes A
    PCP_CHECK(queue.take(taken));  // executor-2 takes B
    PCP_CHECK_EQ(queue.depth(), std::size_t{1});
    PCP_CHECK_EQ(queue.unresolved(), 3);

    bool refused = false;
    try {
        queue.admit(record_at(3));
    } catch (const pcp::ProtocolError& violation) {
        refused = true;
        PCP_CHECK_CONTAINS(std::string(violation.what()), "3 were already unresolved");
    }
    PCP_CHECK(refused);
}

PCP_TEST(only_a_verdict_frees_a_slot, "reporting a record is what makes room for the next") {
    pcp::DispatchQueue queue;
    queue.configure(1);
    queue.admit(record_at(0));
    v1::DispatchRecord taken;
    PCP_CHECK(queue.take(taken));  // the only executor takes it
    PCP_CHECK_EQ(queue.depth(), std::size_t{0});

    bool refused = false;
    try {
        queue.admit(record_at(1));
    } catch (const pcp::ProtocolError&) {
        refused = true;
    }
    PCP_CHECK(refused);

    queue.settle();  // the report
    bool admitted = true;
    try {
        queue.admit(record_at(1));
    } catch (const pcp::ProtocolError&) {
        admitted = false;
    }
    PCP_CHECK(admitted);
}

PCP_TEST(discarding_is_a_verdict_too, "records discarded at shutdown stop counting against the ceiling") {
    pcp::DispatchQueue queue;
    queue.configure(2);
    queue.admit(record_at(0));
    queue.admit(record_at(1));

    PCP_CHECK_EQ(queue.discard_queued(), 2);
    PCP_CHECK_EQ(queue.unresolved(), 0);
}

PCP_TEST(stop_handout_leaves_queued_records_alone, "stopping hand-out stops the queue, not the ceiling") {
    pcp::DispatchQueue queue;
    queue.configure(2);
    queue.admit(record_at(0));
    queue.stop_handout();

    v1::DispatchRecord taken;
    PCP_CHECK(!queue.take(taken));
    // The record is still unresolved: hand-out stopping is not a verdict, and inventing one for work
    // this client did not do is exactly what the protocol forbids.
    PCP_CHECK_EQ(queue.unresolved(), 1);
}

}  // namespace
