// Copyright (C) 2026 Antony Stubbs and contributors
//
// The user's function, and what it returns.
//
// C++ HAS EXCEPTIONS, so this surface has both halves the authoring guide's §1 describes: an
// explicit `Outcome::failure(reason)` for code that decides a record failed, and a single
// translation of a THROWN exception into a failure outcome. That translation happens in exactly one
// place - Client::run_one - so there is no second spelling of it to keep in step.

#ifndef PARALLELCONSUMER_PROXY_OUTCOME_H
#define PARALLELCONSUMER_PROXY_OUTCOME_H

#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "record.h"

namespace parallelconsumer::proxy {

/// What one invocation decided about one record.
class Outcome {
public:
    /// The record was processed, with no output.
    static Outcome success() { return Outcome(true, {}, {}); }

    /// The record was processed, and the proxy should produce these records with its own producer
    /// before the input record's offset may become eligible to commit. This is the only sanctioned
    /// route for worker output to Kafka.
    static Outcome success(std::vector<OutboundRecord> produce) {
        return Outcome(true, std::move(produce), {});
    }

    /// The record failed and should be redelivered. The reason travels to the proxy and comes back
    /// on the next delivery verbatim: DO NOT put record payload or credentials in it.
    static Outcome failure(std::string reason) { return Outcome(false, {}, std::move(reason)); }

    [[nodiscard]] bool is_success() const { return success_; }
    [[nodiscard]] const std::vector<OutboundRecord>& produce() const { return produce_; }
    [[nodiscard]] const std::string& reason() const { return reason_; }

private:
    Outcome(bool success, std::vector<OutboundRecord> produce, std::string reason)
        : success_(success), produce_(std::move(produce)), reason_(std::move(reason)) {}

    bool success_;
    std::vector<OutboundRecord> produce_;
    std::string reason_;
};

/// The user's function: takes one record, returns its outcome, or throws.
///
/// It is invoked on an executor thread and shared by all of them, so an implementation that holds
/// state must be safe to call concurrently. A `std::function` rather than a virtual interface
/// because a lambda is what a C++ caller reaches for first, and a stateful callable is expressible
/// either way.
using RecordProcessor = std::function<Outcome(const InboundRecord&)>;

}  // namespace parallelconsumer::proxy

#endif  // PARALLELCONSUMER_PROXY_OUTCOME_H
