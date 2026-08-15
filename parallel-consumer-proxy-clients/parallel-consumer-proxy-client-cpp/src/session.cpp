// Copyright (C) 2026 Antony Stubbs and contributors

#include "session.h"

#include <algorithm>
#include <string>

#include "error.h"
#include "parallelconsumer/proxy/v1/proxy.pb.h"

namespace parallelconsumer::proxy {

bool Session::negotiated(const std::string& token) const {
    return std::find(capabilities.begin(), capabilities.end(), token) != capabilities.end();
}

Session Session::from_wire(const v1::Configured& configured) {
    // Absence is a protocol violation, never "unlimited": the ceiling is always finite and always
    // reported, and it is also what this client admits against, so there is nothing to fall back on.
    if (!configured.has_max_concurrency() || configured.max_concurrency() < 1) {
        throw ProtocolError(
            "Configured carried no usable max_concurrency - the in-flight ceiling is always reported, "
            "and its absence is never a licence to treat the session as unbounded");
    }
    if (!configured.has_executor_count() || configured.executor_count() < 1) {
        throw ProtocolError("Configured carried no usable executor_count");
    }

    Session session;
    session.topics.assign(configured.topics().begin(), configured.topics().end());
    if (configured.has_topic_pattern()) {
        session.topic_pattern = configured.topic_pattern();
    }
    session.max_concurrency = configured.max_concurrency();
    session.executor_count = configured.executor_count();
    session.capabilities.assign(configured.capabilities().begin(), configured.capabilities().end());
    if (configured.has_terminal_topic()) {
        session.terminal_topic = configured.terminal_topic();
    }
    return session;
}

std::string Session::describe() const {
    std::string granted;
    for (const auto& token : capabilities) {
        if (!granted.empty()) {
            granted += ",";
        }
        granted += token;
    }
    std::string subscription;
    for (const auto& topic : topics) {
        if (!subscription.empty()) {
            subscription += ",";
        }
        subscription += topic;
    }
    if (topic_pattern) {
        subscription = "pattern:" + *topic_pattern;
    }
    return "Session{topics=[" + subscription + "], maxConcurrency=" + std::to_string(max_concurrency) +
           ", executorCount=" + std::to_string(executor_count) + ", capabilities=[" + granted + "]}";
}

}  // namespace parallelconsumer::proxy
