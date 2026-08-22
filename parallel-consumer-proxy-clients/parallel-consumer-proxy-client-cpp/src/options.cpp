// Copyright (C) 2026 Antony Stubbs and contributors

#include "options.h"

#include <string>
#include <vector>

#include "error.h"
#include "google/protobuf/duration.pb.h"
#include "parallelconsumer/proxy/v1/proxy.pb.h"

namespace parallelconsumer::proxy {
namespace {

void set_duration(google::protobuf::Duration* target, std::chrono::milliseconds value) {
    const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(value);
    target->set_seconds(seconds.count());
    target->set_nanos(static_cast<std::int32_t>((value - seconds).count() * 1000000));
}

v1::ProcessingOrder wire_order(ProcessingOrder order) {
    switch (order) {
        case ProcessingOrder::Unordered:
            return v1::PROCESSING_ORDER_UNORDERED;
        case ProcessingOrder::Partition:
            return v1::PROCESSING_ORDER_PARTITION;
        case ProcessingOrder::Key:
            return v1::PROCESSING_ORDER_KEY;
    }
    return v1::PROCESSING_ORDER_UNSPECIFIED;
}

bool is_absolute(const std::string& path) { return !path.empty() && path.front() == '/'; }

}  // namespace

const std::vector<std::string>& implemented_capabilities() {
    static const std::vector<std::string> tokens{capability::kDispatch};
    return tokens;
}

void ClientOptions::validate() const {
    if (sidecar_path.empty()) {
        throw OptionsError("sidecar_path is required");
    }
    if (!is_absolute(sidecar_path)) {
        throw OptionsError("sidecar_path must be absolute, got '" + sidecar_path +
                           "' - a relative or PATH-resolved sidecar is a binary an attacker can influence");
    }
    if (topics.empty() == !topic_pattern.has_value()) {
        throw OptionsError("exactly one of topics or topic_pattern must be set");
    }
    if (max_concurrency.has_value() && *max_concurrency < 1) {
        throw OptionsError("max_concurrency must be >= 1 or absent for the proxy's default, got " +
                           std::to_string(*max_concurrency));
    }
}

void ClientOptions::write_configure(v1::Configure& configure) const {
    for (const auto& topic : topics) {
        configure.add_topics(topic);
    }
    if (topic_pattern) {
        configure.set_topic_pattern(*topic_pattern);
    }
    if (max_concurrency) {
        configure.set_max_concurrency(*max_concurrency);
    }
    for (const auto& entry : kafka_properties) {
        (*configure.mutable_kafka_properties())[entry.first] = entry.second;
    }
    const auto& declared = capabilities.empty() ? implemented_capabilities() : capabilities;
    for (const auto& token : declared) {
        configure.add_capabilities(token);
    }
    if (ordering) {
        configure.set_ordering(wire_order(*ordering));
    }
    if (commit_interval) {
        set_duration(configure.mutable_commit_interval(), *commit_interval);
    }
    if (default_message_retry_delay) {
        set_duration(configure.mutable_default_message_retry_delay(), *default_message_retry_delay);
    }
    if (drain_timeout) {
        set_duration(configure.mutable_drain_timeout(), *drain_timeout);
    }
    if (terminal_topic) {
        configure.set_terminal_topic(*terminal_topic);
    }
    if (instance_tag) {
        configure.set_pc_instance_tag(*instance_tag);
    }
}

std::string ClientOptions::describe() const {
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
    return "ClientOptions{sidecarPath=" + sidecar_path + ", topics=[" + subscription + "]" +
           ", maxConcurrency=" + (max_concurrency ? std::to_string(*max_concurrency) : "<proxy default>") +
           ", kafkaProperties=<redacted: " + std::to_string(kafka_properties.size()) + " entries>" +
           ", instanceTag=" + instance_tag.value_or("<none>") + "}";
}

}  // namespace parallelconsumer::proxy
