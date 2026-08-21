// Copyright (C) 2026 Antony Stubbs and contributors
//
// C++'S OWN KAFKA CLIENT, which in C++ means librdkafka - there is no other. It is here for two
// jobs, and only one of them is an arm of the comparison:
//
//   1. the AK core arm - a consumer, one record at a time, the same sleep every other arm runs;
//   2. the fixture - creating the topic and pre-producing the backlog every arm then replays.
//
// The second is not a hole in the sidecar's claim. "The application does no Kafka I/O on the
// sidecar path" is a statement about THAT PATH, not about this process: a comparison needs both
// sides, so the same binary that runs the foreign-client arm also seeds the topic with an ordinary
// Kafka client. A genuinely foreign application carries no Kafka client library at all, which is
// the property the sidecar arm stands in for.
//
// THE C API RATHER THAN librdkafka's C++ ONE, deliberately: the C++ binding has no admin client, so
// creating the topic would drop to the C API anyway, and one API in one file beats two.

#ifndef PARALLELCONSUMER_PROXY_DEMO_BROKER_H
#define PARALLELCONSUMER_PROXY_DEMO_BROKER_H

#include <librdkafka/rdkafka.h>

#include <map>
#include <memory>
#include <string>

namespace parallelconsumer::proxy::demo {

/// An owned `rd_kafka_t`. librdkafka's handles are C, and every early return in this demo would
/// otherwise leak one.
class KafkaHandle {
public:
    KafkaHandle() = default;
    explicit KafkaHandle(rd_kafka_t* handle) : handle_(handle) {}
    KafkaHandle(const KafkaHandle&) = delete;
    KafkaHandle& operator=(const KafkaHandle&) = delete;
    KafkaHandle(KafkaHandle&& other) noexcept : handle_(other.handle_) { other.handle_ = nullptr; }
    KafkaHandle& operator=(KafkaHandle&& other) noexcept {
        if (this != &other) {
            reset();
            handle_ = other.handle_;
            other.handle_ = nullptr;
        }
        return *this;
    }
    ~KafkaHandle() { reset(); }

    [[nodiscard]] rd_kafka_t* get() const { return handle_; }

    void reset() {
        if (handle_ != nullptr) {
            rd_kafka_destroy(handle_);
            handle_ = nullptr;
        }
    }

private:
    rd_kafka_t* handle_ = nullptr;
};

/// The broker this demo reads from, however the reader got here.
///
/// ONE WAY IN, AND THAT IS THE DIVERGENCE THIS LANGUAGE HAS. The Java reference starts a broker
/// with Testcontainers when none was supplied; C++ has no Testcontainers, and the demo runs only
/// inside its own container, where a demo container is never granted the host Docker socket
/// (plan unit U35) and so could not start one anyway. The address always arrives from outside -
/// from the compose sibling, or from `--bootstrap` - and a missing one is an error with an
/// explanation rather than a silently different run. See demo/README.md.
///
/// The same door serves own-cluster mode, where the address is the user's real cluster, so nothing
/// here logs or echoes it.
class DemoBroker {
public:
    explicit DemoBroker(std::string bootstrap);

    /// Creates the demo's topic, tolerating one a previous run left behind - but never one with a
    /// DIFFERENT partition count, because the fingerprint would then print a `--partitions` value
    /// that never applied, and that block is the demo's whole reproducibility promise.
    ///
    /// @throws std::runtime_error
    void ensure_topic(const std::string& topic, int partitions) const;

    /// Produces the backlog every arm then replays.
    ///
    /// PRE-PRODUCED RATHER THAN PRODUCED ALONGSIDE THE ARMS, which is what makes the workload
    /// closed-loop - and in turn why no arm reports latency. A per-record timing here would be
    /// flattered by however far an arm had fallen behind, so throughput is the only honest number
    /// this shape can produce.
    ///
    /// @throws std::runtime_error if any record failed to reach the broker. A discarded delivery
    ///         report would let the demo report a full backlog, run every arm against a short one,
    ///         and print numbers for a workload that never existed.
    void seed(const std::string& topic, int from, int to) const;

    /// The Kafka properties every arm needs to reach this broker.
    ///
    /// `enable.auto.commit=false` is set for the same reason the Java reference sets it: Parallel
    /// Consumer owns offset commits and refuses a consumer that also commits for itself. The
    /// sidecar forces the setting whatever the map says, so on the sidecar arm this line is
    /// belt-and-braces; the AK core arm below genuinely needs it, since it commits nothing and a
    /// half-auto-committed run would replay a different backlog next time.
    [[nodiscard]] std::map<std::string, std::string> consumer_properties(const std::string& group_id) const;

    /// A subscribed consumer for the AK core arm, built from consumer_properties().
    ///
    /// @throws std::runtime_error
    [[nodiscard]] KafkaHandle subscribed_consumer(const std::string& topic, const std::string& group_id) const;

private:
    std::string bootstrap_;
};

}  // namespace parallelconsumer::proxy::demo

#endif  // PARALLELCONSUMER_PROXY_DEMO_BROKER_H
