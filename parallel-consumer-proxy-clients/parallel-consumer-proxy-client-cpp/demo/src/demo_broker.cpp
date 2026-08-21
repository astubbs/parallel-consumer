// Copyright (C) 2026 Antony Stubbs and contributors

#include "demo_broker.h"

#include <atomic>
#include <cstddef>
#include <iostream>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace parallelconsumer::proxy::demo {
namespace {

/// The key space the seeded records spread over. Ordering is UNORDERED in every arm, so this
/// changes nothing today; it exists so that a KEY-ordered lane added later has more than one key to
/// shard across, rather than needing the seeding rewritten first. The same number as the reference
/// demo's, so the two are seeding the same shape of backlog.
constexpr int kKeySpace = 1000;

constexpr int kAdminTimeoutMs = 30000;
constexpr int kMetadataTimeoutMs = 10000;
constexpr int kFlushTimeoutMs = 120000;

/// What the delivery reports said, collected off librdkafka's own poll thread.
struct DeliveryOutcome {
    std::mutex mutex;
    std::string first_failure;
    std::atomic<int> failed{0};

    void record(const std::string& reason) {
        const std::lock_guard<std::mutex> lock(mutex);
        ++failed;
        if (first_failure.empty()) {
            first_failure = reason;
        }
    }
};

void delivery_report(rd_kafka_t* /*handle*/, const rd_kafka_message_t* message, void* opaque) {
    if (message->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
        return;
    }
    static_cast<DeliveryOutcome*>(opaque)->record(rd_kafka_err2str(message->err));
}

/// Builds a configuration from a property map, refusing anything librdkafka will not take.
///
/// The returned configuration is OWNED BY THE CALLER until `rd_kafka_new` consumes it - which it
/// does only on success, hence the explicit destroy on the failure path in `handle()`.
rd_kafka_conf_t* configuration(const std::map<std::string, std::string>& properties) {
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    char problem[512];
    for (const auto& property : properties) {
        if (rd_kafka_conf_set(conf, property.first.c_str(), property.second.c_str(), problem,
                              sizeof(problem)) != RD_KAFKA_CONF_OK) {
            rd_kafka_conf_destroy(conf);
            // The KEY only. A property map is where credentials live, and the natural rendering of
            // a configuration error is exactly how one reaches a log file.
            throw std::runtime_error("librdkafka rejected the property '" + property.first + "': " + problem);
        }
    }
    return conf;
}

KafkaHandle handle(rd_kafka_type_t type, rd_kafka_conf_t* conf) {
    char problem[512];
    rd_kafka_t* created = rd_kafka_new(type, conf, problem, sizeof(problem));
    if (created == nullptr) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(std::string("could not create the Kafka client: ") + problem);
    }
    return KafkaHandle(created);
}

int partitions_of(rd_kafka_t* client, const std::string& topic) {
    rd_kafka_topic_t* subject = rd_kafka_topic_new(client, topic.c_str(), nullptr);
    if (subject == nullptr) {
        throw std::runtime_error("could not describe the existing topic " + topic);
    }
    const struct rd_kafka_metadata* metadata = nullptr;
    const rd_kafka_resp_err_t error = rd_kafka_metadata(client, 0, subject, &metadata, kMetadataTimeoutMs);
    rd_kafka_topic_destroy(subject);
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw std::runtime_error("could not describe the existing topic " + topic + ": "
                                 + rd_kafka_err2str(error));
    }
    int found = 0;
    for (int index = 0; index < metadata->topic_cnt; ++index) {
        if (topic == metadata->topics[index].topic) {
            found = metadata->topics[index].partition_cnt;
        }
    }
    rd_kafka_metadata_destroy(metadata);
    return found;
}

}  // namespace

DemoBroker::DemoBroker(std::string bootstrap) : bootstrap_(std::move(bootstrap)) {
    if (bootstrap_.empty()) {
        throw std::runtime_error("no broker address");
    }
}

std::map<std::string, std::string> DemoBroker::consumer_properties(const std::string& group_id) const {
    return {{"bootstrap.servers", bootstrap_},
            {"group.id", group_id},
            {"auto.offset.reset", "earliest"},
            {"enable.auto.commit", "false"}};
}

void DemoBroker::ensure_topic(const std::string& topic, int partitions) const {
    // A producer handle, because librdkafka's admin operations hang off an ordinary client and a
    // producer is the one that needs no group.
    KafkaHandle admin = handle(RD_KAFKA_PRODUCER, configuration({{"bootstrap.servers", bootstrap_}}));

    char problem[512];
    rd_kafka_NewTopic_t* requested =
            rd_kafka_NewTopic_new(topic.c_str(), partitions, 1, problem, sizeof(problem));
    if (requested == nullptr) {
        throw std::runtime_error("could not describe the topic to create: " + std::string(problem));
    }

    rd_kafka_AdminOptions_t* options =
            rd_kafka_AdminOptions_new(admin.get(), RD_KAFKA_ADMIN_OP_CREATETOPICS);
    rd_kafka_AdminOptions_set_request_timeout(options, kAdminTimeoutMs, problem, sizeof(problem));
    rd_kafka_queue_t* replies = rd_kafka_queue_new(admin.get());
    rd_kafka_CreateTopics(admin.get(), &requested, 1, options, replies);

    rd_kafka_event_t* reply = rd_kafka_queue_poll(replies, kAdminTimeoutMs + 5000);

    // Everything below reads out of `reply`, so the strings are copied before any of these are
    // destroyed - a librdkafka error string does not outlive its event.
    std::string failure;
    bool already_exists = false;
    if (reply == nullptr) {
        failure = "the broker did not answer the create-topic request within "
                  + std::to_string(kAdminTimeoutMs / 1000) + "s";
    } else if (rd_kafka_event_error(reply) != RD_KAFKA_RESP_ERR_NO_ERROR) {
        failure = rd_kafka_event_error_string(reply);
    } else {
        std::size_t count = 0;
        const rd_kafka_topic_result_t** results =
                rd_kafka_CreateTopics_result_topics(rd_kafka_event_CreateTopics_result(reply), &count);
        for (std::size_t index = 0; index < count; ++index) {
            const rd_kafka_resp_err_t error = rd_kafka_topic_result_error(results[index]);
            if (error == RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS) {
                already_exists = true;
            } else if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
                failure = rd_kafka_topic_result_error_string(results[index]);
            }
        }
    }

    if (reply != nullptr) {
        rd_kafka_event_destroy(reply);
    }
    rd_kafka_queue_destroy(replies);
    rd_kafka_AdminOptions_destroy(options);
    rd_kafka_NewTopic_destroy(requested);

    if (!failure.empty()) {
        throw std::runtime_error("could not create the demo topic " + topic + ": " + failure);
    }
    if (!already_exists) {
        std::cout << "Created topic " << topic << " with " << partitions << " partitions" << std::endl;
        return;
    }

    // Reusing a topic silently is fine; reusing one with a DIFFERENT partition count is not,
    // because the effective-configuration block would print a --partitions value that never
    // applied - and that block is the demo's whole reproducibility promise.
    const int existing = partitions_of(admin.get(), topic);
    if (existing != partitions) {
        throw std::runtime_error("topic " + topic + " already exists with " + std::to_string(existing)
                                 + " partitions, but this run asked for " + std::to_string(partitions)
                                 + " - pass --topic to name a fresh one, or --partitions "
                                 + std::to_string(existing));
    }
    std::cout << "Topic " << topic << " already exists with the requested " << partitions
              << " partitions, reusing it" << std::endl;
}

void DemoBroker::seed(const std::string& topic, int from, int to) const {
    if (to <= from) {
        return;
    }

    DeliveryOutcome outcome;
    rd_kafka_conf_t* conf = configuration({{"bootstrap.servers", bootstrap_}, {"linger.ms", "20"}});
    rd_kafka_conf_set_opaque(conf, &outcome);
    rd_kafka_conf_set_dr_msg_cb(conf, delivery_report);
    KafkaHandle producer = handle(RD_KAFKA_PRODUCER, conf);

    std::cout << "Producing records " << from << " to " << to << "..." << std::endl;
    for (int index = from; index < to; ++index) {
        const std::string key = "key-" + std::to_string(index % kKeySpace);
        const std::string value = "record-" + std::to_string(index);
        for (;;) {
            const rd_kafka_resp_err_t error = rd_kafka_producev(
                    producer.get(), RD_KAFKA_V_TOPIC(topic.c_str()),
                    RD_KAFKA_V_KEY(key.data(), key.size()),
                    RD_KAFKA_V_VALUE(const_cast<char*>(value.data()), value.size()),
                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY), RD_KAFKA_V_END);
            if (error == RD_KAFKA_RESP_ERR_NO_ERROR) {
                break;
            }
            if (error != RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                throw std::runtime_error("the demo could not seed its backlog: "
                                         + std::string(rd_kafka_err2str(error)));
            }
            // A full local queue is back-pressure, not a failure: serve the delivery reports that
            // will drain it and try the same record again.
            rd_kafka_poll(producer.get(), 100);
        }
    }

    const rd_kafka_resp_err_t flushed = rd_kafka_flush(producer.get(), kFlushTimeoutMs);
    if (flushed != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw std::runtime_error("the demo could not seed its backlog: "
                                 + std::string(rd_kafka_err2str(flushed)) + ", "
                                 + std::to_string(rd_kafka_outq_len(producer.get()))
                                 + " records still unsent");
    }
    if (outcome.failed.load() > 0) {
        throw std::runtime_error("the demo could not seed its backlog: " + std::to_string(outcome.failed.load())
                                 + " records were not delivered, the first because "
                                 + outcome.first_failure);
    }
    std::cout << "Produced " << (to - from) << " records" << std::endl;
}

KafkaHandle DemoBroker::subscribed_consumer(const std::string& topic, const std::string& group_id) const {
    KafkaHandle consumer = handle(RD_KAFKA_CONSUMER, configuration(consumer_properties(group_id)));
    // Routes the main queue onto the consumer queue, so rd_kafka_consumer_poll sees everything.
    rd_kafka_poll_set_consumer(consumer.get());

    rd_kafka_topic_partition_list_t* subscription = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(subscription, topic.c_str(), RD_KAFKA_PARTITION_UA);
    const rd_kafka_resp_err_t error = rd_kafka_subscribe(consumer.get(), subscription);
    rd_kafka_topic_partition_list_destroy(subscription);
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw std::runtime_error("could not subscribe to " + topic + ": " + rd_kafka_err2str(error));
    }
    return consumer;
}

}  // namespace parallelconsumer::proxy::demo
