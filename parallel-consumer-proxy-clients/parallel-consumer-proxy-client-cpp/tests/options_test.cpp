// Copyright (C) 2026 Antony Stubbs and contributors
//
// Credential hygiene, the declared capability set, and the option validation that has to happen
// before anything is spawned. None of these is reachable from a live session: a leak in a rendering
// only shows up in somebody's log file, and a client that declared the wrong capabilities looks
// healthy right up until the proxy sends it a duty it does not implement.

#include "error.h"
#include "options.h"
#include "parallelconsumer/proxy/v1/proxy.pb.h"
#include "record.h"
#include "session.h"
#include "test_support.h"

namespace {

namespace pcp = parallelconsumer::proxy;
namespace v1 = parallelconsumer::proxy::v1;

pcp::ClientOptions usable() {
    pcp::ClientOptions options;
    options.sidecar_path = "/opt/parallel-consumer/proxy";
    options.topics = {"orders"};
    return options;
}

PCP_TEST(describe_redacts_credentials, "describe() never prints a Kafka property, key or value") {
    pcp::ClientOptions options = usable();
    options.kafka_properties = {{"sasl.jaas.config", "password=hunter2"}};

    const std::string rendered = options.describe();

    PCP_CHECK_ABSENT(rendered, "hunter2");
    PCP_CHECK_ABSENT(rendered, "sasl.jaas.config");
    PCP_CHECK_CONTAINS(rendered, "<redacted: 1 entries>");
}

PCP_TEST(only_implemented_capabilities_are_declared,
         "the handshake declares exactly the duties this client performs") {
    v1::Configure configure;
    usable().write_configure(configure);

    // NOT an empty list: empty means "the whole v1 baseline" on the wire, which would earn this
    // client heartbeat, manifest, worker-death and shutdown duties it does not perform - and
    // un-answered heartbeats arm a lease-expiry redelivery loop.
    PCP_CHECK_EQ(configure.capabilities_size(), 1);
    PCP_CHECK_EQ(configure.capabilities(0), std::string(pcp::capability::kDispatch));
}

PCP_TEST(a_subscription_is_exactly_one_form, "exactly one of topics or topic_pattern must be set") {
    pcp::ClientOptions neither;
    neither.sidecar_path = "/opt/proxy";
    bool refused = false;
    try {
        neither.validate();
    } catch (const pcp::OptionsError&) {
        refused = true;
    }
    PCP_CHECK(refused);

    pcp::ClientOptions both = usable();
    both.topic_pattern = "orders.*";
    refused = false;
    try {
        both.validate();
    } catch (const pcp::OptionsError&) {
        refused = true;
    }
    PCP_CHECK(refused);

    usable().validate();  // topics alone is fine; a throw here fails the test through the harness
}

PCP_TEST(a_relative_sidecar_path_is_refused, "the sidecar path must be absolute") {
    pcp::ClientOptions options = usable();
    options.sidecar_path = "proxy";

    bool refused = false;
    try {
        options.validate();
    } catch (const pcp::OptionsError& problem) {
        refused = true;
        PCP_CHECK_CONTAINS(std::string(problem.what()), "must be absolute");
    }
    PCP_CHECK(refused);
}

PCP_TEST(configured_without_a_ceiling_is_a_violation,
         "a Configured missing its ceiling or executor count is a protocol violation") {
    v1::Configured configured;
    configured.set_executor_count(2);
    configured.add_capabilities("dispatch");

    bool refused = false;
    try {
        pcp::Session::from_wire(configured);
    } catch (const pcp::ProtocolError& violation) {
        refused = true;
        PCP_CHECK_CONTAINS(std::string(violation.what()), "max_concurrency");
    }
    PCP_CHECK(refused);

    v1::Configured no_executors;
    no_executors.set_max_concurrency(3);
    refused = false;
    try {
        pcp::Session::from_wire(no_executors);
    } catch (const pcp::ProtocolError& violation) {
        refused = true;
        PCP_CHECK_CONTAINS(std::string(violation.what()), "executor_count");
    }
    PCP_CHECK(refused);
}

PCP_TEST(negotiation_is_read_from_what_came_back,
         "a duty exists on a session only if its token survived the handshake") {
    v1::Configured configured;
    configured.set_max_concurrency(3);
    configured.set_executor_count(2);
    configured.add_capabilities("dispatch");
    configured.add_topics("orders");

    const pcp::Session session = pcp::Session::from_wire(configured);

    PCP_CHECK(session.negotiated(pcp::capability::kDispatch));
    PCP_CHECK(!session.negotiated(pcp::capability::kShutdown));
    PCP_CHECK(!session.negotiated(pcp::capability::kHeartbeat));
    PCP_CHECK_EQ(session.max_concurrency, 3);
    PCP_CHECK_EQ(session.executor_count, 2);
    // The mock harness ignores the subscription entirely, so the echo is the only evidence a test
    // has that the subscription it sent arrived at all.
    PCP_CHECK_EQ(session.topics.size(), std::size_t{1});
    PCP_CHECK_EQ(session.topics[0], std::string("orders"));
}

PCP_TEST(records_never_render_their_payload, "a record's rendering identifies it without its bytes") {
    pcp::InboundRecord inbound;
    inbound.topic = "orders";
    inbound.partition = 2;
    inbound.offset = 41;
    inbound.attempt = 2;
    inbound.key = std::string("customer-7");
    inbound.value = std::string("SECRET-PAYLOAD");

    const std::string rendered = inbound.describe();

    PCP_CHECK_ABSENT(rendered, "SECRET-PAYLOAD");
    PCP_CHECK_ABSENT(rendered, "customer-7");
    PCP_CHECK_CONTAINS(rendered, "topic=orders");
    PCP_CHECK_CONTAINS(rendered, "offset=41");
    PCP_CHECK_CONTAINS(rendered, "attempt=2");

    pcp::OutboundRecord outbound;
    outbound.topic = "results";
    outbound.value = std::string("SECRET-OUTPUT");
    const std::string outbound_rendered = outbound.describe();
    PCP_CHECK_ABSENT(outbound_rendered, "SECRET-OUTPUT");
    PCP_CHECK_CONTAINS(outbound_rendered, "valueBytes=13");
}

}  // namespace
