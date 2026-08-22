// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE PORTABILITY ASSERTION, and the successor to the toolchain smoke this module used to carry.
//
// It is extracted from the image by bin/build-client.sh and RUN ON THE HOST, beside a dynamically
// linked build of the same source that is expected to FAIL there. That pair is the whole evidence
// that the static link is what makes the artifact portable, rather than the host happening to look
// like the image - a run where both work proves nothing, and the script says so and fails.
//
// It proves the same three things the smoke did, and one more, which is why the smoke is gone:
// the frozen proxy.proto generates C++ inside the image, the generated code links against gRPC and
// protobuf, the binary runs off-image - AND the CLIENT LIBRARY itself links and initialises, which
// the smoke could not say because it had no library to link.
//
// It deliberately makes no network call: constructing a channel is lazy in gRPC, and spawning a
// sidecar needs a sidecar. What exercises the live path is the conformance runner beside it.

#include <cstdio>
#include <string>

#include "parallel_consumer_proxy_client.h"
#include "parallelconsumer/proxy/v1/proxy.grpc.pb.h"
#include "parallelconsumer/proxy/v1/proxy.pb.h"

namespace pcp = parallelconsumer::proxy;

int main() {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // The library's own rendering of a Configure, so a generated-code mismatch fails here rather
    // than at the first live session.
    pcp::ClientOptions options;
    options.sidecar_path = "/opt/parallel-consumer/proxy";
    options.topics = {"selftest"};
    options.max_concurrency = 4;
    options.kafka_properties = {{"sasl.jaas.config", "password=hunter2"}};
    try {
        options.validate();
    } catch (const pcp::ClientError& refused) {
        std::fprintf(stderr, "selftest: valid options were refused: %s\n", refused.what());
        return 1;
    }

    // §10.4, asserted rather than asserted about: the options type cannot render its own credentials.
    const std::string rendered = options.describe();
    if (rendered.find("hunter2") != std::string::npos || rendered.find("sasl.jaas.config") != std::string::npos) {
        std::fprintf(stderr, "selftest: credentials leaked into describe(): %s\n", rendered.c_str());
        return 1;
    }

    parallelconsumer::proxy::v1::ClientMessage message;
    options.write_configure(*message.mutable_configure());

    std::string wire;
    if (!message.SerializeToString(&wire)) {
        std::fprintf(stderr, "selftest: serialization failed\n");
        return 1;
    }
    parallelconsumer::proxy::v1::ClientMessage parsed;
    if (!parsed.ParseFromString(wire)) {
        std::fprintf(stderr, "selftest: parse failed\n");
        return 1;
    }
    if (parsed.configure().topics_size() != 1 || parsed.configure().max_concurrency() != 4 ||
        parsed.configure().capabilities_size() != 1 ||
        parsed.configure().capabilities(0) != pcp::capability::kDispatch) {
        std::fprintf(stderr, "selftest: round-trip mismatch\n");
        return 1;
    }

    // Touch the gRPC runtime: a channel and the generated stub for the frozen service.
    const std::shared_ptr<grpc::Channel> channel =
        grpc::CreateChannel("127.0.0.1:1", grpc::InsecureChannelCredentials());
    const std::unique_ptr<parallelconsumer::proxy::v1::ProxyService::Stub> stub =
        parallelconsumer::proxy::v1::ProxyService::NewStub(channel);
    if (stub == nullptr) {
        std::fprintf(stderr, "selftest: stub creation failed\n");
        return 1;
    }

    std::printf("selftest: ok\n");
    std::printf("selftest: protobuf %d\n", GOOGLE_PROTOBUF_VERSION);
    std::printf("selftest: grpc %s\n", grpc::Version().c_str());
    std::printf("selftest: message %s (%zu wire bytes)\n",
                parallelconsumer::proxy::v1::ClientMessage::descriptor()->full_name().c_str(), wire.size());
    std::printf("selftest: service %s\n", parallelconsumer::proxy::v1::ProxyService::service_full_name());
    std::printf("selftest: options %s\n", rendered.c_str());
    return 0;
}
