// Copyright (C) 2026 Antony Stubbs and contributors
//
// Toolchain smoke, not client code. It exists to prove the CONTAINERISED BUILD ENVIRONMENT
// (astubbs#242): that the frozen proxy.proto generates C++ inside the image, that the generated
// code links against gRPC and protobuf, and - because the binary is extracted and run on the host
// by bin/build-client.sh - that the static link makes the artifact portable off the image. The C++
// wave owns the real client and may delete this file the moment it has a target of its own that
// proves the same three things.
//
// It deliberately makes no network call: constructing a channel is lazy in gRPC, so this exercises
// the linked runtime without needing a proxy to be listening.

#include <cstdio>

#include <google/protobuf/stubs/common.h>
#include <grpcpp/grpcpp.h>

#include "parallelconsumer/proxy/v1/proxy.pb.h"
#include "parallelconsumer/proxy/v1/proxy.grpc.pb.h"

namespace pcp = parallelconsumer::proxy::v1;

int main() {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Round-trip a real message of the frozen schema, so a generated-code mismatch fails here
    // rather than at the first live session.
    pcp::ClientMessage message;
    pcp::Configure* configure = message.mutable_configure();
    configure->add_topics("toolchain-smoke");
    configure->set_max_concurrency(4);

    std::string wire;
    if (!message.SerializeToString(&wire)) {
        std::fprintf(stderr, "smoke: serialization failed\n");
        return 1;
    }

    pcp::ClientMessage parsed;
    if (!parsed.ParseFromString(wire)) {
        std::fprintf(stderr, "smoke: parse failed\n");
        return 1;
    }
    if (parsed.configure().topics_size() != 1 || parsed.configure().max_concurrency() != 4) {
        std::fprintf(stderr, "smoke: round-trip mismatch\n");
        return 1;
    }

    // Touch the gRPC runtime: a channel and the generated stub for the frozen service.
    std::shared_ptr<grpc::Channel> channel =
            grpc::CreateChannel("127.0.0.1:1", grpc::InsecureChannelCredentials());
    std::unique_ptr<pcp::ProxyService::Stub> stub = pcp::ProxyService::NewStub(channel);
    if (stub == nullptr) {
        std::fprintf(stderr, "smoke: stub creation failed\n");
        return 1;
    }

    std::printf("smoke: ok\n");
    std::printf("smoke: protobuf %d\n", GOOGLE_PROTOBUF_VERSION);
    std::printf("smoke: grpc %s\n", grpc::Version().c_str());
    std::printf("smoke: message %s (%zu wire bytes)\n",
                pcp::ClientMessage::descriptor()->full_name().c_str(), wire.size());
    std::printf("smoke: service %s\n", pcp::ProxyService::service_full_name());
    return 0;
}
