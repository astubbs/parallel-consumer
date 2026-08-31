// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE PORTABILITY ASSERTION, and the successor to the toolchain smoke this module used to carry.
//
// It is extracted from the image by bin/build-client.sh and RUN ON THE HOST, beside a dynamically
// linked build of the same source that is expected to FAIL there. That pair is the whole evidence
// that --static-swift-stdlib is what makes the artifact portable, rather than the host happening to
// look like the image - a run where both work proves nothing, and the script says so and fails.
//
// It proves the same three things the smoke did, and one more, which is why the smoke is gone: the
// frozen proxy.proto generates Swift inside the image, the generated code links against grpc-swift
// and swift-protobuf, the binary runs off-image - AND the CLIENT LIBRARY itself links and
// initialises, which the smoke could not say because it had no library to link.
//
// It deliberately makes no network call: it names the transport type without connecting, and
// spawning a sidecar needs a sidecar. What exercises the live path is the conformance runner beside
// it.

import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2
import ParallelConsumerProxyClient
import ParallelConsumerProxyProtocol

@main
struct Selftest {
    static func main() {
        var options = ClientOptions(sidecarPath: "/opt/parallel-consumer/proxy")
        options.topics = ["selftest"]
        options.maxConcurrency = 4
        options.kafkaProperties = ["sasl.jaas.config": "password=hunter2"]

        do {
            try options.validate()
        } catch {
            fail("valid options were refused: \(error)")
        }

        // Section 10.4, asserted rather than asserted about: the options type cannot render its own
        // credentials.
        let rendered = options.describe()
        if rendered.contains("hunter2") || rendered.contains("sasl.jaas.config") {
            fail("credentials leaked into describe(): \(rendered)")
        }

        // The capability set is DECLARED rather than left empty: an empty list means "the whole v1
        // baseline" on the wire, which would entitle the proxy to send traffic this client does not
        // answer.
        guard options.declaredCapabilities == [Capability.dispatch] else {
            fail("expected exactly [dispatch], got \(options.declaredCapabilities)")
        }

        // Round-trip a real message of the frozen schema, so a generated-code mismatch fails here
        // rather than at the first live session. The PCP prefix comes from the schema's own
        // `swift_prefix` option, not from any command line.
        var message = PCPClientMessage()
        message.configure = {
            var configure = PCPConfigure()
            configure.topics = options.topics
            configure.maxConcurrency = 4
            configure.capabilities = options.declaredCapabilities
            return configure
        }()

        let wire: Data
        do {
            wire = try message.serializedData()
        } catch {
            fail("serialization failed: \(error)")
        }
        let parsed: PCPClientMessage
        do {
            parsed = try PCPClientMessage(serializedBytes: wire)
        } catch {
            fail("parse failed: \(error)")
        }
        guard parsed.configure.topics == ["selftest"],
            parsed.configure.maxConcurrency == 4,
            parsed.configure.capabilities == [Capability.dispatch]
        else {
            fail("round-trip mismatch")
        }

        // Touch the gRPC runtime: the generated service descriptor, and the HTTP/2 transport type
        // the client uses over loopback.
        let descriptor: ServiceDescriptor = PCPProxyService.descriptor
        let transport = HTTP2ClientTransport.Posix.self

        print("selftest: ok")
        print("selftest: message \(PCPClientMessage.protoMessageName) (\(wire.count) wire bytes)")
        print("selftest: service \(descriptor.fullyQualifiedService)")
        print("selftest: methods \(PCPProxyService.Method.descriptors.map(\.method))")
        print("selftest: transport \(transport)")
        print("selftest: options \(rendered)")
    }

    private static func fail(_ message: String) -> Never {
        FileHandle.standardError.write(Data("selftest: \(message)\n".utf8))
        exit(1)
    }
}
