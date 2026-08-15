// Copyright (C) 2026 Antony Stubbs and contributors
//
// Toolchain smoke, not client code. It exists to prove the CONTAINERISED BUILD ENVIRONMENT
// (astubbs#242): that the frozen proxy.proto generates Swift inside the image, that the generated
// code links against grpc-swift and swift-protobuf, and - because the binary is extracted and run
// on the host by bin/build-client.sh - that --static-swift-stdlib makes the artifact portable off
// the image. The Swift wave owns the real client and may delete this file the moment it has a
// target of its own that proves the same three things.
//
// It deliberately makes no network call: it names the transport type without connecting, so the
// linked runtime is exercised without a proxy having to be listening.

import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2
import SwiftProtobuf

// Round-trip a real message of the frozen schema, so a generated-code mismatch fails here rather
// than at the first live session. The PCP prefix comes from the schema's own `swift_prefix`.
var message = PCPClientMessage()
message.configure = {
    var configure = PCPConfigure()
    configure.topics = ["toolchain-smoke"]
    configure.maxConcurrency = 4
    return configure
}()

let wire: Data
do {
    wire = try message.serializedData()
} catch {
    FileHandle.standardError.write(Data("smoke: serialization failed: \(error)\n".utf8))
    exit(1)
}

let parsed: PCPClientMessage
do {
    parsed = try PCPClientMessage(serializedBytes: wire)
} catch {
    FileHandle.standardError.write(Data("smoke: parse failed: \(error)\n".utf8))
    exit(1)
}

guard parsed.configure.topics == ["toolchain-smoke"], parsed.configure.maxConcurrency == 4 else {
    FileHandle.standardError.write(Data("smoke: round-trip mismatch\n".utf8))
    exit(1)
}

// Touch the gRPC runtime: the generated service descriptor, and the HTTP/2 transport type the
// client will use over loopback.
let descriptor: ServiceDescriptor = PCPProxyService.descriptor
let transport = HTTP2ClientTransport.Posix.self

print("smoke: ok")
print("smoke: message \(PCPClientMessage.protoMessageName) (\(wire.count) wire bytes)")
print("smoke: service \(descriptor.fullyQualifiedService)")
print("smoke: methods \(PCPProxyService.Method.descriptors.map { $0.method })")
print("smoke: transport \(transport)")
