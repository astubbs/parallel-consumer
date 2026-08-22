// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE ONLY COMMITTED FILE IN THIS TARGET. Everything else here is generated from the frozen
// `parallel-consumer-proxy-protocol/src/main/proto/parallelconsumer/proxy/v1/proxy.proto` by protoc,
// INSIDE the image the Dockerfile beside this package builds - `protoc-gen-swift` for the messages
// and `protoc-gen-grpc-swift-2` for the client stub. Nothing generated is committed, so this file is
// what makes the directory exist for SwiftPM to load the target.
//
// The generated type names all carry the `PCP` prefix, and that prefix comes from the schema's own
// `swift_prefix` option rather than from any command line: `PCPClientMessage`, `PCPConfigure`,
// `PCPDispatchRecord`, `PCPProxyService`. Placement is the .proto's to decide for every language at
// once - `buf breaking`'s FILE category treats a change to one of those options as breaking, so an
// override on a protoc invocation would be a second, silently divergent authority.
//
// It is generated at `Visibility=Public` because it is a separate module from the client library
// that uses it. That is a build-graph consequence rather than an invitation: an application uses
// `ParallelConsumerProxyClient`, whose surface is its own types, and nothing in this module appears
// in it. The split exists so that a release build does not recompile 124 KB of generated protobuf
// every time a hand-written line changes, and so that the hand-written targets can turn warnings
// into errors without holding generated code to a rule a swift-protobuf bump could break.

/// Marks this module as the generated half of the Swift proxy client.
///
/// It carries no behaviour. Reading it is how you discover that the rest of this module is not in
/// git, and where it comes from.
public enum ParallelConsumerProxyProtocol {
    /// The proto package the generated types belong to, as the frozen schema declares it.
    public static let protoPackage = "parallelconsumer.proxy.v1"
}
