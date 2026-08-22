// swift-tools-version:6.0
// Copyright (C) 2026 Antony Stubbs and contributors
//
// The Swift proxy client (astubbs#242). This package is only ever built INSIDE the image built by
// the Dockerfile beside it: Swift.org publishes Linux toolchains for Ubuntu, Amazon Linux and RHEL
// only, and the development box is Debian 13, so there is no `swift` on the host and there will not
// be (docs/inflight/parked-containerised-toolchains-and-runtime.md).
//
// TOOLS VERSION 6.0 IS LOAD-BEARING, NOT A FLOOR PICKED AT RANDOM: it turns on the Swift 6 language
// mode, whose strict concurrency checking is this module's real static analysis. The clients
// workflow's swift row runs `swift format lint`, which is a formatter and finds no defects; Swift
// has no mature standalone bug-finder in the staticcheck class, and the compiler is where the
// answer actually lives. Every `Sendable` conformance and every isolation boundary below is
// therefore CHECKED rather than asserted - a data race in this client is a compile error.
//
// Versions are EXACT rather than ranges, for the same reason the toolchain smoke this replaced gave:
// the image is the reproducibility boundary, so a floating dependency would make the same Dockerfile
// produce different artifacts on different days.

import PackageDescription

let package = Package(
    name: "ParallelConsumerProxyClient",
    products: [
        .library(name: "ParallelConsumerProxyClient", targets: ["ParallelConsumerProxyClient"]),
        // The portability pair bin/build-client.sh looks for. The selftest is built twice - once
        // statically linked, once not - and the script runs both on the host: the static one must
        // work there and the dynamic one must not, or the static link proved nothing.
        .executable(name: "pc-swift-selftest", targets: ["pc-swift-selftest"]),
        .executable(name: "pc-swift-conformance-runner", targets: ["pc-swift-conformance-runner"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-protobuf.git", exact: "1.38.1"),
        // The v2 line lives at grpc-swift-2.git; that URL is the package IDENTITY every other
        // grpc-swift package resolves against, so using the older grpc-swift.git URL here would
        // fork the graph into two copies of the same package. The generator binary is
        // protoc-gen-grpc-swift-2 and its protoc flag is --grpc-swift-2_out - the v1 spellings
        // silently do not exist.
        .package(url: "https://github.com/grpc/grpc-swift-2.git", exact: "2.4.2"),
        .package(url: "https://github.com/grpc/grpc-swift-protobuf.git", exact: "2.4.1"),
        .package(url: "https://github.com/grpc/grpc-swift-nio-transport.git", exact: "2.9.1"),
        // The ecosystem's logging facade - Swift's SLF4J, and the reason this client does NOT do
        // what the C++ and TypeScript ones do (an injectable closure, which is the right answer
        // only where the ecosystem has no facade). Pinned at 1.10.0 rather than the newest: 1.10.1
        // onwards declare swift-tools-version 6.2, which the pinned Swift 6.1 toolchain in the
        // image cannot parse, so a bump here waits on a base-image bump.
        .package(url: "https://github.com/apple/swift-log.git", exact: "1.10.0"),
    ],
    targets: [
        // GENERATED CODE ONLY, and it is a separate target for two reasons that happen to agree.
        // Release builds are whole-module, so generated code sharing a module with hand-written
        // code would recompile 124 KB of protobuf on every edit to a hand-written line; and the
        // hand-written target below turns warnings into errors, which is a rule this module cannot
        // hold generated code to - a swift-protobuf bump would then break the build for a reason
        // nobody here can fix. protoc writes into this directory INSIDE THE IMAGE; only its README
        // target file is committed, which is also what makes the directory exist for SwiftPM.
        .target(
            name: "ParallelConsumerProxyProtocol",
            dependencies: [
                .product(name: "SwiftProtobuf", package: "swift-protobuf"),
                .product(name: "GRPCCore", package: "grpc-swift-2"),
                .product(name: "GRPCProtobuf", package: "grpc-swift-protobuf"),
            ]
        ),
        .target(
            name: "ParallelConsumerProxyClient",
            dependencies: [
                "ParallelConsumerProxyProtocol",
                .product(name: "SwiftProtobuf", package: "swift-protobuf"),
                .product(name: "GRPCCore", package: "grpc-swift-2"),
                .product(name: "GRPCProtobuf", package: "grpc-swift-protobuf"),
                .product(name: "GRPCNIOTransportHTTP2", package: "grpc-swift-nio-transport"),
                .product(name: "Logging", package: "swift-log"),
            ],
            swiftSettings: [
                // Applied to the hand-written targets only - see the generated target above. It is
                // `unsafeFlags` because SwiftPM 6.0 has no `treatAllWarnings(as:)`; the restriction
                // that carries (a package using unsafeFlags cannot be depended on remotely) costs
                // nothing here, since this package is published nowhere and is built from this
                // checkout by definition.
                .unsafeFlags(["-warnings-as-errors"])
            ]
        ),
        .executableTarget(
            name: "pc-swift-selftest",
            dependencies: ["ParallelConsumerProxyClient", "ParallelConsumerProxyProtocol"],
            swiftSettings: [.unsafeFlags(["-warnings-as-errors"])]
        ),
        .executableTarget(
            name: "pc-swift-conformance-runner",
            dependencies: [
                "ParallelConsumerProxyClient",
                .product(name: "Logging", package: "swift-log"),
            ],
            swiftSettings: [.unsafeFlags(["-warnings-as-errors"])]
        ),
        .testTarget(
            name: "ParallelConsumerProxyClientTests",
            dependencies: ["ParallelConsumerProxyClient", "ParallelConsumerProxyProtocol"],
            swiftSettings: [.unsafeFlags(["-warnings-as-errors"])]
        ),
    ]
)
