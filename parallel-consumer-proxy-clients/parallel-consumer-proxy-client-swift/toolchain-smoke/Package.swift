// swift-tools-version:6.0
// Copyright (C) 2026 Antony Stubbs and contributors
//
// Toolchain smoke for the containerised Swift build environment (astubbs#242). Separate from the
// module's own Package.swift, which the Swift wave owns: this package is only ever built INSIDE
// the image built by the Dockerfile beside it, which is the only place a Swift toolchain exists on
// a Debian 13 box (Swift.org publishes no Debian build - see
// docs/inflight/parked-containerised-toolchains-and-runtime.md).
//
// Versions are EXACT, not ranges. The image is the reproducibility boundary, so a floating
// dependency would make the same Dockerfile produce different artifacts on different days, which
// is the failure mode a pinned container is bought to prevent.

import PackageDescription

let package = Package(
    name: "pc-swift-toolchain-smoke",
    dependencies: [
        .package(url: "https://github.com/apple/swift-protobuf.git", exact: "1.38.1"),
        // The v2 line lives at grpc-swift-2.git; that URL is the package IDENTITY every other
        // grpc-swift package resolves against, so using the older grpc-swift.git URL here would
        // fork the graph into two copies of the same package.
        .package(url: "https://github.com/grpc/grpc-swift-2.git", exact: "2.4.2"),
        .package(url: "https://github.com/grpc/grpc-swift-protobuf.git", exact: "2.4.1"),
        .package(url: "https://github.com/grpc/grpc-swift-nio-transport.git", exact: "2.9.1"),
    ],
    targets: [
        .executableTarget(
            name: "pc-swift-toolchain-smoke",
            dependencies: [
                .product(name: "SwiftProtobuf", package: "swift-protobuf"),
                .product(name: "GRPCCore", package: "grpc-swift-2"),
                .product(name: "GRPCProtobuf", package: "grpc-swift-protobuf"),
                .product(name: "GRPCNIOTransportHTTP2", package: "grpc-swift-nio-transport"),
            ],
            path: "Sources/Smoke"
        )
    ]
)
