// swift-tools-version:6.0
// Copyright (C) 2026 Antony Stubbs and contributors
//
// The Swift demo (astubbs#242, plan unit U35). Like the client library beside it, this package is
// only ever built INSIDE a container - there is no Swift toolchain on a developer box here - and
// the image that builds it is `demo/Dockerfile`.
//
// IT IS ITS OWN PACKAGE, NOT A TARGET OF THE CLIENT'S, and that is the whole reason this file
// exists. The demo needs Swift's own Kafka client for its AK core arm, which drags in librdkafka,
// zstd, OpenSSL and SwiftNIO. Putting that in the library's manifest would add all of it to the
// dependency graph of every consumer of the client - and to the committed `Package.resolved` that
// the clients workflow keys its cache on - to serve a demo none of them build.
//
// THE CLIENT ARRIVES AS A PATH DEPENDENCY, which is also what makes it legal: the library's targets
// use `unsafeFlags`, and a package that does is barred from being depended on by VERSION. A local
// path dependency is exempt, and this demo is built from this checkout by definition.

import PackageDescription

let package = Package(
    name: "ParallelConsumerSwiftDemo",
    products: [
        .executable(name: "pc-swift-demo", targets: ["pc-swift-demo"])
    ],
    dependencies: [
        // The client library this demo exists to exercise. An earlier version of the seed demo
        // spoke the protocol by hand; it proved the engine worked and said nothing about the
        // artifact users touch.
        .package(path: ".."),
        // SWIFT'S OWN KAFKA CLIENT, for the AK core arm - the ecosystem's package rather than a
        // hand-rolled binding, because the arm's whole claim is "what this language's users
        // already use". It wraps librdkafka, which it vendors as a git submodule and compiles from
        // source, so this dependency is why the demo image installs libssl-dev and libsasl2-dev.
        //
        // Pinned EXACTLY, and to a pre-release, because that is all this package has published:
        // the 1.0.0-alpha line is the whole tag list. `main` is not usable here - it declares
        // swift-tools-version 6.2.3, which the pinned Swift 6.1 toolchain cannot parse, so the
        // failure arrives at manifest load and reads as a resolution error rather than a version
        // conflict. A toolchain bump is what unblocks a move.
        .package(url: "https://github.com/swift-server/swift-kafka-client.git", exact: "1.0.0-alpha.9"),
        // Declared even though swift-kafka-client already depends on it: its `KafkaConsumer` and
        // `KafkaProducer` are `Service`s and must be run inside a `ServiceGroup`, and a package may
        // only use products of dependencies it declares itself.
        .package(url: "https://github.com/swift-server/swift-service-lifecycle.git", from: "2.1.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.10.0"),
    ],
    targets: [
        .executableTarget(
            name: "pc-swift-demo",
            dependencies: [
                // The package identity of a PATH dependency is the last component of its path, NOT the
                // `name:` in its manifest - which is `ParallelConsumerProxyClient` and does not work
                // here. That is also why demo/Dockerfile checks the client module into a directory
                // of exactly this name.
                .product(name: "ParallelConsumerProxyClient", package: "parallel-consumer-proxy-client-swift"),
                .product(name: "Kafka", package: "swift-kafka-client"),
                .product(name: "ServiceLifecycle", package: "swift-service-lifecycle"),
                .product(name: "Logging", package: "swift-log"),
            ]
            // NO `-warnings-as-errors` HERE, unlike the library targets next door, and the
            // difference is deliberate. That flag is right for code this project owns; on a demo it
            // means a deprecation in somebody else's pre-release Kafka client fails a build whose
            // job is to run, and the reader who hits it has no way to fix it. The Swift 6 language
            // mode this tools version turns on is still in force, so a data race here is still a
            // compile error.
        )
    ]
)
