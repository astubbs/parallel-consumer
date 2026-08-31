// swift-tools-version:5.9
// Copyright (C) 2026 Antony Stubbs and contributors
//
// Build scaffolding for the Swift proxy client (astubbs#242). No dependencies, so `swift build`
// needs no network and toolchain presence is the only thing deciding whether this module runs.

import PackageDescription

let package = Package(
    name: "hello",
    targets: [
        .executableTarget(name: "hello", path: "Sources/hello")
    ]
)
