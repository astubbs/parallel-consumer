// Copyright (C) 2026 Antony Stubbs and contributors
//
// What the shared conformance suite cannot see from outside the process: that no rendering in this
// library can print a credential or a record payload, that the capability set is DECLARED rather
// than left empty, and that options which cannot open a session are refused before anything spawns.
//
// The credential and payload rules are section 6 and section 10.4/10.5 of the authoring guide, and
// the mechanism they name is Swift's: any struct interpolated into a string prints every stored
// property it has, so the defence has to be a hand-written renderer on the type rather than
// discipline at the call sites.

import Foundation
import XCTest

@testable import ParallelConsumerProxyClient
@testable import ParallelConsumerProxyProtocol

final class RedactionAndOptionsTests: XCTestCase {

    func testTheOptionsRenderingCannotPrintACredential() {
        var options = ClientOptions(sidecarPath: "/opt/proxy")
        options.topics = ["orders"]
        options.kafkaProperties = [
            "sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule "
                + "required username='admin' password='hunter2';",
            "ssl.truststore.password": "changeit",
        ]

        let rendered = options.describe()
        XCTAssertFalse(rendered.contains("hunter2"), rendered)
        XCTAssertFalse(rendered.contains("changeit"), rendered)
        XCTAssertFalse(rendered.contains("sasl.jaas.config"), rendered)
        XCTAssertFalse(rendered.contains("PlainLoginModule"), rendered)
        // A count is a useful diagnostic and discloses nothing.
        XCTAssertTrue(rendered.contains("2 entries (redacted)"), rendered)
    }

    func testTheRecordRenderingsCannotPrintPayload() {
        let inbound = InboundRecord(
            topic: "orders",
            partition: 3,
            offset: 4242,
            key: Data("customer-4815162342".utf8),
            value: Data("{\"pan\":\"4111111111111111\"}".utf8),
            attempt: 2,
            hasFailedBefore: true,
            lastFailureReason: "downstream refused")

        let rendered = inbound.describe()
        XCTAssertFalse(rendered.contains("customer-4815162342"), rendered)
        XCTAssertFalse(rendered.contains("4111111111111111"), rendered)
        // Topic, partition, offset and attempt identify a record completely for every diagnostic
        // purpose, and none of them is user data.
        XCTAssertTrue(rendered.contains("orders"), rendered)
        XCTAssertTrue(rendered.contains("4242"), rendered)
        XCTAssertTrue(rendered.contains("attempt=2"), rendered)

        let outbound = OutboundRecord(topic: "orders.out", key: "k-secret", value: "v-secret")
        let outboundRendered = outbound.describe()
        XCTAssertFalse(outboundRendered.contains("k-secret"), outboundRendered)
        XCTAssertFalse(outboundRendered.contains("v-secret"), outboundRendered)
        XCTAssertTrue(outboundRendered.contains("keyBytes=8"), outboundRendered)
    }

    func testTheDeclaredCapabilitySetIsExactlyDispatch() {
        let options = ClientOptions(sidecarPath: "/opt/proxy")
        // DECLARING NOTHING WOULD BE WORSE THAN DECLARING A SUBSET: an empty list means the whole v1
        // baseline on the wire, which entitles the proxy to send heartbeat, manifest, worker-death
        // and shutdown traffic this client does not answer.
        XCTAssertEqual(options.declaredCapabilities, ["dispatch"])
        XCTAssertEqual(implementedCapabilities, [Capability.dispatch])
    }

    func testConfigureCarriesTheSubscriptionCredentialsAndCapabilities() {
        var options = ClientOptions(sidecarPath: "/opt/proxy")
        options.topics = ["a-scenario"]
        options.maxConcurrency = 5
        options.kafkaProperties = ["bootstrap.servers": "localhost:9092"]
        options.commitInterval = .milliseconds(100)
        options.defaultMessageRetryDelay = .milliseconds(50)
        options.instanceTag = "runner"

        let configure = options.makeConfigure()
        XCTAssertEqual(configure.topics, ["a-scenario"])
        XCTAssertEqual(configure.maxConcurrency, 5)
        XCTAssertEqual(configure.capabilities, ["dispatch"])
        XCTAssertEqual(configure.kafkaProperties, ["bootstrap.servers": "localhost:9092"])
        XCTAssertEqual(configure.commitInterval.seconds, 0)
        XCTAssertEqual(configure.commitInterval.nanos, 100_000_000)
        XCTAssertEqual(configure.defaultMessageRetryDelay.nanos, 50_000_000)
        XCTAssertEqual(configure.pcInstanceTag, "runner")
        // Absent rather than defaulted: an unset optional means "take the proxy's default", and
        // Configured reports what it chose.
        XCTAssertFalse(configure.hasTerminalTopic)
        XCTAssertFalse(configure.hasOrdering)
    }

    func testARelativeSidecarPathIsRefusedBeforeAnythingSpawns() {
        var options = ClientOptions(sidecarPath: "proxy")
        options.topics = ["orders"]
        XCTAssertThrowsError(try options.validate()) { thrown in
            guard case ProxyClientError.options(let detail)? = thrown as? ProxyClientError else {
                return XCTFail("expected an options error, got \(thrown)")
            }
            XCTAssertTrue(detail.contains("absolute"), detail)
        }
    }

    func testExactlyOneSubscriptionFormIsRequired() {
        var neither = ClientOptions(sidecarPath: "/opt/proxy")
        XCTAssertThrowsError(try neither.validate())

        neither.topics = ["orders"]
        neither.topicPattern = "orders.*"
        XCTAssertThrowsError(try neither.validate(), "both forms at once is not a subscription")

        var named = ClientOptions(sidecarPath: "/opt/proxy")
        named.topics = ["orders"]
        XCTAssertNoThrow(try named.validate())

        var patterned = ClientOptions(sidecarPath: "/opt/proxy")
        patterned.topicPattern = "orders.*"
        XCTAssertNoThrow(try patterned.validate())
    }

    func testAConfiguredMissingItsCeilingOrExecutorCountIsAViolation() {
        var withoutCeiling = PCPConfigured()
        withoutCeiling.executorCount = 2
        XCTAssertThrowsError(try Session.from(wire: withoutCeiling)) { thrown in
            guard case ProxyClientError.protocolViolation(let detail)? = thrown as? ProxyClientError
            else {
                return XCTFail("expected a protocol violation, got \(thrown)")
            }
            // Absence is never a licence to treat the session as unbounded.
            XCTAssertTrue(detail.contains("unbounded"), detail)
        }

        var withoutExecutors = PCPConfigured()
        withoutExecutors.maxConcurrency = 3
        XCTAssertThrowsError(try Session.from(wire: withoutExecutors))

        var usable = PCPConfigured()
        usable.maxConcurrency = 3
        usable.executorCount = 2
        usable.topics = ["orders"]
        usable.capabilities = ["dispatch"]
        let session = try? Session.from(wire: usable)
        XCTAssertEqual(session?.maxConcurrency, 3)
        XCTAssertEqual(session?.executorCount, 2)
        XCTAssertEqual(session?.topics, ["orders"])
        XCTAssertEqual(session?.negotiated(Capability.dispatch), true)
        XCTAssertEqual(session?.negotiated(Capability.shutdown), false)
    }

    func testTheSessionRenderingCarriesTheNegotiatedSetAndNoCredentials() {
        var wire = PCPConfigured()
        wire.maxConcurrency = 8
        wire.executorCount = 4
        wire.topics = ["orders"]
        wire.capabilities = ["dispatch"]
        guard let session = try? Session.from(wire: wire) else { return XCTFail("unusable") }

        let rendered = session.describe()
        XCTAssertTrue(rendered.contains("maxConcurrency=8"), rendered)
        XCTAssertTrue(rendered.contains("executorCount=4"), rendered)
        XCTAssertTrue(rendered.contains("capabilities=[dispatch]"), rendered)
    }
}
