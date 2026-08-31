// Copyright (C) 2026 Antony Stubbs and contributors
//
// The lifecycle channel. The specification says the port is stdout's FIRST line; the conformance
// harness logs before it, and the authoring guide says a client absorbs that by SCANNING rather than
// by reading exactly one line. A client that reads one line passes every unit test written against a
// well-behaved fixture and then fails its first conformance run, so the scan is what is tested here.

import XCTest

@testable import ParallelConsumerProxyClient

final class SidecarTests: XCTestCase {

    func testAPortLineIsRecognised() {
        XCTAssertEqual(Sidecar.parsePortLine("port: 1234"), 1234)
        XCTAssertEqual(Sidecar.parsePortLine("port:  44321  "), 44321)
    }

    func testAnythingThatIsNotAPortLineIsNotOne() {
        XCTAssertNil(Sidecar.parsePortLine("INFO  starting the mock harness"))
        XCTAssertNil(Sidecar.parsePortLine("port:"))
        XCTAssertNil(Sidecar.parsePortLine("port: "))
        XCTAssertNil(Sidecar.parsePortLine("port: not-a-number"))
        XCTAssertNil(Sidecar.parsePortLine("port: 12ab"))
        XCTAssertNil(Sidecar.parsePortLine("listening on port: 8080"), "the prefix anchors the line")
        // Out of range at both ends: 0 is not a port a client can connect to, and 65536 does not
        // exist. Both would otherwise parse as integers and be dialled.
        XCTAssertNil(Sidecar.parsePortLine("port: 0"))
        XCTAssertNil(Sidecar.parsePortLine("port: 70000"))
    }

    func testThePortIsFoundAMONGTheHarnessesLogChatter() {
        // A real transcript shape: the test-mode harness logs before it announces, which is exactly
        // the divergence the guide tells a client to absorb.
        let transcript = [
            "SLF4J: No SLF4J providers were found.",
            "INFO  bz.stub.parallelconsumer.proxy.testmode.TestModeMain - mock consumer seeded",
            "INFO  bz.stub.parallelconsumer.proxy.ProxyServer - binding loopback",
            "port: 41287",
            "INFO  bz.stub.parallelconsumer.proxy.ProxyServer - session accepted",
        ]

        let found = transcript.compactMap(Sidecar.parsePortLine)
        XCTAssertEqual(found, [41287], "the scan must reach past the chatter and stop at one port")

        // A client that read only the FIRST line - the specification's literal contract - would find
        // nothing here, which is the failure this test exists to pin.
        XCTAssertNil(Sidecar.parsePortLine(transcript[0]))
    }
}
