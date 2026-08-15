// Copyright (C) 2026 Antony Stubbs and contributors
//
// The lifecycle channel's one piece of parsing. It is a test rather than an obviously-correct
// three-liner because the test-mode harness LOGS BEFORE ITS PORT LINE: a client that read exactly
// one line would take a log line for a port and fail its handshake, and one that accepted anything
// containing "port:" would take a log line mentioning a port for the real thing.

#include "sidecar.h"
#include "test_support.h"

namespace {

namespace pcp = parallelconsumer::proxy;

PCP_TEST(the_port_line_is_recognised_among_log_lines,
         "the port line is found by scanning, and nothing else is mistaken for it") {
    PCP_CHECK(pcp::Sidecar::parse_port_line("port: 43117").value_or(0) == 43117);
    PCP_CHECK(pcp::Sidecar::parse_port_line("port: 43117 ").value_or(0) == 43117);

    PCP_CHECK(!pcp::Sidecar::parse_port_line("12:01:02 INFO  starting up").has_value());
    PCP_CHECK(!pcp::Sidecar::parse_port_line("port: not-a-number").has_value());
    // The prefix must start the line: a log line ABOUT the port is not the port line.
    PCP_CHECK(!pcp::Sidecar::parse_port_line("the port: 43117").has_value());
    PCP_CHECK(!pcp::Sidecar::parse_port_line("port: 0").has_value());
    PCP_CHECK(!pcp::Sidecar::parse_port_line("port: 70000").has_value());
    PCP_CHECK(!pcp::Sidecar::parse_port_line("port: ").has_value());
}

}  // namespace
