// Copyright (C) 2026 Antony Stubbs and contributors
//
// The effective configuration a session is running with.

#ifndef PARALLELCONSUMER_PROXY_SESSION_H
#define PARALLELCONSUMER_PROXY_SESSION_H

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace parallelconsumer::proxy::v1 {
class Configured;
}

namespace parallelconsumer::proxy {

/// What the proxy replied it is ACTUALLY running with, after its own defaults and the capability
/// negotiation.
///
/// Assert on this, never on ClientOptions. What was asked for and what is running are different
/// things, and only this one governs what the client owes.
struct Session {
    /// The subscription by name, as the proxy echoed it. The mock harness ignores the subscription
    /// entirely, so this echo - not the seeded records - is the only evidence a test has that the
    /// subscription it sent arrived.
    std::vector<std::string> topics;
    /// The subscription by pattern, as the proxy echoed it.
    std::optional<std::string> topic_pattern;
    /// The proxy's in-flight ceiling: the records it may have dispatched to this client and not yet
    /// had a verdict for. Never absent, and never "unlimited".
    std::int32_t max_concurrency = 0;
    /// How many executors to run. A pure function of connect-time configuration: computed once,
    /// sent once, never revised - and clients must not assume any formula relating it to
    /// max_concurrency.
    std::int32_t executor_count = 0;
    /// The negotiated intersection of what this client declared and what the proxy implements.
    std::vector<std::string> capabilities;
    /// The effective terminal-outcome destination, present exactly when terminal reporting is on.
    std::optional<std::string> terminal_topic;

    /// Whether a capability token survived the handshake. EVERY duty in this protocol is gated by
    /// one, so this is the question to ask before sending anything.
    [[nodiscard]] bool negotiated(const std::string& token) const;

    /// Reads the effective session out of a Configured, refusing one the protocol forbids.
    ///
    /// @throws ProtocolError if the ceiling or the executor count is missing or unusable - absence
    ///         is a violation, never a licence to guess a default
    static Session from_wire(const v1::Configured& configured);

    /// The negotiated configuration, for the one INFO line worth writing about it. No credentials
    /// can reach here: Configured structurally has no kafka_properties field.
    [[nodiscard]] std::string describe() const;
};

}  // namespace parallelconsumer::proxy

#endif  // PARALLELCONSUMER_PROXY_SESSION_H
