// Copyright (C) 2026 Antony Stubbs and contributors
//
// The effective configuration a session is running with.

import ParallelConsumerProxyProtocol

/// What the proxy replied it is ACTUALLY running with, after its own defaults and the capability
/// negotiation.
///
/// Assert on this, never on ``ClientOptions``. What was asked for and what is running are different
/// things, and only this one governs what the client owes.
public struct Session: Sendable {
    /// The subscription by name, as the proxy echoed it.
    ///
    /// The mock harness ignores the subscription entirely, so this echo - not the seeded records -
    /// is the only evidence a test has that the subscription it sent arrived.
    public let topics: [String]
    /// The subscription by pattern, as the proxy echoed it.
    public let topicPattern: String?
    /// The proxy's in-flight ceiling: the records it may have dispatched to this client and not yet
    /// had a verdict for. Never absent, and never "unlimited".
    public let maxConcurrency: Int32
    /// How many executors to run. A pure function of connect-time configuration: computed once, sent
    /// once, never revised - and clients must not assume any formula relating it to
    /// ``maxConcurrency``.
    public let executorCount: Int32
    /// The negotiated intersection of what this client declared and what the proxy implements.
    public let capabilities: [String]
    /// The effective terminal-outcome destination, present exactly when terminal reporting is on.
    public let terminalTopic: String?

    /// Whether a capability token survived the handshake.
    ///
    /// EVERY duty in this protocol is gated by one, so this is the question to ask before sending
    /// anything.
    public func negotiated(_ token: String) -> Bool {
        capabilities.contains(token)
    }

    /// Reads the effective session out of a `Configured`, refusing one the protocol forbids.
    ///
    /// - Throws: ``ProxyClientError/protocolViolation(_:)`` if the ceiling or the executor count is
    ///   missing or unusable - absence is a violation, never a licence to guess a default.
    static func from(wire configured: PCPConfigured) throws -> Session {
        // Absence is a protocol violation, never "unlimited": the ceiling is always finite and
        // always reported, and it is also what this client admits against, so there is nothing to
        // fall back on.
        guard configured.hasMaxConcurrency, configured.maxConcurrency >= 1 else {
            throw ProxyClientError.protocolViolation(
                "Configured carried no usable max_concurrency - the in-flight ceiling is always "
                    + "reported, and its absence is never a licence to treat the session as unbounded")
        }
        guard configured.hasExecutorCount, configured.executorCount >= 1 else {
            throw ProxyClientError.protocolViolation("Configured carried no usable executor_count")
        }
        return Session(
            topics: configured.topics,
            topicPattern: configured.hasTopicPattern ? configured.topicPattern : nil,
            maxConcurrency: configured.maxConcurrency,
            executorCount: configured.executorCount,
            capabilities: configured.capabilities,
            terminalTopic: configured.hasTerminalTopic ? configured.terminalTopic : nil
        )
    }

    /// The negotiated configuration, for the one INFO line worth writing about it.
    ///
    /// No credential can reach here: `Configured` structurally has no `kafka_properties` field.
    public func describe() -> String {
        let subscription = topicPattern.map { "pattern:\($0)" } ?? topics.joined(separator: ",")
        return "Session{topics=[\(subscription)], maxConcurrency=\(maxConcurrency), "
            + "executorCount=\(executorCount), "
            + "capabilities=[\(capabilities.joined(separator: ","))]}"
    }
}
