// Copyright (C) 2026 Antony Stubbs and contributors
//
// The library's own failure type - distinct from a failure OUTCOME, which is the user function's
// verdict on a record and an ordinary part of the protocol rather than a fault.
//
// NO CASE HERE EVER CARRIES A KAFKA PROPERTY. `kafkaProperties` holds credentials, and the natural
// rendering of a configuration error would put them in a log line, so these messages name property
// KEYS at most and the `Configure` message is never formatted into one.
//
// An enum rather than a class hierarchy because that is what Swift reaches for, and because it makes
// the set exhaustive at every `catch` site: a new failure mode cannot be added without every
// switch over it being revisited.

/// Everything this library throws.
public enum ProxyClientError: Error, Sendable {
    /// The options could not be used to open a session - caught before the sidecar is spawned.
    case options(String)

    /// The sidecar process could not be started, did not announce a port, or could not be reaped.
    case sidecar(String)

    /// The gRPC connection or the session stream failed.
    case transport(String)

    /// The proxy did something the frozen protocol does not permit - including dispatching past the
    /// in-flight ceiling it declared itself.
    case protocolViolation(String)

    /// A step of connecting or shutting down did not finish inside its budget.
    case timedOut(String)

    /// `poll` was called twice on one client.
    case alreadyPolling
}

extension ProxyClientError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .options(let detail):
            return "the client options are not usable: \(detail)"
        case .sidecar(let detail):
            return "the sidecar process: \(detail)"
        case .transport(let detail):
            return "the session transport: \(detail)"
        case .protocolViolation(let detail):
            return "protocol violation: \(detail)"
        case .timedOut(let detail):
            return "timed out \(detail)"
        case .alreadyPolling:
            return "poll has already been called on this client"
        }
    }
}
