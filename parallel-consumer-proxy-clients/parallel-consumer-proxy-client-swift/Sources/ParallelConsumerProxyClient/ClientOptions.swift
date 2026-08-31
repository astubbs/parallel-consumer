// Copyright (C) 2026 Antony Stubbs and contributors
//
// Connect-time configuration: the whole of what a session is configured with, and the only place
// configuration ever travels. Nothing here reaches the proxy by argv, environment or file.

import Foundation
import Logging
import ParallelConsumerProxyProtocol
import SwiftProtobuf

/// The capability tokens this protocol defines.
///
/// A duty exists on a session IF AND ONLY IF its token is in the negotiated set that comes back in
/// `Session.capabilities`, so this is how a client decides what it owes rather than what it hopes.
public enum Capability {
    /// `Dispatch` waves, proxy to client.
    public static let dispatch = "dispatch"
    /// `Heartbeat` and the liveness lease, client to proxy.
    public static let heartbeat = "heartbeat"
    /// `Manifest` reconnects and the `Drop` replies to them.
    public static let manifest = "manifest"
    /// `WorkerDied`, client to proxy.
    public static let workerDeath = "worker-death"
    /// `Shutdown`, proxy to client, and the `Released` outcome that answers it.
    public static let shutdown = "shutdown"
    /// The `Terminal` outcome.
    public static let terminal = "terminal"
}

/// What this library honours today, and therefore exactly what it declares when the caller names
/// nothing.
///
/// DECLARING NOTHING WOULD BE WORSE THAN DECLARING A SUBSET: an empty list means "the v1 baseline"
/// on the wire - the complete frozen v1 message set - which entitles the proxy to send heartbeat,
/// manifest, worker-death and shutdown traffic this client does not answer, and un-answered
/// heartbeats arm a lease-expiry redelivery loop. The wave that implements a duty adds its token
/// here, so the declaration cannot fall out of step with the code by omission.
public let implementedCapabilities: [String] = [Capability.dispatch]

/// Where the sidecar's own diagnostics go.
public enum SidecarStandardError: Sendable {
    /// Inherit this process's stderr, so the sidecar's log lines appear alongside the application's.
    ///
    /// The default: silencing a child process's diagnostics by default is how a misconfigured broker
    /// becomes an unexplained hang. Inheriting is also safe by construction - there is no pipe to
    /// fill, so nothing can block the writer.
    case inherit
    /// Send the sidecar's stderr to the null device. Safe for the same reason - a null device is not
    /// a pipe. (Closing the descriptor outright is NOT an option this enum offers: the child would
    /// then write to a closed descriptor whose number is free to be reused by the next file it
    /// opens.)
    case discard
}

/// The engine's ordering modes. `nil` means "take the proxy's default"; the effective value comes
/// back in `Session`.
public enum ProcessingOrder: Sendable {
    case unordered
    case partition
    case key
}

/// The whole of a session's configuration.
public struct ClientOptions: Sendable {
    /// The ABSOLUTE path of the sidecar binary.
    ///
    /// It is never resolved through `PATH` or relative to the working directory: this process hands
    /// the sidecar the Kafka credentials, so which binary runs is security-relevant.
    public var sidecarPath: String

    /// Arguments passed to that binary verbatim.
    ///
    /// They carry no proxy configuration - the conformance harness takes its fixture selection this
    /// way, which is its own documented exception, not a licence to configure a shipped sidecar by
    /// flag.
    public var sidecarArguments: [String] = []

    /// Where the sidecar's stderr goes.
    public var sidecarStandardError: SidecarStandardError = .inherit

    /// The subscription, fixed for the sidecar's lifetime. Exactly one of this and `topicPattern`
    /// must be set.
    public var topics: [String] = []

    /// A subscription by pattern instead of by name.
    public var topicPattern: String?

    /// The proxy's in-flight ceiling. `nil` means the proxy's default. There is no "unlimited".
    public var maxConcurrency: Int32?

    /// The Kafka connection settings and credentials the proxy builds its clients from.
    ///
    /// THIS DICTIONARY IS NEVER LOGGED, never echoed in an error, and never written anywhere but the
    /// stream - including by this type's own ``describe()``, which prints its size and not its
    /// contents.
    public var kafkaProperties: [String: String] = [:]

    /// The capability tokens to declare. Empty declares ``implementedCapabilities``, which is the
    /// right answer for every caller that has not extended this library.
    public var capabilities: [String] = []

    /// The processing order to ask for.
    public var ordering: ProcessingOrder?

    /// How often the proxy commits.
    public var commitInterval: Duration?

    /// How long a failed record waits before redelivery.
    public var defaultMessageRetryDelay: Duration?

    /// How long the proxy's own drain may take at shutdown.
    public var drainTimeout: Duration?

    /// Asks for terminal-outcome resolution to this topic. It only takes effect on a session that
    /// also negotiates ``Capability/terminal``.
    public var terminalTopic: String?

    /// Tags the engine's metrics and logging.
    public var instanceTag: String?

    /// Budget for the whole of connecting: spawning the sidecar, reading its port line, the TCP
    /// connection, and the handshake.
    public var connectTimeout: Duration = .seconds(30)

    /// How long ``ParallelConsumerClient/shutdown()`` waits for the proxy to complete the stream,
    /// and then for the sidecar to exit, before it stops being polite.
    public var shutdownGrace: Duration = .seconds(15)

    /// Where this library's own log lines go.
    ///
    /// THE DEFAULT IS SILENCE, and it has to be spelled out: swift-log's own default handler writes
    /// to standard output at `info`, so a `Logger` taken from `LoggingSystem` with no bootstrap
    /// would have this library printing into a program whose stdout may be data. `SwiftLogNoOpLogHandler`
    /// is the facade's own way of saying nothing. An application plugs in by assigning its own
    /// `Logger`, configured however it configures logging.
    public var logger: Logger = Logger(label: "bz.stub.parallelconsumer.proxy") { _ in
        SwiftLogNoOpLogHandler()
    }

    public init(sidecarPath: String) {
        self.sidecarPath = sidecarPath
    }

    /// Refuses options that cannot open a session, before anything is spawned.
    public func validate() throws {
        guard !sidecarPath.isEmpty else {
            throw ProxyClientError.options("sidecarPath is required")
        }
        guard sidecarPath.hasPrefix("/") else {
            throw ProxyClientError.options(
                "sidecarPath must be absolute, so that which binary receives the Kafka credentials "
                    + "is not decided by PATH or the working directory; got '\(sidecarPath)'")
        }
        let named = !topics.isEmpty
        let patterned = !(topicPattern ?? "").isEmpty
        guard named != patterned else {
            throw ProxyClientError.options(
                "exactly one of topics and topicPattern must be set (topics: \(topics.count), "
                    + "topicPattern: \(patterned ? "set" : "unset"))")
        }
        if let maxConcurrency, maxConcurrency < 1 {
            throw ProxyClientError.options("maxConcurrency must be at least 1 when set")
        }
    }

    /// The capability tokens this session will actually declare.
    public var declaredCapabilities: [String] {
        capabilities.isEmpty ? implementedCapabilities : capabilities
    }

    /// Renders these options as the first message of a fresh session.
    func makeConfigure() -> PCPConfigure {
        var configure = PCPConfigure()
        configure.topics = topics
        if let topicPattern { configure.topicPattern = topicPattern }
        if let maxConcurrency { configure.maxConcurrency = maxConcurrency }
        configure.kafkaProperties = kafkaProperties
        configure.capabilities = declaredCapabilities
        if let ordering {
            switch ordering {
            case .unordered: configure.ordering = .unordered
            case .partition: configure.ordering = .partition
            case .key: configure.ordering = .key
            }
        }
        if let commitInterval { configure.commitInterval = .from(commitInterval) }
        if let defaultMessageRetryDelay {
            configure.defaultMessageRetryDelay = .from(defaultMessageRetryDelay)
        }
        if let drainTimeout { configure.drainTimeout = .from(drainTimeout) }
        if let terminalTopic { configure.terminalTopic = terminalTopic }
        if let instanceTag { configure.pcInstanceTag = instanceTag }
        return configure
    }

    /// A rendering that CANNOT print a credential.
    ///
    /// Hand-written rather than derived, and that is the rule rather than the call sites' discipline:
    /// Swift will interpolate any struct into a string using a synthesised description that prints
    /// every stored property it has, so an options type carrying `kafkaProperties` would put the
    /// whole credential map into any log line that mentioned the object. A type that cannot render
    /// its own credentials is safe by construction; relying on call-site discipline means auditing
    /// every future log line. The same applies to the generated `PCPConfigure`, whose protobuf text
    /// format prints the map in full - it is never logged whole.
    public func describe() -> String {
        let subscription = topicPattern.map { "pattern:\($0)" } ?? topics.joined(separator: ",")
        return "ClientOptions{sidecar=\(sidecarPath), topics=[\(subscription)], "
            + "maxConcurrency=\(maxConcurrency.map(String.init) ?? "<proxy default>"), "
            + "kafkaProperties=\(kafkaProperties.count) entries (redacted), "
            + "capabilities=[\(declaredCapabilities.joined(separator: ","))]}"
    }
}

extension Google_Protobuf_Duration {
    /// The wire form of a Swift `Duration`. Attoseconds are truncated to nanoseconds, which is the
    /// finest resolution the protobuf type has.
    static func from(_ duration: Duration) -> Google_Protobuf_Duration {
        let (seconds, attoseconds) = duration.components
        var wire = Google_Protobuf_Duration()
        wire.seconds = seconds
        wire.nanos = Int32(attoseconds / 1_000_000_000)
        return wire
    }
}
