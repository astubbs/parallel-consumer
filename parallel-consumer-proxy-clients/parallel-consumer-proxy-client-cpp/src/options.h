// Copyright (C) 2026 Antony Stubbs and contributors
//
// Connect-time configuration: the whole of what a session is configured with, and the only place
// configuration ever travels. Nothing here reaches the proxy by argv, environment or file.

#ifndef PARALLELCONSUMER_PROXY_OPTIONS_H
#define PARALLELCONSUMER_PROXY_OPTIONS_H

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "logging.h"

namespace parallelconsumer::proxy::v1 {
class Configure;
}

namespace parallelconsumer::proxy {

/// The capability tokens this protocol defines. A duty exists on a session IFF its token is in the
/// negotiated set that comes back in Session::capabilities, so this is how a client decides what it
/// owes rather than what it hopes.
namespace capability {
/// `Dispatch` waves, proxy to client.
inline constexpr const char* kDispatch = "dispatch";
/// `Heartbeat` and the liveness lease, client to proxy.
inline constexpr const char* kHeartbeat = "heartbeat";
/// `Manifest` reconnects and the `Drop` replies to them.
inline constexpr const char* kManifest = "manifest";
/// `WorkerDied`, client to proxy.
inline constexpr const char* kWorkerDeath = "worker-death";
/// `Shutdown`, proxy to client, and the `Released` outcome that answers it.
inline constexpr const char* kShutdown = "shutdown";
/// The `Terminal` outcome.
inline constexpr const char* kTerminal = "terminal";
}  // namespace capability

/// Where the sidecar's own diagnostics go.
enum class SidecarStderr {
    /// Inherit this process's stderr, so the sidecar's log lines appear alongside the application's.
    /// The default: silencing a child process's diagnostics by default is how a misconfigured
    /// broker becomes an unexplained hang. Inheriting is also safe by construction - there is no
    /// pipe to fill, so nothing can block the writer.
    Inherit,
    /// Send the sidecar's stderr to the null device. Safe for the same reason - a null device is
    /// not a pipe. (Closing the descriptor outright is NOT an option this enum offers: the child
    /// would then write to a closed descriptor whose number is free to be reused by the next file
    /// it opens.)
    Null,
};

/// The engine's ordering modes. Absent means "take the proxy's default"; the effective value comes
/// back in Session.
enum class ProcessingOrder { Unordered, Partition, Key };

/// The whole of a session's configuration.
struct ClientOptions {
    /// The ABSOLUTE path of the sidecar binary. It is never resolved through PATH or relative to
    /// the working directory: this process hands the sidecar the Kafka credentials, so which binary
    /// runs is security-relevant.
    std::string sidecar_path;

    /// Arguments passed to that binary verbatim. They carry no proxy configuration - the
    /// conformance harness takes its fixture selection this way, which is its own documented
    /// exception, not a licence to configure a shipped sidecar by flag.
    std::vector<std::string> sidecar_args;

    /// Where the sidecar's stderr goes.
    SidecarStderr sidecar_stderr = SidecarStderr::Inherit;

    /// The subscription, fixed for the sidecar's lifetime. Exactly one of this and topic_pattern
    /// must be set.
    std::vector<std::string> topics;

    /// A subscription by pattern instead of by name.
    std::optional<std::string> topic_pattern;

    /// The proxy's in-flight ceiling. Absent means the proxy's default. There is no "unlimited".
    std::optional<std::int32_t> max_concurrency;

    /// The Kafka connection settings and credentials the proxy builds its clients from.
    ///
    /// THIS MAP IS NEVER LOGGED, never echoed in an error, and never written anywhere but the
    /// stream - including by this type's own describe(), which prints its size and not its contents.
    std::map<std::string, std::string> kafka_properties;

    /// The capability tokens to declare. Empty declares exactly what this library implements, which
    /// is the right answer for every caller that has not extended it.
    std::vector<std::string> capabilities;

    /// The processing order to ask for.
    std::optional<ProcessingOrder> ordering;

    /// How often the proxy commits.
    std::optional<std::chrono::milliseconds> commit_interval;

    /// How long a failed record waits before redelivery.
    std::optional<std::chrono::milliseconds> default_message_retry_delay;

    /// How long the proxy's own drain may take at shutdown.
    std::optional<std::chrono::milliseconds> drain_timeout;

    /// Asks for terminal-outcome resolution to this topic. It only takes effect when the session
    /// also negotiates capability::kTerminal.
    std::optional<std::string> terminal_topic;

    /// Tags the engine's metrics and logging.
    std::optional<std::string> instance_tag;

    /// Budget for the whole of connecting: spawning the sidecar, reading its port line, the TCP
    /// connection, and the handshake.
    std::chrono::milliseconds connect_timeout{std::chrono::seconds(30)};

    /// How long shutdown() waits for the proxy to complete the stream, and then for the sidecar to
    /// exit, before it stops being polite.
    std::chrono::milliseconds shutdown_grace{std::chrono::seconds(15)};

    /// Where this library's own log lines go. Empty - the default - means it emits nothing at all.
    Logger logger;

    /// Refuses options that cannot open a session, before anything is spawned.
    ///
    /// @throws OptionsError naming the field
    void validate() const;

    /// Renders these options as the first message of a fresh session.
    void write_configure(v1::Configure& configure) const;

    /// A rendering that CANNOT print a credential.
    ///
    /// Hand-written rather than derived, and that is the rule rather than the call sites' discipline:
    /// every language has a default renderer that prints every field it has - a record's ToString, a
    /// dataclass's __repr__, a Lombok @ToString - and this type's would print the whole property map
    /// into any line that mentioned the object. A type that cannot render its own credentials is
    /// safe by construction; relying on call-site discipline means auditing every future log line.
    [[nodiscard]] std::string describe() const;
};

/// What this library honours today, and therefore exactly what it declares when the caller names
/// nothing.
///
/// DECLARING NOTHING WOULD BE WORSE THAN DECLARING A SUBSET: an empty list means "the v1 baseline"
/// on the wire, which entitles the proxy to send heartbeat, manifest, worker-death and shutdown
/// traffic this client does not answer - and un-answered heartbeats arm a lease-expiry redelivery
/// loop. The wave that implements a duty adds its token here, so the declaration cannot fall out of
/// step with the code by omission.
const std::vector<std::string>& implemented_capabilities();

}  // namespace parallelconsumer::proxy

#endif  // PARALLELCONSUMER_PROXY_OPTIONS_H
