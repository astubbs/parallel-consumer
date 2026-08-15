// Copyright (C) 2026 Antony Stubbs and contributors
//
// The sidecar child process and the lifecycle pipe that keeps it alive.

#ifndef PARALLELCONSUMER_PROXY_SIDECAR_H
#define PARALLELCONSUMER_PROXY_SIDECAR_H

#include <sys/types.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include "options.h"

namespace parallelconsumer::proxy {

/// The proxy child process.
///
/// THE STDIN PIPE IS THE PARENT-DEATH SIGNAL: this process holds the write end and never writes to
/// it, so EOF on the child's stdin is proof the parent is gone. That is why the binary is launched
/// DIRECTLY, with execv and never through a shell - a shell wrapper would hold the write end open
/// and leak a JVM that still holds group membership.
class Sidecar {
public:
    /// Spawns the sidecar and waits for its port line.
    ///
    /// @throws SidecarError if it cannot be started or its stdout ends without announcing a port
    /// @throws TimeoutError if the port line does not arrive inside the connect budget
    explicit Sidecar(const ClientOptions& options);

    Sidecar(const Sidecar&) = delete;
    Sidecar& operator=(const Sidecar&) = delete;

    /// Closes the lifecycle pipe and reaps the child, killing it only if it will not go.
    ~Sidecar();

    /// The loopback port the proxy announced.
    [[nodiscard]] std::uint16_t port() const { return port_; }

    /// Closes the lifecycle pipe and waits up to `grace` for the child to exit.
    ///
    /// CLOSING STDIN IS THE REAP: it is the parent-death signal the proxy watches, and it is also
    /// the only thing that ends the conformance harness, which serves until stdin EOF and does not
    /// exit after a clean drain. Killing is the backstop for a child that honours neither.
    ///
    /// @return empty when the child exited on its own; otherwise what went wrong, for the caller to
    ///         report - a destructor cannot throw, and this is called from one
    std::optional<std::string> stop(std::chrono::milliseconds grace);

    /// The last lines the sidecar wrote, most recent last.
    ///
    /// Bounded, because an unbounded buffer of a chatty child's output is a leak of its own; kept at
    /// all, because the last lines before a crash are the whole explanation and a spawn that fails
    /// without them costs an afternoon.
    [[nodiscard]] std::string recent_output() const;

    /// The port from a lifecycle line, if this line is one.
    ///
    /// The specification's contract is that the port is stdout's FIRST line. The conformance harness
    /// diverges - it logs before it - and the authoring guide says a test absorbs that rather than
    /// asserting the position, so this SCANS rather than reading exactly one line. Scanning
    /// satisfies both.
    static std::optional<std::uint16_t> parse_port_line(const std::string& line);

private:
    void drain_stdout();
    void record_line(const std::string& line);

    std::string path_;
    int lifecycle_fd_ = -1;  // the child's stdin write end: held open, never written to
    int stdout_fd_ = -1;
    pid_t pid_ = -1;
    std::uint16_t port_ = 0;
    std::thread drain_;

    mutable std::mutex mutex_;
    std::deque<std::string> tail_;
    std::optional<std::uint16_t> announced_port_;
    bool stdout_ended_ = false;
    std::condition_variable port_arrived_;
};

}  // namespace parallelconsumer::proxy

#endif  // PARALLELCONSUMER_PROXY_SIDECAR_H
