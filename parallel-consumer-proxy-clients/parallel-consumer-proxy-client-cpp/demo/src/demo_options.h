// Copyright (C) 2026 Antony Stubbs and contributors
//
// The demo's dials, and C++'s copy of the interface every per-language demo mirrors
// (parallel-consumer-proxy/demo/README.md).
//
// FLAGS BEAT THE ENVIRONMENT BEATS THE DEFAULTS - the ordinary convention, and the one the Java
// reference states rather than implies: a container passes configuration by environment while a
// person at a terminal passes flags, and both must be able to override the other's layer.
//
// R39 DOES NOT GOVERN A DEMO. R39 constrains how configuration reaches the PROXY - connect-time,
// over the protocol, never by argv or environment. A demo is an application, so its own flags are
// not a violation of it. Without this note somebody reads `--records` as breaking the plan's rule
// and deletes it.

#ifndef PARALLELCONSUMER_PROXY_DEMO_OPTIONS_H
#define PARALLELCONSUMER_PROXY_DEMO_OPTIONS_H

#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace parallelconsumer::proxy::demo {

/// Where environment values come from. Injected rather than read from `getenv` at the point of use,
/// so the precedence rule can be tested without mutating this process's own environment.
using Environment = std::function<std::optional<std::string>(const std::string&)>;

/// This process's environment, for the one caller that should use it.
Environment process_environment();

/// The prefix every variable this demo reads carries, so a reader can grep one string.
inline constexpr const char* kEnvironmentPrefix = "PC_DEMO_";

/// The demo's effective configuration.
struct DemoOptions {
    int records = 2000;
    int delay_ms = 2;
    int max_concurrency = 100;
    int partitions = 10;
    int replay_factor = 20;

    /// The broker to use. Empty means none was supplied - which this demo cannot recover from, and
    /// says so; see the README's divergence note. Never printed, never logged.
    std::string bootstrap;

    /// The topic to use. Empty means the demo names its own.
    std::string topic;

    /// Whether the caller asked for the usage text rather than a run.
    ///
    /// Answered here rather than only in run.sh because the script is not the only way in:
    /// `docker compose run demo --help` reaches this binary directly.
    static bool help_requested(const std::vector<std::string>& args);

    /// Parses the command line over the environment over the defaults.
    ///
    /// @throws std::invalid_argument on an unknown flag, a missing value, or a value that is not a
    ///         number in range. A demo that silently ignored a misspelled flag would report numbers
    ///         for settings the user did not ask for.
    static DemoOptions parse(const std::vector<std::string>& args, const Environment& environment);

    /// The records the big replay consumes in total, including the small replay's own.
    [[nodiscard]] int big_replay_records() const;

    /// Whether the big replay is worth running at all; a factor of 1 or less skips it.
    [[nodiscard]] bool big_replay_wanted() const;

    /// The effective configuration, printed before the run.
    ///
    /// A number without its settings is not reproducible, so this is part of the contract every
    /// language's demo keeps rather than a debugging aid. THE BOOTSTRAP ADDRESS IS DELIBERATELY
    /// ABSENT: own-cluster mode puts a user's real broker there, and the credential-hygiene rule
    /// that binds the client library binds a demo too.
    [[nodiscard]] std::string describe() const;
};

/// The usage text, identical in wording to the reference demo's.
std::string usage();

}  // namespace parallelconsumer::proxy::demo

#endif  // PARALLELCONSUMER_PROXY_DEMO_OPTIONS_H
