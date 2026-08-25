// Copyright (C) 2026 Antony Stubbs and contributors
//
// THE LIBRARY SAYS NOTHING UNTIL THE APPLICATION ASKS IT TO. A library that writes to stdout or
// stderr unasked corrupts programs whose stdout is data, and appears in logs whose format it does
// not know.
//
// C++ HAS NO LOGGING FACADE, and the client-authoring guide's §10.2 table has no C++ row - it names
// SLF4J for the JVM, `logging` for Python, `log/slog` for Go and so on, and for TypeScript it
// settles on an injectable interface with the note that the absence of a facade "is the answer, not
// a gap". C++ is in exactly that position, only more so: spdlog, glog, Boost.Log and absl::log all
// exist and none is the ecosystem's, and a client library that picked one would impose it on every
// application that linked this in. So the mechanism here is TypeScript's: a std::function on the
// options, absent by default, and nothing is emitted while it is absent.
//
// The guide is accumulative and this row is owed back to it - see docs/inflight/clients/cpp.md.

#ifndef PARALLELCONSUMER_PROXY_LOGGING_H
#define PARALLELCONSUMER_PROXY_LOGGING_H

#include <functional>
#include <string>

namespace parallelconsumer::proxy {

/// What a line is worth, so the application can route or drop it without parsing text.
///
/// The levels mean what the authoring guide's §10.3 says they mean: INFO is once-per-session facts
/// (the port, the connection, what negotiation granted, the session ending and why), WARN is
/// degraded-but-alive, ERROR is over-and-the-application-must-act, DEBUG is per-record and off by
/// default. Roughly four INFO lines for a healthy run is the target.
enum class LogLevel { Debug, Info, Warn, Error };

/// The sink an application plugs in. Empty (the default) means silence.
using Logger = std::function<void(LogLevel, const std::string&)>;

/// Renders a level for a sink that wants to print one.
inline const char* to_string(LogLevel level) {
    switch (level) {
        case LogLevel::Debug:
            return "DEBUG";
        case LogLevel::Info:
            return "INFO";
        case LogLevel::Warn:
            return "WARN";
        case LogLevel::Error:
            return "ERROR";
    }
    return "INFO";
}

}  // namespace parallelconsumer::proxy

#endif  // PARALLELCONSUMER_PROXY_LOGGING_H
