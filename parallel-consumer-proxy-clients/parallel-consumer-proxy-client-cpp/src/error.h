// Copyright (C) 2026 Antony Stubbs and contributors
//
// The library's own failure type - distinct from a failure OUTCOME, which is the user function's
// verdict on a record and an ordinary part of the protocol rather than a fault.
//
// NO EXCEPTION HERE EVER CARRIES A KAFKA PROPERTY. `kafka_properties` holds credentials, and the
// natural rendering of a configuration error would put them in a log line, so these messages name
// property KEYS at most and the Configure message is never formatted into one.

#ifndef PARALLELCONSUMER_PROXY_ERROR_H
#define PARALLELCONSUMER_PROXY_ERROR_H

#include <stdexcept>
#include <string>

namespace parallelconsumer::proxy {

/// Base of everything this library throws. Catch this to catch a session fault of any kind.
class ClientError : public std::runtime_error {
public:
    explicit ClientError(const std::string& message) : std::runtime_error(message) {}
};

/// The options could not be used to open a session - caught before the sidecar is spawned.
class OptionsError : public ClientError {
public:
    explicit OptionsError(const std::string& message)
        : ClientError("the client options are not usable: " + message) {}
};

/// The sidecar process could not be started, did not announce a port, or could not be reaped.
class SidecarError : public ClientError {
public:
    explicit SidecarError(const std::string& message) : ClientError("the sidecar process: " + message) {}
};

/// The gRPC connection or the session stream failed.
class TransportError : public ClientError {
public:
    explicit TransportError(const std::string& message) : ClientError("the session transport: " + message) {}
};

/// The proxy did something the frozen protocol does not permit - including dispatching past the
/// in-flight ceiling it declared itself.
class ProtocolError : public ClientError {
public:
    explicit ProtocolError(const std::string& message) : ClientError("protocol violation: " + message) {}
};

/// A step of connecting or shutting down did not finish inside its budget.
class TimeoutError : public ClientError {
public:
    explicit TimeoutError(const std::string& message) : ClientError("timed out " + message) {}
};

}  // namespace parallelconsumer::proxy

#endif  // PARALLELCONSUMER_PROXY_ERROR_H
