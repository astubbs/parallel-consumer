// Copyright (C) 2026 Antony Stubbs and contributors
//
// The session: one sidecar process, one gRPC stream, one dispatch queue, `executor_count`
// executors.

#ifndef PARALLELCONSUMER_PROXY_CLIENT_H
#define PARALLELCONSUMER_PROXY_CLIENT_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "dispatch_queue.h"
#include "options.h"
#include "outcome.h"
#include "parallelconsumer/proxy/v1/proxy.grpc.pb.h"
#include "parallelconsumer/proxy/v1/proxy.pb.h"
#include "session.h"
#include "sidecar.h"

namespace parallelconsumer::proxy {

/// One session, from the handshake to the half-close.
///
/// connect() opens it, poll() starts processing, shutdown() ends it cleanly. Destroying the client
/// without shutting it down still stops the sidecar - closing the lifecycle pipe is the parent-death
/// signal - but it skips the drain, so the proxy recovers by rebalance rather than by a clean commit.
///
/// DOES poll() BLOCK? No, and the caller observes the session's end through session_end().
/// The authoring guide settled that the SHAPE is each language's own and the PROPERTY is not: the
/// caller must be able to learn the session ended, and why, without ending the client to find out.
/// std::shared_future<void> is the C++ spelling of the JVM's CompletionStage<Void> sessionEnd() -
/// wait on it, poll it with wait_for, or ignore it; get() rethrows the cause when the session died
/// of one. It is an accessor rather than poll()'s return value because a session can die before or
/// without a poll: a client that only connected still has an end to observe.
class Client {
public:
    /// Spawns the sidecar, connects to it, and completes the fresh-session handshake. It returns
    /// once the proxy's effective configuration has arrived - only then is the session open.
    ///
    /// @throws OptionsError, SidecarError, TransportError, TimeoutError, ProtocolError
    static std::unique_ptr<Client> connect(ClientOptions options);

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    /// Stops the session without draining. shutdown() is the supported route.
    ~Client();

    /// The effective configuration this session is running with - what the proxy replied, including
    /// the negotiated capability set. Assert on this, never on the options.
    [[nodiscard]] const Session& session() const { return session_; }

    /// Starts processing with the user's function and RETURNS IMMEDIATELY. At most once per client.
    ///
    /// @throws ClientError if this client is already processing
    void poll(RecordProcessor processor);

    /// Becomes ready when the session's stream has ended - because the proxy completed it, because
    /// it failed, or because this client shut it down. get() rethrows the cause if it was a fault.
    [[nodiscard]] std::shared_future<void> session_end() const { return session_end_; }

    /// The client-initiated shutdown: stop handing records out, let executing records finish and
    /// report, then half-close the stream and reap the sidecar.
    ///
    /// THE HALF-CLOSE IS THE SHUTDOWN SIGNAL - there is no shutdown-request message, because a
    /// client that has reported everything it ran has nothing left to say.
    ///
    /// @throws the session's FIRST fault, if it had one - including one the transport thread
    ///         recorded while the application was doing something else
    void shutdown();

private:
    class SettleGuard;

    Client(ClientOptions options, std::unique_ptr<Sidecar> sidecar);

    void transport_loop();
    void writer_loop();
    void executor_loop(const RecordProcessor& processor);
    void run_one(const RecordProcessor& processor, const v1::DispatchRecord& dispatched);

    void send(v1::ClientMessage&& message);
    void close_outbound();
    void fail(std::exception_ptr problem);
    void end_session();
    void log(LogLevel level, const std::string& line) const;

    ClientOptions options_;
    std::unique_ptr<Sidecar> sidecar_;

    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<v1::ProxyService::Stub> stub_;
    std::unique_ptr<grpc::ClientContext> context_;
    std::unique_ptr<grpc::ClientReaderWriter<v1::ClientMessage, v1::ProxyMessage>> stream_;

    Session session_;
    std::promise<void> configured_;

    /// The dispatch queue AND the unresolved count, in one place - see dispatch_queue.h for why
    /// those two are the same object.
    DispatchQueue queue_;

    mutable std::mutex outbound_mutex_;
    std::condition_variable outbound_cv_;
    std::deque<v1::ClientMessage> outbound_;
    bool outbound_closed_ = false;

    mutable std::mutex failure_mutex_;
    std::exception_ptr failure_;

    std::promise<void> session_end_promise_;
    std::shared_future<void> session_end_;
    std::once_flag session_ended_;

    std::thread transport_;
    std::thread writer_;
    std::vector<std::thread> executors_;
    std::atomic<bool> polled_{false};
};

}  // namespace parallelconsumer::proxy

#endif  // PARALLELCONSUMER_PROXY_CLIENT_H
