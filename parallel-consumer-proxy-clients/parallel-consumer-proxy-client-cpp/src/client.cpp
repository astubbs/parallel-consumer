// Copyright (C) 2026 Antony Stubbs and contributors

#include "client.h"

#include <chrono>
#include <exception>
#include <string>
#include <utility>

#include "error.h"

namespace parallelconsumer::proxy {
namespace {

/// Whether a stream status is the ordinary end of a session rather than a fault: the call being
/// cancelled by this client, or the peer going away once it has drained.
bool is_session_end(const grpc::Status& status) {
    return status.ok() || status.error_code() == grpc::StatusCode::CANCELLED ||
           status.error_code() == grpc::StatusCode::UNAVAILABLE;
}

/// The NAME of a proxy message, never its content - a dispatch's records carry payload, and an
/// error message is not the place for it.
const char* message_kind(const v1::ProxyMessage& message) {
    switch (message.message_case()) {
        case v1::ProxyMessage::kConfigured:
            return "Configured";
        case v1::ProxyMessage::kDispatch:
            return "Dispatch";
        case v1::ProxyMessage::kDrop:
            return "Drop";
        case v1::ProxyMessage::kShutdown:
            return "Shutdown";
        case v1::ProxyMessage::kSetExecutorCount:
            return "SetExecutorCount";
        case v1::ProxyMessage::MESSAGE_NOT_SET:
            break;
    }
    return "an empty message";
}

InboundRecord to_inbound(const v1::DispatchRecord& dispatched) {
    const v1::Record& record = dispatched.record();
    InboundRecord inbound;
    inbound.topic = record.topic();
    inbound.partition = record.partition();
    inbound.offset = record.offset();
    // Absence and emptiness are different in both byte fields: a null key is not an empty key, and
    // a tombstone is not an empty value.
    if (record.has_key()) {
        inbound.key = record.key();
    }
    if (record.has_value()) {
        inbound.value = record.value();
    }
    inbound.attempt = dispatched.attempt();
    inbound.has_failed_before = dispatched.has_last_failure_at();
    if (dispatched.has_last_failure_reason()) {
        inbound.last_failure_reason = dispatched.last_failure_reason();
    }
    return inbound;
}

}  // namespace

/// Frees the record's slot against the in-flight ceiling when the scope ends, however it ends.
///
/// A destructor rather than a call at the bottom of run_one, because §3.2 requires the decrement
/// where an executor dying mid-record cannot skip it - the C++ spelling of the guide's
/// finally/ensure/defer. Skip it once and the ceiling shrinks permanently, one slot per crash, and
/// the client eventually declares a protocol violation against a correct proxy.
class Client::SettleGuard {
public:
    explicit SettleGuard(Client& client) : client_(client) {}
    SettleGuard(const SettleGuard&) = delete;
    SettleGuard& operator=(const SettleGuard&) = delete;
    ~SettleGuard() { client_.queue_.settle(); }

private:
    Client& client_;
};

Client::Client(ClientOptions options, std::unique_ptr<Sidecar> sidecar)
    : options_(std::move(options)), sidecar_(std::move(sidecar)) {
    session_end_ = session_end_promise_.get_future().share();
}

std::unique_ptr<Client> Client::connect(ClientOptions options) {
    options.validate();

    auto sidecar = std::make_unique<Sidecar>(options);
    const std::uint16_t port = sidecar->port();
    // unique_ptr rather than make_unique: the constructor is private, which is deliberate - a client
    // only ever exists connected.
    std::unique_ptr<Client> client(new Client(std::move(options), std::move(sidecar)));

    // The ordinary host:port authority the proxy's loopback allowlist expects; no TLS, no
    // interceptors, no load balancing - the deliberately narrow slice of gRPC the protocol permits,
    // so that every language's implementation suffices.
    const std::string endpoint = "127.0.0.1:" + std::to_string(port);
    client->channel_ = grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
    const auto deadline = std::chrono::system_clock::now() + client->options_.connect_timeout;
    if (!client->channel_->WaitForConnected(deadline)) {
        throw TransportError("could not connect to the sidecar on " + endpoint + " within " +
                             std::to_string(client->options_.connect_timeout.count()) + "ms");
    }
    client->stub_ = v1::ProxyService::NewStub(client->channel_);
    client->context_ = std::make_unique<grpc::ClientContext>();
    client->stream_ = client->stub_->Session(client->context_.get());

    // Configure travels through the ordinary outbound queue, so there is exactly one writer of this
    // stream for the whole session and no special case at the handshake.
    v1::ClientMessage configure;
    client->options_.write_configure(*configure.mutable_configure());
    // NOTE what is NOT logged or put in an error anywhere on this path: the Configure message
    // itself. It carries kafka_properties, and its natural rendering would put credentials in a log
    // line.
    {
        const std::lock_guard<std::mutex> lock(client->outbound_mutex_);
        client->outbound_.push_back(std::move(configure));
    }

    // Both threads start NOW rather than at poll: the proxy may dispatch the moment it is
    // configured, and the stream also carries the control plane, so an admin that is not reading
    // head-of-line-blocks itself.
    client->writer_ = std::thread([raw = client.get()] { raw->writer_loop(); });
    client->transport_ = std::thread([raw = client.get()] { raw->transport_loop(); });

    auto configured = client->configured_.get_future();
    if (configured.wait_for(client->options_.connect_timeout) != std::future_status::ready) {
        client->context_->TryCancel();
        client->end_session();
        throw TimeoutError("awaiting Configured from the sidecar on " + endpoint);
    }
    configured.get();  // rethrows a handshake that failed rather than merely not arriving

    client->log(LogLevel::Info, "session open on " + endpoint + ": " + client->session_.describe());
    return client;
}

Client::~Client() {
    // Best effort, because a destructor cannot report anything: stop hand-out, close the stream, let
    // the sidecar's lifecycle pipe close with this client. shutdown() is the supported route, and
    // the only one that drains.
    queue_.stop_handout();
    queue_.close();
    for (auto& executor : executors_) {
        if (executor.joinable()) {
            executor.join();
        }
    }
    close_outbound();
    if (context_) {
        context_->TryCancel();
    }
    if (transport_.joinable()) {
        transport_.join();
    }
    if (writer_.joinable()) {
        writer_.join();
    }
    sidecar_.reset();
}

void Client::poll(RecordProcessor processor) {
    if (polled_.exchange(true)) {
        throw ClientError("poll has already been called on this client");
    }
    executors_.reserve(static_cast<std::size_t>(session_.executor_count));
    for (std::int32_t index = 0; index < session_.executor_count; ++index) {
        executors_.emplace_back([this, processor] { executor_loop(processor); });
    }
    log(LogLevel::Info, "processing with " + std::to_string(session_.executor_count) + " executors");
}

void Client::shutdown() {
    queue_.stop_handout();
    for (auto& executor : executors_) {
        if (executor.joinable()) {
            executor.join();
        }
    }
    executors_.clear();

    // QUEUED RECORDS ARE DISCARDED, and that is the specification's own consequence rather than a
    // shortcut. The guide says to report them Released - but Released is gated by the `shutdown`
    // capability, which this client does not implement and therefore does not declare, and sending
    // an outcome outside the negotiated set is itself a violation. So they are dropped and the proxy
    // returns them to scheduling by the same path it uses for a lost connection, attempt counts
    // unchanged, because it never committed their offsets. The wave that implements the drain sends
    // Released here, under a session_.negotiated(capability::kShutdown) test.
    if (const std::int32_t dropped = queue_.discard_queued(); dropped > 0) {
        log(LogLevel::Warn, "dropped " + std::to_string(dropped) +
                                " queued records at shutdown: this session did not negotiate 'shutdown', "
                                "so Released is not on it and the proxy reclaims them");
    }

    // Half-close: no more sends, ever. Everything run has been reported.
    close_outbound();

    // Give the proxy its drain: it commits, completes the stream, and the transport thread ends on
    // its own. A stream that will not end is cancelled rather than waited on forever.
    if (session_end_.valid() &&
        session_end_.wait_for(options_.shutdown_grace) != std::future_status::ready && context_) {
        log(LogLevel::Warn, "the proxy did not complete the stream within " +
                                std::to_string(options_.shutdown_grace.count()) + "ms of the half-close");
        context_->TryCancel();
    }
    if (transport_.joinable()) {
        transport_.join();
    }
    if (writer_.joinable()) {
        writer_.join();
    }

    if (sidecar_) {
        // Closing the lifecycle pipe is the reap. Never kill a sidecar with the stream still open -
        // that turns a clean drain into a reconnect-window recovery for the next group member.
        if (const auto problem = sidecar_->stop(options_.shutdown_grace)) {
            fail(std::make_exception_ptr(SidecarError(*problem)));
        }
        sidecar_.reset();
    }

    std::exception_ptr problem;
    {
        const std::lock_guard<std::mutex> lock(failure_mutex_);
        problem = failure_;
    }
    if (problem) {
        std::rethrow_exception(problem);
    }
}

void Client::transport_loop() {
    bool configured_seen = false;
    v1::ProxyMessage message;
    while (stream_->Read(&message)) {
        if (message.message_case() == v1::ProxyMessage::kConfigured) {
            if (configured_seen) {
                fail(std::make_exception_ptr(ProtocolError("a second Configured arrived on one session")));
                break;
            }
            try {
                session_ = Session::from_wire(message.configured());
                queue_.configure(session_.max_concurrency);
                configured_seen = true;
                configured_.set_value();
            } catch (...) {
                configured_seen = true;  // answered, even though the answer was a refusal
                fail(std::current_exception());
                configured_.set_exception(std::current_exception());
                break;
            }
        } else if (message.message_case() == v1::ProxyMessage::kDispatch) {
            if (!configured_seen) {
                fail(std::make_exception_ptr(ProtocolError("a Dispatch arrived before Configured")));
                break;
            }
            try {
                // Queued in record order; hand-out is FIFO by arrival and, within a wave, by the
                // wave's own order.
                for (const auto& record : message.dispatch().records()) {
                    queue_.admit(record);
                }
            } catch (...) {
                fail(std::current_exception());
                // Cancelling the call is the WHOLE of a gRPC client's vocabulary for "I am ending
                // this": only a server sets a status, so FAILED_PRECONDITION is not available and
                // the counts travel in the local error instead.
                context_->TryCancel();
                break;
            }
        } else {
            // Every remaining proxy message is gated by a capability this client does not declare,
            // and the rule for an un-negotiated message is that the receiver never acts on it.
            // Recording it keeps the violation visible without failing an otherwise healthy stream.
            fail(std::make_exception_ptr(ProtocolError(
                std::string("the proxy sent ") + message_kind(message) +
                " outside the negotiated capability set - ignored")));
            log(LogLevel::Warn, std::string("dropped an un-negotiated ") + message_kind(message));
        }
        message.Clear();
    }

    // Stop the writer before Finish: gRPC requires WritesDone to have happened, and the writer is
    // the only thread that calls it.
    close_outbound();
    if (writer_.joinable() && writer_.get_id() != std::this_thread::get_id()) {
        writer_.join();
    }
    const grpc::Status status = stream_->Finish();
    if (!is_session_end(status)) {
        fail(std::make_exception_ptr(
            TransportError("the session stream ended: " + status.error_message() + " (code " +
                           std::to_string(static_cast<int>(status.error_code())) + ")")));
    }

    if (!configured_seen) {
        try {
            throw ProtocolError("the stream ended before Configured arrived");
        } catch (...) {
            configured_.set_exception(std::current_exception());
        }
    }

    // Nothing more will arrive, so executors waiting on the queue stop waiting.
    queue_.close();
    end_session();
}

void Client::writer_loop() {
    for (;;) {
        v1::ClientMessage message;
        {
            std::unique_lock<std::mutex> lock(outbound_mutex_);
            outbound_cv_.wait(lock, [this] { return !outbound_.empty() || outbound_closed_; });
            if (outbound_.empty()) {
                break;  // closed and drained
            }
            message = std::move(outbound_.front());
            outbound_.pop_front();
        }
        if (!stream_->Write(message)) {
            break;  // the peer is gone; the read side reports why
        }
    }
    stream_->WritesDone();
}

void Client::executor_loop(const RecordProcessor& processor) {
    v1::DispatchRecord record;
    while (queue_.take(record)) {
        run_one(processor, record);
    }
}

void Client::run_one(const RecordProcessor& processor, const v1::DispatchRecord& dispatched) {
    const SettleGuard guard(*this);

    v1::ClientMessage message;
    v1::Report* report = message.mutable_report();
    // THE TOKEN IS ECHOED VERBATIM - the message the proxy sent, never one rebuilt from parsed
    // parts. It is opaque: nothing here reads record_id or compares epochs.
    *report->mutable_token() = dispatched.token();

    try {
        const Outcome outcome = processor(to_inbound(dispatched));
        if (outcome.is_success()) {
            v1::Report_Success* success = report->mutable_success();
            for (const auto& produced : outcome.produce()) {
                v1::ProduceRecord* wire = success->add_produce();
                if (produced.topic) {
                    wire->set_topic(*produced.topic);
                }
                if (produced.key) {
                    wire->set_key(*produced.key);
                }
                if (produced.value) {
                    wire->set_value(*produced.value);
                }
            }
        } else {
            report->mutable_failure()->set_reason(outcome.reason());
        }
    } catch (const std::exception& thrown) {
        // THE ONE PLACE a thrown exception becomes a failure outcome. A worker that falls over must
        // produce a failure report, not tear down the session.
        report->mutable_failure()->set_reason(thrown.what());
    } catch (...) {
        report->mutable_failure()->set_reason("the record processor threw a non-standard exception");
    }

    send(std::move(message));
    // The slot is freed by the guard, which destructs AFTER this send - a report frees the slot, not
    // an executor picking the record up.
}

void Client::send(v1::ClientMessage&& message) {
    {
        const std::lock_guard<std::mutex> lock(outbound_mutex_);
        if (outbound_closed_) {
            // The session ended before this outcome could be reported. The engine's own paths return
            // the record to scheduling; there is nothing to report to.
            return;
        }
        outbound_.push_back(std::move(message));
    }
    outbound_cv_.notify_one();
}

void Client::close_outbound() {
    {
        const std::lock_guard<std::mutex> lock(outbound_mutex_);
        outbound_closed_ = true;
    }
    outbound_cv_.notify_all();
}

void Client::fail(std::exception_ptr problem) {
    bool first = false;
    {
        const std::lock_guard<std::mutex> lock(failure_mutex_);
        // The session's FIRST fault. Later ones are consequences of it far more often than they are
        // new information.
        if (!failure_) {
            failure_ = std::move(problem);
            first = true;
        }
    }
    if (!first) {
        return;
    }
    // Logged OUTSIDE the lock: the sink is application code, and holding a lock across a call into
    // it is how a library deadlocks its caller.
    //
    // A client that cannot yet tell its caller the session died must at minimum not be silent about
    // it. This one can - session_end() carries the cause - so this line is the diagnostic floor
    // rather than the mechanism.
    try {
        const std::lock_guard<std::mutex> lock(failure_mutex_);
        std::rethrow_exception(failure_);
    } catch (const std::exception& recorded) {
        log(LogLevel::Error, std::string("the session failed: ") + recorded.what());
    } catch (...) {
        log(LogLevel::Error, "the session failed for a reason that is not a std::exception");
    }
}

void Client::end_session() {
    std::call_once(session_ended_, [this] {
        std::exception_ptr problem;
        {
            const std::lock_guard<std::mutex> lock(failure_mutex_);
            problem = failure_;
        }
        if (problem) {
            session_end_promise_.set_exception(problem);
        } else {
            log(LogLevel::Info, "the session ended cleanly");
            session_end_promise_.set_value();
        }
    });
}

void Client::log(LogLevel level, const std::string& line) const {
    if (options_.logger) {
        options_.logger(level, line);
    }
}

}  // namespace parallelconsumer::proxy
