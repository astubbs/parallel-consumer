// Copyright (C) 2026 Antony Stubbs and contributors

#include "dispatch_queue.h"

#include <string>

#include "error.h"

namespace parallelconsumer::proxy {

void DispatchQueue::configure(std::int32_t max_concurrency) {
    const std::lock_guard<std::mutex> lock(mutex_);
    ceiling_ = max_concurrency;
}

void DispatchQueue::admit(const v1::DispatchRecord& record) {
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        if (unresolved_ >= ceiling_) {
            throw ProtocolError(overflow_message(unresolved_, record.token()));
        }
        ++unresolved_;
        queued_.push_back(record);
    }
    changed_.notify_one();
}

bool DispatchQueue::take(v1::DispatchRecord& record) {
    std::unique_lock<std::mutex> lock(mutex_);
    for (;;) {
        changed_.wait(lock, [this] { return stopped_ || closed_ || !queued_.empty(); });
        if (stopped_) {
            return false;
        }
        if (!queued_.empty()) {
            record = std::move(queued_.front());
            queued_.pop_front();
            return true;
        }
        if (closed_) {
            return false;
        }
    }
}

void DispatchQueue::settle() {
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        if (unresolved_ > 0) {
            --unresolved_;
        }
    }
    changed_.notify_one();
}

void DispatchQueue::stop_handout() {
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        stopped_ = true;
    }
    changed_.notify_all();
}

void DispatchQueue::close() {
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        closed_ = true;
    }
    changed_.notify_all();
}

std::int32_t DispatchQueue::discard_queued() {
    std::int32_t dropped = 0;
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        while (!queued_.empty()) {
            queued_.pop_front();
            if (unresolved_ > 0) {
                --unresolved_;
            }
            ++dropped;
        }
        closed_ = true;
    }
    changed_.notify_all();
    return dropped;
}

std::int32_t DispatchQueue::unresolved() const {
    const std::lock_guard<std::mutex> lock(mutex_);
    return unresolved_;
}

std::size_t DispatchQueue::depth() const {
    const std::lock_guard<std::mutex> lock(mutex_);
    return queued_.size();
}

std::string DispatchQueue::overflow_message(std::int32_t already_unresolved, const v1::Token& token) const {
    // RENDERING THE TOKEN IS NOT A BREACH OF ITS OPACITY, and the specification requires it here.
    // Opacity forbids DERIVING - parsing record_id, comparing epochs, branching on either - and says
    // nothing about printing. So the token's fields are printed as they arrived, by the generated
    // message's own renderer, never assembled from parts this client interpreted. A Token carries no
    // credentials, unlike the Configure message §10.4 forbids rendering whole. The guide records
    // that no client did this yet; this one does.
    return "the proxy dispatched record {" + token.ShortDebugString() + "} while " +
           std::to_string(already_unresolved) +
           " were already unresolved - queued plus executing - past the max_concurrency of " +
           std::to_string(ceiling_) +
           " it declared itself, so this is a protocol violation and not load; the call is cancelled "
           "rather than answered with FAILED_PRECONDITION, because a gRPC client cannot set a status";
}

}  // namespace parallelconsumer::proxy
