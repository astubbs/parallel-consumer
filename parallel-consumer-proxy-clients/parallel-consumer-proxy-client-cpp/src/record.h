// Copyright (C) 2026 Antony Stubbs and contributors
//
// The records crossing the user-facing surface. Keys and values are BYTES: the proxy never
// deserializes and neither does this library - deserialization is the user's code, in the user's
// language.
//
// Both types carry a hand-written `describe()` and NO operator<<, deliberately. The authoring
// guide's §10.5 rule is that record keys and values appear in no log line at any level, and the way
// that rule gets broken is a default rendering that prints every field it has. A type that cannot
// render its own payload is safe by construction; one that can is safe only until the next log line
// is written.

#ifndef PARALLELCONSUMER_PROXY_RECORD_H
#define PARALLELCONSUMER_PROXY_RECORD_H

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace parallelconsumer::proxy {

/// One Kafka record as the user's function sees it, plus the delivery state an in-process function
/// would have had.
///
/// An absent key and an empty key are different, deliberately, and so are an absent and an empty
/// value: Kafka distinguishes a null key from an empty one, and a tombstone from an empty payload.
struct InboundRecord {
    /// The topic the record was consumed from.
    std::string topic;
    /// The partition it came from.
    std::int32_t partition = 0;
    /// Its offset within that partition.
    std::int64_t offset = 0;
    /// The record key, or absent for a null key.
    std::optional<std::string> key;
    /// The record value, or absent for a tombstone.
    std::optional<std::string> value;

    /// 1 on first delivery, 2 on the first redelivery. Product data, distinct from the opaque
    /// fencing token.
    std::int32_t attempt = 0;

    /// Whether this delivery follows a recorded failure. Presence of the failure timestamp is the
    /// wire's way of saying "this has failed before", never a zero timestamp.
    bool has_failed_before = false;

    /// The previous failure's text, verbatim. Worker-supplied and may embed record payload: treat
    /// it as untrusted input wherever it is handled.
    std::optional<std::string> last_failure_reason;

    /// Topic, partition, offset and attempt - which identify a record completely for every
    /// diagnostic purpose, and none of which is user data. The payload is deliberately absent.
    [[nodiscard]] std::string describe() const;
};

/// A record the user's function asks the proxy to produce on success.
///
/// Workers never touch Kafka themselves: output rides the success report and the proxy produces it
/// with its own producer.
struct OutboundRecord {
    /// The destination topic, or absent for the proxy's configured default.
    std::optional<std::string> topic;
    /// The key to produce, or absent for a null key.
    std::optional<std::string> key;
    /// The value to produce, or absent for a tombstone.
    std::optional<std::string> value;

    /// The topic and the payload SIZES - never the payload itself. See the file comment.
    [[nodiscard]] std::string describe() const;
};

}  // namespace parallelconsumer::proxy

#endif  // PARALLELCONSUMER_PROXY_RECORD_H
