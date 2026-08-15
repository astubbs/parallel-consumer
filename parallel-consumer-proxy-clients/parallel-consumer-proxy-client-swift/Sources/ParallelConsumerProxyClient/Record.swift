// Copyright (C) 2026 Antony Stubbs and contributors
//
// The records crossing the user-facing surface. Keys and values are BYTES: the proxy never
// deserializes and neither does this library - deserialization is the user's code, in the user's
// language.
//
// Both types carry a hand-written `describe()` and NO `CustomStringConvertible`, deliberately. The
// authoring guide's 10.5 rule is that record keys and values appear in no log line at any level,
// and the way that rule gets broken is a default rendering that prints every field it has - which in
// Swift is exactly what a struct's synthesised `debugDescription` and string interpolation of a
// struct give you for free. A type that cannot render its own payload is safe by construction; one
// that can is safe only until the next log line is written.

import Foundation

/// One Kafka record as the user's function sees it, plus the delivery state an in-process function
/// would have had.
///
/// An absent key and an empty key are different, deliberately, and so are an absent and an empty
/// value: Kafka distinguishes a null key from an empty one, and a tombstone from an empty payload,
/// so these are `Data?` rather than `Data`.
public struct InboundRecord: Sendable {
    /// The topic the record was consumed from.
    public let topic: String
    /// The partition it came from.
    public let partition: Int32
    /// Its offset within that partition.
    public let offset: Int64
    /// The record key, or `nil` for a null key.
    public let key: Data?
    /// The record value, or `nil` for a tombstone.
    public let value: Data?

    /// 1 on first delivery, 2 on the first redelivery. Product data, distinct from the opaque
    /// fencing token, which never reaches this surface at all.
    public let attempt: Int32

    /// Whether this delivery follows a recorded failure. Presence of the failure timestamp is the
    /// wire's way of saying "this has failed before", never a zero timestamp.
    public let hasFailedBefore: Bool

    /// The previous failure's text, verbatim. Worker-supplied and may embed record payload: treat it
    /// as untrusted input wherever it is handled.
    public let lastFailureReason: String?

    /// The key decoded as UTF-8, for the common case of a text key.
    ///
    /// A convenience over `key`, not a change of contract: the wire carries bytes, and a key that is
    /// not valid UTF-8 is `nil` here while still being present in `key`.
    public var keyText: String? {
        guard let key else { return nil }
        return String(data: key, encoding: .utf8)
    }

    /// Topic, partition, offset and attempt - which identify a record completely for every
    /// diagnostic purpose, and none of which is user data. The payload is deliberately absent.
    public func describe() -> String {
        "InboundRecord{topic=\(topic), partition=\(partition), offset=\(offset), attempt=\(attempt)}"
    }
}

/// A record the user's function asks the proxy to produce on success.
///
/// Workers never touch Kafka themselves: output rides the success report and the proxy produces it
/// with its own producer.
public struct OutboundRecord: Sendable {
    /// The destination topic, or `nil` for the proxy's configured default.
    public let topic: String?
    /// The key to produce, or `nil` for a null key.
    public let key: Data?
    /// The value to produce, or `nil` for a tombstone.
    public let value: Data?

    public init(topic: String? = nil, key: Data? = nil, value: Data? = nil) {
        self.topic = topic
        self.key = key
        self.value = value
    }

    /// Convenience for the common text case. The wire still carries bytes.
    public init(topic: String? = nil, key: String?, value: String?) {
        self.init(
            topic: topic,
            key: key.map { Data($0.utf8) },
            value: value.map { Data($0.utf8) }
        )
    }

    /// The topic and the payload SIZES - never the payload itself. See the file comment.
    public func describe() -> String {
        "OutboundRecord{topic=\(topic ?? "<default>"), keyBytes=\(key?.count ?? 0), "
            + "valueBytes=\(value?.count ?? 0)}"
    }
}
