// Copyright (C) 2026 Antony Stubbs and contributors

//! The records crossing the user-facing surface. Keys and values are **bytes**: the proxy never
//! deserializes and neither does this crate - deserialization is the user's code, in the user's
//! language.

use std::time::SystemTime;

use crate::proto;

/// One Kafka record as the user's function sees it, plus the delivery state an in-process function
/// would have had.
///
/// `None` and `Some(vec![])` are different, deliberately, in both byte fields: a `None` key is a
/// null key and a `None` value is a tombstone, neither of which is an empty payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundRecord {
    /// The topic the record was consumed from.
    pub topic: String,
    /// The partition it came from.
    pub partition: i32,
    /// Its offset within that partition.
    pub offset: i64,
    /// The record key, or `None` for a null key.
    pub key: Option<Vec<u8>>,
    /// The record value, or `None` for a tombstone.
    pub value: Option<Vec<u8>>,

    /// 1 on first delivery, 2 on the first redelivery. Product data: distinct from the opaque
    /// fencing token, which also counts redeliveries that consumed no attempt.
    pub attempt: i32,

    /// When this record last failed. `None` on a first delivery - presence is the wire's way of
    /// saying "this has failed before", never a zero timestamp.
    pub last_failure_at: Option<SystemTime>,

    /// The previous failure's text, verbatim. Worker-supplied and may embed record payload: treat
    /// it as untrusted input wherever it is handled.
    pub last_failure_reason: Option<String>,
}

impl InboundRecord {
    /// Whether this delivery follows a recorded failure.
    #[must_use]
    pub fn has_failed_before(&self) -> bool {
        self.last_failure_at.is_some()
    }

    pub(crate) fn from_wire(dispatched: &proto::DispatchRecord) -> Self {
        let record = dispatched.record.clone().unwrap_or_default();
        Self {
            topic: record.topic.unwrap_or_default(),
            partition: record.partition.unwrap_or_default(),
            offset: record.offset.unwrap_or_default(),
            key: record.key,
            value: record.value,
            attempt: dispatched.attempt.unwrap_or_default(),
            last_failure_at: dispatched.last_failure_at.and_then(|at| SystemTime::try_from(at).ok()),
            last_failure_reason: dispatched.last_failure_reason.clone(),
        }
    }
}

/// A record the user's function asks the proxy to produce on success.
///
/// Workers never touch Kafka themselves: output rides the success report and the proxy produces it
/// with its own producer before the input record's offset may become eligible to commit.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutboundRecord {
    /// The destination topic, or `None` for the proxy's configured default.
    pub topic: Option<String>,
    /// The key to produce, or `None` for a null key.
    pub key: Option<Vec<u8>>,
    /// The value to produce, or `None` for a tombstone.
    pub value: Option<Vec<u8>>,
}

impl OutboundRecord {
    /// A record for the given topic, carrying the given key and value.
    pub fn new(topic: impl Into<String>, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            topic: Some(topic.into()),
            key: Some(key.into()),
            value: Some(value.into()),
        }
    }

    pub(crate) fn into_wire(self) -> proto::ProduceRecord {
        proto::ProduceRecord {
            topic: self.topic,
            key: self.key,
            value: self.value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tombstone_is_not_an_empty_value() {
        let tombstone = InboundRecord::from_wire(&proto::DispatchRecord {
            record: Some(proto::Record {
                topic: Some("orders".to_owned()),
                key: Some(b"k".to_vec()),
                value: None,
                ..Default::default()
            }),
            attempt: Some(1),
            ..Default::default()
        });

        assert_eq!(tombstone.value, None);
        assert_eq!(tombstone.key, Some(b"k".to_vec()));
        assert!(!tombstone.has_failed_before());
    }

    #[test]
    fn a_redelivery_carries_its_failure_history() {
        let redelivered = InboundRecord::from_wire(&proto::DispatchRecord {
            record: Some(proto::Record::default()),
            attempt: Some(2),
            last_failure_at: Some(prost_types::Timestamp {
                seconds: 1_760_000_000,
                nanos: 0,
            }),
            last_failure_reason: Some("the downstream call timed out".to_owned()),
            ..Default::default()
        });

        assert_eq!(redelivered.attempt, 2);
        assert!(redelivered.has_failed_before());
        assert_eq!(
            redelivered.last_failure_reason.as_deref(),
            Some("the downstream call timed out")
        );
    }
}
