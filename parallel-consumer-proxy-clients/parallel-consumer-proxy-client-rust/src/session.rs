// Copyright (C) 2026 Antony Stubbs and contributors

//! The effective configuration a session is running with.

use crate::error::ClientError;
use crate::proto;

/// What the proxy replied it is actually running with, after its own defaults and the capability
/// negotiation.
///
/// **Assert on this, never on [`ClientOptions`](crate::ClientOptions).** What was asked for and
/// what is running are different things, and only this one governs what the client owes.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Session {
    /// The subscription by name, as the proxy echoed it.
    pub topics: Vec<String>,
    /// The subscription by pattern, as the proxy echoed it.
    pub topic_pattern: Option<String>,
    /// The proxy's in-flight ceiling, which is also this client's dispatch-queue depth.
    pub max_concurrency: i32,
    /// How many executors to run. A pure function of connect-time configuration: computed once,
    /// sent once, never revised - and clients must not assume any formula relating it to
    /// [`max_concurrency`](Self::max_concurrency).
    pub executor_count: i32,
    /// The negotiated intersection of what this client declared and what the proxy implements.
    pub capabilities: Vec<String>,
    /// The effective terminal-outcome destination, present exactly when terminal reporting is on.
    pub terminal_topic: Option<String>,
}

impl Session {
    /// Whether a capability token survived the handshake. Every duty in this protocol is gated by
    /// one, so this is the question to ask before sending anything.
    #[must_use]
    pub fn negotiated(&self, token: &str) -> bool {
        self.capabilities.iter().any(|granted| granted == token)
    }

    pub(crate) fn from_wire(configured: proto::Configured) -> Result<Self, ClientError> {
        // Absence is a protocol violation, never "unlimited": the ceiling is always finite and
        // always reported, and it is also this client's queue depth, so there is nothing to fall
        // back on.
        let max_concurrency = configured.max_concurrency.filter(|value| *value >= 1).ok_or_else(|| {
            ClientError::Protocol(
                "Configured carried no usable max_concurrency - the in-flight ceiling is always \
                 reported"
                    .to_owned(),
            )
        })?;
        let executor_count = configured
            .executor_count
            .filter(|value| *value >= 1)
            .ok_or_else(|| ClientError::Protocol("Configured carried no usable executor_count".to_owned()))?;

        Ok(Self {
            topics: configured.topics,
            topic_pattern: configured.topic_pattern,
            max_concurrency,
            executor_count,
            capabilities: configured.capabilities,
            terminal_topic: configured.terminal_topic,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configured() -> proto::Configured {
        proto::Configured {
            max_concurrency: Some(3),
            executor_count: Some(2),
            capabilities: vec!["dispatch".to_owned()],
            ..Default::default()
        }
    }

    #[test]
    fn a_configured_without_a_ceiling_is_a_protocol_violation() {
        let missing = proto::Configured {
            max_concurrency: None,
            ..configured()
        };

        let message = Session::from_wire(missing).unwrap_err().to_string();

        assert!(message.contains("max_concurrency"), "{message}");
    }

    #[test]
    fn a_configured_without_an_executor_count_is_a_protocol_violation() {
        let missing = proto::Configured {
            executor_count: None,
            ..configured()
        };

        let message = Session::from_wire(missing).unwrap_err().to_string();

        assert!(message.contains("executor_count"), "{message}");
    }

    #[test]
    fn negotiation_is_read_from_what_came_back() {
        let session = Session::from_wire(configured()).unwrap();

        assert!(session.negotiated("dispatch"));
        assert!(!session.negotiated("heartbeat"));
    }
}
