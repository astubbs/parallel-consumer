// Copyright (C) 2026 Antony Stubbs and contributors

//! Connect-time configuration: the whole of what a session is configured with, and the only place
//! configuration ever travels. Nothing here reaches the proxy by argv, environment or file.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use crate::error::ClientError;
use crate::proto;

/// The capability tokens this protocol defines. A duty exists on a session **iff** its token is in
/// the negotiated set that comes back in [`Session::capabilities`](crate::Session), so this is how
/// a client decides what it owes rather than what it hopes.
pub mod capability {
    /// `Dispatch` waves, proxy to client.
    pub const DISPATCH: &str = "dispatch";
    /// `Heartbeat` and the liveness lease, client to proxy.
    pub const HEARTBEAT: &str = "heartbeat";
    /// `Manifest` reconnects and the `Drop` replies to them.
    pub const MANIFEST: &str = "manifest";
    /// `WorkerDied`, client to proxy.
    pub const WORKER_DEATH: &str = "worker-death";
    /// `Shutdown`, proxy to client, and the `Released` outcome that answers it.
    pub const SHUTDOWN: &str = "shutdown";
    /// The `Terminal` outcome.
    pub const TERMINAL: &str = "terminal";
}

/// What this crate honours today, and therefore exactly what it declares.
///
/// **Declaring nothing would be worse than declaring a subset**: an empty list means "the v1
/// baseline" on the wire, which entitles the proxy to send heartbeat, manifest, worker-death and
/// shutdown traffic this client does not answer - and un-answered heartbeats arm a lease-expiry
/// redelivery loop. The wave that implements a duty adds its token here, so the declaration cannot
/// fall out of step with the code by omission.
const IMPLEMENTED_CAPABILITIES: &[&str] = &[capability::DISPATCH];

/// Where the sidecar's own diagnostics go.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SidecarStderr {
    /// Inherit this process's stderr, so the sidecar's log lines appear alongside the
    /// application's. The default: silencing a child process's diagnostics by default is how a
    /// misconfigured broker becomes an unexplained hang.
    #[default]
    Inherit,
    /// Discard the sidecar's stderr.
    Null,
}

/// The engine's ordering modes. Absent means "take the proxy's default"; the effective value comes
/// back in [`Session`](crate::Session).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessingOrder {
    /// No ordering constraint between records.
    Unordered,
    /// Records of one partition are processed in order.
    Partition,
    /// Records sharing a key are processed in order; distinct keys run concurrently.
    Key,
}

impl ProcessingOrder {
    fn wire(self) -> i32 {
        match self {
            Self::Unordered => proto::ProcessingOrder::Unordered as i32,
            Self::Partition => proto::ProcessingOrder::Partition as i32,
            Self::Key => proto::ProcessingOrder::Key as i32,
        }
    }
}

/// The whole of a session's configuration.
///
/// A plain struct with public fields rather than a builder: every field's default already means
/// "take the proxy's default", which is the wire's own convention, so the two agree without a
/// translation table. Fill in what you mean and take the rest from [`Default`]:
///
/// ```no_run
/// # use parallel_consumer_proxy_client::ClientOptions;
/// let options = ClientOptions {
///     sidecar_path: "/opt/parallel-consumer/proxy".into(),
///     topics: vec!["orders".to_owned()],
///     ..Default::default()
/// };
/// ```
/// Deliberately NOT `#[non_exhaustive]`: the struct-update construction above is the documented
/// way to build these options, and `#[non_exhaustive]` would forbid it outside this crate. The
/// cost is that adding a field is a breaking change - which is the honest signal for a type whose
/// every field is part of the wire contract.
#[derive(Clone)]
pub struct ClientOptions {
    /// The ABSOLUTE path of the sidecar binary. It is never resolved through `PATH` or relative to
    /// the working directory: this process hands the sidecar the Kafka credentials, so which
    /// binary runs is security-relevant.
    pub sidecar_path: PathBuf,

    /// Arguments passed to that binary verbatim. They carry no proxy configuration - the
    /// conformance harness takes its fixture selection this way, which is its own documented
    /// exception, not a licence to configure a shipped sidecar by flag.
    pub sidecar_args: Vec<String>,

    /// Where the sidecar's stderr goes.
    pub sidecar_stderr: SidecarStderr,

    /// The subscription, fixed for the sidecar's lifetime. Exactly one of this and
    /// [`topic_pattern`](Self::topic_pattern) must be set.
    pub topics: Vec<String>,

    /// A subscription by pattern instead of by name.
    pub topic_pattern: Option<String>,

    /// The proxy's in-flight ceiling, and therefore this client's dispatch-queue depth. `None`
    /// means the proxy's default. There is no "unlimited".
    pub max_concurrency: Option<i32>,

    /// The Kafka connection settings and credentials the proxy builds its clients from.
    ///
    /// **This map is never logged, never echoed in an error, and never written anywhere but the
    /// stream** - including by this type's own [`Debug`], which redacts it.
    pub kafka_properties: HashMap<String, String>,

    /// The capability tokens to declare. `None` declares exactly what this crate implements, which
    /// is the right answer for every caller that has not extended it.
    pub capabilities: Option<Vec<String>>,

    /// The processing order to ask for.
    pub ordering: Option<ProcessingOrder>,

    /// How often the proxy commits.
    pub commit_interval: Option<Duration>,

    /// How long a failed record waits before redelivery.
    pub default_message_retry_delay: Option<Duration>,

    /// How long the proxy's own drain may take at shutdown.
    pub drain_timeout: Option<Duration>,

    /// Asks for terminal-outcome resolution to this topic. It only takes effect when the session
    /// also negotiates [`capability::TERMINAL`]; the effective session reports whether it did.
    pub terminal_topic: Option<String>,

    /// Tags the engine's metrics and logging.
    pub instance_tag: Option<String>,

    /// Budget for the whole of connecting: spawning the sidecar, reading its port line, the TCP
    /// connection, and the handshake.
    pub connect_timeout: Duration,

    /// How long [`shutdown`](crate::ParallelConsumerClient::shutdown) waits for the proxy to
    /// complete the stream, and then for the sidecar to exit, before it stops being polite.
    pub shutdown_grace: Duration,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            sidecar_path: PathBuf::new(),
            sidecar_args: Vec::new(),
            sidecar_stderr: SidecarStderr::default(),
            topics: Vec::new(),
            topic_pattern: None,
            max_concurrency: None,
            kafka_properties: HashMap::new(),
            capabilities: None,
            ordering: None,
            commit_interval: None,
            default_message_retry_delay: None,
            drain_timeout: None,
            terminal_topic: None,
            instance_tag: None,
            connect_timeout: Duration::from_secs(30),
            shutdown_grace: Duration::from_secs(15),
        }
    }
}

/// Hand-written so that `{:?}` on the options - in a log line, a test failure, an error report -
/// cannot print credentials. The derived implementation would print the whole property map, which
/// is precisely the leak the credential-hygiene rules forbid.
impl fmt::Debug for ClientOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientOptions")
            .field("sidecar_path", &self.sidecar_path)
            .field("sidecar_args", &self.sidecar_args)
            .field("sidecar_stderr", &self.sidecar_stderr)
            .field("topics", &self.topics)
            .field("topic_pattern", &self.topic_pattern)
            .field("max_concurrency", &self.max_concurrency)
            .field(
                "kafka_properties",
                &format_args!("<redacted: {} entries>", self.kafka_properties.len()),
            )
            .field("capabilities", &self.capabilities)
            .field("ordering", &self.ordering)
            .field("commit_interval", &self.commit_interval)
            .field("default_message_retry_delay", &self.default_message_retry_delay)
            .field("drain_timeout", &self.drain_timeout)
            .field("terminal_topic", &self.terminal_topic)
            .field("instance_tag", &self.instance_tag)
            .field("connect_timeout", &self.connect_timeout)
            .field("shutdown_grace", &self.shutdown_grace)
            .finish()
    }
}

impl ClientOptions {
    /// Options for a sidecar at the given absolute path, with everything else defaulted.
    pub fn new(sidecar_path: impl Into<PathBuf>) -> Self {
        Self {
            sidecar_path: sidecar_path.into(),
            ..Self::default()
        }
    }

    pub(crate) fn validate(&self) -> Result<(), ClientError> {
        if self.sidecar_path.as_os_str().is_empty() {
            return Err(ClientError::Options("sidecar_path is required".to_owned()));
        }
        if !self.sidecar_path.is_absolute() {
            return Err(ClientError::Options(format!(
                "sidecar_path must be absolute, got {} - a relative or PATH-resolved sidecar is a \
                 binary an attacker can influence",
                self.sidecar_path.display()
            )));
        }
        if self.topics.is_empty() == self.topic_pattern.is_none() {
            return Err(ClientError::Options(
                "exactly one of topics or topic_pattern must be set".to_owned(),
            ));
        }
        if self.max_concurrency.is_some_and(|ceiling| ceiling < 1) {
            return Err(ClientError::Options(format!(
                "max_concurrency must be >= 1 or absent for the proxy's default, got {:?}",
                self.max_concurrency
            )));
        }
        Ok(())
    }

    /// Renders the options as the first message of a fresh session.
    pub(crate) fn configure(&self) -> proto::Configure {
        proto::Configure {
            topics: self.topics.clone(),
            topic_pattern: self.topic_pattern.clone(),
            max_concurrency: self.max_concurrency,
            kafka_properties: self.kafka_properties.clone(),
            capabilities: self.capabilities.clone().unwrap_or_else(|| {
                IMPLEMENTED_CAPABILITIES
                    .iter()
                    .map(|token| (*token).to_owned())
                    .collect()
            }),
            ordering: self.ordering.map(ProcessingOrder::wire),
            commit_interval: self.commit_interval.map(duration),
            default_message_retry_delay: self.default_message_retry_delay.map(duration),
            drain_timeout: self.drain_timeout.map(duration),
            terminal_topic: self.terminal_topic.clone(),
            pc_instance_tag: self.instance_tag.clone(),
            ..Default::default()
        }
    }
}

/// Saturating rather than fallible: a `Duration` beyond `i64::MAX` seconds is not a configuration
/// anyone meant, and refusing to connect over it would be a worse answer than clamping.
fn duration(value: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: i64::try_from(value.as_secs()).unwrap_or(i64::MAX),
        nanos: i32::try_from(value.subsec_nanos()).unwrap_or(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_never_prints_a_kafka_property() {
        let options = ClientOptions {
            sidecar_path: PathBuf::from("/opt/proxy"),
            topics: vec!["orders".to_owned()],
            kafka_properties: HashMap::from([("sasl.jaas.config".to_owned(), "password=hunter2".to_owned())]),
            ..Default::default()
        };

        let rendered = format!("{options:?}");

        assert!(
            !rendered.contains("hunter2"),
            "credentials leaked into Debug: {rendered}"
        );
        assert!(
            !rendered.contains("sasl.jaas.config"),
            "a property key leaked: {rendered}"
        );
        assert!(rendered.contains("<redacted: 1 entries>"), "{rendered}");
    }

    #[test]
    fn only_implemented_capabilities_are_declared() {
        let options = ClientOptions {
            sidecar_path: PathBuf::from("/opt/proxy"),
            topics: vec!["orders".to_owned()],
            ..Default::default()
        };

        assert_eq!(options.configure().capabilities, vec![capability::DISPATCH.to_owned()]);
    }

    #[test]
    fn a_subscription_is_exactly_one_of_topics_or_pattern() {
        let base = ClientOptions::new("/opt/proxy");

        assert!(base.validate().is_err(), "neither form set");
        assert!(ClientOptions {
            topics: vec!["a".to_owned()],
            topic_pattern: Some("a.*".to_owned()),
            ..base.clone()
        }
        .validate()
        .is_err());
        assert!(ClientOptions {
            topics: vec!["a".to_owned()],
            ..base.clone()
        }
        .validate()
        .is_ok());
        assert!(ClientOptions {
            topic_pattern: Some("a.*".to_owned()),
            ..base
        }
        .validate()
        .is_ok());
    }

    #[test]
    fn a_relative_sidecar_path_is_refused() {
        let options = ClientOptions {
            topics: vec!["orders".to_owned()],
            ..ClientOptions::new("proxy")
        };

        let message = options.validate().unwrap_err().to_string();

        assert!(message.contains("must be absolute"), "{message}");
    }
}
