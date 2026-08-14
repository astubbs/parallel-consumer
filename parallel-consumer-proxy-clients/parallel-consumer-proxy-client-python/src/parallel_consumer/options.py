# Copyright (C) 2026 Antony Stubbs and contributors

"""Connect-time configuration: what the user tells Parallel Consumer, in one object.

Configuration is code (the language-proxy plan's KTD5): this library reads no file, no
environment variable and no command line, and nothing here reaches the sidecar by argv or
environment - it all travels in the connect-time ``Configure`` message and nowhere else.

**Unset means "take the engine's default".** This class therefore holds no defaults of its own
and never guesses one; ``None`` is the wire's absence, and the proxy reports back the value it
actually used.
"""

from __future__ import annotations

import dataclasses
import enum
from datetime import timedelta
from collections.abc import Mapping

__all__ = ["ClientOptions", "ProcessingOrder"]


class ProcessingOrder(enum.Enum):
    """The ordering guarantee to ask Parallel Consumer for.

    Ordering is enforced by the engine, never by this library.
    """

    UNORDERED = "unordered"
    """No ordering: any record may process concurrently with any other."""

    PARTITION = "partition"
    """Records of one partition process one at a time, in offset order."""

    KEY = "key"
    """Records sharing a key process one at a time; distinct keys proceed concurrently."""


@dataclasses.dataclass(frozen=True)
class ClientOptions:
    """Everything the proxy needs to build its consumer and start dispatching.

    ``kafka_properties`` is credential-bearing (bootstrap servers, SASL secrets). It is never
    logged, never echoed back in an error, and never written to argv, an environment variable or
    a temp file - which is why :meth:`__repr__` omits it. Keep that property when adding a field.

    :param topics: the subscription, fixed for the client's lifetime. Give this or
        ``topic_pattern``, never both.
    :param topic_pattern: a subscription regex, as the alternative to ``topics``.
    :param max_concurrency: the most records the proxy may have in flight to this client at
        once. ``None`` takes the proxy's default; the effective value comes back in the
        handshake and becomes this client's dispatch-queue depth.
    :param ordering: the ordering guarantee; ``None`` takes the proxy's default.
    :param commit_interval: how often offsets are committed; ``None`` takes the proxy's default.
    :param default_message_retry_delay: how long a failed record waits before redelivery.
    :param kafka_properties: the Kafka client configuration. Credential-bearing.
    """

    topics: tuple[str, ...] = ()
    topic_pattern: str | None = None
    max_concurrency: int | None = None
    ordering: ProcessingOrder | None = None
    commit_interval: timedelta | None = None
    default_message_retry_delay: timedelta | None = None
    kafka_properties: Mapping[str, str] = dataclasses.field(default_factory=dict)

    def __post_init__(self) -> None:
        # Accept any iterable of topics but hold a tuple, so an option object cannot be mutated
        # out from under a running session.
        object.__setattr__(self, "topics", tuple(self.topics))
        object.__setattr__(self, "kafka_properties", dict(self.kafka_properties))

        if bool(self.topics) == bool(self.topic_pattern):
            raise ValueError("give exactly one of topics or topic_pattern")
        if self.max_concurrency is not None and self.max_concurrency < 1:
            raise ValueError(f"max_concurrency must be at least 1, got {self.max_concurrency}")

    def __repr__(self) -> str:
        # Deliberately omits kafka_properties: it may carry credentials, and a repr reaches logs
        # and tracebacks by every route there is.
        return (
            f"ClientOptions(topics={self.topics!r}, topic_pattern={self.topic_pattern!r}, "
            f"max_concurrency={self.max_concurrency!r}, ordering={self.ordering!r}, "
            f"commit_interval={self.commit_interval!r}, "
            f"default_message_retry_delay={self.default_message_retry_delay!r})"
        )
