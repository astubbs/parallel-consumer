# Copyright (C) 2026 Antony Stubbs and contributors

"""The records that cross the user's function: one in, zero or more out.

Keys and values are ``bytes``. This library never deserializes - the proxy does not either, and
serialization formats are the user's business, in the user's own code.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime

__all__ = ["InboundRecord", "OutboundRecord"]


@dataclasses.dataclass(frozen=True)
class InboundRecord:
    """One record as delivered to the user's function.

    Carries the Kafka record *plus* the delivery state an in-process Parallel Consumer user
    function would see: which attempt this is, and why the previous one failed. That state is
    product data - nothing about the transport (delivery tokens, connection identity) appears
    here, deliberately.

    ``key`` and ``value`` are ``None`` when Kafka held null, which is a different thing from
    empty bytes: a tombstone has a null value.
    """

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    attempt: int
    """1 on first delivery, 2 on the first redelivery."""

    last_failure_at: datetime | None = None
    """When the previous attempt failed; ``None`` before the first failure."""

    last_failure_reason: str | None = None
    """The previous attempt's reason. Worker-supplied text: untrusted, and it may quote payload."""

    def __repr__(self) -> str:
        # Deliberately omits key and value: payloads are untrusted input and do not belong in
        # log lines or tracebacks.
        return f"InboundRecord({self.topic}-{self.partition}@{self.offset}, attempt {self.attempt})"


@dataclasses.dataclass(frozen=True)
class OutboundRecord:
    """A record a successful outcome asks Parallel Consumer to produce.

    The only sanctioned route for a worker's Kafka output: workers never hold a producer, and
    the proxy produces these with its own before the input record's offset can be committed.
    """

    topic: str
    key: bytes | None = None
    value: bytes | None = None

    def __post_init__(self) -> None:
        if not self.topic:
            raise ValueError("an OutboundRecord needs a destination topic")

    def __repr__(self) -> str:
        # As above: no payload in a repr.
        return f"OutboundRecord({self.topic})"
