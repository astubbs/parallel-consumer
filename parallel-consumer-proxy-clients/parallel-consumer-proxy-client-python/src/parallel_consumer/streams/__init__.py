# Copyright (C) 2026 Antony Stubbs and contributors

"""Kafka Streams from Python, through the language proxy.

EXPERIMENTAL. This is a feasibility proof: five builder methods, one foreign operator, and a
deliberately unfrozen wire. It may change or disappear.

The engine runs Kafka Streams; this side describes the topology and supplies the per-record
function. A function is registered under a token and the token is what crosses - Python calls
Python, and nothing re-enters this runtime from an engine thread.
"""

from ._session import (
    RecordFunction,
    StreamsError,
    StreamsSession,
    StreamsTransport,
    TopologyBuilder,
)
from ._transport import GrpcStreamsTransport

__all__ = [
    "GrpcStreamsTransport",
    "RecordFunction",
    "StreamsError",
    "StreamsSession",
    "StreamsTransport",
    "TopologyBuilder",
]
