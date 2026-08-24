# Copyright (C) 2026 Antony Stubbs and contributors

"""Kafka Streams from Python, through the language proxy.

EXPERIMENTAL. This is a feasibility proof: a handful of builder methods, three foreign operator
shapes, and a deliberately unfrozen wire. It may change or disappear.

The engine runs Kafka Streams; this side describes the topology and supplies the per-record
function. A function is registered under a token and the token is what crosses - Python calls
Python, and nothing re-enters this runtime from an engine thread.

Registered functions run on this session's own worker pool, so they may be called concurrently and
must be thread-safe. That is also what lets one call back into the engine - ``get``, ``describe``
or a builder call from inside a mapper - which hung while the reader thread ran them.
"""

from ._session import (
    DataType,
    FunctionKind,
    Handle,
    HandleKind,
    JoinerFunction,
    RecordFunction,
    ReducerFunction,
    StreamsError,
    StreamsSession,
    StreamsTransport,
    TopologyBuilder,
)
from ._transport import GrpcStreamsTransport

__all__ = [
    "DataType",
    "FunctionKind",
    "GrpcStreamsTransport",
    "Handle",
    "HandleKind",
    "JoinerFunction",
    "RecordFunction",
    "ReducerFunction",
    "StreamsError",
    "StreamsSession",
    "StreamsTransport",
    "TopologyBuilder",
]
