# Copyright (C) 2026 Antony Stubbs and contributors

"""The gRPC carrier for a Streams session.

Deliberately not shared with the proxy client's transport, even though the two are nearly the same
shape. That one carries the frozen wire; this one carries an experimental schema that may change or
disappear, and merging them would let a change here reach a stable client. The duplication is small
and it is the cheap side of that trade.
"""

from __future__ import annotations

import queue
from collections.abc import Iterator
from typing import Any

import grpc

from .._generated import streams_pb2 as pb
from .._generated import streams_pb2_grpc as pb_grpc

__all__ = ["GrpcStreamsTransport"]

_HALF_CLOSE = object()
"""Sentinel that ends the request stream. A value, not a flag, so the queue keeps its ordering."""


class GrpcStreamsTransport:
    """A loopback connection to a Streams engine this client spawned."""

    def __init__(self, port: int) -> None:
        self._sends: queue.Queue[Any] = queue.Queue()
        self._channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        # protoc's gRPC output carries no annotations, so the strict pass sees an untyped call
        # here - the same single crossing into generated code the proxy transport documents.
        self._call = pb_grpc.StreamsServiceStub(  # type: ignore[no-untyped-call]
            self._channel).Session(self._requests())

    def _requests(self) -> Iterator[pb.StreamsClientMessage]:
        """The outbound half of the stream.

        Blocking on the queue is what holds the call open while the client thinks. gRPC pulls this
        lazily, so an empty queue is a quiet stream rather than a closed one.
        """
        while True:
            message = self._sends.get()
            if message is _HALF_CLOSE:
                return
            yield message

    def send(self, message: pb.StreamsClientMessage) -> None:
        self._sends.put(message)

    def responses(self) -> Iterator[pb.StreamsServerMessage]:
        return self._call  # type: ignore[no-any-return]

    def close(self) -> None:
        """Ends the request stream, then drops the channel.

        Half-closing first rather than cancelling: the engine treats end-of-stream as the signal to
        stop the topology cleanly, and cancelling would leave it to notice a dead peer instead.
        """
        self._sends.put(_HALF_CLOSE)
        self._channel.close()
