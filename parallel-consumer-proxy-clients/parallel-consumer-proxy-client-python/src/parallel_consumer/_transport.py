# Copyright (C) 2026 Antony Stubbs and contributors

"""What carries a session's frames, and the two things that can carry them.

The session state machine never mentions gRPC. It pushes a ``ClientMessage``, pulls a
``ProxyMessage``, says there will be no more, and cancels. That is the whole of its dependency on
the transport, and naming it here is what lets the same session run over an embedded engine.

The protocol's frames are not tied to gRPC in the first place: the same serialised bytes can cross
a C ABI into a Parallel Consumer linked into this very process. See ``../../ffi``.
"""

from __future__ import annotations

import queue
from collections.abc import Iterator
from typing import Any, Protocol

import grpc

from ._generated import proxy_pb2 as pb
from ._generated import proxy_pb2_grpc as pb_grpc

# Sentinel for "no more requests". The outbound queue carries messages, and None is not one.
_HALF_CLOSE = None


class Transport(Protocol):
    """One session's carrier."""

    #: Exception types that mean the stream broke. Kept as the transport's own types rather than
    #: wrapped in a common error, so ``Session.failure`` still surfaces what actually happened.
    errors: tuple[type[BaseException], ...]

    #: The inbound frames. One iterator for the session's whole life - the handshake takes the
    #: first item off it and the reader loop continues from there.
    responses: Iterator[Any]

    def send(self, message: pb.ClientMessage) -> None: ...

    def half_close(self) -> None:
        """No more requests. This IS the shutdown signal; there is no shutdown message."""

    def cancel(self) -> None:
        """Tear the stream down now, without a drain - the response to a protocol violation."""

    def close(self) -> None: ...

    def describe(self, error: BaseException) -> str:
        """Render a broken-stream error. gRPC statuses and C error codes read nothing alike."""


class GrpcTransport:
    """The sidecar transport: a loopback connection to a process this client spawned."""

    errors: tuple[type[BaseException], ...] = (grpc.RpcError,)

    def __init__(self, port: int, first_message: pb.ClientMessage) -> None:
        self._sends: queue.Queue[Any] = queue.Queue()
        # Queued BEFORE the call is created. The request generator is consumed lazily by gRPC, so
        # the handshake message has to be waiting when it first pulls.
        self._sends.put(first_message)
        self._channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        # protoc's gRPC output carries no annotations, so the strict pass sees an untyped call
        # here. Suppressed at the site rather than by relaxing the module: this is the ONE
        # crossing into generated code, and pyproject's warn_unused_ignores deletes this line for
        # us the day protoc starts emitting stubs.
        self._call = pb_grpc.ProxyServiceStub(  # type: ignore[no-untyped-call]
            self._channel).Session(self._requests())
        self.responses: Iterator[Any] = self._call

    def _requests(self) -> Any:
        """The outbound half of the stream: everything the client says, in one place."""
        while True:
            message = self._sends.get()
            if message is _HALF_CLOSE:
                return
            yield message

    def send(self, message: pb.ClientMessage) -> None:
        self._sends.put(message)

    def half_close(self) -> None:
        self._sends.put(_HALF_CLOSE)

    def cancel(self) -> None:
        self._call.cancel()

    def close(self) -> None:
        self._channel.close()

    def describe(self, error: BaseException) -> str:
        status = error.code().name if hasattr(error, "code") else type(error).__name__
        details = error.details() if hasattr(error, "details") else str(error)
        return f"{status} - {details}"
