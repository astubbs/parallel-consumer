# Copyright (C) 2026 Antony Stubbs and contributors

"""A Kafka Streams session: describe a topology, register functions, answer invocations.

The engine runs the topology; this side describes it and supplies the per-record function. Nothing
of the user's code crosses the boundary - a function is registered under an integer token, and the
engine names the token when it wants an answer. The host calls the host's own function.
"""

from __future__ import annotations

import itertools
import logging
import threading
from typing import Protocol
from collections.abc import Callable, Iterator

from .._generated import streams_pb2 as pb

log = logging.getLogger(__name__)

#: A per-record function: (key, value) -> value. Bytes in, bytes out; this side owns serialization.
RecordFunction = Callable[[bytes, bytes], bytes]


class StreamsTransport(Protocol):
    """What a session needs from whatever carries it. Narrow, so tests need no server."""

    def send(self, message: pb.StreamsClientMessage) -> None: ...

    def responses(self) -> Iterator[pb.StreamsServerMessage]: ...

    def close(self) -> None: ...


class StreamsError(RuntimeError):
    """The engine refused something, or the session broke."""


class TopologyBuilder:
    """Issues builder calls and hands back the handles the engine minted.

    Each call blocks until the engine answers, because the next call has to name the handle this
    one produced. That is the shape of a builder chain, not a limitation of the transport.
    """

    def __init__(self, session: StreamsSession) -> None:
        self._session = session

    def source(self, topic: str) -> int:
        return self._session._call(pb.BuilderCall(source=pb.Source(topic=topic)))

    def map_values(self, handle: int, function: RecordFunction) -> int:
        token = self._session.register(function)
        return self._session._call(
            pb.BuilderCall(map_values=pb.MapValues(handle=handle, function_token=token)))

    def group_by_key(self, handle: int) -> int:
        return self._session._call(pb.BuilderCall(group_by_key=pb.GroupByKey(handle=handle)))

    def count(self, handle: int, store_name: str) -> int:
        return self._session._call(
            pb.BuilderCall(count=pb.Count(handle=handle, store_name=store_name)))

    def sink(self, handle: int, topic: str) -> None:
        self._session._call(pb.BuilderCall(sink=pb.Sink(handle=handle, topic=topic)))


class StreamsSession:
    """One session over one transport.

    A reader thread services the engine: it answers handle requests, and it runs the user's function
    when an invocation arrives. The engine has a stream thread blocked on every invocation, so this
    side must not be slower than it has to be - but correctness first, and this is a proof.
    """

    def __init__(self, transport: StreamsTransport) -> None:
        self._transport = transport
        self._functions: dict[int, RecordFunction] = {}
        self._tokens = itertools.count(1)
        self._call_ids = itertools.count(1)
        self._pending: dict[int, threading.Event] = {}
        self._handles: dict[int, int] = {}
        self._ready = threading.Event()
        self._fault: str | None = None
        self._lock = threading.Lock()
        self._reader: threading.Thread | None = None

    # ---- lifecycle ---------------------------------------------------------------

    def open(
        self, application_id: str, kafka_properties: dict[str, str], timeout: float = 30.0,
    ) -> None:
        self._reader = threading.Thread(target=self._read, name="pc-streams-reader", daemon=True)
        self._reader.start()
        self._transport.send(pb.StreamsClientMessage(
            open=pb.Open(application_id=application_id, kafka_properties=kafka_properties)))
        if not self._ready.wait(timeout):
            raise StreamsError("the engine did not answer the handshake")
        self._raise_if_faulted()

    def builder(self) -> TopologyBuilder:
        return TopologyBuilder(self)

    def start(self) -> None:
        """Ends the description. The engine builds the topology and starts it on this message."""
        self._transport.send(pb.StreamsClientMessage(describe_complete=pb.DescribeComplete()))

    def close(self) -> None:
        self._transport.close()

    # ---- function registry -------------------------------------------------------

    def register(self, function: RecordFunction) -> int:
        """Registers a function under a token. The token crosses; the function never does."""
        token = next(self._tokens)
        self._functions[token] = function
        self._transport.send(pb.StreamsClientMessage(
            register_function=pb.RegisterFunction(
                token=token, description=getattr(function, "__name__", ""))))
        return token

    # ---- internals ---------------------------------------------------------------

    def _call(self, call: pb.BuilderCall, timeout: float = 30.0) -> int:
        call_id = next(self._call_ids)
        call.call_id = call_id
        answered = threading.Event()
        with self._lock:
            self._pending[call_id] = answered
        self._transport.send(pb.StreamsClientMessage(builder_call=call))
        if not answered.wait(timeout):
            raise StreamsError(f"the engine did not answer builder call {call_id}")
        self._raise_if_faulted()
        with self._lock:
            return self._handles.pop(call_id, 0)

    def _read(self) -> None:
        try:
            for message in self._transport.responses():
                kind = message.WhichOneof("message")
                if kind == "ready":
                    self._ready.set()
                elif kind == "handle_assigned":
                    self._on_handle(message.handle_assigned)
                elif kind == "invocation":
                    self._on_invocation(message.invocation)
                elif kind == "fault":
                    self._on_fault(message.fault.reason)
                else:
                    self._on_fault(f"the engine sent {kind}, which this client does not handle")
        except Exception as broken:
            self._on_fault(str(broken))

    def _on_handle(self, assigned: pb.HandleAssigned) -> None:
        with self._lock:
            self._handles[assigned.call_id] = assigned.handle
            waiting = self._pending.pop(assigned.call_id, None)
        if waiting is not None:
            waiting.set()

    def _on_invocation(self, invocation: pb.Invocation) -> None:
        function = self._functions.get(invocation.function_token)
        if function is None:
            self._answer(
                invocation.correlation,
                error=f"no function registered under token {invocation.function_token}")
            return
        try:
            value = function(invocation.key, invocation.value)
        except Exception as failed:
            self._answer(invocation.correlation, error=repr(failed))
            return
        self._answer(invocation.correlation, value=value)

    def _answer(
        self, correlation: int, *, value: bytes | None = None, error: str | None = None,
    ) -> None:
        result = pb.InvocationResult(correlation=correlation)
        if error is not None:
            result.error = error
        else:
            result.value = value or b""
        self._transport.send(pb.StreamsClientMessage(invocation_result=result))

    def _on_fault(self, reason: str) -> None:
        log.error("streams session faulted: %s", reason)
        self._fault = reason
        self._ready.set()
        with self._lock:
            waiting = list(self._pending.values())
            self._pending.clear()
        for event in waiting:
            event.set()

    def _raise_if_faulted(self) -> None:
        if self._fault is not None:
            raise StreamsError(self._fault)

