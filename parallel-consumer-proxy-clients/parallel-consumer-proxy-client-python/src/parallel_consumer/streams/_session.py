# Copyright (C) 2026 Antony Stubbs and contributors

"""A Kafka Streams session: describe a topology, register functions, answer invocations.

The engine runs the topology; this side describes it and supplies the per-record function. Nothing
of the user's code crosses the boundary - a function is registered under an integer token, and the
engine names the token when it wants an answer. The host calls the host's own function.
"""

from __future__ import annotations

import enum
import itertools
import logging
import struct
import threading
from typing import Protocol
from collections.abc import Callable, Iterator

from .._generated import streams_pb2 as pb

log = logging.getLogger(__name__)

#: A per-record function: (key, value) -> value. Bytes in, bytes out; this side owns serialization.
RecordFunction = Callable[[bytes, bytes], bytes]


class HandleKind(enum.IntEnum):
    """What a handle names on the engine side. Mirrors the wire's ``HandleKind`` values exactly.

    ``UNKNOWN`` is this client's own member, not a wire value: it is what any unrecognised wire
    value degrades to, so an engine newer than this client produces an explicit "I do not know
    this" rather than a crash or a silent guess.
    """

    UNKNOWN = -1
    UNSPECIFIED = 0
    STREAM = 1
    GROUPED_STREAM = 2
    TABLE = 3

    @classmethod
    def _missing_(cls, value: object) -> HandleKind:
        return cls.UNKNOWN


class DataType(enum.IntEnum):
    """A key or value type as the engine recorded it. Mirrors the wire's ``DataType`` values.

    Decoding lives here so that ``key_type`` and ``value_type`` share one mechanism, and so a
    count's long stops being tribal knowledge: ``handle.value_type.decode(raw)`` is the whole
    story. ``UNKNOWN`` degrades unrecognised wire values, as on :class:`HandleKind`.
    """

    UNKNOWN = -1
    UNSPECIFIED = 0
    BYTES = 1
    LONG = 2

    @classmethod
    def _missing_(cls, value: object) -> DataType:
        return cls.UNKNOWN

    def decode(self, data: bytes) -> bytes | int:
        """Decodes one Kafka-serialised key or value of this type.

        Bytes pass through; a long is Kafka's ``Serdes.Long()`` - 8 bytes, big-endian, signed.
        A type this client cannot decode is refused by name rather than guessed at: returning
        the raw bytes for an unknown type would silently hand the caller the wrong thing.
        """
        if self is DataType.BYTES:
            return data
        if self is DataType.LONG:
            value: int = struct.unpack(">q", data)[0]
            return value
        raise StreamsError(
            f"cannot decode a value of type {self.name}: this client has no decoder for it")


class Handle(int):
    """A minted handle: the engine's integer, carrying what the engine says it is.

    An ``int`` subclass, deliberately: every existing call site, proto field assignment and
    equality check keeps working unchanged, while the host can now ask ``handle.kind`` and
    ``handle.value_type`` instead of knowing by convention that a count is a table of longs.
    """

    kind: HandleKind
    key_type: DataType
    value_type: DataType

    def __new__(
        cls, value: int, kind: HandleKind, key_type: DataType, value_type: DataType,
    ) -> Handle:
        handle = super().__new__(cls, value)
        handle.kind = kind
        handle.key_type = key_type
        handle.value_type = value_type
        return handle

    @classmethod
    def from_assigned(cls, assigned: pb.HandleAssigned) -> Handle:
        """Builds the typed handle a ``HandleAssigned`` describes.

        A non-minting answer (sink) carries neither handle nor type and yields the zero handle
        with UNKNOWN types; nothing may be named back with it, so its only job is to be inert.
        """
        if assigned.HasField("type"):
            return cls(
                assigned.handle,
                HandleKind(assigned.type.kind),
                DataType(assigned.type.key_type),
                DataType(assigned.type.value_type),
            )
        return cls(assigned.handle, HandleKind.UNKNOWN, DataType.UNKNOWN, DataType.UNKNOWN)


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

    def source(self, topic: str) -> Handle:
        return self._session._call(pb.BuilderCall(source=pb.Source(topic=topic)))

    def map_values(self, handle: int, function: RecordFunction) -> Handle:
        token = self._session.register(function)
        return self._session._call(
            pb.BuilderCall(map_values=pb.MapValues(handle=handle, function_token=token)))

    def group_by_key(self, handle: int) -> Handle:
        return self._session._call(pb.BuilderCall(group_by_key=pb.GroupByKey(handle=handle)))

    def count(self, handle: int, store_name: str) -> Handle:
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
        self._handles: dict[int, Handle] = {}
        self._ready = threading.Event()
        self._described = threading.Event()
        self._description: pb.TopologyDescription | None = None
        self._fault: str | None = None
        self._closing = False
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

    def describe(self, timeout: float = 30.0) -> pb.TopologyDescription:
        """Asks the engine what the topology it assembled actually looks like.

        This side does not already know. It issued builder calls and holds opaque handles; the
        engine is the only one that has seen the assembled graph - the nodes Kafka Streams generated
        itself, the sub-topology split it chose, and whether an aggregation needed a repartition.

        The ``text`` field is what every existing Kafka Streams visualiser parses, so a language
        with no Streams tooling of its own gets all of it by printing this string.
        """
        self._described.clear()
        self._transport.send(pb.StreamsClientMessage(describe=pb.Describe()))
        if not self._described.wait(timeout):
            raise StreamsError("the engine did not answer the describe request")
        self._raise_if_faulted()
        description = self._description
        if description is None:
            # The event fired without a description attached. Raised rather than asserted: an
            # assert is stripped under -O, and this would then return None from a method whose
            # signature promises otherwise.
            raise StreamsError("the engine signalled a description but sent none")
        return description

    def start(self) -> None:
        """Ends the description. The engine builds the topology and starts it on this message."""
        self._transport.send(pb.StreamsClientMessage(describe_complete=pb.DescribeComplete()))

    def close(self) -> None:
        """Ends the session. Sets the closing flag first, so the reader knows what it is seeing.

        Order matters: closing the transport breaks the response stream, and a reader that learned
        about the close afterwards would report the breakage as an engine fault.
        """
        self._closing = True
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

    def _call(self, call: pb.BuilderCall, timeout: float = 30.0) -> Handle:
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
            return self._handles.pop(
                call_id, Handle(0, HandleKind.UNKNOWN, DataType.UNKNOWN, DataType.UNKNOWN))

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
                elif kind == "topology_description":
                    # Assigned before the event is set: a waiter that woke first and read None
                    # would report "no description" for a description that had in fact arrived.
                    self._description = message.topology_description
                    self._described.set()
                elif kind == "fault":
                    self._on_fault(message.fault.reason)
                else:
                    self._on_fault(f"the engine sent {kind}, which this client does not handle")
        except Exception as broken:
            # A transport that breaks because we closed it is not a fault, it is the close. Only a
            # break we did not ask for says something went wrong.
            if not self._closing:
                self._on_fault(str(broken))

    def _on_handle(self, assigned: pb.HandleAssigned) -> None:
        with self._lock:
            self._handles[assigned.call_id] = Handle.from_assigned(assigned)
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
        # Every waiter is released, not just the handshake. A waiter left blocked on a session that
        # has already failed turns a reported error into a timeout, which reads as a hang.
        self._ready.set()
        self._described.set()
        with self._lock:
            waiting = list(self._pending.values())
            self._pending.clear()
        for event in waiting:
            event.set()

    def _raise_if_faulted(self) -> None:
        if self._fault is not None:
            raise StreamsError(self._fault)

