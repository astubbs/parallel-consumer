# Copyright (C) 2026 Antony Stubbs and contributors

"""The Streams session, driven against a fake engine.

No broker, no server, no Docker - this suite is deliberately free of all three, and the end-to-end
run lives in the demo instead. What is reachable here is the part that matters on this side: that
the builder names handles the engine minted, that a function is represented by a token and never by
anything else, and that an invocation is answered on its own correlation.
"""

from __future__ import annotations

import queue
import struct
import threading
from collections.abc import Iterator
from typing import cast

import pytest

from parallel_consumer._generated import streams_pb2 as pb
from parallel_consumer.streams import DataType, HandleKind, StreamsError, StreamsSession


class FakeEngine:
    """Answers like the engine would: Ready for the handshake, a minted handle per builder call."""

    def __init__(self, *, fault_on_call: int | None = None) -> None:
        self.sent: list[pb.StreamsClientMessage] = []
        self._outbound: queue.Queue[pb.StreamsServerMessage | None] = queue.Queue()
        self._next_handle = 100
        self._fault_on_call = fault_on_call
        self._forced_type: pb.HandleType | None = None
        self._lock = threading.Lock()

    # -- the transport surface the session uses --
    def send(self, message: pb.StreamsClientMessage) -> None:
        with self._lock:
            self.sent.append(message)
        kind = message.WhichOneof("message")
        if kind == "open":
            self._outbound.put(pb.StreamsServerMessage(
                ready=pb.Ready(application_id=message.open.application_id)))
        elif kind == "describe":
            self._outbound.put(pb.StreamsServerMessage(
                topology_description=pb.TopologyDescription(
                    text="Topologies:\n   Sub-topology: 0\n    Source: KSTREAM-SOURCE-0000000000",
                    subtopologies=[pb.Subtopology(id=0, nodes=[
                        pb.Node(name="KSTREAM-SOURCE-0000000000",
                                kind=pb.NODE_KIND_SOURCE, topics=["input"])])])))
        elif kind == "builder_call":
            call_id = message.builder_call.call_id
            if self._fault_on_call == call_id:
                self._outbound.put(pb.StreamsServerMessage(
                    fault=pb.Fault(
                        reason=f"call {call_id} names handle 4242, which does not exist")))
                return
            method = message.builder_call.WhichOneof("call")
            if method == "sink":
                # A sink mints nothing: its answer carries neither handle nor type, like the
                # engine's.
                self._outbound.put(pb.StreamsServerMessage(
                    handle_assigned=pb.HandleAssigned(call_id=call_id)))
                return
            self._next_handle += 1
            answered_type = self._forced_type
            if answered_type is None:
                answered_type = self._type_of(method)
            self._forced_type = None
            self._outbound.put(pb.StreamsServerMessage(
                handle_assigned=pb.HandleAssigned(
                    call_id=call_id, handle=self._next_handle, type=answered_type)))

    @staticmethod
    def _type_of(method: str | None) -> pb.HandleType:
        """The type the real engine records for each minting method."""
        if method == "group_by_key":
            return pb.HandleType(
                kind=pb.HANDLE_KIND_GROUPED_STREAM,
                key_type=pb.DATA_TYPE_BYTES, value_type=pb.DATA_TYPE_BYTES)
        if method == "count":
            return pb.HandleType(
                kind=pb.HANDLE_KIND_TABLE,
                key_type=pb.DATA_TYPE_BYTES, value_type=pb.DATA_TYPE_LONG)
        return pb.HandleType(
            kind=pb.HANDLE_KIND_STREAM,
            key_type=pb.DATA_TYPE_BYTES, value_type=pb.DATA_TYPE_BYTES)

    def responses(self) -> Iterator[pb.StreamsServerMessage]:
        while True:
            message = self._outbound.get()
            if message is None:
                return
            yield message

    def close(self) -> None:
        self._outbound.put(None)

    # -- test seam: answer the next builder call with a caller-chosen type --
    def answer_next_call_with_type(self, handle_type: pb.HandleType) -> None:
        self._forced_type = handle_type

    # -- test seam: make the engine ask for a record to be mapped --
    def invoke(self, correlation: int, token: int, key: bytes, value: bytes) -> None:
        self._outbound.put(pb.StreamsServerMessage(
            invocation=pb.Invocation(
                correlation=correlation, function_token=token, key=key, value=value)))

    def await_client_message(self, kind: str, timeout: float = 5.0) -> pb.StreamsClientMessage:
        deadline = threading.Event()
        for _ in range(int(timeout * 200)):
            with self._lock:
                for message in reversed(self.sent):
                    if message.WhichOneof("message") == kind:
                        return message
            deadline.wait(0.005)
        raise AssertionError(f"the client never sent a {kind}")


@pytest.fixture()
def engine() -> Iterator[FakeEngine]:
    fake = FakeEngine()
    yield fake
    fake.close()


def test_the_builder_issues_the_five_calls_each_naming_the_prior_handle(engine: FakeEngine) -> None:
    session = StreamsSession(engine)
    session.open("counts", {"bootstrap.servers": "localhost:19092"})
    builder = session.builder()

    source = builder.source("in")
    mapped = builder.map_values(source, lambda key, value: value)
    grouped = builder.group_by_key(mapped)
    counted = builder.count(grouped, "counts-store")
    builder.sink(counted, "out")

    calls = [m.builder_call for m in engine.sent if m.WhichOneof("message") == "builder_call"]
    assert [c.WhichOneof("call") for c in calls] == [
        "source", "map_values", "group_by_key", "count", "sink"]
    # Each call names the handle the previous one produced - which is the whole point of handles.
    assert calls[1].map_values.handle == source
    assert calls[2].group_by_key.handle == mapped
    assert calls[3].count.handle == grouped
    assert calls[4].sink.handle == counted
    session.close()


def test_a_handle_knows_what_it_is_and_what_it_carries(engine: FakeEngine) -> None:
    session = StreamsSession(engine)
    session.open("counts", {})
    builder = session.builder()

    source = builder.source("in")
    counted = builder.count(builder.group_by_key(source), "counts-store")

    assert source.kind is HandleKind.STREAM
    assert source.value_type is DataType.BYTES
    # The mint whose value the host never supplied: the type field is the only way to know this.
    assert counted.kind is HandleKind.TABLE
    assert counted.key_type is DataType.BYTES
    assert counted.value_type is DataType.LONG
    session.close()


def test_the_reported_type_decodes_a_sink_value_without_tribal_knowledge() -> None:
    # Kafka's Serdes.Long() writes 8 bytes, big-endian, signed - and now nobody has to know that.
    assert DataType.LONG.decode(struct.pack(">q", 1002)) == 1002
    assert DataType.LONG.decode(struct.pack(">q", -7)) == -7
    payload = b"as-supplied"
    assert DataType.BYTES.decode(payload) is payload


def test_decoding_a_type_this_client_does_not_know_is_refused_by_name() -> None:
    with pytest.raises(StreamsError, match="UNKNOWN"):
        DataType.UNKNOWN.decode(b"\x00")
    with pytest.raises(StreamsError, match="UNSPECIFIED"):
        DataType.UNSPECIFIED.decode(b"\x00")


def test_a_wire_type_this_client_does_not_recognise_degrades_to_unknown(
    engine: FakeEngine,
) -> None:
    session = StreamsSession(engine)
    session.open("counts", {})

    # An engine newer than this client: enum values it has never heard of. proto3 enums are open,
    # so the values travel; the client must degrade explicitly rather than crash or guess bytes.
    # The casts exist because the generated stubs (rightly) type the fields to the KNOWN values -
    # exactly the mismatch this test manufactures.
    engine.answer_next_call_with_type(pb.HandleType(
        kind=cast(pb.HandleKind, 99),
        key_type=cast(pb.DataType, 98),
        value_type=cast(pb.DataType, 97)))
    handle = session.builder().source("in")

    assert handle.kind is HandleKind.UNKNOWN
    assert handle.key_type is DataType.UNKNOWN
    assert handle.value_type is DataType.UNKNOWN
    session.close()


def test_the_python_enums_mirror_the_wire_constants_exactly() -> None:
    """The hand-mirrored enums are the one place client and engine could drift.

    Both directions: every client member (bar UNKNOWN, which is client-local) is a wire value
    with the same number, and every wire value has a client member - so a DataType added to the
    proto without a mirrored member fails here instead of at a host's runtime.
    """
    wire_kinds = {name.removeprefix("HANDLE_KIND_"): number
                  for name, number in pb.HandleKind.items()}
    assert {m.name: m.value for m in HandleKind if m is not HandleKind.UNKNOWN} == wire_kinds

    wire_types = {name.removeprefix("DATA_TYPE_"): number
                  for name, number in pb.DataType.items()}
    assert {m.name: m.value for m in DataType if m is not DataType.UNKNOWN} == wire_types


def test_a_sink_answer_without_handle_or_type_is_inert_rather_than_fatal(
    engine: FakeEngine,
) -> None:
    session = StreamsSession(engine)
    session.open("counts", {})
    builder = session.builder()

    source = builder.source("in")
    builder.sink(source, "out")

    # The sink's answer carried neither handle nor type; the call completed and the session lives.
    grouped = builder.group_by_key(source)
    assert grouped.kind is HandleKind.GROUPED_STREAM
    session.close()


def test_a_function_is_represented_by_a_token_and_nothing_else(engine: FakeEngine) -> None:
    session = StreamsSession(engine)
    session.open("counts", {})
    builder = session.builder()

    def upper(key: bytes, value: bytes) -> bytes:
        return value.upper()

    source = builder.source("in")
    builder.map_values(source, upper)

    registration = engine.await_client_message("register_function")
    assert registration.register_function.token > 0

    # Nothing about the callable itself may cross: not its address, not its bytecode, not its
    # source.
    on_the_wire = b"".join(m.SerializeToString() for m in engine.sent)
    assert b"lambda" not in on_the_wire
    assert b"return value.upper()" not in on_the_wire
    assert str(id(upper)).encode() not in on_the_wire
    session.close()


def test_an_invocation_is_answered_on_its_own_correlation(engine: FakeEngine) -> None:
    session = StreamsSession(engine)
    session.open("counts", {})
    builder = session.builder()
    source = builder.source("in")
    builder.map_values(source, lambda key, value: value + b"!")

    token = engine.await_client_message("register_function").register_function.token
    engine.invoke(correlation=77, token=token, key=b"k", value=b"v")

    answer = engine.await_client_message("invocation_result").invocation_result
    assert answer.correlation == 77
    assert answer.value == b"v!"
    assert not answer.HasField("error")
    session.close()


def test_a_failing_function_reports_an_error_rather_than_a_substitute_value(
    engine: FakeEngine,
) -> None:
    session = StreamsSession(engine)
    session.open("counts", {})
    builder = session.builder()
    source = builder.source("in")

    def explode(key: bytes, value: bytes) -> bytes:
        raise ValueError("no")

    builder.map_values(source, explode)
    token = engine.await_client_message("register_function").register_function.token
    engine.invoke(correlation=88, token=token, key=b"k", value=b"v")

    answer = engine.await_client_message("invocation_result").invocation_result
    assert answer.correlation == 88
    assert "ValueError" in answer.error
    # A failed record is visible; a substitute value entering an aggregation is a wrong count.
    assert not answer.HasField("value")
    session.close()


def test_an_invocation_for_an_unregistered_token_is_answered_rather_than_ignored(
    engine: FakeEngine,
) -> None:
    session = StreamsSession(engine)
    session.open("counts", {})

    engine.invoke(correlation=99, token=4242, key=b"k", value=b"v")

    answer = engine.await_client_message("invocation_result").invocation_result
    assert answer.correlation == 99
    # Silence would leave a stream thread blocked until its timeout for no reason.
    assert "4242" in answer.error
    session.close()


def test_a_fault_surfaces_to_the_caller_rather_than_hanging() -> None:
    engine = FakeEngine(fault_on_call=1)
    session = StreamsSession(engine)
    session.open("counts", {})

    with pytest.raises(StreamsError, match="4242"):
        session.builder().source("in")
    engine.close()


class BreakingEngine(FakeEngine):
    """An engine whose response stream *raises* when it ends, the way a cancelled gRPC call does.

    The base fake ends its iterator cleanly, which is the one shape that cannot reproduce the bug
    this pair of tests covers: a clean end is silent either way.
    """

    def __init__(self) -> None:
        super().__init__()
        self.breakage = RuntimeError("Stream removed (Channel closed!)")

    def responses(self) -> Iterator[pb.StreamsServerMessage]:
        while True:
            message = self._outbound.get()
            if message is None:
                raise self.breakage
            yield message

    def break_unasked(self) -> None:
        """Breaks the stream without the client having asked for it."""
        self._outbound.put(None)


def _await_reader_exit(session: StreamsSession, timeout: float = 5.0) -> None:
    reader = session._reader
    assert reader is not None
    reader.join(timeout)
    assert not reader.is_alive(), "the reader thread did not exit"


def test_closing_the_session_is_not_reported_as_a_fault() -> None:
    """A deliberate close breaks the stream; that is the close, not an engine failure.

    Without this the demo printed a CANCELLED error on every clean shutdown, which trains a reader
    to ignore the one channel that would carry a real failure.
    """
    engine = BreakingEngine()
    session = StreamsSession(engine)
    session.open("counts", {})

    session.close()
    _await_reader_exit(session)

    assert session._fault is None


def test_a_stream_that_breaks_on_its_own_is_still_reported_as_a_fault() -> None:
    """The other half: silence must be bought by the close, not by the exception handler."""
    engine = BreakingEngine()
    session = StreamsSession(engine)
    session.open("counts", {})

    engine.break_unasked()
    _await_reader_exit(session)

    assert session._fault is not None
    assert "Channel closed" in session._fault


def test_describe_returns_the_graph_the_engine_assembled(engine: FakeEngine) -> None:
    """The host asks because it does not know: it holds handles, not a graph."""
    session = StreamsSession(engine)
    session.open("counts", {})

    description = session.describe()

    assert description.subtopologies[0].nodes[0].topics == ["input"]
    assert description.subtopologies[0].nodes[0].kind == pb.NODE_KIND_SOURCE
    # The text matters as much as the structure: it is what every existing Kafka Streams
    # visualiser parses, and it is the whole reason a language with no such tooling gets any.
    assert description.text.startswith("Topologies:")


class SlowDescribeEngine(FakeEngine):
    """Answers a describe LATE, and differently each time.

    Both properties are load-bearing, and the first version of these tests had neither - it used
    the base fake, which answers inside ``send`` before ``describe`` even begins waiting. That
    hides the two defects these tests exist for: an answer already sitting there satisfies a stale
    event just as well as a fresh one, and a waiter that never actually waits cannot be woken by
    anything. Both tests passed against deliberately broken code until this class existed.
    """

    def __init__(self, *, delay: float = 0.05, answer: bool = True) -> None:
        super().__init__()
        self._delay = delay
        self._answer = answer
        self.describes = 0

    def send(self, message: pb.StreamsClientMessage) -> None:
        if message.WhichOneof("message") == "describe":
            with self._lock:
                self.sent.append(message)
                self.describes += 1
                nth = self.describes
            if self._answer:
                threading.Timer(self._delay, self._deliver, args=(nth,)).start()
            return
        super().send(message)

    def _deliver(self, nth: int) -> None:
        self._outbound.put(pb.StreamsServerMessage(
            topology_description=pb.TopologyDescription(text=f"description {nth}")))

    def fault_later(self, reason: str) -> None:
        threading.Timer(self._delay, self._outbound.put, args=(
            pb.StreamsServerMessage(fault=pb.Fault(reason=reason)),)).start()


def test_a_second_describe_waits_for_its_own_answer() -> None:
    """The event from the previous answer must not satisfy the next request.

    Without the reset, the second call returns instantly holding the FIRST description - the host
    asks a changed topology what it looks like and is told what it used to look like.
    """
    engine = SlowDescribeEngine()
    session = StreamsSession(engine)
    session.open("counts", {})

    first = session.describe()
    second = session.describe()

    assert first.text == "description 1"
    assert second.text == "description 2"


def test_a_fault_releases_a_describe_waiter_rather_than_letting_it_time_out() -> None:
    """A session that fails WHILE a describe is waiting must report that, not hang to the timeout.

    The fault has to arrive after the wait begins and the engine must never answer - which is the
    real shape of the bug. At the call site a missed wakeup is indistinguishable from a hang, and a
    hang gets diagnosed as a network problem rather than as the error the engine actually sent.
    """
    engine = SlowDescribeEngine(answer=False)
    session = StreamsSession(engine)
    session.open("counts", {})

    engine.fault_later("the engine gave up")

    # Generous enough that a real answer would arrive, short enough that a missed wakeup shows up
    # as a failure here rather than as a slow suite.
    with pytest.raises(StreamsError, match="the engine gave up"):
        session.describe(timeout=3.0)
