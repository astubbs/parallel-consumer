# Copyright (C) 2026 Antony Stubbs and contributors

"""The Streams session, driven against a fake engine.

No broker, no server, no Docker - this suite is deliberately free of all three, and the end-to-end
run lives in the demo instead. What is reachable here is the part that matters on this side: that
the builder names handles the engine minted, that a function is represented by a token and never by
anything else, and that an invocation is answered on its own correlation.
"""

from __future__ import annotations

import queue
import threading
from collections.abc import Iterator

import pytest

from parallel_consumer._generated import streams_pb2 as pb
from parallel_consumer.streams import StreamsError, StreamsSession


class FakeEngine:
    """Answers like the engine would: Ready for the handshake, a minted handle per builder call."""

    def __init__(self, *, fault_on_call: int | None = None) -> None:
        self.sent: list[pb.StreamsClientMessage] = []
        self._outbound: queue.Queue[pb.StreamsServerMessage | None] = queue.Queue()
        self._next_handle = 100
        self._fault_on_call = fault_on_call
        self._lock = threading.Lock()

    # -- the transport surface the session uses --
    def send(self, message: pb.StreamsClientMessage) -> None:
        with self._lock:
            self.sent.append(message)
        kind = message.WhichOneof("message")
        if kind == "open":
            self._outbound.put(pb.StreamsServerMessage(
                ready=pb.Ready(application_id=message.open.application_id)))
        elif kind == "builder_call":
            call_id = message.builder_call.call_id
            if self._fault_on_call == call_id:
                self._outbound.put(pb.StreamsServerMessage(
                    fault=pb.Fault(
                        reason=f"call {call_id} names handle 4242, which does not exist")))
                return
            self._next_handle += 1
            self._outbound.put(pb.StreamsServerMessage(
                handle_assigned=pb.HandleAssigned(call_id=call_id, handle=self._next_handle)))

    def responses(self) -> Iterator[pb.StreamsServerMessage]:
        while True:
            message = self._outbound.get()
            if message is None:
                return
            yield message

    def close(self) -> None:
        self._outbound.put(None)

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
