# Copyright (C) 2026 Antony Stubbs and contributors

"""What happens when a registered function calls back INTO the engine while it is being invoked.

Every crossing in the Streams session so far runs one way: the engine asks this process to compute
something, and this process answers. These tests pin what the wire does when the host tries to ask
a question of its own from inside that answer - a mapper that wants the current count for the key
it is mapping, say, which is the obvious thing to reach for and reads as perfectly reasonable code.

**These are characterisation tests: they assert the CURRENT behaviour, which is a hang.** They are
not asserting that the hang is correct. The session has exactly one thread reading the wire, and
that thread is the one running the user's function, so an answer the engine has already sent cannot
be delivered while the function that is waiting for it is still on the stack. When the wire gains
per-caller correlation and something other than the reader thread can settle a waiter, these tests
must be inverted rather than deleted - they are the record of what that change is for.

The fake is faithful in the one respect that matters here: it delivers answers through an iterator
that only the session's reader thread drains, exactly as ``GrpcStreamsTransport.responses`` does.
An answer produced instantly is still an answer nobody can collect.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator

import pytest

from parallel_consumer._generated import streams_pb2 as pb
from parallel_consumer.streams import StreamsError, StreamsSession

from test_streams_session import FakeEngine

#: Long enough that an answer which CAN be delivered arrives with room to spare - the fake answers
#: inside ``send`` - and short enough that a hang costs the suite under a second per case.
BLOCKED_FOR = 0.75


@pytest.fixture()
def engine() -> Iterator[FakeEngine]:
    """This module's own fake, closed on the way out.

    A duplicate of the fixture beside ``FakeEngine`` rather than a shared one: pytest does not
    export a fixture across test modules, and moving it to ``conftest.py`` would edit a file this
    change has no other reason to touch.
    """
    fake = FakeEngine()
    yield fake
    fake.close()


def _count_sent(engine: FakeEngine, kind: str) -> int:
    with engine._lock:
        return sum(1 for message in engine.sent if message.WhichOneof("message") == kind)


def _await_nth(engine: FakeEngine, kind: str, nth: int, timeout: float = 10.0) -> None:
    """Waits for the *nth* message of a kind.

    ``FakeEngine.await_client_message`` returns the most recent match and so returns instantly once
    one has ever been sent - useless here, where the point is that a SECOND query is now in flight
    behind an abandoned first.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _count_sent(engine, kind) >= nth:
            return
        time.sleep(0.005)
    raise AssertionError(f"the client never sent {nth} {kind} messages")


def _crossing_get(session: StreamsSession) -> None:
    session.get("counted", b"k", timeout=BLOCKED_FOR)


def _crossing_describe(session: StreamsSession) -> None:
    session.describe(timeout=BLOCKED_FOR)


def _crossing_builder_call(session: StreamsSession) -> None:
    # Through the private, because ``TopologyBuilder`` exposes no timeout - a builder call from
    # inside a function would otherwise block the stream thread for the full 30-second default.
    session._call(pb.BuilderCall(source=pb.Source(topic="in")), timeout=BLOCKED_FOR)


@pytest.mark.parametrize(
    ("name", "crossing"),
    [
        ("get", _crossing_get),
        ("describe", _crossing_describe),
        ("builder call", _crossing_builder_call),
    ],
)
def test_any_host_to_engine_call_from_inside_a_function_blocks_until_its_own_timeout(
    engine: FakeEngine, name: str, crossing: Callable[[StreamsSession], None],
) -> None:
    """The whole class, not one instance of it: every crossing that waits for an answer hangs.

    ``get`` is the one a host reaches for first, but the shape is shared by every call that waits
    on an event only the reader thread sets - the handshake, each builder call, ``describe`` and
    ``get`` alike. Parametrised rather than written once for ``get`` so that a fix which correlates
    queries and leaves ``describe`` alone cannot look complete.

    The engine is NOT slow here and never fails to answer: the fake answers inside ``send``, before
    the waiting even begins. The answer simply cannot be delivered, because delivering it is the
    job of the thread that is waiting for it.
    """
    session = StreamsSession(engine)
    session.open("reentrancy", {})
    blocked_for: list[float] = []
    refusal: list[str] = []

    def mapper(key: bytes, value: bytes) -> bytes:
        started = time.monotonic()
        try:
            crossing(session)
        except StreamsError as timed_out:
            refusal.append(str(timed_out))
        blocked_for.append(time.monotonic() - started)
        return b"mapped"

    token = session.register(mapper)
    engine.invoke(correlation=42, token=token, key=b"k", value=b"v")

    answered = engine.await_client_message("invocation_result", timeout=BLOCKED_FOR + 5.0)

    # The function ran, and its own call sat blocked for the whole timeout rather than returning.
    assert blocked_for, f"the {name} never returned at all"
    assert blocked_for[0] >= BLOCKED_FOR, (
        f"the {name} returned in {blocked_for[0]:.3f}s - if the session gained a second reader, "
        f"or a way to settle a waiter off the reader thread, this test has served its purpose and "
        f"must be inverted rather than relaxed")
    # It fails as a timeout, which at the call site is indistinguishable from an unreachable engine.
    assert refusal and "did not answer" in refusal[0]

    # And the record was held hostage for the whole of it: the engine's stream thread stays blocked
    # in InvocationRegistry.awaitResult for as long as this side takes, so the cost is not confined
    # to the host.
    assert answered.invocation_result.correlation == 42
    assert answered.invocation_result.value == b"mapped"
    session.close()


class ReleasesAnswersOneAtATime(FakeEngine):
    """Delivers query answers only as released, so an ordering can be constructed rather than raced.

    Two holds are needed, not one, and finding that out is half the finding. Holding only the
    abandoned answer back still loses: released, the reader thread delivers it and immediately
    picks up the following answer, overwriting the single ``_get_result`` slot before the waiting
    thread has been scheduled to read it. The test then sees the right value for the wrong reason
    and passes against the defect it exists to demonstrate - which is what the first version of it
    did, on every run.

    So every answer waits for its own permit. The mis-delivery is then a property of the code
    rather than of the scheduler.
    """

    def __init__(self) -> None:
        super().__init__()
        self._permits = threading.Semaphore(0)

    def release_one_answer(self) -> None:
        self._permits.release()

    def responses(self) -> Iterator[pb.StreamsServerMessage]:
        for message in super().responses():
            if message.WhichOneof("message") == "get_result":
                self._permits.acquire()
            yield message

    def close(self) -> None:
        # Enough permits that the reader is never left parked on one, or the session's close would
        # deadlock on the very mechanism this class exists to create.
        for _ in range(100):
            self._permits.release()
        super().close()


def test_the_answer_a_timed_out_reentrant_query_abandoned_is_collected_by_the_next_caller() -> None:
    """The hang is not the whole cost: it leaves an unclaimed answer on the wire.

    A query carries no correlation - unlike a builder call, which has ``call_id`` - so the session
    holds exactly one ``_get_result`` slot and one ``_got`` event for all queries. When a reentrant
    query gives up, its answer is still coming, and the next caller to wait on that one event
    collects it. Different store, different key, and no error anywhere: the host is handed a
    confident wrong value.

    This is why "document the reentrancy limitation and move on" does not close the issue. The
    hang is visible and survivable; this is silent.
    """
    engine = ReleasesAnswersOneAtATime()
    engine._get_answer = pb.GetResult(
        found=True, value=b"ABANDONED", value_type=pb.DATA_TYPE_BYTES)
    session = StreamsSession(engine)
    session.open("reentrancy", {})

    def mapper(key: bytes, value: bytes) -> bytes:
        with pytest.raises(StreamsError):
            session.get("counted", key, timeout=0.3)
        return b"mapped"

    token = session.register(mapper)
    engine.invoke(correlation=42, token=token, key=b"k", value=b"v")
    engine.await_client_message("invocation_result", timeout=10.0)

    # A different question entirely, asked by a different thread, after the first one gave up.
    engine._get_answer = pb.GetResult(found=True, value=b"FRESH", value_type=pb.DATA_TYPE_BYTES)
    collected: list[bytes | int | None] = []

    def ask() -> None:
        collected.append(session.get("counted", b"a-different-key", timeout=10.0))

    asker = threading.Thread(target=ask, name="asker")
    asker.start()
    _await_nth(engine, "get", 2)
    # The fresh query is now sent and waiting; let the abandoned answer through ahead of it, and
    # ONLY that one - the fresh answer stays held, so what the asker collects cannot be an accident
    # of which thread the interpreter happened to run next.
    engine.release_one_answer()
    asker.join(15.0)

    assert collected, "the second query never returned"
    assert collected[0] == b"ABANDONED", (
        "the stale answer was no longer mis-delivered - if queries gained a correlation, invert "
        "this test to assert b'FRESH' rather than relaxing it")
    session.close()


@pytest.mark.xfail(strict=True, reason="single-slot query state: Get carries no correlation, so "
                                       "two concurrent callers share one answer. Remove this "
                                       "marker when Get/GetResult gain a correlation.")
def test_two_concurrent_queries_each_receive_their_own_answer() -> None:
    """Two ordinary host threads querying at once. No invocation, no mapper, no re-entrancy.

    Marked ``xfail(strict=True)`` rather than written as a characterisation test, and the
    difference is deliberate. The tests above pin an accepted PoC limitation, so they assert what
    the code DOES. This one pins a defect, so it asserts what the code SHOULD do and records that
    it does not yet - and ``strict`` means that the moment somebody adds the correlation, this
    turns into an XPASS failure telling them to delete the marker. Pinning a defect as passing
    behaviour would instead make the eventual fix look like a regression.

    The session holds one ``_got`` event and one ``_get_result`` slot for every query, so the
    second answer to arrive overwrites the first and both callers read whichever landed last.
    ``describe()`` has the identical shape (one ``_described``, one ``_description``).

    The existing ``test_a_second_query_waits_for_its_own_answer`` does not cover this: two
    SEQUENTIAL queries are handled correctly by the ``_got.clear()`` in ``get``.
    """
    class AnswersWithTheKeyItWasAsked(FakeEngine):
        """Echoes the key back as the value, so a mis-delivery names the query it belongs to."""

        def send(self, message: pb.StreamsClientMessage) -> None:
            super().send(message)
            if message.WhichOneof("message") == "get":
                self._outbound.put(pb.StreamsServerMessage(get_result=pb.GetResult(
                    found=True, value=message.get.key, value_type=pb.DATA_TYPE_BYTES)))

    engine = AnswersWithTheKeyItWasAsked()
    session = StreamsSession(engine)
    session.open("concurrent", {})

    answers: dict[bytes, bytes | int | None] = {}
    both_ready = threading.Barrier(2)

    def ask(key: bytes) -> None:
        both_ready.wait(timeout=5)
        answers[key] = session.get("store", key, timeout=5.0)

    threads = [threading.Thread(target=ask, args=(k,)) for k in (b"alpha", b"beta")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    session.close()
    assert answers == {b"alpha": b"alpha", b"beta": b"beta"}
