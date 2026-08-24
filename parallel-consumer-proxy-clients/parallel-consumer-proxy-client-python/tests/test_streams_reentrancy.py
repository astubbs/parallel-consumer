# Copyright (C) 2026 Antony Stubbs and contributors

"""A registered function calling back INTO the engine while it is being invoked.

Every other crossing in the Streams session runs one way: the engine asks this process to compute
something, and this process answers. These tests cover the host asking a question of its own from
inside that answer - a mapper that wants the current count for the key it is mapping, say, which is
the obvious thing to reach for and reads as perfectly reasonable code.

**These were characterisation tests asserting a hang, and they are INVERTED here rather than
deleted** - they are the record of what the two changes underneath them were for, in the order the
changes had to happen:

1. ``Get`` and ``Describe`` gained a ``call_id``, so a query's answer reaches the caller that asked
   it. Before that, one answer slot served every query in the session.
2. Registered functions moved off the reader thread onto a worker pool. Before that, the thread
   running the user's function was the only thread that could deliver an answer to it, so every
   waiting crossing - ``get``, ``describe``, a builder call - blocked until its own timeout while
   the answer sat undelivered. The engine was never at fault: it answers a query on its transport
   thread while every stream thread is blocked, which is why the answer existed to be abandoned.

Doing (2) first would have made things worse rather than better, turning re-entrant queries from a
hang into more concurrent callers contending for the one answer slot (1) removed.

The fake is faithful in the one respect that matters here: it delivers answers through an iterator
that only the session's reader thread drains, exactly as ``GrpcStreamsTransport.responses`` does.
An answer produced instantly is still an answer nobody can collect if nobody is reading.
"""

from __future__ import annotations

import logging
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

#: How many host threads query at once in the concurrency test. Well above the two the defect was
#: first seen with: one pair crossing its answers is a coin flip that a lucky schedule hides, and
#: the defect DID hide that way on some runs. Every asker uses a distinct key, so a crossed answer
#: is named rather than merely counted.
CONCURRENT_ASKERS = 16


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


def _crossing_get(session: StreamsSession) -> object:
    return session.get("counted", b"k", timeout=BLOCKED_FOR)


def _crossing_describe(session: StreamsSession) -> object:
    return session.describe(timeout=BLOCKED_FOR)


def _crossing_builder_call(session: StreamsSession) -> object:
    # Through the private, because ``TopologyBuilder`` exposes no timeout - a builder call from
    # inside a function would otherwise block the stream thread for the full 30-second default.
    return session._call(pb.BuilderCall(source=pb.Source(topic="in")), timeout=BLOCKED_FOR)


def _is_the_stored_value(answer: object) -> bool:
    return answer == b"stored"


def _is_a_description(answer: object) -> bool:
    return isinstance(answer, pb.TopologyDescription) and "Sub-topology" in answer.text


def _is_a_minted_handle(answer: object) -> bool:
    return isinstance(answer, int) and answer > 0


@pytest.mark.parametrize(
    ("name", "crossing", "is_a_real_answer"),
    [
        ("get", _crossing_get, _is_the_stored_value),
        ("describe", _crossing_describe, _is_a_description),
        ("builder call", _crossing_builder_call, _is_a_minted_handle),
    ],
)
def test_any_host_to_engine_call_from_inside_a_function_is_answered(
    engine: FakeEngine, name: str, crossing: Callable[[StreamsSession], object],
    is_a_real_answer: Callable[[object], bool],
) -> None:
    """The whole class, not one instance of it: every crossing that waits for an answer gets one.

    This asserted the opposite until registered functions moved off the reader thread. ``get`` is
    the one a host reaches for first, but the shape is shared by every call that waits on a waiter
    the reader settles - each builder call, ``describe`` and ``get`` alike. Parametrised rather
    than written once for ``get`` so that a fix which unblocked queries and left ``describe``
    hanging cannot look complete.

    The answer is checked for CONTENT, not merely for not raising: the failure this replaces was a
    timeout, and a crossing that returned ``None`` promptly would satisfy a timing assertion while
    being just as useless.
    """
    session = StreamsSession(engine)
    session.open("reentrancy", {})
    engine._get_answer = pb.GetResult(
        found=True, value=b"stored", value_type=pb.DATA_TYPE_BYTES)
    took: list[float] = []
    answers: list[object] = []
    refusals: list[str] = []

    def mapper(key: bytes, value: bytes) -> bytes:
        started = time.monotonic()
        try:
            answers.append(crossing(session))
        except StreamsError as refused:
            refusals.append(str(refused))
        took.append(time.monotonic() - started)
        return b"mapped"

    token = session.register(mapper)
    engine.invoke(correlation=42, token=token, key=b"k", value=b"v")

    answered = engine.await_client_message("invocation_result", timeout=BLOCKED_FOR + 5.0)

    assert not refusals, f"the {name} from inside a function was refused: {refusals}"
    assert answers and is_a_real_answer(answers[0]), (
        f"the {name} returned {answers!r}, which is not the answer the engine sent")
    # Well inside its own timeout, so a pass cannot be a slow near-miss of the deadline that used
    # to be hit exactly.
    assert took[0] < BLOCKED_FOR / 2, (
        f"the {name} took {took[0]:.3f}s of its {BLOCKED_FOR}s budget - it is being delivered "
        f"late rather than promptly, which is the old hang wearing a shorter timeout")

    # And the record was not held hostage: the engine's stream thread stays blocked in
    # InvocationRegistry.awaitResult for as long as this side takes, so the cost of a hang here was
    # never confined to the host.
    assert answered.invocation_result.correlation == 42
    assert answered.invocation_result.value == b"mapped"
    session.close()


def test_a_blocked_user_function_does_not_stop_the_next_invocation_being_served(
        engine: FakeEngine) -> None:
    """The property underneath the inversion above, asserted directly rather than as a side effect.

    The first function cannot finish until the second one runs. On the reader thread that is a
    deadlock by construction - the second invocation is never even read - so this test cannot pass
    unless invocations are dispatched off it. It is also what makes the re-entrancy fix a real
    fix rather than a special case for ``get``: what a function waits for does not matter, only
    that waiting no longer stops the wire being read.
    """
    session = StreamsSession(engine)
    session.open("reentrancy", {})
    first_running = threading.Event()
    release_first = threading.Event()

    def mapper(key: bytes, value: bytes) -> bytes:
        if key == b"first":
            first_running.set()
            assert release_first.wait(10), "the second invocation never ran"
            return b"first-done"
        release_first.set()
        return b"second-done"

    token = session.register(mapper)
    engine.invoke(correlation=1, token=token, key=b"first", value=b"v")
    assert first_running.wait(5), "the first invocation never ran"
    engine.invoke(correlation=2, token=token, key=b"second", value=b"v")

    _await_nth(engine, "invocation_result", 2)
    results = {
        message.invocation_result.correlation: message.invocation_result.value
        for message in engine.sent
        if message.WhichOneof("message") == "invocation_result"
    }
    assert results == {1: b"first-done", 2: b"second-done"}
    session.close()


def test_closing_the_session_from_inside_a_user_function_does_not_deadlock(
        engine: FakeEngine) -> None:
    """A host closing the session from inside a mapper is ordinary, and must not hang.

    The worker pool makes this a live hazard rather than a hypothetical one: the function calling
    ``close`` is itself running on a worker, so a shutdown that waits for the pool to drain would
    have that thread wait for itself. Asserted through an event rather than by the test simply
    completing, so a regression fails in seconds instead of hanging the suite.
    """
    session = StreamsSession(engine)
    session.open("reentrancy", {})
    closed = threading.Event()

    def mapper(key: bytes, value: bytes) -> bytes:
        session.close()
        closed.set()
        return b"mapped"

    token = session.register(mapper)
    engine.invoke(correlation=7, token=token, key=b"k", value=b"v")

    assert closed.wait(10), "close() from inside a registered function never returned"


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


def test_an_abandoned_answer_is_dropped_rather_than_collected_by_the_next_caller(
        caplog: pytest.LogCaptureFixture) -> None:
    """The inverted half of the single-slot defect: a late answer now reaches nobody.

    A query carries its own ``call_id`` and the engine echoes it, so a caller that timed out and
    withdrew leaves no slot for its answer to land in. When that answer finally arrives it names a
    call nobody is waiting for and is dropped with a warning - where it used to be handed to the
    next thread to wait, which then held a confident value for a key it never asked about, from a
    store it may never have named.

    Written as a re-entrant query because that is the shape the defect was found in, but the
    mechanism has nothing to do with re-entrancy: any query that gives up before its answer lands
    used to leak it onto the next caller.
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

    # The fresh query is now sent and waiting. Let the ABANDONED answer through, and only that one
    # - so if it were still being mis-delivered, the asker would return holding it.
    with caplog.at_level(logging.WARNING, logger="parallel_consumer.streams._session"):
        caplog.clear()
        engine.release_one_answer()
        asker.join(1.0)
        assert not collected, "the abandoned answer was delivered to a caller that never asked it"
        # A positive assertion, not just the absence of a delivery: it pins that the answer was
        # SEEN and refused, so a session that silently stopped reading would fail here too.
        assert "nobody is waiting for" in caplog.text, (
            "the abandoned answer was neither delivered nor dropped - it went somewhere unrecorded")

    # And the asker's own answer, once released, is the one it gets.
    engine.release_one_answer()
    asker.join(15.0)
    assert collected == [b"FRESH"]
    session.close()


def test_two_concurrent_queries_each_receive_their_own_answer() -> None:
    """Two ordinary host threads querying at once. No invocation, no mapper, no re-entrancy.

    This was the silent member of the family, and it carried an ``xfail(strict=True)`` marker
    until ``Get``/``GetResult`` gained a ``call_id``: the session held one answer slot for every
    query in it, so the second answer to arrive overwrote the first and both callers read
    whichever landed last. No exception, no fault, no log line - just a confident value for a key
    nobody asked about. The marker turned the fix into an XPASS failure rather than letting it
    look like a regression, which is how it came to be deleted here.

    ``test_a_second_query_waits_for_its_own_answer`` does not cover this and never did: two
    SEQUENTIAL queries were always handled correctly.
    """
    class AnswersEveryQueryAtOnceInReverse(FakeEngine):
        """Holds every query until all of them are in, then answers them BACKWARDS.

        Three properties, each of which a weaker fake cost this test:

        * It echoes the key as the value, so a mis-delivery names the query it belongs to.
        * It answers the query itself rather than delegating to the base fake, which answers every
          ``get`` with the arranged ``_get_answer`` - two answers per query, and the first a
          default ``found=False``. Under one shared slot that failed whichever way the race fell,
          so the test failed for a reason it never claimed.
        * It answers in REVERSE order, and that is what makes the test sensitive to the
          correlation rather than to arrival order. A client that matched each answer to the
          OLDEST waiting caller instead of the one it names passes when answers come back in the
          order they were asked - which is exactly what a fake answering inside ``send`` produces,
          and it passed a deliberate sabotage of that shape.
        """

        def __init__(self, expected: int) -> None:
            super().__init__()
            self._expected = expected
            self._held: list[pb.StreamsServerMessage] = []

        def send(self, message: pb.StreamsClientMessage) -> None:
            if message.WhichOneof("message") != "get":
                super().send(message)
                return
            with self._lock:
                self.sent.append(message)
                self._held.append(pb.StreamsServerMessage(get_result=pb.GetResult(
                    call_id=message.get.call_id, found=True, value=message.get.key,
                    value_type=pb.DATA_TYPE_BYTES)))
                if len(self._held) < self._expected:
                    return
                answers = list(reversed(self._held))
                self._held = []
            for answer in answers:
                self._outbound.put(answer)

    # More than two: the original defect was masked by scheduling on some runs, so a single pair
    # proves very little. Every key is distinct, so any crossed answer is named rather than counted.
    keys = [f"key-{n}".encode() for n in range(CONCURRENT_ASKERS)]
    engine = AnswersEveryQueryAtOnceInReverse(len(keys))
    session = StreamsSession(engine)
    session.open("concurrent", {})

    answers: dict[bytes, bytes | int | None] = {}
    answers_lock = threading.Lock()
    all_ready = threading.Barrier(len(keys))

    def ask(key: bytes) -> None:
        all_ready.wait(timeout=10)
        answer = session.get("store", key, timeout=10.0)
        with answers_lock:
            answers[key] = answer

    threads = [threading.Thread(target=ask, args=(key,), name=key.decode()) for key in keys]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=20)

    session.close()
    assert answers == {key: key for key in keys}
