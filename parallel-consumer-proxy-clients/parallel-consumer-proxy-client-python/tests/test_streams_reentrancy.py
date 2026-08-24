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
