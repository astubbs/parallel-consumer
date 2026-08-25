# Copyright (C) 2026 Antony Stubbs and contributors

"""The windowed aggregation from the host's side: windowed_by, aggregate, to_stream.

Driven against the FakeEngine, like the rest of the builder surface - no broker, no server. What
is reachable here is what this side owns: that the three new builder calls carry their fields,
that a returned windowed handle exposes the window the engine recorded, and that a three-argument
aggregator is dispatched with key, value and accumulator in that order.

Instrument check (R4), recorded here because the FakeEngine answers synchronously and this module
has hidden dead assertions before: with ``FakeEngine._type_of`` deliberately transposing size_ms
and advance_ms into the echoed window, ``test_a_windowed_handle_exposes_the_window_the_engine_
recorded`` failed at ``assert windowed.window == HOPPING_HOUR`` (size 300000 against the expected
3600000, advance the mirror image) - so the assertion does read the spec field by field, and a
fake inventing its own window cannot pass. Sabotage removed after the red run.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from parallel_consumer.streams import (
    DataType,
    FunctionKind,
    HandleKind,
    StreamsSession,
    TimeWindow,
)
from parallel_consumer._generated import streams_pb2 as pb
from parallel_consumer.streams._session import _arguments

from test_streams_session import FakeEngine

#: One hour tumbling with a five-minute advance would be hopping; the tests below use distinct
#: values for every field so a transposition can never read as a pass.
HOPPING_HOUR = TimeWindow(size_ms=3_600_000, advance_ms=300_000, grace_ms=60_000,
                          retention_ms=7_200_000)


@pytest.fixture()
def engine() -> Iterator[FakeEngine]:
    # A duplicate of the fixture beside FakeEngine rather than a shared one: pytest does not
    # import fixtures across files without a conftest, and the sidecar conftest is not for fakes.
    fake = FakeEngine()
    yield fake
    fake.close()


def test_the_builder_issues_the_three_windowing_calls_naming_prior_handles(
    engine: FakeEngine,
) -> None:
    """U4 scenario 11: the full windowed chain round-trips through the builder."""
    session = StreamsSession(engine)
    session.open("windows", {})
    builder = session.builder()

    grouped = builder.group_by_key(builder.source("in"))
    windowed = builder.windowed_by(grouped, HOPPING_HOUR)
    table = builder.aggregate(windowed, lambda key, value, acc: acc + value, b"seed", "agg-store")
    restreamed = builder.to_stream(table)
    builder.sink(restreamed, "out")

    calls = [m.builder_call for m in engine.sent if m.WhichOneof("message") == "builder_call"]
    assert [c.WhichOneof("call") for c in calls] == [
        "source", "group_by_key", "windowed_by", "aggregate", "to_stream", "sink"]
    assert calls[2].windowed_by.handle == grouped
    # All four window fields travel, distinct values each, so a transposition cannot pass.
    assert calls[2].windowed_by.window.size_ms == 3_600_000
    assert calls[2].windowed_by.window.advance_ms == 300_000
    assert calls[2].windowed_by.window.grace_ms == 60_000
    assert calls[2].windowed_by.window.retention_ms == 7_200_000
    assert calls[3].aggregate.handle == windowed
    assert calls[3].aggregate.initial == b"seed"
    assert calls[3].aggregate.function_token > 0
    assert calls[3].aggregate.store_name == "agg-store"
    assert calls[4].to_stream.handle == table
    assert calls[5].sink.handle == restreamed
    session.close()


def test_a_windowed_handle_exposes_the_window_the_engine_recorded(engine: FakeEngine) -> None:
    """The window rides on the handle, read back from the HandleAssigned - never guessed locally."""
    session = StreamsSession(engine)
    session.open("windows", {})
    builder = session.builder()

    windowed = builder.windowed_by(builder.group_by_key(builder.source("in")), HOPPING_HOUR)
    table = builder.aggregate(windowed, lambda key, value, acc: acc, b"", "agg-store")
    restreamed = builder.to_stream(table)

    assert windowed.kind is HandleKind.TIME_WINDOWED_STREAM
    assert windowed.window == HOPPING_HOUR
    # The aggregate's table carries the same window; the re-keyed stream carries none - to_stream
    # DROPS the window, which is its whole job.
    assert table.kind is HandleKind.TABLE
    assert table.window == HOPPING_HOUR
    assert restreamed.kind is HandleKind.STREAM
    assert restreamed.window is None
    assert restreamed.value_type is DataType.BYTES
    session.close()


def test_the_arguments_table_orders_every_kind_the_way_its_function_declares() -> None:
    """U4 scenario 13, directly on the table: arity and order live in ONE place.

    Every shape is bytes, so any pairing type-checks - only an assertion on which field lands in
    which position can tell a transposition from a pass. The map, reduce and join rows pin the
    behaviour the arity change inherited; the aggregate row is the reason the table exists.
    """
    invocation = pb.Invocation(key=b"k", value=b"v", aggregate=b"acc", right=b"r")

    assert _arguments(FunctionKind.MAP, invocation) == (b"k", b"v")
    assert _arguments(FunctionKind.REDUCE, invocation) == (b"acc", b"v")
    assert _arguments(FunctionKind.JOIN, invocation) == (b"v", b"r")
    assert _arguments(FunctionKind.AGGREGATE, invocation) == (b"k", b"v", b"acc")


def test_an_aggregator_receives_key_value_and_accumulator_in_that_order(
    engine: FakeEngine,
) -> None:
    """U4 scenario 12: the three-argument dispatch, end to end through the session."""
    session = StreamsSession(engine)
    session.open("windows", {})
    seen: list[tuple[bytes, bytes, bytes]] = []

    def fold(key: bytes, value: bytes, accumulator: bytes) -> bytes:
        seen.append((key, value, accumulator))
        return accumulator + value

    token = session.register(fold, kind=FunctionKind.AGGREGATE)
    engine.aggregate_invoke(1, token, key=b"k", value=b"v", aggregate=b"acc")

    result = engine.await_client_message("invocation_result")
    assert seen == [(b"k", b"v", b"acc")]
    assert result.invocation_result.value == b"accv"
    session.close()


def test_an_aggregate_invocation_for_an_unregistered_token_is_answered_with_an_error(
    engine: FakeEngine,
) -> None:
    """U4 scenario 14: matching the single-record behaviour - an error answer, never a drop.

    A drop would leave the engine's stream thread blocked for the whole invocation timeout to
    learn what it could have been told immediately.
    """
    session = StreamsSession(engine)
    session.open("windows", {})

    engine.aggregate_invoke(7, token=4242, key=b"k", value=b"v", aggregate=b"acc")

    answer = engine.await_client_message("invocation_result").invocation_result
    assert answer.correlation == 7
    assert "4242" in answer.error
    session.close()


def test_an_aggregation_sent_to_a_mapper_is_refused_rather_than_guessed(
    engine: FakeEngine,
) -> None:
    """The kind check holds for the fourth shape too - a mapper must not fold three arguments."""
    session = StreamsSession(engine)
    session.open("windows", {})
    called = False

    def transform(key: bytes, value: bytes) -> bytes:
        nonlocal called
        called = True
        return value

    token = session.register(transform)
    engine.aggregate_invoke(1, token, key=b"k", value=b"v", aggregate=b"acc")

    result = engine.await_client_message("invocation_result")
    assert not called
    assert "registered as MAP" in result.invocation_result.error
    assert "arrived as AGGREGATE" in result.invocation_result.error
    session.close()
