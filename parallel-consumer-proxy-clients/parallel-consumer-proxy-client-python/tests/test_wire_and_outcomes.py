# Copyright (C) 2026 Antony Stubbs and contributors

"""The two client-side contracts that have no wire test: the token echo, and what a raise means."""

from __future__ import annotations

from parallel_consumer import InboundRecord, Outcome, OutboundRecord
from parallel_consumer import _wire as wire
from parallel_consumer._generated import proxy_pb2 as pb
from parallel_consumer.outcomes import resolve_outcome

RECORD = InboundRecord(topic="t", partition=0, offset=7, key=None, value=b"v", attempt=1)


def test_the_token_is_echoed_byte_for_byte():
    dispatched = pb.Token(record_id="t-0-7", epoch=3).SerializeToString()

    report = wire.report_message(dispatched, Outcome.success())

    # Byte-identical, not merely equal field by field: the client treats the token as opaque and
    # must never derive meaning from either half of it, so re-encoding it is not good enough.
    assert report.token.SerializeToString() == dispatched
    assert report.WhichOneof("outcome") == "success"


def test_a_success_carries_its_produce_records():
    token = pb.Token(record_id="t-0-7", epoch=1).SerializeToString()

    report = wire.report_message(
        token, Outcome.success([OutboundRecord(topic="out", key=b"k", value=b"v")]))

    assert [(p.topic, p.key, p.value) for p in report.success.produce] == [("out", b"k", b"v")]


def test_a_released_record_names_no_outcome_of_its_own():
    token = pb.Token(record_id="t-0-7", epoch=1).SerializeToString()

    report = wire.released_report(token)

    # Released is not a verdict - it is the client declining to invent one for work it never ran.
    assert report.WhichOneof("outcome") == "released"


def test_a_tombstone_keeps_its_null_value():
    dispatched = pb.DispatchRecord(
        token=pb.Token(record_id="t-0-7", epoch=1),
        record=pb.Record(topic="t", partition=0, offset=7, key=b"k"),
        attempt=1)

    record = wire.inbound_record(dispatched)

    assert record.key == b"k"
    assert record.value is None, "a null value must not arrive as empty bytes"


def test_returning_nothing_is_a_success():
    assert resolve_outcome(lambda record: None, RECORD).succeeded


def test_raising_is_a_failure_carrying_the_message():
    def explode(record):
        raise RuntimeError("boom")

    outcome = resolve_outcome(explode, RECORD)

    assert not outcome.succeeded
    assert outcome.reason == "boom"


def test_returning_something_that_is_not_an_outcome_fails_the_record_not_the_client():
    # The whole point of the test is a processor whose return type is wrong, so the type error
    # here is the fixture, not a defect - it is suppressed at the one line that needs it.
    outcome = resolve_outcome(lambda record: "done", RECORD)  # type: ignore[arg-type,return-value]

    assert not outcome.succeeded
    assert outcome.reason is not None
    assert "return an Outcome" in outcome.reason
