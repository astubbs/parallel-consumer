# Copyright (C) 2026 Antony Stubbs and contributors

"""Translation between this library's surface and the frozen protocol's messages.

Kept in one module so the wire appears in exactly one place: nothing above it handles a protobuf
message, and nothing below it handles a :class:`~parallel_consumer.records.InboundRecord`.

The delivery token is the exception, and deliberately so. It travels as **opaque bytes** from
dispatch to report - out to the worker process and back - and is re-attached without either field
ever being read. That is what lets this client hold no request map, no completion registry and no
dedupe cache: it is stateless per record, and the fencing that needs the token's meaning is the
proxy's job.
"""

from __future__ import annotations

from datetime import timezone

from ._generated import proxy_pb2 as pb
from .options import ClientOptions, ProcessingOrder
from .outcomes import Outcome
from .records import InboundRecord

__all__ = ["configure_message", "inbound_record", "released_report", "report_message"]

_ORDERING = {
    ProcessingOrder.UNORDERED: pb.PROCESSING_ORDER_UNORDERED,
    ProcessingOrder.PARTITION: pb.PROCESSING_ORDER_PARTITION,
    ProcessingOrder.KEY: pb.PROCESSING_ORDER_KEY,
}

DISPATCH = "dispatch"
"""The one capability this wave implements. See :func:`configure_message`."""


def configure_message(options: ClientOptions, capabilities: tuple[str, ...]) -> pb.Configure:
    """Builds the connect-time ``Configure``.

    Capabilities are declared explicitly rather than left empty. Empty declares the complete v1
    baseline, which would claim duties this client does not yet perform; naming what it does
    implement lets the proxy hold it to exactly that.
    """
    configure = pb.Configure(kafka_properties=dict(options.kafka_properties),
                             capabilities=list(capabilities))
    if options.topics:
        configure.topics.extend(options.topics)
    if options.topic_pattern is not None:
        configure.topic_pattern = options.topic_pattern
    if options.max_concurrency is not None:
        configure.max_concurrency = options.max_concurrency
    if options.ordering is not None:
        configure.ordering = _ORDERING[options.ordering]
    if options.commit_interval is not None:
        configure.commit_interval.FromTimedelta(options.commit_interval)
    if options.default_message_retry_delay is not None:
        configure.default_message_retry_delay.FromTimedelta(options.default_message_retry_delay)
    return configure


def inbound_record(dispatched: pb.DispatchRecord) -> InboundRecord:
    """Turns one dispatched record into what the user's function sees.

    ``key`` and ``value`` keep Kafka's null/empty distinction: presence on the wire is what says
    the field was there at all, so a tombstone arrives as ``None`` and not as ``b""``.
    """
    record = dispatched.record
    return InboundRecord(
        topic=record.topic,
        partition=record.partition,
        offset=record.offset,
        key=record.key if record.HasField("key") else None,
        value=record.value if record.HasField("value") else None,
        attempt=dispatched.attempt,
        last_failure_at=(
            dispatched.last_failure_at.ToDatetime(tzinfo=timezone.utc)
            if dispatched.HasField("last_failure_at")
            else None
        ),
        last_failure_reason=(
            dispatched.last_failure_reason if dispatched.HasField("last_failure_reason") else None
        ),
    )


def report_message(token: bytes, outcome: Outcome) -> pb.Report:
    """Builds the ``Report`` for one resolved record, echoing the token verbatim."""
    report = pb.Report()
    report.token.ParseFromString(token)
    if outcome.succeeded:
        # SetInParent first: reading a oneof's submessage does not select the arm, and a bare
        # success (the common case) has nothing else to write that would select it.
        report.success.SetInParent()
        for outbound in outcome.produce:
            produce = report.success.produce.add()
            produce.topic = outbound.topic
            if outbound.key is not None:
                produce.key = outbound.key
            if outbound.value is not None:
                produce.value = outbound.value
    else:
        report.failure.SetInParent()
        if outcome.reason is not None:
            report.failure.reason = outcome.reason
    return report


def released_report(token: bytes) -> pb.Report:
    """Builds the ``Report`` for a record this client queued but never ran.

    Released is not a verdict: it returns the record to scheduling with its attempt count
    unchanged, which is how the client avoids inventing an outcome for work it did not do.
    """
    report = pb.Report()
    report.token.ParseFromString(token)
    report.released.SetInParent()
    return report
