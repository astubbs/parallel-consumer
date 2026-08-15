# Copyright (C) 2026 Antony Stubbs and contributors

"""The one-record scenario, end to end, over the real wire.

``a-processed-record-advances-the-committed-offset`` is the baseline every client in every
language runs first: one record in, processed once, offset advances past it. The offset half of
that assertion is engine state a client cannot see, and the harness has no verdict channel, so
what this test asserts is the wire-observable consequence - **the record arrives exactly once,
and a success is followed by silence rather than a redelivery.** A report the proxy rejected
(a token this client mangled, say) would leave the record in flight and the run would not end
cleanly.

Real everything: a real child process, a real gRPC stream, a real worker process running a
closure. The only unreality is the sidecar's Kafka, which is mock clients seeded with the
scenario's records.
"""

from __future__ import annotations

import multiprocessing
import queue

import pytest

from parallel_consumer import ClientOptions, ParallelConsumerClient

SCENARIO = "a-processed-record-advances-the-committed-offset"

QUIET_PERIOD = 3.0
"""Seconds of silence that stand for 'not redelivered'. The scenario's retry delay is shorter."""


@pytest.mark.sidecar
def test_one_record_goes_through_and_is_not_redelivered(sidecar_for):
    deliveries: multiprocessing.Queue[tuple[str, int, int, int]] = multiprocessing.Queue()

    # A closure, over a queue this process created - which is the whole point of forking the
    # worker pool before any channel exists. The user's function is never an importable name.
    def process(record):
        deliveries.put((record.topic, record.partition, record.offset, record.attempt))

    options = ClientOptions(topics=[SCENARIO], max_concurrency=4)

    with ParallelConsumerClient(options, sidecar=sidecar_for(SCENARIO)) as client:
        client.poll(process)

        first = deliveries.get(timeout=30)
        assert first[0] == SCENARIO
        assert first[3] == 1, "a first delivery is attempt 1"

        with pytest.raises(queue.Empty):
            # Anything arriving here is a redelivery, which is what a report the proxy rejected
            # would eventually produce.
            deliveries.get(timeout=QUIET_PERIOD)

    # Leaving the context drains the session, stops the workers and reaps the sidecar. A client
    # that leaked the JVM would leave a process still holding Kafka group membership.
    assert client._sidecar is not None, "the client never spawned a sidecar"
    assert client._sidecar.returncode is not None, "the sidecar outlived its parent"
