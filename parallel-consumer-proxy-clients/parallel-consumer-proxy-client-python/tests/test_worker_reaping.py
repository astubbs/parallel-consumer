# Copyright (C) 2026 Antony Stubbs and contributors

"""Closing must leave nothing behind - including a worker whose function never returned.

A user function that does not return is not a pathological case here: it is what the product is
for. Records stay in flight until the function finishes, and a worker holding one while the
application shuts down is an ordinary Tuesday. What must never happen is the shutdown *losing* that
worker: a process orphaned to init keeps every descriptor it inherited, so the application's own
stdout stays open after the application is gone and whoever was reading it never sees EOF.

That is exactly how this was found (astubbs#242). The cross-language conformance suite prescribes a
worker that never returns as its negative control; the Java side then read an EMPTY transcript from
a Python runner that had printed its line, exited, and been reaped - because a stray interpreter
still held the write end of the pipe. The cause was two waits with the same number: the parent gave
the launcher exactly as long as the launcher gave its workers, so the parent terminated the launcher
at the instant it was about to reap them.

THE ASSERTION IS AN INHERITED PIPE, not a process count, because a process count is exactly what the
bug hid from: the parent's own ``multiprocessing`` bookkeeping shows the launcher gone and knows
nothing about the launcher's children. An inherited descriptor is held by whoever is still alive,
whether or not anybody is keeping track of them.
"""

from __future__ import annotations

import os
import select
import time

import pytest

from parallel_consumer._pool import WorkerPool
from parallel_consumer.records import InboundRecord

# Long enough that a worker merely slow to be signalled is not mistaken for an orphan, short
# enough that a real orphan does not stall the suite: the reap costs the drain plus a small grace.
EOF_BUDGET_SECONDS = 20.0

DRAIN_SECONDS = 1.0


@pytest.mark.skipif("fork" not in __import__("multiprocessing").get_all_start_methods(),
                    reason="the worker holds an inherited descriptor, which needs fork")
def test_a_worker_that_never_returns_is_reaped_and_releases_what_it_inherited():
    read_end, write_end = os.pipe()

    def never_returns(record: InboundRecord) -> None:
        # The record is taken and its function does not return: the shape the product supports,
        # and the shape the shutdown has to survive.
        while True:
            time.sleep(3600)

    pool = WorkerPool.launch(never_returns)
    try:
        pool.start(1)
        pool.submit(b"token", InboundRecord(topic="t", partition=0, offset=0, key=None,
                                            value=b"never-finishes", attempt=1))
        # Given to the worker before this process lets go of its own copy, so the only holder left
        # is the worker itself.
        _wait_for_worker(pool)
    finally:
        os.close(write_end)
        pool.close(timeout=DRAIN_SECONDS)

    assert _reached_eof(read_end, EOF_BUDGET_SECONDS), (
        "the pipe the worker inherited never reached EOF, so a worker process outlived the pool "
        "that started it - orphaned to init, holding descriptors nobody can account for"
    )
    os.close(read_end)


def _wait_for_worker(pool: WorkerPool) -> None:
    """Gives the launcher a moment to fork the worker and hand it the record."""
    time.sleep(2.0)
    assert pool.size == 1


def _reached_eof(fd: int, budget: float) -> bool:
    """Whether the pipe's last writer has gone: readable, and reading returns zero bytes."""
    deadline = time.monotonic() + budget
    while time.monotonic() < deadline:
        left = max(0.0, deadline - time.monotonic())
        readable, _, _ = select.select([fd], [], [], min(1.0, left))
        if readable:
            return os.read(fd, 1) == b""
    return False
