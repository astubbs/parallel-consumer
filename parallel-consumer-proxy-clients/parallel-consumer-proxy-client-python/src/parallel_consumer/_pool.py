# Copyright (C) 2026 Antony Stubbs and contributors

"""Worker **processes**, and the launcher that forks them without ever seeing a gRPC channel.

Python's GIL is the reason this product exists in this language, so the workers here are
processes rather than threads: the admin holds the single gRPC stream, and the user's function
runs in a separate interpreter with its own GIL. Workers never speak gRPC. They exchange records
and outcomes with the admin over ordinary ``multiprocessing`` queues.

**Why a launcher process exists.** gRPC Core does not support forking a process that holds an
active channel: fork after a channel has issued RPCs and you get deadlocks or corrupted wire
data. The worker count, though, arrives in ``Configured`` - *after* the handshake, on a channel
that is by then very much active. Those two facts cannot both be satisfied in one process, so
this module forks one extra process, the launcher, **before any channel exists**, and it is the
launcher that forks the workers when the count arrives. The launcher's image never contained a
channel and it never creates one, so every worker is forked from a channel-free process. The
property is structural rather than something the caller must maintain: by the time a channel
exists, this process never forks again.

The count is fixed for the connection's life, so no later count change can put that property
back in question.

Where ``fork`` is unavailable (macOS defaults to ``spawn``, Windows has only ``spawn``), the
user's function must be picklable rather than a closure. That is a real semantic difference, so
it is stated in an error the moment it bites rather than silently changed.
"""

from __future__ import annotations

import logging
import multiprocessing
import pickle
import time
from multiprocessing.context import ForkContext, SpawnContext
from typing import Any

from .outcomes import RecordProcessor, resolve_outcome
from .records import InboundRecord

__all__ = ["WorkerPool"]

log = logging.getLogger(__name__)

_STOP = None
"""Queue sentinel: a worker that receives it has no more records coming."""

_TERMINATE_GRACE = 5.0
"""How long the launcher waits for a worker it has just signalled to actually die."""

_LAUNCHER_REAP_GRACE = 5.0
"""How much longer the parent waits for the launcher than the launcher waits for its workers.

**Strictly more, and the strictness is the whole point.** Both waits used to be the same number, so
the parent's patience ran out at the same instant the launcher's did - and the parent's answer to a
launcher that had not finished is to terminate it, which kills the process that was about to reap
the workers. Any worker still executing was then orphaned to init, holding every descriptor it
inherited: on a shared stdout, that is a pipe whose reader never sees EOF.

Found by the cross-language conformance suite (astubbs#242), which prescribes a worker that never
returns as its negative control: the Java side read an empty transcript from a runner that had
printed, exited and been reaped, because a stray interpreter still held the write end.
"""


def _context() -> ForkContext | SpawnContext:
    """``fork`` where the platform has it - it is what lets the user's function be a closure.

    The return type names the two concrete contexts rather than their ``BaseContext`` supertype,
    which is not cosmetic: ``BaseContext`` does not declare ``Process``, so the wider annotation
    made every ``context.Process(...)`` below unverifiable (mypy: ``"BaseContext" has no attribute
    "Process"``). Narrowing it is what lets the strict pass check those calls at all.
    """
    if "fork" in multiprocessing.get_all_start_methods():
        return multiprocessing.get_context("fork")
    return multiprocessing.get_context("spawn")


def _worker_main(processor: RecordProcessor, work_queue: Any, outcome_queue: Any) -> None:
    """One worker: take a record, run the user's function, hand back the outcome. Forever."""
    while True:
        item = work_queue.get()
        if item is _STOP:
            return
        token, record = item
        outcome = resolve_outcome(processor, record)
        # The token goes back exactly as it arrived. This process never parses it, and holds
        # nothing about the record once the outcome is queued.
        outcome_queue.put((token, outcome))


def _launcher_main(processor: RecordProcessor, control_queue: Any, work_queue: Any,
                   outcome_queue: Any) -> None:
    """Forks workers on demand, from an image that has never held a gRPC channel."""
    context = _context()
    workers: list[Any] = []
    while True:
        command, argument = control_queue.get()
        if command == "start":
            for _ in range(argument):
                worker = context.Process(
                    target=_worker_main,
                    args=(processor, work_queue, outcome_queue),
                    name="pc-worker",
                    daemon=False,
                )
                worker.start()
                workers.append(worker)
        elif command == "stop":
            for _ in workers:
                work_queue.put(_STOP)
            # ONE DEADLINE FOR ALL OF THEM, not one each. Per-worker timeouts made the launcher's
            # own worst case `argument * len(workers)`, which is longer than the parent is willing
            # to wait for the launcher - and a launcher killed mid-reap orphans the very workers it
            # was reaping. Every worker was told to stop before this loop began, so they drain
            # concurrently and the whole stop costs one drain, whatever the executor count.
            deadline = time.monotonic() + argument
            for worker in workers:
                worker.join(timeout=max(0.0, deadline - time.monotonic()))
            for worker in workers:
                if worker.is_alive():
                    worker.terminate()
                    worker.join(timeout=_TERMINATE_GRACE)
            return


class WorkerPool:
    """The admin's handle on the worker processes.

    Create it with :meth:`launch` **before opening any gRPC channel**; call :meth:`start` with
    the executor count once the handshake supplies it.
    """

    def __init__(self, processor: RecordProcessor) -> None:
        context = _context()
        if context.get_start_method() == "spawn":
            self._require_picklable(processor)
        self._context = context
        self.work_queue = context.Queue()
        self.outcome_queue = context.Queue()
        self._control_queue = context.Queue()
        self._launcher = context.Process(
            target=_launcher_main,
            args=(processor, self._control_queue, self.work_queue, self.outcome_queue),
            name="pc-worker-launcher",
            # Not daemonic: a daemonic process may not have children, and forking
            # children is this process's entire job.
            daemon=False,
        )
        self._started = 0
        self._closed = False

    @classmethod
    def launch(cls, processor: RecordProcessor) -> WorkerPool:
        """Forks the launcher. Call this before a channel exists; see this module's docstring."""
        pool = cls(processor)
        pool._launcher.start()
        return pool

    def start(self, count: int) -> None:
        """Forks ``count`` workers, in the launcher, at the size the handshake supplied."""
        if count < 1:
            raise ValueError(f"executor count must be at least 1, got {count}")
        if self._started:
            raise RuntimeError("this pool's workers have already been started")
        self._started = count
        self._control_queue.put(("start", count))

    def submit(self, token: bytes, record: InboundRecord) -> None:
        """Hands one record to the next free worker. FIFO by arrival at the queue."""
        self.work_queue.put((token, record))

    def outcomes(self) -> Any:
        """The queue the admin reads outcomes from, as ``(token, Outcome)`` pairs."""
        return self.outcome_queue

    @property
    def size(self) -> int:
        return self._started

    def close(self, timeout: float = 30.0) -> None:
        """Stops every worker and reaps the launcher. Idempotent."""
        if self._closed:
            return
        self._closed = True
        if self._launcher.is_alive():
            self._control_queue.put(("stop", timeout))
            # The launcher gets the workers' drain PLUS a grace, so it is never killed in the middle
            # of reaping them - see _LAUNCHER_REAP_GRACE, which is where the orphan came from.
            self._launcher.join(timeout=timeout + _LAUNCHER_REAP_GRACE)
            if self._launcher.is_alive():
                self._launcher.terminate()
        for queue in (self.work_queue, self.outcome_queue, self._control_queue):
            queue.close()

    @staticmethod
    def _require_picklable(processor: RecordProcessor) -> None:
        try:
            pickle.dumps(processor)
        except Exception as unpicklable:
            raise ValueError(
                "this platform starts worker processes with 'spawn', so the record processor "
                "must be picklable - use a module-level function rather than a closure, a lambda "
                f"or a local (pickling it failed: {unpicklable})"
            ) from unpicklable
