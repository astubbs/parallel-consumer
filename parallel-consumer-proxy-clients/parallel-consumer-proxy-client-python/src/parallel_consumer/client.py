# Copyright (C) 2026 Antony Stubbs and contributors

"""The client: what a user of this library actually touches.

    from parallel_consumer import ClientOptions, ParallelConsumerClient

    options = ClientOptions(topics=["orders"], max_concurrency=64,
                            kafka_properties={"bootstrap.servers": "localhost:9092",
                                              "group.id": "orders-processor"})

    with ParallelConsumerClient(options, sidecar="/opt/pc/parallel-consumer-proxy") as client:
        client.poll(lambda record: print(record.topic, record.offset))
        client.wait()

Ordered concurrent consumption, from one consumer, with the user's function running in worker
processes so Python's GIL is not the ceiling. The sidecar owns Kafka entirely; nothing in this
process holds a broker connection.

**Nothing here opens a channel until :meth:`~ParallelConsumerClient.poll` is called**, and that
is a constraint rather than an implementation detail: a module-level channel is exactly the shape
a later fork inherits, and this library forks.
"""

from __future__ import annotations

import logging
import os
import threading

from . import sidecar as _sidecar
from ._pool import WorkerPool
from ._session import Session
from .options import ClientOptions
from .outcomes import RecordProcessor

__all__ = ["ParallelConsumerClient"]

log = logging.getLogger(__name__)


class ParallelConsumerClient:
    """A configured, not-yet-running client. Start it with :meth:`poll`; stop it with :meth:`close`.

    Use it as a context manager where you can - the sidecar process, the worker processes and the
    stream are all resources, and leaving any of them behind leaks a JVM that still holds Kafka
    group membership.

    :param options: connect-time configuration, which travels in the handshake and nowhere else.
    :param sidecar: the sidecar binary, as an **absolute** path, or a
        :class:`~parallel_consumer.sidecar.SidecarCommand` when it needs arguments. Never a name
        resolved through ``PATH``: this process hands the sidecar its Kafka credentials.
    :param drain_timeout: how long :meth:`close` waits for executing records to report.
    """

    def __init__(self, options: ClientOptions,
                 *, sidecar: _sidecar.SidecarCommand | str | os.PathLike[str],
                 drain_timeout: float = 30.0) -> None:
        self._options = options
        self._command = _sidecar.SidecarCommand.coerce(sidecar)
        self._drain_timeout = drain_timeout
        self._lock = threading.Lock()
        self._sidecar: _sidecar.Sidecar | None = None
        self._pool: WorkerPool | None = None
        self._session: Session | None = None
        self._polled = False
        self._closed = False

    def poll(self, processor: RecordProcessor) -> None:
        """Starts consuming, handing every record to ``processor``. At most once per client.

        Returns as soon as consumption is running - it does not block. Call :meth:`wait` if the
        calling thread has nothing else to do.

        ``processor`` runs in a worker process, not here. On Linux it may be a closure over
        whatever it likes; where the platform starts processes with ``spawn`` it must be
        picklable, and you get an error saying so rather than a surprise.

        The order of the three steps below is load-bearing and is the reason this method is not
        three smaller ones: **the fork happens first, from the quietest image this process ever
        has.** gRPC Core does not support forking a process that holds an active channel, and a
        fork also inherits every lock held by threads that do not survive it - so the pool is
        created before the channel exists and before the sidecar's output threads do.
        """
        with self._lock:
            if self._polled:
                raise RuntimeError("poll() may be called at most once per client")
            self._polled = True

        # 1. The workers, while this process holds no channel and has started no thread of ours.
        #    They idle until step 3 tells the pool how many to run.
        self._pool = WorkerPool.launch(processor)

        # 2. The sidecar: a child process, launched directly, told nothing by argv.
        self._sidecar = _sidecar.Sidecar(self._command)
        port = self._sidecar.start()

        # 3. Only now the channel, the handshake, and the count that sizes the pool.
        session = Session(port, self._options, self._pool, drain_timeout=self._drain_timeout)
        self._pool.start(session.executor_count)
        session.start()
        self._session = session
        log.debug("polling with %d executor(s), queue depth %d",
                  session.executor_count, session.max_concurrency)

    def wait(self, timeout: float | None = None) -> bool:
        """Blocks until the session ends - by the proxy draining it, or by an error.

        :returns: ``True`` when the session has ended, ``False`` when the timeout ran out first.
        """
        if self._session is None:
            raise RuntimeError("nothing to wait for: poll() has not been called")
        return self._session.wait(timeout)

    def close(self) -> None:
        """Drains and shuts everything down, in the order that keeps the drain clean. Idempotent.

        Records still queued are *released* rather than run or abandoned - they go back to
        Parallel Consumer's scheduling with their attempt counts unchanged, because this client
        never invents an outcome for work it did not do. Executing records are given until
        ``drain_timeout`` to report. Only then does the sidecar lose its parent, which is what
        turns this into a clean drain rather than the next group member's problem.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True

        try:
            if self._session is not None:
                self._session.close()
        finally:
            if self._pool is not None:
                self._pool.close(timeout=self._drain_timeout)
            if self._sidecar is not None:
                self._sidecar.close()

        failure = None if self._session is None else self._session.failure
        if failure is not None:
            raise failure

    def __enter__(self) -> ParallelConsumerClient:
        return self

    def __exit__(self, *exception: object) -> None:
        self.close()

    def __repr__(self) -> str:
        state = "polling" if self._polled and not self._closed else "idle"
        return f"ParallelConsumerClient({state}, {self._options!r})"
