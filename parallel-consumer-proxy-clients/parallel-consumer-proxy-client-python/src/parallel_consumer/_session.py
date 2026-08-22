# Copyright (C) 2026 Antony Stubbs and contributors

"""The admin: one gRPC stream, one dispatch queue, and the threads that keep both moving.

Three threads and one rule between them. **The reader never blocks.** The stream carries the
control plane as well as records, so an admin that stops reading to slow the proxy down
head-of-line-blocks its own shutdown; the reader therefore only ever appends to the dispatch
queue and returns to the stream. Hand-out to the workers is a second thread's job, and reporting
outcomes back is a third's.

The dispatch queue's rules are the protocol's, not this library's invention - depth is the
proxy's own in-flight ceiling, hand-out is FIFO, overflow is a protocol violation rather than a
load condition, and on shutdown the queue is *released* rather than run or abandoned. They are
specified once for every language so that ten clients behave comparably.
"""

from __future__ import annotations

import dataclasses
import logging
import queue
import threading
import time
from typing import Any

import grpc

from . import _wire
from ._generated import proxy_pb2 as pb
from ._generated import proxy_pb2_grpc as pb_grpc
from ._pool import WorkerPool
from .errors import ProtocolViolation
from .options import ClientOptions
from .records import InboundRecord

log = logging.getLogger(__name__)

_HALF_CLOSE = None
"""Send-queue sentinel: the request iterator ends, which is how a client half-closes."""

_POLL = 0.05
"""Seconds a blocking wait parks for before re-checking the session's state."""

CAPABILITIES = (_wire.DISPATCH,)
"""What this client declares it implements. Each wave that adds a duty adds its token here."""


@dataclasses.dataclass(frozen=True)
class _Queued:
    """A dispatched record waiting for a free worker. The token stays opaque bytes throughout."""

    token: bytes
    record: InboundRecord


class Session:
    """One connection's worth of protocol: handshake, dispatch, report, drain.

    Constructing it performs the handshake, so the caller has an executor count to size the pool
    with the moment it returns. Nothing here opens a channel before it is constructed - and it is
    constructed after the worker pool exists, which is what keeps the fork safe.
    """

    def __init__(self, port: int, options: ClientOptions, pool: WorkerPool,
                 *, drain_timeout: float = 30.0) -> None:
        self._pool = pool
        self._drain_timeout = drain_timeout
        self._queue: queue.Queue[_Queued] = queue.Queue()
        self._sends: queue.Queue[Any] = queue.Queue()
        self._lock = threading.Lock()
        self._outstanding = 0
        self._draining = False
        self._stopping = False
        self._closed = False
        self._finished = threading.Event()
        self._failure: BaseException | None = None
        self._threads: list[threading.Thread] = []

        self._channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        self._sends.put(pb.ClientMessage(
            configure=_wire.configure_message(options, CAPABILITIES)))
        # protoc's gRPC output carries no annotations, so the strict pass sees an untyped call here.
        # Suppressed at the site rather than by relaxing the module: this is the ONE crossing into
        # generated code, and pyproject's warn_unused_ignores deletes this line for us the day
        # protoc starts emitting stubs.
        self._call = pb_grpc.ProxyServiceStub(  # type: ignore[no-untyped-call]
            self._channel).Session(self._requests())

        configured = self._handshake()
        self.max_concurrency: int = configured.max_concurrency
        self.executor_count: int = configured.executor_count
        self.capabilities: frozenset[str] = frozenset(configured.capabilities)
        self._permits = threading.Semaphore(self.executor_count)

    # ---- lifecycle -----------------------------------------------------------------

    def start(self) -> None:
        """Starts the three threads. The pool's workers must already be running."""
        for name, target in (("pc-reader", self._read),
                             ("pc-handout", self._hand_out),
                             ("pc-reporter", self._report)):
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self._threads.append(thread)

    def wait(self, timeout: float | None = None) -> bool:
        """Blocks until the session ends. Returns ``False`` if it is still running."""
        return self._finished.wait(timeout)

    def close(self) -> None:
        """Client-initiated shutdown: the same drain the proxy would ask for, unprompted.

        Stop handing records out, release everything queued, let executing records report, then
        half-close. The half-close *is* the shutdown signal - there is no request message for it.
        """
        if self._closed:
            return
        self._closed = True
        if not self._stopping:
            self._drain()
            self._stopping = True
            self._sends.put(_HALF_CLOSE)
        for thread in self._threads:
            thread.join(timeout=self._drain_timeout)
        self._channel.close()
        self._finished.set()

    @property
    def failure(self) -> BaseException | None:
        """The error that ended the session, if one did."""
        return self._failure

    # ---- the stream ----------------------------------------------------------------

    def _requests(self) -> Any:
        """The outbound half of the stream: everything the client says, in one place."""
        while True:
            message = self._sends.get()
            if message is _HALF_CLOSE:
                return
            yield message

    def _send(self, message: pb.ClientMessage) -> None:
        self._sends.put(message)

    def _handshake(self) -> pb.Configured:
        """Sends Configure, reads Configured. Inline, so the caller has the count on return.

        No deadline of its own: a proxy that accepts a connection and then says nothing is a
        liveness failure, and liveness is the lease's job rather than a one-off timer here.
        """
        try:
            first = next(self._call)
        except grpc.RpcError as refused:
            raise ProtocolViolation(
                f"the proxy refused the session: {refused.code().name} - {refused.details()}"
            ) from refused
        if first.WhichOneof("message") != "configured":
            raise ProtocolViolation(
                f"the handshake must answer Configure with Configured, got "
                f"{first.WhichOneof('message')}"
            )
        configured = first.configured
        for field in ("max_concurrency", "executor_count"):
            if not configured.HasField(field):
                raise ProtocolViolation(
                    f"Configured must always carry {field}; absence never means unlimited"
                )
        log.debug("configured: max_concurrency=%d executor_count=%d capabilities=%s",
                  configured.max_concurrency, configured.executor_count,
                  sorted(configured.capabilities))
        # Same crossing as the stub call above: the generated message class is untyped, so its
        # fields read as Any. The declared return type is the assertion; the ignore is the receipt.
        return configured  # type: ignore[no-any-return]

    def _read(self) -> None:
        """The reader. Appends and returns to the stream; it never waits for a worker."""
        try:
            for message in self._call:
                kind = message.WhichOneof("message")
                if kind == "dispatch":
                    self._on_dispatch(message.dispatch)
                elif kind == "shutdown":
                    self._on_shutdown()
                elif kind == "set_executor_count":
                    # Declared in the schema and never sent by a v1 proxy: the executor count is
                    # a pure function of connect-time configuration, computed once.
                    self._violated("the proxy sent SetExecutorCount, which a v1 proxy never sends")
                    return
                else:
                    self._violated(f"the proxy sent {kind}, which this session did not negotiate")
                    return
        except grpc.RpcError as broken:
            if not self._stopping:
                self._failure = broken
        finally:
            self._finished.set()

    def _on_dispatch(self, dispatch: pb.Dispatch) -> None:
        for dispatched in dispatch.records:
            with self._lock:
                self._outstanding += 1
                outstanding = self._outstanding
            if outstanding > self.max_concurrency:
                # Not a load condition: the queue's depth IS the proxy's declared in-flight
                # ceiling, so exceeding it means the proxy broke its own contract. Dropping the
                # record or growing the queue would hide that.
                self._violated(
                    f"the proxy dispatched {outstanding} records at once, over the "
                    f"max_concurrency of {self.max_concurrency} it declared"
                )
                return
            self._queue.put(_Queued(token=dispatched.token.SerializeToString(),
                                    record=_wire.inbound_record(dispatched)))

    def _on_shutdown(self) -> None:
        """Proxy-initiated drain: release the queue, let executing records finish, half-close."""
        log.debug("proxy asked for shutdown; draining")
        self._drain()
        self._stopping = True
        self._sends.put(_HALF_CLOSE)

    # ---- hand-out and reporting ----------------------------------------------------

    def _hand_out(self) -> None:
        """FIFO by arrival, and within one Dispatch by the order its records appear."""
        while not self._stopping:
            try:
                item = self._queue.get(timeout=_POLL)
            except queue.Empty:
                continue
            if self._draining or not self._await_permit():
                self._release(item)
                continue
            self._pool.submit(item.token, item.record)

    def _await_permit(self) -> bool:
        """Waits for a worker to come free, giving up if the session starts draining."""
        while not (self._draining or self._stopping):
            if self._permits.acquire(timeout=_POLL):
                return True
        return False

    def _report(self) -> None:
        """Sends each outcome as it arrives - independently, and out of dispatch order."""
        outcomes = self._pool.outcomes()
        while not self._stopping:
            try:
                token, outcome = outcomes.get(timeout=_POLL)
            except queue.Empty:
                continue
            except (OSError, ValueError):  # the pool's queue closed under us
                return
            self._send(pb.ClientMessage(report=_wire.report_message(token, outcome)))
            self._permits.release()
            self._settled()

    def _release(self, item: _Queued) -> None:
        """Returns a queued record the client never ran, attempt count unchanged."""
        if "shutdown" in self.capabilities:
            self._send(pb.ClientMessage(report=_wire.released_report(item.token)))
        # Without the shutdown capability negotiated there is no Released to send: the proxy
        # reclaims what it dispatched when the stream ends, and sending outside the negotiated
        # set would be this client's own protocol violation.
        self._settled()

    def _settled(self) -> None:
        with self._lock:
            self._outstanding -= 1

    def _drain(self) -> None:
        """Stops hand-out, releases the queue, waits for executing records to report."""
        self._draining = True
        while True:
            try:
                self._release(self._queue.get_nowait())
            except queue.Empty:
                break
        deadline = time.monotonic() + self._drain_timeout
        while time.monotonic() < deadline:
            with self._lock:
                if self._outstanding <= 0:
                    return
            time.sleep(_POLL)
        log.warning("drain timed out with %d record(s) still unreported", self._outstanding)

    def _violated(self, problem: str) -> None:
        """Fails the stream rather than absorbing a contract breach."""
        self._failure = ProtocolViolation(problem)
        log.error("protocol violation: %s", problem)
        self._stopping = True
        self._call.cancel()
        self._finished.set()
