# Copyright (C) 2026 Antony Stubbs and contributors

"""The property this client's shape exists to protect: nothing forks after a channel opens.

gRPC Core does not support forking a process that holds an active channel, and the failure is
silent - deadlocks and corrupted wire data, not an exception at the fork. So it is asserted
directly rather than inferred from the absence of a crash: the worker pool must be created while
this process has opened no channel at all.

The sidecar and the stream are faked here. The pool is real, and so is the order in which
:meth:`ParallelConsumerClient.poll` does its three steps - which is the thing under test.
"""

from __future__ import annotations

import pathlib

import pytest

from parallel_consumer import ClientOptions, ParallelConsumerClient
from parallel_consumer import _transport as transport_module
from parallel_consumer import client as client_module
from parallel_consumer._generated import proxy_pb2 as pb
from parallel_consumer._pool import WorkerPool


class _FakeCall:
    """The proxy's half of the stream: one Configured, then nothing until cancelled."""

    def __init__(self, configured: pb.Configured) -> None:
        self._messages = iter([pb.ProxyMessage(configured=configured)])
        self.cancelled = False

    def __iter__(self):
        return self._messages

    def __next__(self):
        return next(self._messages)

    def cancel(self) -> None:
        self.cancelled = True


@pytest.fixture
def trace(monkeypatch):
    """Records what happened, in order, across the pieces poll() puts together."""
    events: list[str] = []

    class FakeSidecar:
        def __init__(self, command):
            events.append("sidecar-constructed")
            self.returncode = 0

        def start(self, timeout=60.0):
            events.append("sidecar-started")
            return 12345

        def close(self, timeout=30.0):
            events.append("sidecar-closed")
            return 0

    def fake_channel(target):
        events.append("channel-opened")
        return _FakeChannel()

    class _FakeChannel:
        def close(self):
            events.append("channel-closed")

    def fake_stub(channel):
        class Stub:
            def Session(self, requests):
                events.append("stream-opened")
                configured = pb.Configured(max_concurrency=4, executor_count=2,
                                           capabilities=["dispatch"])
                return _FakeCall(configured)

        return Stub()

    original_launch = WorkerPool.launch

    # A classmethod defined outside a class body, then monkeypatched onto one - legal at runtime,
    # and the only way to keep launch()'s binding while counting calls. mypy has no shape for it.
    @classmethod  # type: ignore[misc]
    def traced_launch(cls, processor):
        events.append("pool-created")
        return original_launch.__func__(cls, processor)  # type: ignore[attr-defined]

    monkeypatch.setattr(client_module._sidecar, "Sidecar", FakeSidecar)
    monkeypatch.setattr(transport_module.grpc, "insecure_channel", fake_channel)
    monkeypatch.setattr(transport_module.pb_grpc, "ProxyServiceStub", fake_stub)
    monkeypatch.setattr(WorkerPool, "launch", traced_launch)
    return events


def _noop(record):
    return None


def test_the_worker_pool_is_created_before_any_channel_exists(trace):
    client = ParallelConsumerClient(ClientOptions(topics=["t"]), sidecar=pathlib.Path("/bin/sh"))
    try:
        client.poll(_noop)
    finally:
        client.close()

    assert "pool-created" in trace, "the pool was never created"
    assert "channel-opened" in trace, "no channel was opened, so the ordering proves nothing"
    assert trace.index("pool-created") < trace.index("channel-opened"), (
        f"a channel existed before the workers were forked: {trace}"
    )


def test_the_executor_count_comes_from_the_handshake(trace):
    client = ParallelConsumerClient(ClientOptions(topics=["t"]), sidecar=pathlib.Path("/bin/sh"))
    try:
        client.poll(_noop)
        # Sized by what Configured said, not by anything this client chose or computed.
        assert client._pool is not None and client._session is not None
        assert client._pool.size == 2
        assert client._session.max_concurrency == 4
    finally:
        client.close()
