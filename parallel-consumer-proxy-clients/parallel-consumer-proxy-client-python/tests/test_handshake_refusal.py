# Copyright (C) 2026 Antony Stubbs and contributors

"""The handshake, against a real sidecar process, over the real wire.

This module's one against-a-real-process test, and the only claim it can honestly make on this
stack. The sidecar spawned is ``parallel-consumer-proxy``'s production entry point - a real bind,
the real authority allowlist, the real single-connection guard, and the real session service. That
service hosts no engine and refuses every session, so there is no dispatch to observe here and none
is invented.

What **is** observed is everything this library does before an engine would matter: fork the worker
pool while the process holds no channel, launch the child directly, read ``port:`` off its stdout,
hold its stdin as the parent-death lifeline, open the channel, put ``Configure`` on the wire, and
turn what came back into a Python exception. The dispatch scenarios - one record end to end, the
in-flight ceiling, the redelivery history - belong to the shared conformance suite and are deferred
until an engine exists to run them against.

**The status code is the assertion, not merely "it failed".** A refusal from the authority allowlist
is ``PERMISSION_DENIED`` and one from the admission slot is ``RESOURCE_EXHAUSTED``, both raised by
interceptors *before* the service method runs. Only ``UNIMPLEMENTED`` can have come from the service
itself, so the code is what separates "the connection was turned away" from "the handshake was
delivered and answered".
"""

from __future__ import annotations

import pathlib
import stat

import pytest

from conftest import NO_ENGINE_DESCRIPTION
from parallel_consumer import ClientOptions, ParallelConsumerClient, SidecarCommand
from parallel_consumer.errors import ProtocolViolation


@pytest.mark.sidecar
def test_the_handshake_reaches_the_session_service_and_its_refusal_reaches_the_caller(
        engine_less_sidecar):
    options = ClientOptions(topics=["handshake-topic"], max_concurrency=4)
    client = ParallelConsumerClient(options, sidecar=engine_less_sidecar)

    with pytest.raises(ProtocolViolation) as refused:
        client.poll(lambda record: None)

    message = str(refused.value)
    assert "UNIMPLEMENTED" in message, (
        "UNIMPLEMENTED is the only code the session SERVICE raises - the allowlist answers "
        f"PERMISSION_DENIED and the admission slot RESOURCE_EXHAUSTED, both before the service "
        f"method runs, so this code is what proves the Configure was delivered: {message}")
    assert NO_ENGINE_DESCRIPTION in message, (
        "the refusal must name what is missing, or a client author debugs their own code: "
        f"{message}")

    client.close()
    assert client._sidecar is not None, "the client never spawned a sidecar"
    assert client._sidecar.returncode is not None, "the sidecar outlived its parent"


@pytest.mark.sidecar
def test_a_sidecar_that_is_not_listening_fails_differently_from_one_that_refuses(tmp_path):
    """The control arm, permanent rather than a one-off demonstration.

    Pointed at a port nothing is listening on, the same client fails in a way that is not the
    refusal above. Without it, the test that matters could be passing on any failure at all - which
    is the shape of an assertion that cannot fail for the reason it names.

    The stand-in announces a port and then holds its stdin, which is the spawning contract's whole
    client-visible surface, so the library takes its REAL connect path at a dead port rather than
    the different path a child that printed nothing would take. ``printf`` and ``read`` are shell
    builtins, so it is one process holding its own lifeline and no grandchild survives the reap.
    """
    dead_port = _reserve_then_release_a_port()
    announcer = tmp_path / "announcer.sh"
    announcer.write_text(
        "#!/bin/sh\n"
        f"printf 'port: {dead_port}\\n'\n"
        "while read -r _ignored; do :; done\n"
        "exit 0\n",
        encoding="utf-8")
    announcer.chmod(announcer.stat().st_mode | stat.S_IXUSR)

    options = ClientOptions(topics=["handshake-topic"], max_concurrency=4)
    client = ParallelConsumerClient(
        options, sidecar=SidecarCommand(executable=pathlib.Path(announcer), args=()))

    with pytest.raises(Exception) as failed:
        client.poll(lambda record: None)
    assert NO_ENGINE_DESCRIPTION not in str(failed.value), (
        "nothing answered, so nothing can have refused: " + str(failed.value))
    client.close()


def _reserve_then_release_a_port() -> int:
    """A loopback port the OS has just handed out and nothing is listening on."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reserved:
        reserved.bind(("127.0.0.1", 0))
        return reserved.getsockname()[1]
