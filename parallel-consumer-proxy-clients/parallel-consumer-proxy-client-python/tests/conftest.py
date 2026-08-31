# Copyright (C) 2026 Antony Stubbs and contributors

"""Fixtures for driving the real sidecar.

The sidecar is the proxy module's production ``Main``, which is a JVM classpath invocation rather
than a binary path. Maven writes that classpath at ``target/sidecar-classpath.txt`` (see the
``python-sidecar-harness`` profile in ``pom.xml``); ``make test`` produces it through the same
wiring when pytest is run on its own.

**The sidecar on this stack hosts no Parallel Consumer engine** - it binds, announces its port,
admits one connection, and answers every session ``UNIMPLEMENTED``
(astubbs/parallel-consumer#384). So what a sidecar-marked test here can prove is the whole
client-side path up to and including the handshake, and nothing past it. The dispatch scenarios
belong to the shared conformance suite and are deferred until an engine exists; nothing here
stands in for one.

Everything below is test scaffolding. Nothing here is part of the library's surface, and the
library itself knows only what every application knows: an absolute path to a binary, and
arguments that are not configuration.
"""

from __future__ import annotations

import os
import pathlib
import shutil

import pytest

from parallel_consumer import SidecarCommand

MODULE_ROOT = pathlib.Path(__file__).resolve().parent.parent
CLASSPATH_FILE = MODULE_ROOT / "target" / "sidecar-classpath.txt"
SIDECAR_MAIN = "bz.stub.parallelconsumer.proxy.Main"

NO_ENGINE_DESCRIPTION = "hosts no Parallel Consumer engine"
"""What the sidecar's refusal must name, so a client author does not debug their own code."""

_HOW_TO_BUILD_IT = (
    "run `make test` (which produces it), or "
    "`./mvnw -pl :parallel-consumer-proxy-client-python -am -Dpc.foreignClients -DskipTests "
    "test-compile` from the repository root"
)


@pytest.fixture(scope="session")
def java() -> str:
    """The JVM to run the sidecar with. ``JAVA_HOME`` wins, as it does everywhere else here."""
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = pathlib.Path(java_home) / "bin" / "java"
        if candidate.is_file():
            return str(candidate)
    found = shutil.which("java")
    if found is None:
        pytest.fail("no java found: set JAVA_HOME, or put a JDK 17 java on PATH")
    return found


@pytest.fixture(scope="session")
def sidecar_classpath() -> str:
    """The sidecar's classpath, as Maven resolved it."""
    from_environment = os.environ.get("PC_PROXY_SIDECAR_CLASSPATH")
    if from_environment:
        return from_environment
    if not CLASSPATH_FILE.is_file():
        pytest.fail(f"{CLASSPATH_FILE} is missing - {_HOW_TO_BUILD_IT}")
    classpath = CLASSPATH_FILE.read_text(encoding="utf-8").strip()
    if not classpath:
        pytest.fail(f"{CLASSPATH_FILE} is empty - {_HOW_TO_BUILD_IT}")
    return classpath


@pytest.fixture
def engine_less_sidecar(java: str, sidecar_classpath: str) -> SidecarCommand:
    """The launch command for the real sidecar shell.

    NO ARGUMENTS, and that is the sidecar's own rule rather than this fixture being terse: it
    takes none and refuses to start when given one, because everything is configured connect-time
    over the protocol.
    """
    return SidecarCommand(
        executable=pathlib.Path(java),
        args=("-cp", sidecar_classpath, SIDECAR_MAIN),
    )
