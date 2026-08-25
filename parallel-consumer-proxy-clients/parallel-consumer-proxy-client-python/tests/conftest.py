# Copyright (C) 2026 Antony Stubbs and contributors

"""Fixtures for driving the real test-mode sidecar.

The sidecar is the proxy module's ``TestModeMain``, which lives in that module's **test** jar -
it must never reach a client package - so running it needs a JVM classpath rather than a binary
path. Maven writes that classpath at ``target/sidecar-classpath.txt`` (see the
``python-e2e-harness`` profile in ``pom.xml``); ``make test`` produces it through the same wiring
when pytest is run on its own.

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
SIDECAR_MAIN = "bz.stub.parallelconsumer.proxy.testmode.TestModeMain"

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
    """The test-mode sidecar's classpath, as Maven resolved it."""
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
def sidecar_for(java: str, sidecar_classpath: str):
    """Builds the launch command for one named conformance scenario.

    The scenario name is also the topic name - the harness seeds that scenario's records on a
    mock consumer and serves them from a topic of the same name.
    """

    def build(scenario: str) -> SidecarCommand:
        return SidecarCommand(
            executable=pathlib.Path(java),
            args=("-cp", sidecar_classpath, SIDECAR_MAIN, "--mock", "--scenario", scenario),
        )

    return build
