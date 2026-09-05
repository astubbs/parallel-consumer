# Copyright (C) 2026 Antony Stubbs and contributors

"""Finding the JVM the demos launch their engine with.

Extracted from ``reference_demo.py`` when the Streams demo needed the same answer. A second copy
would have been three lines shorter to write and would have drifted the first time one of them
learned about a new way to find a JDK - and the two demos must agree, because a reader comparing
them has to know they ran on the same JVM.
"""

from __future__ import annotations

import os
import pathlib
import shutil

__all__ = ["java_binary"]


def java_binary() -> str:
    """The JVM to run the engine with. ``JAVA_HOME`` wins, as it does everywhere else here."""
    explicit = os.environ.get("PC_DEMO_JAVA")
    if explicit:
        return explicit
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = pathlib.Path(java_home) / "bin" / "java"
        if candidate.is_file():
            return str(candidate)
    found = shutil.which("java")
    if found is None:
        raise FileNotFoundError(
            "no java found for the sidecar: set JAVA_HOME, PC_DEMO_JAVA, or put a JDK 17 java on "
            "PATH")
    return found
