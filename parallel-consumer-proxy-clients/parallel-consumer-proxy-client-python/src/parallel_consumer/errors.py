# Copyright (C) 2026 Antony Stubbs and contributors

"""The exceptions this library raises.

Exceptions rather than error returns, because that is what Python callers expect - the Java
reference surface returns an :class:`~parallel_consumer.outcomes.Outcome` from the *user's*
function for a per-record verdict, which is a different thing and stays a return value here too.
Bad arguments raise the built-in ``ValueError``/``TypeError`` rather than a bespoke type: a
library-specific exception for "you passed the wrong thing" is a Java-ism.
"""

from __future__ import annotations

__all__ = [
    "ParallelConsumerError",
    "ProtocolViolation",
    "SidecarError",
]


class ParallelConsumerError(Exception):
    """Base class for every error this library raises deliberately."""


class ProtocolViolation(ParallelConsumerError):
    """The proxy did something the frozen v1 protocol does not permit.

    Raised rather than worked around, deliberately. The dispatch queue's overflow is the
    clearest example: its depth is the proxy's own in-flight ceiling, so it can never overflow
    while both sides obey the contract, and treating an overflow as a load condition (dropping
    records, or growing without bound) would hide the defect that caused it.
    """


class SidecarError(ParallelConsumerError):
    """The sidecar process could not be started, or died in a way the session cannot survive."""
