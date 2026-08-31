# Copyright (C) 2026 Antony Stubbs and contributors

"""The per-record verdict, and the single place a Python function becomes one.

The user's function may say what happened in either of the two ways Python people already write:

* **return nothing** - the ordinary Python function that just does the work. That is a success.
* **raise** - the ordinary Python failure. That is a failure, with the exception's text as the
  reason, and the record goes back to Parallel Consumer's own retry scheduling.

:class:`Outcome` is for the cases those two cannot express: a success that also produces records,
or a failure the function decided on rather than raised.

Two arms only, and deliberately no third: a function that cannot decide has not finished
processing. Terminal outcomes and released records exist on the wire, but they are not verdicts a
user function returns - the first is a later wave's surface, the second is this library's own
answer for work it never ran.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Sequence

from .records import InboundRecord, OutboundRecord

__all__ = ["Outcome", "RecordProcessor"]

RecordProcessor = Callable[[InboundRecord], "Outcome | None"]
"""The user's function: one record in; an :class:`Outcome`, ``None`` for success, or a raise."""


@dataclasses.dataclass(frozen=True)
class Outcome:
    """What happened to one record. Build one with :meth:`success` or :meth:`failure`."""

    succeeded: bool
    produce: tuple[OutboundRecord, ...] = ()
    reason: str | None = None

    @classmethod
    def success(cls, produce: Sequence[OutboundRecord] = ()) -> Outcome:
        """The record is done; its offset may advance.

        :param produce: records for Parallel Consumer to produce - the only sanctioned route for
            a worker's Kafka output.
        """
        return cls(succeeded=True, produce=tuple(produce))

    @classmethod
    def failure(cls, reason: str | None = None) -> Outcome:
        """The record failed; it returns to Parallel Consumer's retry scheduling.

        The reason travels with the redelivery as
        :attr:`~parallel_consumer.records.InboundRecord.last_failure_reason`.
        """
        return cls(succeeded=False, reason=reason)


def resolve_outcome(processor: RecordProcessor, record: InboundRecord) -> Outcome:
    """Runs the user's function and turns whatever it did into exactly one :class:`Outcome`.

    The ONE place that translation happens, so every path - return, ``None``, raise - is decided
    identically and can be read in one screen. It runs inside the worker process; nothing it can
    be handed may take the client down, because one bad record must not stop the consumer.
    """
    try:
        outcome = processor(record)
    except KeyboardInterrupt:
        raise
    except Exception as failure:
        return Outcome.failure(str(failure) or type(failure).__name__)

    if outcome is None:
        return Outcome.success()
    if isinstance(outcome, Outcome):
        return outcome
    return Outcome.failure(
        f"the record processor returned {type(outcome).__name__}; "
        "return an Outcome, or None for success"
    )
