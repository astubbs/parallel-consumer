# Copyright (C) 2026 Antony Stubbs and contributors

"""Parallel Consumer for Python: ordered concurrent Kafka consumption, without the GIL ceiling.

One consumer, many workers, ordering kept per key or per partition - and the workers are
processes, so the user's function gets a whole interpreter each. Kafka itself is spoken by a
sidecar process this library starts; nothing here holds a broker connection.

Start with :class:`~parallel_consumer.client.ParallelConsumerClient`.

**Importing this package starts nothing** - no channel, no process, no thread. That is deliberate:
this library forks worker processes, and a channel that exists at import time is one a later fork
would inherit.
"""

from .client import ParallelConsumerClient
from .errors import ParallelConsumerError, ProtocolViolation, SidecarError
from .options import ClientOptions, ProcessingOrder
from .outcomes import Outcome, RecordProcessor
from .records import InboundRecord, OutboundRecord
from .sidecar import SidecarCommand

__all__ = [
    "ClientOptions",
    "InboundRecord",
    "OutboundRecord",
    "Outcome",
    "ParallelConsumerClient",
    "ParallelConsumerError",
    "ProcessingOrder",
    "ProtocolViolation",
    "RecordProcessor",
    "SidecarCommand",
    "SidecarError",
]

__version__ = "0.6.0.0.dev0"
