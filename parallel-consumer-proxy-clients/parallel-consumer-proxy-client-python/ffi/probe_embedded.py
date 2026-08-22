# Copyright (C) 2026 Antony Stubbs and contributors

"""End to end: does a Python process consume records with Parallel Consumer linked into it?

No sidecar, no gRPC, no JVM. The oracle is the deterministic pair - records and distinct keys -
because a rate can look plausible while nothing was actually processed.

Run against a topic that already has records in it:

    PC_BROKER=localhost:19092 PC_TOPIC=pc-ffi-demo PC_EXPECT=200 python3 ffi/probe_embedded.py
"""

from __future__ import annotations

import multiprocessing
import os
import sys
import time
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from parallel_consumer import ClientOptions, ParallelConsumerClient  # noqa: E402
from parallel_consumer.options import ProcessingOrder  # noqa: E402

BUDGET_SECONDS = 120.0


def main() -> int:
    broker = os.environ.get("PC_BROKER", "localhost:19092")
    topic = os.environ.get("PC_TOPIC", "pc-ffi-demo")
    target = int(os.environ.get("PC_EXPECT", "200"))

    # The processor runs in worker PROCESSES, so ordinary ints would be duplicated by the fork and
    # each worker would count only its own. Created before the client exists, so every worker
    # inherits the same shared memory.
    context = multiprocessing.get_context("fork")
    counted = context.Value("i", 0)
    finished = context.Event()
    manager = context.Manager()
    seen_keys = manager.dict()

    def process(record: Any) -> None:
        if record.key is not None:
            seen_keys[bytes(record.key)] = True
        with counted.get_lock():
            counted.value += 1
            reached = counted.value >= target
        if reached:
            finished.set()
        return None

    options = ClientOptions(
        embedded=True,
        topics=[topic],
        max_concurrency=32,
        ordering=ProcessingOrder.UNORDERED,
        kafka_properties={
            "bootstrap.servers": broker,
            "group.id": f"pc-python-embedded-{int(time.time())}",
            "auto.offset.reset": "earliest",
        },
    )

    print(f"broker={broker} topic={topic} expecting {target} records")
    started = time.monotonic()
    with ParallelConsumerClient(options) as client:
        print(f"ok   session configured: {client.executor_count} executor(s), "
              f"queue depth {client.max_concurrency}"
              if hasattr(client, "executor_count") else "ok   session configured")
        client.poll(process)
        if not finished.wait(BUDGET_SECONDS):
            print(f"FAIL stalled at {counted.value} of {target}")
            return 1
        elapsed = time.monotonic() - started

    processed = counted.value
    distinct = len(seen_keys)
    print(f"\n  {processed} records over {distinct} keys in {elapsed:.1f}s")
    if processed < target:
        print(f"FAIL ended early at {processed} of {target}")
        return 1
    print("\nPARALLEL CONSUMER RAN INSIDE THIS PYTHON PROCESS - no sidecar, no gRPC, no JVM")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
