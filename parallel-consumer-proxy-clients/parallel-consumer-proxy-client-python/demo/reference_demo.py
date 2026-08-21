# Copyright (C) 2026 Antony Stubbs and contributors

"""**The Python demo**: the same records through Python's own Kafka client, and through Parallel
Consumer reached over the sidecar.

The contract it keeps is `parallel-consumer-proxy/demo/README.md` - same flags, same environment
variables, same defaults but one, same two tables, the effective configuration printed first, and
no latency. What is specific to Python is in `demo/README.md` beside this file.

## The two arms, and why there are exactly two

* **AK core (confluent-kafka)** - `confluent_kafka.Consumer`, one record at a time, in this
  process. Always spelled "AK core", never bare "core", which reads as `parallel-consumer-core`
  (`CONCEPTS.md`) - and always with the client that actually ran, because "AK core" is a category
  and Python has more than one client in it.
* **python-grpc (this client)** - this repository's Python client library, which spawns the sidecar
  as a child process, receives records over a socket, runs the user's function in **worker
  processes**, and reports outcomes back. **The application does no Kafka I/O on this path**: the
  sidecar owns the consumer, the producer, the group membership and the offsets.

The seed carries four more arms because one JVM can hold every engine at once. Python cannot, and
a language whose only Kafka client is its own has nothing to compare a wrapper or a raw wire
against - so **two arms is the whole contract here**, and adding a hand-rolled gRPC arm would only
re-measure the engine while saying nothing about the client library, which is the artifact users
actually touch.

## What the clock covers, and why it is not the seed's window exactly

Both arms are timed **from the first record reaching the user's function to the last one being
counted**. The seed starts its clock a line earlier, just before consumption begins, having already
built its client and spawned its sidecar outside the window - "no other arm charges itself for
client construction or teardown", as its AK core arm puts it.

Python cannot draw the line in the same place: :meth:`ParallelConsumerClient.poll` forks the worker
pool, spawns the sidecar, completes the handshake and starts consumption, and it is one call. So
this demo keeps the seed's *rule* rather than its line number - start-up is outside the window for
both arms, measured identically, at the first record either of them sees. Including a JVM boot in a
throughput figure would have reported roughly a quarter of the arm's real rate, which is a worse
answer than a documented window.

## Run it

    parallel-consumer-proxy-clients/parallel-consumer-proxy-client-python/demo/run.sh
"""

from __future__ import annotations

import dataclasses
import logging
import multiprocessing
import os
import pathlib
import re
import shutil
import sys
import time
from multiprocessing.context import ForkContext, SpawnContext
from typing import Any

# The demo's own modules sit beside this file rather than on the installed package's path: the demo
# is not part of the shipped library, and a reader should be able to see all of it in one directory.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import demo_kafka
import demo_options
from confluent_kafka import Consumer, KafkaError, KafkaException

from parallel_consumer import (
    ClientOptions,
    ParallelConsumerClient,
    ProcessingOrder,
    SidecarCommand,
)

log = logging.getLogger("demo")

BANNER = """\
================================================================
  PARALLEL CONSUMER  -  Python demo
  The same records, twice: one at a time, then all at once.
================================================================"""
"""The first thing this demo prints, and the same in every language bar its own name.

Contract, not decoration. The first line used to be an arm announcing its executor count, which
tells a reader nothing about what they are watching - so the shape is fixed in
``parallel-consumer-proxy/demo/README.md`` and every language prints it identically.

``PC_DEMO_BANNER_PRINTED`` suppresses it, and is a **statement of fact rather than a preference**:
``demo/run.sh`` prints the banner before its own setup lines, so that the product's name is the
first thing on screen even when a Maven build and a broker start-up come between that and the run.
It then sets this, because the same banner twice in one screen is worse than either alone. Nothing
else sets it, so ``docker compose up`` and ``python demo/reference_demo.py`` both still print it.
"""

AK_CORE = "AK core (confluent-kafka)"
"""**"AK core" is a category; ``confluent-kafka`` is the client that actually ran.**

A reader cannot judge a comparison without knowing what produced it, and the answer differs in
every language - `rdkafka` in Ruby, `franz-go` in Go, `kafkajs` in TypeScript. Python has more
than one serious Kafka client, which is exactly why naming this one matters; ``demo/README.md``
says which and why this demo runs ``confluent-kafka``.
"""

PYTHON_GRPC = "python-grpc (this client)"
"""The sidecar arm, labelled with what drives it: this repository's own Python client library."""

SIDECAR_MAIN = "bz.stub.parallelconsumer.proxy.Main"
"""The sidecar's entry point. It is a JVM today; :func:`resolve_sidecar` is where that stops
mattering the moment a native binary exists."""

ARM_BUDGET_SECONDS = 600.0
"""No arm may take longer than this before the demo calls it stalled rather than slow."""


@dataclasses.dataclass(frozen=True)
class ArmResult:
    """One arm's finished run: what it was, what it did, and how fast it did it.

    ``processed`` and ``keys`` are the contract's **deterministic** pair - every language running
    the same records reports the same two figures, which is what makes them comparable when
    elapsed and msg/s never can be. They also demonstrate the run rather than asserting it: a
    short arm is a failed arm, not a fast one, and a single key repeated would mean the backlog
    was never spread at all.
    """

    arm: str
    elapsed_seconds: float
    processed: int
    keys: int

    @property
    def rate_per_second(self) -> float:
        return 0.0 if self.elapsed_seconds <= 0 else self.processed / self.elapsed_seconds


class KeyTally:
    """The distinct keys an arm saw, countable from **worker processes**.

    This is Python's problem alone, and it is the fork that causes it: the sidecar arm's user
    function runs in a worker process, so an ordinary ``set`` closed over by the processor would be
    duplicated by the fork and every worker would report only its own keys. A
    :class:`multiprocessing.Manager` dictionary would count exactly and generally, and was
    rejected: it puts an IPC round trip on the critical path of the very arm the demo exists to
    time, and almost every record in the small replay carries a key no worker has seen.

    So the tally is a shared **byte per key slot** - one unsynchronised write, because the only
    value ever written is 1 and two workers writing it to the same slot cannot disagree. Both arms
    use it, so the two rows are counted the same way rather than one exactly and one approximately.

    ``foreign`` counts records this demo did not seed. It should always be zero; if it is not, the
    keys column is an undercount and the demo says so rather than printing a number it cannot
    stand behind.
    """

    def __init__(self, context: ForkContext | SpawnContext) -> None:
        self._seen = context.Array("b", demo_kafka.KEY_SPACE, lock=False)
        self._foreign = context.Value("i", 0)

    def observe(self, key: bytes | None) -> None:
        slot = demo_kafka.key_slot(key)
        if slot is None:
            with self._foreign.get_lock():
                self._foreign.value += 1
            return
        self._seen[slot] = 1

    @property
    def distinct(self) -> int:
        return sum(self._seen)

    @property
    def foreign(self) -> int:
        return int(self._foreign.value)


def main(argv: list[str]) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    # BEFORE ANYTHING ELSE, including the usage text and any complaint about a bad flag: a reader
    # must be told what they are looking at before they are told anything else about it.
    if not os.environ.get("PC_DEMO_BANNER_PRINTED"):
        print(BANNER)

    if demo_options.is_help_requested(argv):
        print(demo_options.USAGE)
        return 0
    try:
        options = demo_options.parse(argv)
    except demo_options.OptionError as bad:
        # A misspelled flag must not be reported as a result for settings nobody asked for.
        print(f"{bad}\n\n{demo_options.USAGE}", file=sys.stderr)
        return 2

    if not options.bootstrap:
        print(
            "This demo needs a broker address and cannot start one itself.\n"
            "Run it through demo/run.sh, which starts a broker for you, or pass --bootstrap "
            "ADDR / set PC_DEMO_BOOTSTRAP.",
            file=sys.stderr)
        return 2

    try:
        sidecar = resolve_sidecar()
    except FileNotFoundError as missing:
        print(str(missing), file=sys.stderr)
        return 2

    topic = options.topic or f"pc-demo-{time.time_ns()}"
    run(options, sidecar, topic)
    return 0


def run(options: demo_options.DemoOptions, sidecar: SidecarCommand, topic: str) -> list[ArmResult]:
    """Runs the whole demo and hands back every arm's result.

    Returns the results rather than only printing them, so a test can assert what the arms
    actually did against the same code path a reader runs.
    """
    assert options.bootstrap is not None  # noqa: S101 - main() refuses to get here without one
    bootstrap = options.bootstrap

    print(f"\nEffective configuration:\n  {options.fingerprint(topic)}")

    demo_kafka.ensure_topic(bootstrap, topic, options.partitions)
    demo_kafka.seed(bootstrap, topic, 0, options.records)

    small = [
        ak_core_arm(options, topic, options.records),
        sidecar_arm(options, sidecar, topic, options.records),
    ]
    report(f"Small replay - every arm over the same {options.records} records (the comparison)",
           small, baseline_of(small), across_replays=False)

    if not options.big_replay_wanted:
        print(f"\nBig replay skipped (--replay-factor {options.replay_factor}).")
        return small

    total = options.big_replay_records
    demo_kafka.seed(bootstrap, topic, options.records, total)

    # AK core is excluded here because it does not go parallel: it would need
    # total * delay_ms milliseconds to finish a backlog the sidecar arm clears in seconds, and a
    # demo that makes a reader wait that long to learn nothing new is not worth the wall clock.
    serial_estimate = duration(total * options.delay_ms)
    big = [sidecar_arm(options, sidecar, topic, total)]
    report(f"Big replay - {total} records, parallel arms only (AK core is serial and would take "
           f"{serial_estimate}+)", big, baseline_of(small), across_replays=True)

    return small + big


def ak_core_arm(options: demo_options.DemoOptions, topic: str, target: int) -> ArmResult:
    """Python's own Kafka client, one record at a time. The arm every language has.

    Serial by construction: `confluent_kafka.Consumer` hands back one message per poll and the
    user's work happens on this thread before the next one is asked for. That is the shape a
    Python application without Parallel Consumer actually has, which is what makes it the
    denominator worth measuring against.
    """
    print(f"\n=== {AK_CORE} starting over {target} records ===")
    config: dict[str, Any] = dict(
        demo_kafka.consumer_properties(options.bootstrap or "", group_id(AK_CORE)))
    consumer = Consumer(config)
    # The same tally the sidecar arm uses, in a process that could have used a plain set. Two
    # counting methods would make the two rows of the "keys" column mean subtly different things.
    keys = KeyTally(worker_context())
    processed = 0
    started_at: float | None = None
    try:
        consumer.subscribe([topic])
        deadline = time.monotonic() + ARM_BUDGET_SECONDS
        while processed < target:
            # The one arm that does not wait on an event still needs the budget, or a backlog
            # shorter than the target spins here forever with no output.
            if time.monotonic() > deadline:
                raise RuntimeError(f"{AK_CORE} stalled at {processed} of {target}")
            message = consumer.poll(0.5)
            if message is None:
                continue
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(error)
            if started_at is None:
                # The clock starts at the FIRST RECORD, not at subscribe: the group join and the
                # first rebalance are start-up, and the sidecar arm does not charge itself for
                # theirs either. See this module's docstring.
                started_at = time.monotonic()
            keys.observe(message.key())
            simulate_work(options.delay_seconds)
            processed += 1
        elapsed = time.monotonic() - (started_at or time.monotonic())
    finally:
        consumer.close()
    return finished(AK_CORE, elapsed, processed, keys)


def sidecar_arm(options: demo_options.DemoOptions, sidecar: SidecarCommand, topic: str,
                target: int) -> ArmResult:
    """The client library over a real sidecar - the arm the whole design exists for.

    On this path the application does no Kafka I/O: it spawns a binary, receives records over a
    socket, runs its own function on them in **worker processes**, and reports outcomes back,
    while the sidecar owns the consumer, the producer, the group membership and the offsets. That
    is a claim about the *path*, not about this process - the same interpreter seeded the topic and
    runs the AK core arm with an ordinary Kafka client, because a comparison needs both sides. A
    genuinely foreign application carries no Kafka client library at all, which is the property
    this arm stands in for.

    **Nothing here speaks the protocol by hand**, deliberately. An earlier version of the seed did
    exactly that, and it proved the engine worked while saying nothing about the client library.
    """
    print(f"\n=== {PYTHON_GRPC} starting over {target} records "
          f"({options.max_concurrency} worker processes) ===")

    # THE COUNTERS ARE multiprocessing PRIMITIVES, AND THAT IS NOT A STYLE CHOICE. The user's
    # function runs in worker PROCESSES, so a closure over an ordinary int would be duplicated by
    # the fork and every worker would count only its own records - the demo would then wait forever
    # for a total that no single process ever reaches. They are created here, before the client
    # exists, so every worker inherits the same shared memory.
    context = worker_context()
    counted = context.Value("i", 0)
    started_at = context.Value("d", 0.0)
    ended_at = context.Value("d", 0.0)
    finished_event = context.Event()
    keys = KeyTally(context)
    delay = options.delay_seconds

    def process(record: Any) -> None:
        if started_at.value == 0.0:
            with started_at.get_lock():
                if started_at.value == 0.0:
                    started_at.value = time.monotonic()
        keys.observe(record.key)
        simulate_work(delay)
        with counted.get_lock():
            counted.value += 1
            reached = counted.value >= target
        if reached:
            ended_at.value = time.monotonic()
            finished_event.set()
        # Returning None is how an ordinary Python function says "this record succeeded". No
        # Outcome is needed unless the function also produces records or fails deliberately.
        return None

    client_options = ClientOptions(
        topics=[topic],
        max_concurrency=options.max_concurrency,
        ordering=ProcessingOrder.UNORDERED,
        kafka_properties=demo_kafka.consumer_properties(
            options.bootstrap or "", group_id(PYTHON_GRPC)),
    )
    with ParallelConsumerClient(client_options, sidecar=sidecar) as client:
        client.poll(process)
        if not finished_event.wait(ARM_BUDGET_SECONDS):
            raise RuntimeError(f"{PYTHON_GRPC} stalled at {counted.value} of {target}")
        elapsed = ended_at.value - started_at.value
        processed = counted.value

    # Reaching the target is not the only thing that can end the wait - a failed session ends it
    # too. Without this a broken run prints a plausible row at a plausible rate and exits 0, which
    # is the worst thing a demo whose shape ten languages copy can do.
    if processed < target:
        raise RuntimeError(f"{PYTHON_GRPC} ended early at {processed} of {target}")
    return finished(PYTHON_GRPC, elapsed, processed, keys)


def simulate_work(seconds: float) -> None:
    """The user's function, in both arms: a wait that occupies nothing while it waits.

    **The contract's predicate is whether the client is thread-per-record. This one is not** - it
    hands the user's function to a worker process - so Python is one of the six languages that owes
    its own non-occupying wait, and :func:`time.sleep` is it: it releases the GIL and parks the
    calling thread on the kernel's timer, so the wait costs no CPU and no lock. The busy loop it
    rules out (``while time.monotonic() < deadline: pass``) would pin a core per in-flight record.

    What a Python wait *does* occupy is a whole **worker process**, and no wait primitive changes
    that. ``asyncio.sleep`` was considered and rejected: the client hands a worker one record and
    takes one outcome back, so an event loop inside the worker cannot overlap a second record, and
    ``asyncio.run`` per record would hold the process for exactly as long while adding a loop
    set-up to every one.

    That occupancy is **not** the misreport the rule is aimed at, which is a table showing the
    runtime's ceiling while appearing to show the engine's. Here the ceiling *is* the number the
    fingerprint printed, because the proxy's executor count is the worker count - one sleeping
    worker per in-flight record, exactly as many as asked for. TypeScript's case is the other one:
    one event loop, where a blocking sleep caps in-flight work at one whatever the fingerprint
    says.

    So the divergence lands where the cost actually is: ``--concurrency`` defaults to
    :data:`demo_options.DEFAULT_CONCURRENCY` rather than the seed's 100, because in this language
    that number is a process count. ``docs/inflight/clients/python.md`` carries the reasoning.
    """
    if seconds > 0:
        time.sleep(seconds)


def worker_context() -> ForkContext | SpawnContext:
    """The same start method the client's own pool uses, so the counters above are inheritable.

    ``fork`` where the platform has it - that is what lets :func:`sidecar_arm`'s processor be a
    closure over the shared counters. Where only ``spawn`` exists the client library says so
    itself, in an error naming the picklability requirement, rather than failing obscurely.
    """
    if "fork" in multiprocessing.get_all_start_methods():
        return multiprocessing.get_context("fork")
    return multiprocessing.get_context("spawn")


def resolve_sidecar() -> SidecarCommand:
    """Where the sidecar binary is, as an **absolute** path - never a ``PATH`` lookup.

    The client library refuses anything else, and it is right to: this process hands the sidecar
    its Kafka credentials, so which binary runs is a security decision that belongs to the
    application. ``demo/run.sh`` and the demo container both set these variables; a reader running
    ``reference_demo.py`` by hand gets told what to set.

    ``PC_DEMO_SIDECAR`` names a binary directly and is the shape this will take once a native
    sidecar exists. Until then ``PC_DEMO_SIDECAR_CLASSPATH`` names the JVM classpath, and the
    "binary" is ``java`` plus that classpath - which is arguments about the *binary*, not
    configuration: bootstrap servers, credentials, ordering and concurrency still travel only in
    the connect-time handshake.
    """
    binary = os.environ.get("PC_DEMO_SIDECAR")
    if binary:
        return SidecarCommand.coerce(binary)

    classpath = os.environ.get("PC_DEMO_SIDECAR_CLASSPATH")
    if not classpath:
        raise FileNotFoundError(
            "no sidecar: set PC_DEMO_SIDECAR to an absolute binary, or "
            "PC_DEMO_SIDECAR_CLASSPATH to the proxy's JVM classpath. demo/run.sh builds the "
            "second one for you - run that instead.")
    return SidecarCommand(executable=pathlib.Path(java_binary()),
                          args=("-cp", classpath, SIDECAR_MAIN))


def java_binary() -> str:
    """The JVM to run the sidecar with. ``JAVA_HOME`` wins, as it does everywhere else here."""
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


def duration(milliseconds: int) -> str:
    """A wall clock a reader can read, in whichever unit says something.

    Integer seconds are right for the default backlog - 40,000 records at 2ms is ``80s`` - and
    wrong for the small volumes CI and the conformance harness run, where the same arithmetic
    printed ``0s`` and told a reader that a serial arm would take no time at all.
    """
    return f"{milliseconds // 1000}s" if milliseconds >= 1000 else f"{milliseconds}ms"


def group_id(arm: str) -> str:
    """A fresh group per arm per replay, so every arm reads the same records from the beginning.

    The arm's own label now carries the client's name in brackets, and a group id is not the place
    for punctuation - so everything that is not a letter or a digit becomes a dash.
    """
    slug = re.sub(r"[^A-Za-z0-9]+", "-", arm).strip("-")
    return f"pc-demo-{slug}-{time.time_ns()}"


def finished(arm: str, elapsed: float, processed: int, keys: KeyTally) -> ArmResult:
    print(f"=== {arm} finished: {processed} records, {keys.distinct} keys "
          f"in {int(elapsed * 1000)}ms ===")
    if keys.foreign:
        # Never silently. The keys column is contract precisely because it is deterministic, and a
        # topic that already held somebody else's records makes it an undercount.
        print(f"    NOTE: {keys.foreign} record(s) carried a key this demo did not seed, so the "
              "keys figure below is an undercount. Pass --topic with a fresh name.")
    return ArmResult(arm, elapsed, processed, keys.distinct)


def baseline_of(results: list[ArmResult]) -> ArmResult | None:
    return next((result for result in results if result.arm == AK_CORE), None)


def report(title: str, results: list[ArmResult], baseline: ArmResult | None, *,
           across_replays: bool) -> None:
    """The two tables, in the contract's columns and order, so two languages diff cleanly.

    Six columns: what ran, **what it did**, then how fast. ``records`` and ``keys`` come before the
    timings deliberately - throughput alone cannot show the work happened, and a reader scanning
    left to right should meet the evidence before the number it justifies. Column *identity and
    order* are contract; the widths are not, and this demo's are wider than the seed's because an
    arm label now carries its client's name.
    """
    heading = "vs AK core*" if across_replays else "vs AK core"
    lines = ["", "", title,
             f"  {'arm':<26} {'records':>9} {'keys':>7} {'elapsed':>10} {'msg/s':>14} "
             f"{heading:>14}"]
    for result in results:
        ratio = ("-" if baseline is None or baseline.rate_per_second == 0
                 else f"{result.rate_per_second / baseline.rate_per_second:.1f}x")
        lines.append(f"  {result.arm:<26} {result.processed:>9,} {result.keys:>7,} "
                     f"{result.elapsed_seconds:>9.1f}s {int(result.rate_per_second):>14,} "
                     f"{ratio:>14}")
    if across_replays:
        lines.append("")
        lines.append("  * against the SMALL replay's AK core arm. Across replays, so not "
                     "like-for-like.")
    print("\n".join(lines))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
