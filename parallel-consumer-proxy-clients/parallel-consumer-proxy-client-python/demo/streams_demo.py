# Copyright (C) 2026 Antony Stubbs and contributors

"""Kafka Streams, described from Python and counted per key.

The topology lives in the JVM; the per-record function lives here. Python names a source, a value
transform, a group-by-key and a count, and the engine assembles a real ``StreamsBuilder`` from
those calls. When the transform runs, the engine calls back into this process. **No Kafka client
runs on that path in Python** - the engine owns every connection, exactly as the proxy demo's
sidecar arm does.

What this proves is narrow and deliberate: that a language with no Kafka Streams implementation can
describe and run one. It makes no speed claim. The figures it prints are within-session and are
there to show where the time goes, not to compare against anything.

The counts are checked rather than displayed. A count is a changelog, so the sink carries every
intermediate value per key; the demo reads it last-value-per-key and compares against the backlog
it seeded, which it knows exactly.
"""

from __future__ import annotations

import argparse
import logging
import os
import pathlib
import struct
import sys
import time
import threading
from collections import Counter
from typing import Any

# demo_kafka is a sibling, not a package: the same one-line path fix reference_demo.py makes,
# for the same reason - the demo is run as a script, not imported.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient

from demo_jvm import java_binary
from demo_kafka import ensure_topic, key_of, key_slot, seed
from parallel_consumer.sidecar import Sidecar, SidecarCommand
from parallel_consumer.streams import GrpcStreamsTransport, StreamsSession

log = logging.getLogger("streams-demo")

_MAIN_CLASS = "bz.stub.parallelconsumer.streams.StreamsMain"

_SINK_SUFFIX = "-counts"
"""The sink topic's name is derived, not configured: two names to keep in step is one too many."""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bootstrap", default=os.environ.get("PC_DEMO_BOOTSTRAP"),
                        help="broker address; demo/run.sh starts one and sets this")
    parser.add_argument("--topic", default=os.environ.get("PC_DEMO_STREAMS_TOPIC", ""),
                        help="source topic; a unique one per run by default")
    parser.add_argument("--records", type=int,
                        default=int(os.environ.get("PC_DEMO_STREAMS_RECORDS", "2000")),
                        help="records to seed (default 2000)")
    parser.add_argument("--partitions", type=int,
                        default=int(os.environ.get("PC_DEMO_STREAMS_PARTITIONS", "4")),
                        help="partitions on both topics (default 4)")
    parser.add_argument("--function-delay-ms", type=float,
                        default=float(os.environ.get("PC_DEMO_STREAMS_FUNCTION_DELAY_MS", "0")),
                        help="sleep inside the Python transform; over the poll interval this is "
                             "the failure arm, and it is expected to report no counts")
    parser.add_argument("--max-poll-interval-ms", type=int,
                        default=int(os.environ.get("PC_DEMO_STREAMS_MAX_POLL_INTERVAL_MS", "0")),
                        help="max.poll.interval.ms for the engine's consumer; 0 leaves Kafka's "
                             "own default, and the failure arm lowers it so the failure it is "
                             "demonstrating fits inside a demo rather than five minutes")
    parser.add_argument("--timeout", type=float,
                        default=float(os.environ.get("PC_DEMO_STREAMS_TIMEOUT", "120")),
                        help="seconds to wait for the counts to arrive (default 120)")
    return parser.parse_args(argv)


def poll_interval_property(args: argparse.Namespace) -> dict[str, str]:
    """The failure arm's one lever, kept out of the happy path.

    Kafka's default max.poll.interval.ms is five minutes. A demo cannot wait that long to show
    what a too-slow function does, so the slow arm lowers it - and lowering it is the honest way
    round, because the alternative is sleeping for five minutes to prove the same point. The
    happy path never sets it, so the passing run is on Kafka's own defaults.
    """
    if args.max_poll_interval_ms:
        return {"max.poll.interval.ms": str(args.max_poll_interval_ms)}
    if args.function_delay_ms:
        return {"max.poll.interval.ms": "5000"}
    return {}


def resolve_classpath() -> str:
    classpath = os.environ.get("PC_DEMO_STREAMS_CLASSPATH")
    if not classpath:
        raise SystemExit(
            "set PC_DEMO_STREAMS_CLASSPATH - demo/run.sh --streams builds it. By hand:\n"
            "  ./mvnw -pl :parallel-consumer-proxy-streams -am -DskipTests "
            "-DincludeScope=runtime package dependency:build-classpath "
            "'-Dmdep.outputFile=${project.build.directory}/streams-classpath.txt'"
        )
    return classpath


def expected_counts(records: int) -> Counter[int]:
    """What the sink must eventually say, derived from the same rule that seeded the backlog."""
    return Counter(int(key_of(ordinal).decode().removeprefix("key-")) for ordinal in range(records))


def read_counts(bootstrap: str, topic: str, expected: Counter[int], deadline: float) -> tuple[
        dict[int, int], int]:
    """Reads the sink last-value-per-key until it agrees with ``expected`` or time runs out.

    Last-value-per-key rather than a tally of what arrived: the sink carries a KTable changelog, so
    a key with a final count of 12 also carries 1 through 11 ahead of it. Summing the topic would
    report 78 for that key and call the run broken when it was correct.
    """
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"pc-streams-demo-verify-{int(time.time() * 1000)}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    latest: dict[int, int] = {}
    foreign = 0
    try:
        consumer.subscribe([topic])
        while time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                if latest and all(latest.get(k) == v for k, v in expected.items()):
                    break
                continue
            if message.error():
                continue
            slot = key_slot(message.key())
            if slot is None:
                foreign += 1
                continue
            value = message.value()
            if value is None or len(value) != 8:
                foreign += 1
                continue
            # Serdes.Long() is an 8-byte big-endian signed long. Decoded here rather than
            # guessed: a wrong width would silently read a plausible number.
            latest[slot] = struct.unpack(">q", value)[0]
            if all(latest.get(k) == v for k, v in expected.items()):
                break
    finally:
        consumer.close()
    return latest, foreign


class GroupWatch:
    """Samples the engine's consumer group throughout the run, on a thread of its own.

    Sampled continuously rather than once at the end, because a rebalance is a transient: a
    single-member group that gets evicted rejoins within a second or two and reads STABLE again.
    The first version of this demo checked once at the finish, pronounced a run rebalance-free,
    and would have said exactly the same thing about a run that had rebalanced twice in the middle.

    Sampled from *outside* the protocol because there is no inside: the Streams protocol carries no
    state or rebalance signal, so a client cannot ask the engine how it is doing. That gap is
    recorded in the inflight note; until it closes, the admin API is the only observer available.
    """

    def __init__(self, admin: AdminClient, application_id: str, interval: float = 0.5) -> None:
        self._admin = admin
        self._application_id = application_id
        self._interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.observations: Counter[str] = Counter()
        self.samples = 0
        self.errors = 0
        self.settled = False
        self.before_settling = 0

    def __enter__(self) -> GroupWatch:
        self._thread = threading.Thread(target=self._run, name="group-watch", daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop.is_set():
            self._sample()
            self._stop.wait(self._interval)
        self._sample()  # one last look, so a change in the final moments is not missed

    def _sample(self) -> None:
        try:
            future = self._admin.describe_consumer_groups([self._application_id])
            description: Any = future[self._application_id].result(timeout=5)
        except Exception:
            # Cannot tell is not the same as unstable - the group does not exist until the engine
            # joins, and counting that as instability would fail every run at its first sample.
            self.errors += 1
            return
        raw_state = getattr(description, "state", None)
        state = getattr(raw_state, "name", str(raw_state))
        members = len(getattr(description, "members", []) or [])
        observation = f"{state}/{members}"
        if not self.settled:
            # Everything up to the first settled sample is the engine ARRIVING - the group does
            # not exist, then it is forming, then one member is in it. Counting that against the
            # run would fail every run for doing the one thing it has to do first. The measured
            # window starts here.
            if observation != "STABLE/1":
                self.before_settling += 1
                return
            self.settled = True
        self.observations[observation] += 1
        self.samples += 1

    def verdict(self) -> tuple[bool, str]:
        """Whether the group stayed settled from the moment it first settled to the end."""
        joining = f"{self.before_settling} sample(s) while joining"
        if not self.samples:
            return False, f"never settled - {joining}, {self.errors} attempt(s) could not read it"
        summary = ", ".join(f"{key} x{n}" for key, n in sorted(self.observations.items()))
        if set(self.observations) != {"STABLE/1"}:
            return False, f"{summary} (after {joining})"
        return True, f"STABLE/1 for all {self.samples} samples after joining"


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_args(argv)
    if not args.bootstrap:
        raise SystemExit("--bootstrap is required (demo/run.sh sets PC_DEMO_BOOTSTRAP)")

    run_id = int(time.time())
    source = args.topic or f"pc-streams-demo-{run_id}"
    sink = f"{source}{_SINK_SUFFIX}"
    application_id = f"pc-streams-demo-{run_id}"

    # Absolute, because the client library refuses a PATH lookup for the executable it spawns.
    java, classpath = pathlib.Path(java_binary()).resolve(), resolve_classpath()

    log.info("Creating topics %s and %s (%d partitions)...", source, sink, args.partitions)
    ensure_topic(args.bootstrap, source, args.partitions)
    ensure_topic(args.bootstrap, sink, args.partitions)
    seed(args.bootstrap, source, 0, args.records)

    delay = args.function_delay_ms / 1000.0
    invocations = 0
    python_seconds = 0.0
    first_entered = 0.0
    last_left = 0.0

    def upper(key: bytes, value: bytes) -> bytes:
        """The Python that runs inside the topology. A stream thread is blocked on every call."""
        nonlocal invocations, python_seconds, first_entered, last_left
        entered = time.perf_counter()
        if not invocations:
            first_entered = entered
        if delay:
            time.sleep(delay)
        result = value.upper()
        last_left = time.perf_counter()
        python_seconds += last_left - entered
        invocations += 1
        return result

    sidecar = Sidecar(SidecarCommand(java, ("-cp", classpath, _MAIN_CLASS)))
    log.info("Starting the Streams engine...")
    port = sidecar.start(timeout=90)
    session = StreamsSession(GrpcStreamsTransport(port))
    admin = AdminClient({"bootstrap.servers": args.bootstrap})

    try:
        session.open(application_id, {
            "bootstrap.servers": args.bootstrap,
            "auto.offset.reset": "earliest",
            # One thread, so every invocation is serialized behind the one before it. That is what
            # makes the observed rate below a per-thread ceiling rather than an aggregate.
            "num.stream.threads": "1",
            # Emit every update instead of only the flushed ones. The demo checks a final value per
            # key, and a cache would hold the last of them until a timer the demo does not control.
            "statestore.cache.max.bytes": "0",
            "commit.interval.ms": "200",
            **poll_interval_property(args),
        })
        builder = session.builder()
        records = builder.source(source)
        transformed = builder.map_values(records, upper)
        grouped = builder.group_by_key(transformed)
        counts = builder.count(grouped, "counts-store")
        builder.sink(counts, sink)

        log.info("Topology described from Python; starting it...")
        started = time.monotonic()
        session.start()

        deadline = started + args.timeout
        expected = expected_counts(args.records)
        with GroupWatch(admin, application_id) as watch:
            observed, foreign = read_counts(args.bootstrap, sink, expected, deadline)
        elapsed = time.monotonic() - started
        stable, group_state = watch.verdict()
    finally:
        session.close()
        sidecar.close(timeout=30)

    # First-entry to last-exit, NOT the whole run. The run also contains engine startup, a group
    # join and the verifier's polling, none of which is round-trip cost - including them made the
    # first draft of this demo report a rate four times worse than the boundary actually is.
    invocation_window = max(last_left - first_entered, 0.0)
    return report(args, expected, observed, foreign, invocations, python_seconds,
                  invocation_window, elapsed, stable, group_state)


def report(args: argparse.Namespace, expected: Counter[int], observed: dict[int, int], foreign: int,
           invocations: int, python_seconds: float, invocation_window: float, elapsed: float,
           stable: bool, group_state: str) -> int:
    """Prints the outcome and returns the exit code. Counts first: they are the claim."""
    missing = sorted(k for k in expected if k not in observed)
    wrong = sorted(k for k, v in expected.items() if k in observed and observed[k] != v)
    matched = len(expected) - len(missing) - len(wrong)

    print()
    print(f"Keys expected           {len(expected)}")
    print(f"Keys matching exactly   {matched}")
    if missing:
        print(f"Keys with no count      {len(missing)} (first few: {missing[:5]})")
    if wrong:
        example = wrong[0]
        print(f"Keys with a wrong count {len(wrong)} "
              f"(e.g. key-{example}: saw {observed[example]}, expected {expected[example]})")
    if foreign:
        print(f"Records not ours        {foreign} (a pre-existing topic, counted separately)")
    print()
    print(f"Python invocations      {invocations}")
    print(f"Run                     {elapsed:.1f}s end to end, including engine startup")
    if invocations > 1 and invocation_window > 0:
        # invocations - 1: the window spans the GAPS between calls, and n calls have n-1 of them.
        per_call_us = invocation_window / (invocations - 1) * 1e6
        python_us = python_seconds / invocations * 1e6
        print(f"Invocation window       {invocation_window:.2f}s, first entry to last exit")
        print(f"Per invocation          {per_call_us:.0f}us round trip, "
              f"of which {python_us:.1f}us was Python "
              f"({python_us / per_call_us * 100:.1f}%)")
        print(f"Single-thread ceiling   {1e6 / per_call_us:,.0f} invocations/sec "
              "(within-session, one stream thread, this machine)")
    print(f"Consumer group          {group_state}")
    print()

    if wrong:
        # Wrong is never acceptable, in either arm. A key whose final count is a plausible but
        # incorrect number is the failure this whole demo exists to rule out, and no amount of
        # deliberate slowness excuses it.
        print("CORRUPTION - a key's final count is wrong, not merely missing. That is not a "
              "slow-function symptom; something counted incorrectly.")
        return 1

    if args.function_delay_ms:
        if not stable:
            print("The engine's consumer left the group: the slow transform held the stream "
                  "thread past the poll interval and Kafka evicted it. Counts are missing "
                  "because the run never finished - but every count that did arrive was "
                  "correct, which is the property that matters. A slow foreign function fails "
                  "VISIBLY here rather than quietly returning numbers that look finished.")
            return 0
        if missing:
            print("The group stayed settled and the counts are simply incomplete - the run ran "
                  "out of time. Worth reading carefully, because it is NOT what this arm was "
                  "written expecting: a transform this slow was predicted to blow the poll "
                  "interval and get evicted, and instead Kafka Streams interleaved its polling "
                  "and kept its membership. So the real failure mode of a slow foreign function "
                  "is collapsed throughput, not a broken group. Every count that arrived was "
                  "correct.")
            return 0
        print("The slow arm completed cleanly, so it demonstrated nothing. Raise "
              "--function-delay-ms or lower --timeout.")
        return 1

    if missing:
        print("INCOMPLETE - the sink is missing counts the backlog says should be there. If the "
              "run was simply too short, raise --timeout.")
        return 1
    if not stable:
        print("Counts matched, but the engine's consumer group was not settled throughout the "
              "window, so this run does not support the rebalance-free claim.")
        return 1
    print(f"OK - {matched} keys counted correctly by a topology described entirely from Python.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
