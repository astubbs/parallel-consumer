# Copyright (C) 2026 Antony Stubbs and contributors

"""Measurement lab for the windowing falsification spike - one harness, an experiment selector.

Built for the plan `docs/plans/2026-08-25-001-feat-streams-windowed-aggregation-plan.md` (U2) and
its results note `docs/inflight/perf-streams-windowing-multiplier.md`. It reuses the streams demo's
session/engine setup (`streams_demo.py`), broker contract (`demo_kafka.py` - THE LAB NEVER STARTS A
BROKER; `demo/run.sh` shows how the compose broker comes up) and JVM discovery (`demo_jvm.py`),
rather than copying them - sibling scripts drift (the plan's KTD14).

Experiments are named and selected, so later units add a function here instead of a sibling file.

Experiment ``hot-key`` (U2): whether an aggregation over ONE hot key can be rescued by anything the
parked bundling work offers. An aggregation is a serial dependency per key - accumulator n+1 needs
accumulator n - so it cannot be batched across a hot key. It uses the EXISTING ``reduce`` operator,
already a per-key serial dependency across the boundary. ``reduce`` skips each key's first value,
which is exactly why the two arms are matched on HOST INVOCATIONS ``I``, never on record count:

- arm A ("hot"):    ``I + 1`` records under one key  -> one partition -> one stream thread;
- arm B ("spread"): ``I + K`` records over ``K`` keys spread across the partitions.

Method constraints inherited from the three prior crossing measurements
(`docs/inflight/perf-streams-crossing-attribution.md`, `perf-crossing-is-cpu-and-serialised.md`,
`perf-crossing-fixed-versus-per-byte.md`) - each cost an experiment to learn:

- **interleave the arms** (A,B,A,B...) rather than running blocks, so machine drift lands on both;
- **sweep in crossings** (invocations), because per-invocation cost falls for tens of thousands of
  invocations - a sweep in records would put the arms in different parts of the ramp;
- **fit the slope** across the sweep, so the fixed component (startup, join, JIT ramp) cancels;
  every sweep point is at or above 32,000 invocations, the warm-up discard line;
- **read the broker's log-append clock** (sink created with ``message.timestamp.type=
  LogAppendTime``), never wall clock - the broker stamps each update as the engine produces it;
- **quiet machine**: 1-minute load is read before every measured run, recorded with the run, and a
  run does not start while it exceeds the limit.

``commit.interval.ms`` is set explicitly and printed - neither ``demo_kafka.py`` nor
``demo_options.py`` sets it, and an unstated interval is an unreproducible run.

The instrument check (the plan's R4): arm A re-run with the reducer artificially slowed, paired
run-for-run with an unslowed arm A at the same invocation count. Throughput must fall by roughly
the added delay per invocation; a number that does not move is measuring something else.
"""

from __future__ import annotations

import argparse
import dataclasses
import logging
import os
import pathlib
import statistics
import sys
import threading
import time

# Siblings, not a package: the same one-line path fix every demo script makes.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from confluent_kafka import TIMESTAMP_LOG_APPEND_TIME, Consumer, Producer
from confluent_kafka.admin import AdminClient

from demo_jvm import java_binary
from demo_kafka import _payload, ensure_topic
from streams_demo import _MAIN_CLASS, GroupWatch, resolve_classpath
from parallel_consumer.sidecar import Sidecar, SidecarCommand
from parallel_consumer.streams import GrpcStreamsTransport, StreamsSession

log = logging.getLogger("streams-windowing-lab")

_KEY_PREFIX = "lab-key-"
"""The lab's own key rule. demo_kafka's ``key_of`` is pinned to its 1,000-key space; arm A needs
one key and arm B needs thousands, so the lab seeds its own keys and reads them back by prefix."""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("experiment", choices=sorted(EXPERIMENTS),
                        help="which measurement to run; each is one plan unit")
    parser.add_argument("--bootstrap", default=os.environ.get("PC_DEMO_BOOTSTRAP"),
                        help="broker address; demo/run.sh shows how the compose broker starts")
    parser.add_argument("--sweep", default="32000,64000,128000",
                        help="comma-separated invocation counts I; every point must be at or "
                             "above the 32,000-invocation warm-up line (default "
                             "32000,64000,128000)")
    parser.add_argument("--reps", type=int, default=3,
                        help="measured repetitions per (arm, I) point (default 3)")
    parser.add_argument("--keys", type=int, default=8_000,
                        help="arm B's key count K (default 8000)")
    parser.add_argument("--partitions", type=int, default=8,
                        help="partitions on source and sink (default 8)")
    parser.add_argument("--stream-threads", type=int, default=8,
                        help="num.stream.threads for the engine (default 8)")
    parser.add_argument("--payload-bytes", type=int, default=1024,
                        help="record payload size (default 1024)")
    parser.add_argument("--commit-interval-ms", type=int, default=200,
                        help="commit.interval.ms for the engine, set explicitly and printed "
                             "(default 200, matching every prior crossing measurement)")
    parser.add_argument("--instrument-delay-ms", type=float, default=1.0,
                        help="the R4 instrument check's added per-invocation delay (default 1)")
    parser.add_argument("--instrument-reps", type=int, default=3,
                        help="paired (plain, delayed) repetitions for the instrument check "
                             "(default 3); 0 skips it")
    parser.add_argument("--load-limit", type=float, default=8.0,
                        help="1-minute load above which a measured run waits (default 8)")
    parser.add_argument("--keep-topics", action="store_true",
                        help="leave each run's topics on the broker instead of deleting them")
    return parser.parse_args(argv)


@dataclasses.dataclass(frozen=True)
class RunResult:
    """One measured run, with every condition the write-up needs beside the number."""

    arm: str
    invocations: int          # I, the matched term
    records: int              # derived: I+1 (hot) or I+K (spread)
    delay_ms: float           # artificial reducer delay; 0 outside the instrument check
    updates: int              # sink records observed (one per input record, cache off)
    window_s: float           # first own sink append to last, broker log-append clock
    host_invocations: int     # what the reducer actually counted; must equal I
    load1: float              # 1-minute load read immediately before the run
    group_ok: bool
    group_state: str
    log_append_clock: bool

    @property
    def inv_per_s(self) -> float:
        return self.invocations / self.window_s if self.window_s > 0 else 0.0

    @property
    def per_inv_us(self) -> float:
        return self.window_s / self.invocations * 1e6 if self.invocations else 0.0

    @property
    def valid(self) -> bool:
        return (self.host_invocations == self.invocations and self.group_ok
                and self.log_append_clock and self.updates == self.records)


def wait_for_quiet(limit: float) -> float:
    """Blocks until the 1-minute load is under ``limit``; returns the reading that admitted us."""
    while True:
        load1 = os.getloadavg()[0]
        if load1 <= limit:
            return load1
        log.info("1-minute load %.2f exceeds the %.1f limit - pausing 30s rather than "
                 "measuring through contention", load1, limit)
        time.sleep(30)


def seed_keyed(bootstrap: str, topic: str, records: int, keys: int, payload_bytes: int) -> None:
    """Seeds ``records`` 1 KB-class records over ``keys`` keys, round-robin.

    The lab's own seeding rather than ``demo_kafka.seed`` only because the key rule differs (see
    ``_KEY_PREFIX``); producer settings and the incompressible-padding payload are the shared ones.
    """
    failures: list[object] = []

    def delivered(error: object, _message: object) -> None:
        if error is not None:
            failures.append(error)

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "linger.ms": 20,
        "queue.buffering.max.messages": 1_000_000,
    })
    for ordinal in range(records):
        producer.produce(
            topic,
            key=f"{_KEY_PREFIX}{ordinal % keys}".encode(),
            value=_payload(ordinal, payload_bytes),
            on_delivery=delivered,
        )
        if ordinal % 10_000 == 0:
            producer.poll(0)
    producer.flush()
    if failures:
        raise RuntimeError(f"the lab could not seed its backlog: {failures[0]}")


def read_sink(bootstrap: str, topic: str, expected_updates: int,
              deadline: float) -> tuple[int, float, bool]:
    """Counts the lab's own sink updates until ``expected_updates`` arrive or time runs out.

    Returns (updates seen, window seconds, whether every update carried a log-append timestamp).
    The window is first own append to last on the BROKER'S clock - the engine's own timeline,
    immune to how fast this verifier reads.
    """
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"pc-wlab-verify-{time.time_ns()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    updates = 0
    first_ms: int | None = None
    last_ms: int | None = None
    log_append = True
    try:
        consumer.subscribe([topic])
        while time.monotonic() < deadline and updates < expected_updates:
            message = consumer.poll(1.0)
            if message is None or message.error():
                continue
            key = message.key()
            if key is None or not key.startswith(_KEY_PREFIX.encode()):
                continue
            updates += 1
            timestamp_type, timestamp_ms = message.timestamp()
            if timestamp_type != TIMESTAMP_LOG_APPEND_TIME:
                log_append = False
            if first_ms is None or timestamp_ms < first_ms:
                first_ms = timestamp_ms
            if last_ms is None or timestamp_ms > last_ms:
                last_ms = timestamp_ms
    finally:
        consumer.close()
    window = (last_ms - first_ms) / 1000.0 if first_ms is not None and last_ms is not None else 0.0
    return updates, window, log_append


def measure(args: argparse.Namespace, arm: str, invocations: int,
            delay_ms: float = 0.0, show_topology: bool = False) -> RunResult:
    """One measured run: fresh topics, fresh engine, one arm, one invocation count."""
    keys = 1 if arm == "hot" else args.keys
    records = invocations + keys
    load1 = wait_for_quiet(args.load_limit)

    run_id = time.time_ns()
    source = f"pc-wlab-{run_id}"
    sink = f"{source}-out"
    application_id = f"pc-wlab-{run_id}"

    java, classpath = pathlib.Path(java_binary()).resolve(), resolve_classpath()

    ensure_topic(args.bootstrap, source, args.partitions)
    ensure_topic(args.bootstrap, sink, args.partitions,
                 config={"message.timestamp.type": "LogAppendTime"})
    seed_keyed(args.bootstrap, source, records, keys, args.payload_bytes)

    delay = delay_ms / 1000.0
    host_invocations = 0
    accounting = threading.Lock()

    def last_value(_accumulator: bytes, value: bytes) -> bytes:
        """The serial dependency itself: the engine cannot send a key's next record until this
        returns. Keeps the newest value, so the Python work is as close to zero as the shape
        allows and the measured cost is the crossing's."""
        nonlocal host_invocations
        if delay:
            time.sleep(delay)
        with accounting:
            host_invocations += 1
        return value

    sidecar = Sidecar(SidecarCommand(java, ("-cp", classpath, _MAIN_CLASS)))
    port = sidecar.start(timeout=90)
    session = StreamsSession(GrpcStreamsTransport(port))
    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    try:
        session.open(application_id, {
            "bootstrap.servers": args.bootstrap,
            "auto.offset.reset": "earliest",
            "num.stream.threads": str(args.stream_threads),
            # Cache off: every store update forwards, so sink updates == input records and the
            # sink window covers the whole processed backlog.
            "statestore.cache.max.bytes": "0",
            "commit.interval.ms": str(args.commit_interval_ms),
        })
        builder = session.builder()
        grouped = builder.group_by_key(builder.source(source))
        reduced = builder.reduce(grouped, last_value, "wlab-store")
        builder.sink(reduced, sink)
        if show_topology:
            print()
            print(f"Topology, arm {arm} (groupByKey preserves the key - no repartition):")
            for line in session.describe().text.rstrip().split("\n"):
                print(f"    {line}")
        session.start()

        # Generous but bounded: seed replay at the published rates plus the artificial delay,
        # doubled, on top of a fixed 90s for startup, join and cadence.
        timeout = 90 + records * (400e-6 + delay) * 2
        deadline = time.monotonic() + timeout
        with GroupWatch(admin, application_id, args.stream_threads) as watch:
            updates, window, log_append = read_sink(args.bootstrap, sink, records, deadline)
        group_ok, group_state = watch.verdict()
    finally:
        session.close()
        sidecar.close(timeout=30)
        if not args.keep_topics:
            # 24 runs x ~270 MB of topics would fill the compose broker's container disk; each
            # run's numbers are already read, so its topics are dead weight.
            admin.delete_topics([source, sink])

    result = RunResult(arm=arm, invocations=invocations, records=records, delay_ms=delay_ms,
                       updates=updates, window_s=window, host_invocations=host_invocations,
                       load1=load1, group_ok=group_ok, group_state=group_state,
                       log_append_clock=log_append)
    validity = "ok" if result.valid else (
        f"INVALID (host_invocations={host_invocations}, updates={updates}/{records}, "
        f"group={group_state}, log_append={log_append})")
    print(f"  arm={arm:6s} I={invocations:>7,} records={records:>7,} delay={delay_ms:g}ms "
          f"window={window:7.2f}s inv/s={result.inv_per_s:8,.0f} "
          f"per-inv={result.per_inv_us:6.0f}us load1={load1:.2f} {validity}")
    if not result.valid:
        raise RuntimeError(f"run invalid, stopping rather than averaging over it: {validity}")
    return result


def fit_slope(points: list[tuple[int, float]]) -> tuple[float, float]:
    """Least-squares (slope seconds/invocation, intercept seconds) of window against I."""
    n = len(points)
    mean_x = sum(x for x, _ in points) / n
    mean_y = sum(y for _, y in points) / n
    ss_xx = sum((x - mean_x) ** 2 for x, _ in points)
    ss_xy = sum((x - mean_x) * (y - mean_y) for x, y in points)
    slope = ss_xy / ss_xx
    return slope, mean_y - slope * mean_x


def _spread(values: list[float]) -> str:
    if len(values) < 2:
        return "single run"
    return (f"mean {statistics.mean(values):,.0f}, "
            f"min {min(values):,.0f}, max {max(values):,.0f}, n={len(values)}")


def run_hot_key(args: argparse.Namespace) -> int:
    """U2: hot-key versus spread-key reduce throughput, matched on host invocations."""
    sweep = sorted(int(n) for n in args.sweep.split(","))
    below = [n for n in sweep if n < 32_000]
    if below:
        raise SystemExit(f"sweep points {below} are below the 32,000-invocation warm-up line "
                         "the prior measurements established; raise them or justify a new line")

    print("hot-key experiment (plan U2) - conditions:")
    print(f"  machine                 {os.cpu_count()} cores, load1 at start "
          f"{os.getloadavg()[0]:.2f} (limit {args.load_limit:g})")
    print(f"  commit.interval.ms      {args.commit_interval_ms} (set explicitly)")
    print(f"  stream threads          {args.stream_threads}")
    print(f"  partitions              {args.partitions}")
    print(f"  arm B key count K       {args.keys:,}")
    print(f"  payload                 {args.payload_bytes} bytes")
    print(f"  sweep (invocations I)   {', '.join(f'{n:,}' for n in sweep)} x {args.reps} reps, "
          "arms interleaved A,B within each point")
    print("  statestore cache        off (sink updates == records)")
    print()

    results: list[RunResult] = []
    for rep in range(args.reps):
        for point in sweep:
            first = rep == 0 and point == sweep[0]
            results.append(measure(args, "hot", point, show_topology=first))
            results.append(measure(args, "spread", point, show_topology=first))

    checks: list[tuple[RunResult, RunResult]] = []
    for _ in range(args.instrument_reps):
        plain = measure(args, "hot", sweep[0])
        delayed = measure(args, "hot", sweep[0], delay_ms=args.instrument_delay_ms)
        checks.append((plain, delayed))

    print()
    print("Summary (all rates in INVOCATIONS per second; record counts derived and shown):")
    for arm in ("hot", "spread"):
        arm_runs = [r for r in results if r.arm == arm]
        for point in sweep:
            rates = [r.inv_per_s for r in arm_runs if r.invocations == point]
            recs = next(r.records for r in arm_runs if r.invocations == point)
            print(f"  arm {arm:6s} I={point:>7,} (records {recs:>7,}): "
                  f"inv/s {_spread(rates)}")
        slope, intercept = fit_slope([(r.invocations, r.window_s) for r in arm_runs])
        print(f"  arm {arm:6s} fitted slope {slope * 1e6:.0f}us/invocation "
              f"(steady-state {1 / slope:,.0f} inv/s), intercept {intercept:.2f}s, "
              f"over {len(arm_runs)} runs all at or above the 32,000-invocation warm-up line")

    if checks:
        plain_us = statistics.mean(p.per_inv_us for p, _ in checks)
        delayed_us = statistics.mean(d.per_inv_us for _, d in checks)
        delta = delayed_us - plain_us
        print(f"  instrument check (R4)   +{args.instrument_delay_ms:g}ms in the reducer moved "
              f"per-invocation cost {plain_us:.0f} -> {delayed_us:.0f}us "
              f"(delta {delta:.0f}us against {args.instrument_delay_ms * 1000:.0f}us added)")

    mid = sweep[len(sweep) // 2]
    hot_mid = statistics.mean(
        r.inv_per_s for r in results if r.arm == "hot" and r.invocations == mid)
    spread_mid = statistics.mean(
        r.inv_per_s for r in results if r.arm == "spread" and r.invocations == mid)
    print()
    print(f"  at I={mid:,}: hot {hot_mid:,.0f} inv/s, spread {spread_mid:,.0f} inv/s, "
          f"ratio {spread_mid / hot_mid:.2f}x with {args.stream_threads} threads available")
    verdict = ("CONFIRMED" if spread_mid < 4 * hot_mid
               else "REFUTED - near-linear scaling, which reopens bundling and "
                    "one-session-per-stream-thread")
    print(f"  prediction 2 (spread near the eight-thread plateau, NOT {args.stream_threads}x "
          f"the hot arm): ratio strictly under 4x is {verdict}")
    return 0


EXPERIMENTS = {"hot-key": run_hot_key}


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_args(argv)
    if not args.bootstrap:
        raise SystemExit("--bootstrap is required (or PC_DEMO_BOOTSTRAP; demo/run.sh shows how "
                         "the compose broker starts)")
    return EXPERIMENTS[args.experiment](args)


if __name__ == "__main__":
    sys.exit(main())
