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

Experiment ``placement`` (U6): the decisive arms. Windowed aggregation at two placements against a
crossing-free control and the host-reimplementation floor, per window specification, rates in
RECORDS per second (the floors' unit - the multiplier under test IS the invocations-per-record
ratio, so normalising to invocations would divide it out; the plan's R6 names U6 as the
exception). Arms, at the shared load unless stated:

- A: P1 tumbling 1h - host aggregator, multiplier 1;
- B: P1 hopping 1h/5m - host aggregator, multiplier 12;
- C: P1 hopping 1h/30m - the linearity arm, multiplier 2, carries no verdict;
- D: crossing-free control - B's topology with ``combine=LAST_BYTES`` and NO host function, so
  exactly one term changes against B (the crossing; ``APPEND_BYTES`` would change accumulator
  volume too);
- E: P2 hopping 1h/5m, ``combine=APPEND_BYTES``, host at the emit through ``to_stream`` +
  ``map_values``, at its OWN load (at the shared load records-per-key-per-flush is far below one
  and the ratio is guaranteed to read one) - with arm B and arm H re-run at each of E's sweep
  points so no cross-load comparison is ever reported as a placement result;
- H: the reimplementation that DEFINES F2 - a single-threaded ``confluent_kafka`` consumer doing
  the same aggregation in a dict, stateless and non-durable, run per window specification while
  the engine is idle. Experiment ``host-reimpl`` runs it standalone.

Every arm sinks through ``to_stream`` (nothing else consumes a windowed handle). The inherited
last-value-per-key completion predicate does not apply over it: the sink carries one record per
emit per window under COLLIDING inner keys, and on a broker the emit count is exactly what caching
makes nondeterministic - an equality predicate hangs when dedup lands below the expectation. The
replacement is QUIESCENCE: the arm is complete when the sink gains no new records for N commit
intervals (N printed), and the emit count is validated post-hoc as a band, never as the stop
condition. A quiescence break is then CONFIRMED before it is believed: the sink's per-partition
end offsets are captured, re-read after a further 2x the quiet window, and any advance fails the
run as a premature break - an engine stalled longer than the quiet window (a long GC pause, cache
pressure) satisfies the silence predicate mid-backlog, which would truncate the measured window
under an untruncated record count and inflate the rate. The engine group's committed source
offsets are additionally required to cover the whole seeded backlog, because with the cache on a
truncated run's emit count can land inside the post-hoc band.

Event times: every record carries the same producer-assigned constant timestamp, far past the
epoch clamp in ``TimeWindows.windowsFor`` - the multiplier is timestamp-independent past the
clamp, a constant keeps every record in exactly ``ceil(size/advance)`` windows, no window ever
closes and no record is ever late, so call counts are exact and store growth is bounded.

Crossings are counted CLIENT-side: the host is the invocation target, so the registered function
counts every crossing exactly. Arm D registers no function at all, which makes its zero a
measurement rather than an assumption - an engine-side invocation would name an unregistered
token, error the answer, and fail the run rather than pass silently.

Arm E's zero-cache-evictions assertion: Kafka Streams 3.9.2 exposes NO eviction metric
(``cache-size-bytes-total`` and hit-ratio only), so the counters ``ThreadCache`` keeps are read
through its own TRACE logging instead - the E-family engine runs get ``slf4j-simple`` on the
classpath, ``ThreadCache`` at trace into a per-run file, and the lab sums the evicted-entry
counts. A point with evictions FAILS THE ARM at that point rather than reporting its ratio. The
instrument is proven able to show a non-zero (an undersized-cache run must report evictions)
before any zero is believed.
"""

from __future__ import annotations

import argparse
import dataclasses
import logging
import os
import pathlib
import re
import statistics
import sys
import tempfile
import threading
import time

# Siblings, not a package: the same one-line path fix every demo script makes.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from confluent_kafka import (
    OFFSET_BEGINNING,
    TIMESTAMP_LOG_APPEND_TIME,
    Consumer,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient

from demo_jvm import java_binary
from demo_kafka import _payload, ensure_topic
from streams_demo import _MAIN_CLASS, GroupWatch, resolve_classpath
from parallel_consumer.sidecar import Sidecar, SidecarCommand
from parallel_consumer.streams import (
    CombineKind,
    GrpcStreamsTransport,
    StreamsSession,
    TimeWindow,
)

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
    # --- placement (U6) ---
    parser.add_argument("--phase", choices=("all", "shared", "emit"), default="all",
                        help="placement only: which half to run - the shared-load arms "
                             "(A,B,C,D,H) or arm E's own-load sweep (default all)")
    parser.add_argument("--crossings-sweep", default="32000,64000,128000",
                        help="placement/host-reimpl: sweep points in CROSSINGS for the P1 arms; "
                             "each arm's record count is the point divided by its multiplier, so "
                             "every point sits at or above the 32,000-crossing warm-up line "
                             "(default 32000,64000,128000)")
    parser.add_argument("--quiescence-intervals", type=int, default=15,
                        help="completion predicate: the sink is quiescent after this many commit "
                             "intervals with no new record (default 15)")
    parser.add_argument("--emit-keys-sweep", default="3200,800,200,50",
                        help="arm E's own-load sweep: key counts K; records-per-key-per-commit-"
                             "interval then spans the required range as the achieved rate "
                             "divides by K (default 3200,800,200,50)")
    parser.add_argument("--emit-records-per-key", type=int, default=24,
                        help="arm E: records per key, which caps the APPEND_BYTES accumulator at "
                             "records-per-key x payload bytes - 24 x 1 KB = 24 KB per entry, "
                             "inside the plan's low-tens-of-KB bound (default 24)")
    parser.add_argument("--emit-b-records-cap", type=int, default=24_000,
                        help="cap on the matched arm-B re-run's record count at each E sweep "
                             "point; B's per-record cost does not depend on records-per-key, and "
                             "an uncapped B at twelve crossings per record dominates the wall "
                             "time (default 24000)")
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


def seed_keyed(bootstrap: str, topic: str, records: int, keys: int, payload_bytes: int,
               timestamp_ms: int | None = None) -> None:
    """Seeds ``records`` 1 KB-class records over ``keys`` keys, round-robin.

    The lab's own seeding rather than ``demo_kafka.seed`` only because the key rule differs (see
    ``_KEY_PREFIX``); producer settings and the incompressible-padding payload are the shared ones.
    ``timestamp_ms`` pins every record's event time to one constant (the placement arms need it -
    a constant keeps each record in exactly ``ceil(size/advance)`` windows); None keeps the
    producer's own clock, which is what the hot-key experiment always used.
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
        # librdkafka reads timestamp 0 as "now", so the constant-time path is a plain default.
        producer.produce(
            topic,
            key=f"{_KEY_PREFIX}{ordinal % keys}".encode(),
            value=_payload(ordinal, payload_bytes),
            on_delivery=delivered,
            timestamp=timestamp_ms if timestamp_ms is not None else 0,
        )
        if ordinal % 10_000 == 0:
            producer.poll(0)
    producer.flush()
    if failures:
        raise RuntimeError(f"the lab could not seed its backlog: {failures[0]}")


def delete_run_topics(admin: AdminClient, topics: list[str]) -> None:
    """Deletes a run's topics and AWAITS each result - the lab's one cleanup path.

    ``delete_topics`` only queues the requests and returns futures; unawaited, a failed delete (a
    dead controller, a topic mid-recreation) is silent and dead runs accumulate until the compose
    broker's container disk fills. Cleanup must never fail a valid run, so a failed delete logs a
    warning rather than raising.
    """
    for topic, future in admin.delete_topics(topics).items():
        try:
            future.result()
        except Exception as error:  # cleanup must not fail a valid run
            log.warning("failed to delete run topic %s: %s", topic, error)


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
    store = "wlab-store"
    changelog = f"{application_id}-{store}-changelog"

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
        reduced = builder.reduce(grouped, last_value, store)
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
            # run's numbers are already read, so its topics - including the reduce store's
            # changelog - are dead weight.
            delete_run_topics(admin, [source, sink, changelog])

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
    if ss_xx == 0:
        # One distinct x (a single-point sweep): the through-origin rate is the only slope
        # available, and the intercept is unmeasurable rather than zero.
        return mean_y / mean_x, 0.0
    slope = ss_xy / ss_xx
    return slope, mean_y - slope * mean_x


def _spread(values: list[float]) -> str:
    if len(values) < 2:
        return f"{values[0]:,.0f} (single run)" if values else "no runs"
    return (f"mean {statistics.mean(values):,.0f}, "
            f"min {min(values):,.0f}, max {max(values):,.0f}, n={len(values)}")


def _spread_f(values: list[float]) -> str:
    """The float-precision sibling, for per-record ratios that round to nothing at .0f."""
    if len(values) < 2:
        return f"{values[0]:.2f} (single run)" if values else "no runs"
    return (f"mean {statistics.mean(values):.2f}, "
            f"min {min(values):.2f}, max {max(values):.2f}, n={len(values)}")


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


# --------------------------------------------------------------------------------------------
# U6: the placement comparison.
# --------------------------------------------------------------------------------------------

_SIZE_MS = 3_600_000
"""Window size: one hour, every specification in the plan."""
_RETENTION_MS = 7_200_000
"""Two hours, explicitly above Kafka's size+grace default (the plan's KTD17)."""
_TUMBLE = TimeWindow(_SIZE_MS, _SIZE_MS, 0, _RETENTION_MS)
_HOP5 = TimeWindow(_SIZE_MS, 300_000, 0, _RETENTION_MS)
_HOP30 = TimeWindow(_SIZE_MS, 1_800_000, 0, _RETENTION_MS)

_EVENT_TIME_MS = 1_750_000_000_000
"""Every placement record's event time - one constant, producer-assigned, far past the epoch
clamp in ``TimeWindows.windowsFor`` (which needs ``size - advance`` = 55 minutes; this is ~55
years). A constant keeps every record in exactly ``ceil(size/advance)`` windows, so call counts
are exact, no window ever closes, and no record is ever late."""

_STORE = "wlab-agg"

_F1 = 1_000.0
"""The parity floor, records per second - pre-registered in the results note's section 1."""


@dataclasses.dataclass(frozen=True)
class ArmSpec:
    """One placement arm: which window, and where (if anywhere) the host function sits."""

    window: TimeWindow
    placement: str            # "host" (P1), "last" (D: engine LAST_BYTES, no host),
                              # "append" (E: engine APPEND_BYTES, host at the emit)
    multiplier: int           # ceil(size / advance)


_ARMS: dict[str, ArmSpec] = {
    "A": ArmSpec(_TUMBLE, "host", 1),
    "B": ArmSpec(_HOP5, "host", 12),
    "C": ArmSpec(_HOP30, "host", 2),
    "D": ArmSpec(_HOP5, "last", 12),
    "E": ArmSpec(_HOP5, "append", 12),
}


@dataclasses.dataclass(frozen=True)
class PlacementRun:
    """One measured placement run - the rate, the crossings, and every condition beside them."""

    arm: str
    records: int
    keys: int
    multiplier: int
    crossings: int            # host invocations, counted client-side (the host IS the target)
    emits: int                # sink records observed until quiescence
    window_s: float           # first own sink append to last, broker log-append clock
    cache_bytes: int
    commit_ms: int
    evictions: int | None     # ThreadCache eviction count; None when not instrumented
    load1: float
    group_ok: bool
    group_state: str
    log_append: bool
    emit_band: tuple[int, int]

    @property
    def rate(self) -> float:
        """RECORDS per second - the floors' unit (the plan's R6 names U6 as the exception)."""
        return self.records / self.window_s if self.window_s > 0 else 0.0

    @property
    def crossings_per_record(self) -> float:
        return self.crossings / self.records if self.records else 0.0

    @property
    def sec_per_record(self) -> float:
        return self.window_s / self.records if self.records else 0.0


def _end_offsets(consumer: Consumer, topic: str) -> dict[int, int]:
    """Per-partition end offsets, read from the broker rather than any client-side cache."""
    metadata = consumer.list_topics(topic, timeout=10)
    return {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
            for p in metadata.topics[topic].partitions}


def read_emits_quiescent(bootstrap: str, topic: str, quiet_s: float,
                         deadline: float) -> tuple[int, float, bool, bool]:
    """Counts sink records until the topic goes QUIET - the U6 completion predicate.

    Not the inherited last-value-per-key predicate, and not an expected count: after
    ``to_stream`` the sink carries one record per emit per window under colliding inner keys,
    and on a broker the emit count is exactly the quantity caching makes nondeterministic - an
    equality predicate hangs when dedup lands below the expectation and stops early when it does
    not. Quiescence instead: done when no new record has arrived for ``quiet_s`` (N commit
    intervals, N printed by the caller) after at least one arrived. The observed count is
    validated post-hoc as a band, never used to stop.

    A break is then CONFIRMED before it is believed: an engine stalled for longer than
    ``quiet_s`` mid-backlog (a long GC pause, cache pressure) satisfies the silence predicate
    while records are still coming, which would truncate the measured window under an
    untruncated record basis and inflate the rate. So after the break the sink's per-partition
    end offsets are captured, re-read after a further ``2 x quiet_s``, and any advance is
    reported as a premature break for the caller to fail the run on.

    Returns (emits, window seconds on the broker's log-append clock, clock validity,
    premature break).
    """
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"pc-wlab-verify-{time.time_ns()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    emits = 0
    first_ms: int | None = None
    last_ms: int | None = None
    log_append = True
    quiescent = False
    premature = False
    last_new = time.monotonic()
    try:
        consumer.subscribe([topic])
        while time.monotonic() < deadline:
            # Batch consume: arm D emits multiplier x records (1.5M at the largest point), and a
            # poll-per-message verifier would lag the engine it is supposed to be watching.
            batch = consumer.consume(num_messages=2000, timeout=0.2)
            if not batch:
                if emits and time.monotonic() - last_new > quiet_s:
                    quiescent = True
                    break
                continue
            for message in batch:
                if message.error():
                    continue
                key = message.key()
                if key is None or not key.startswith(_KEY_PREFIX.encode()):
                    continue
                emits += 1
                last_new = time.monotonic()
                timestamp_type, timestamp_ms = message.timestamp()
                if timestamp_type != TIMESTAMP_LOG_APPEND_TIME:
                    log_append = False
                if first_ms is None or timestamp_ms < first_ms:
                    first_ms = timestamp_ms
                if last_ms is None or timestamp_ms > last_ms:
                    last_ms = timestamp_ms
        if quiescent:
            # The confirmation wait. End offsets rather than more consuming, so the counted
            # emits stay exactly what the break saw; the flag, not the count, fails the run.
            before = _end_offsets(consumer, topic)
            time.sleep(2 * quiet_s)
            after = _end_offsets(consumer, topic)
            premature = any(after.get(p, offset) > offset for p, offset in before.items())
    finally:
        consumer.close()
    window = (last_ms - first_ms) / 1000.0 if first_ms is not None and last_ms is not None else 0.0
    return emits, window, log_append, premature


def _committed_source_records(bootstrap: str, group: str, topic: str, partitions: int) -> int:
    """The engine group's committed source offsets, summed - the processed-record basis.

    A placement rate divides the SEEDED record count by the observed sink window, so a valid run
    must prove the engine consumed the whole backlog. The emit band alone cannot: with the cache
    on, a truncated run's emit count can land inside the band, and arm E's expected crossing
    count is the emit count itself. Committed offsets are fetched with a plain group-coordinator
    read (no subscribe, so no rebalance against the live engine); a never-committed partition
    reads as zero.
    """
    probe = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "enable.auto.commit": False,
    })
    try:
        committed = probe.committed([TopicPartition(topic, p) for p in range(partitions)],
                                    timeout=30)
        return sum(tp.offset for tp in committed if tp.offset >= 0)
    finally:
        probe.close()


def _slf4j_simple_jar() -> str:
    """The slf4j binding the eviction instrument rides on - the engine classpath has none."""
    root = pathlib.Path.home() / ".m2" / "repository" / "org" / "slf4j" / "slf4j-simple"
    jars = sorted(root.glob("*/slf4j-simple-*.jar"))
    if not jars:
        raise SystemExit(
            "no slf4j-simple in ~/.m2 - the eviction instrument needs it on the engine "
            "classpath; fetch it with mvn dependency:get -Dartifact=org.slf4j:slf4j-simple:2.0.18")
    return str(jars[-1])


def _count_evictions(engine_log: pathlib.Path) -> int:
    """Total ThreadCache evictions from the engine's trace log.

    Kafka Streams 3.9.2 exposes NO eviction metric (hit-ratio and cache-size-bytes-total only),
    so the counters ``ThreadCache`` itself keeps are read through its trace logging: one
    "Evicted {n} entries from cache {ns}" line per put, and cumulative "#evicts={n}" in the
    per-flush stats line. Both are summed/maxed and cross-checked; disagreement is an
    instrument fault worth failing on.
    """
    evicted_sum = 0
    evicts_max = 0
    if not engine_log.exists():
        raise RuntimeError(f"the engine trace log never appeared at {engine_log} - the "
                           "eviction instrument saw nothing, so its zero would be meaningless")
    with engine_log.open("r", errors="replace") as lines:
        for line in lines:
            evicted = re.search(r"Evicted (\d+) entries", line)
            if evicted:
                evicted_sum += int(evicted.group(1))
            stats = re.search(r"#evicts=(\d+)", line)
            if stats:
                evicts_max = max(evicts_max, int(stats.group(1)))
    if (evicted_sum == 0) != (evicts_max == 0):
        raise RuntimeError(f"the two eviction counters disagree (per-put sum {evicted_sum}, "
                           f"flush-stats max {evicts_max}) - instrument fault, not a result")
    return max(evicted_sum, evicts_max)


def measure_placement(args: argparse.Namespace, arm: str, records: int, keys: int,
                      cache_bytes: int, *, trace_cache: bool = False,
                      tolerate_evictions: bool = False,
                      show_topology: bool = False) -> PlacementRun:
    """One measured placement run: fresh topics, fresh engine, one arm."""
    spec = _ARMS[arm]
    load1 = wait_for_quiet(args.load_limit)

    run_id = time.time_ns()
    source = f"pc-wlab-{run_id}"
    sink = f"{source}-out"
    application_id = f"pc-wlab-{run_id}"
    changelog = f"{application_id}-{_STORE}-changelog"

    java, classpath = pathlib.Path(java_binary()).resolve(), resolve_classpath()
    engine_log = pathlib.Path(tempfile.gettempdir()) / f"pc-wlab-engine-{run_id}.log"
    jvm_args: tuple[str, ...] = ("-cp", classpath, _MAIN_CLASS)
    if trace_cache:
        # The eviction instrument: slf4j-simple onto the classpath (the engine ships only the
        # API, so its logging is otherwise a no-op), ThreadCache at trace into a per-run file.
        # Applied identically to arm E and its matched arm-B re-runs, so the (small) logging
        # drag cannot differ between the two sides of the comparison.
        jvm_args = (
            "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
            "-Dorg.slf4j.simpleLogger.log.org.apache.kafka.streams.state.internals.ThreadCache"
            "=trace",
            f"-Dorg.slf4j.simpleLogger.logFile={engine_log}",
            "-cp", f"{classpath}:{_slf4j_simple_jar()}", _MAIN_CLASS)

    # retention.ms=-1: the records carry a constant 2025 event time, and the broker's time-based
    # retention deletes "old" data even out of the active segment by rolling it - a topic that
    # sits past a retention check (every 5 minutes) silently empties. Costed one session run.
    ensure_topic(args.bootstrap, source, args.partitions, config={"retention.ms": "-1"})
    ensure_topic(args.bootstrap, sink, args.partitions,
                 config={"message.timestamp.type": "LogAppendTime", "retention.ms": "-1"})
    seed_keyed(args.bootstrap, source, records, keys, args.payload_bytes,
               timestamp_ms=_EVENT_TIME_MS)

    crossings = 0
    accounting = threading.Lock()

    def aggregate_last(_key: bytes, value: bytes, _accumulator: bytes) -> bytes:
        """P1's host fold: keep the newest value - the same fold LAST_BYTES runs engine-side,
        so arm D differs from arms A/B/C by exactly one term, the crossing (R2/KTD16)."""
        nonlocal crossings
        with accounting:
            crossings += 1
        return value

    def emit_fold(_key: bytes, value: bytes) -> bytes:
        """P2's host-at-the-emit function: fold the collected APPEND_BYTES accumulation down to
        its length. One call per emit - the crossing count under test - and a small answer, so
        the sink does not additionally grow with the accumulation."""
        nonlocal crossings
        with accounting:
            crossings += 1
        return len(value).to_bytes(8, "big")

    sidecar = Sidecar(SidecarCommand(java, jvm_args))
    port = sidecar.start(timeout=90)
    session = StreamsSession(GrpcStreamsTransport(port))
    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    try:
        session.open(application_id, {
            "bootstrap.servers": args.bootstrap,
            "auto.offset.reset": "earliest",
            "num.stream.threads": str(args.stream_threads),
            "statestore.cache.max.bytes": str(cache_bytes),
            "commit.interval.ms": str(args.commit_interval_ms),
        })
        builder = session.builder()
        windowed = builder.windowed_by(builder.group_by_key(builder.source(source)),
                                       spec.window)
        if spec.placement == "host":
            table = builder.aggregate(windowed, aggregate_last, b"", _STORE)
            streamed = builder.to_stream(table)
        elif spec.placement == "last":
            table = builder.aggregate(windowed, store_name=_STORE,
                                      combine=CombineKind.LAST_BYTES)
            streamed = builder.to_stream(table)
        else:
            table = builder.aggregate(windowed, store_name=_STORE,
                                      combine=CombineKind.APPEND_BYTES)
            streamed = builder.map_values(builder.to_stream(table), emit_fold)
        builder.sink(streamed, sink)
        if show_topology:
            print()
            print(f"Topology, arm {arm} (window {spec.window.size_ms / 60000:.0f}m advance "
                  f"{spec.window.advance_ms / 60000:.0f}m, placement {spec.placement}):")
            for line in session.describe().text.rstrip().split("\n"):
                print(f"    {line}")
        session.start()

        quiet_s = args.quiescence_intervals * args.commit_interval_ms / 1000.0
        # A cap, never the predicate: quiescence ends every healthy run long before this.
        timeout = 180 + 2 * (spec.multiplier * records * 400e-6 + records * 2e-3)
        deadline = time.monotonic() + timeout
        with GroupWatch(admin, application_id, args.stream_threads) as watch:
            emits, window, log_append, premature = read_emits_quiescent(
                args.bootstrap, sink, quiet_s, deadline)
        group_ok, group_state = watch.verdict()
        # Read BEFORE the topics are deleted below - deleting a topic purges its group offsets.
        # By here the engine has been idle for 3x quiet_s (45 commit intervals at the defaults),
        # so a shortfall is a truncated run, never a commit still in flight.
        committed = _committed_source_records(args.bootstrap, application_id, source,
                                              args.partitions)
    finally:
        session.close()
        sidecar.close(timeout=30)
        if not args.keep_topics:
            delete_run_topics(admin, [source, sink, changelog])

    evictions: int | None = None
    if trace_cache:
        evictions = _count_evictions(engine_log)
        engine_log.unlink(missing_ok=True)

    # Post-hoc emit band (KTD11: on a broker, caching makes the emit count nondeterministic).
    # With the cache OFF every put forwards, so the band collapses to an exact count.
    if cache_bytes == 0:
        emit_band = (spec.multiplier * records, spec.multiplier * records)
    else:
        # Every touched (key, window) entry emits at least once and at most once per put.
        emit_band = (spec.multiplier * min(keys, records), spec.multiplier * records)

    expected_crossings = {
        "host": spec.multiplier * records,   # once per record per overlapping window
        "last": 0,                           # no function registered at all
        "append": emits,                     # once per emit, at the emit placement
    }[spec.placement]
    problems: list[str] = []
    if premature:
        problems.append("premature quiescence break: sink end offsets advanced during the "
                        "2x-quiet confirmation wait - the engine was stalled, not finished, so "
                        "the window is truncated and the rate would read inflated")
    if committed != records:
        problems.append(f"engine committed {committed:,} source records of {records:,} seeded "
                        "- the rate's record basis is unproven, whatever the emit band says")
    if crossings != expected_crossings:
        problems.append(f"crossings={crossings:,} expected {expected_crossings:,}")
    if not emit_band[0] <= emits <= emit_band[1]:
        problems.append(f"emits={emits:,} outside band {emit_band[0]:,}..{emit_band[1]:,}")
    if not group_ok:
        problems.append(f"group={group_state}")
    if not log_append:
        problems.append("sink not on the log-append clock")
    if evictions and not tolerate_evictions:
        problems.append(f"cache evictions={evictions:,} (the zero-evictions assertion failed: "
                        "an over-pressured cache flushes the whole dirty set, so the ratio "
                        "would read one for a reason with nothing to do with placement)")

    result = PlacementRun(arm=arm, records=records, keys=keys, multiplier=spec.multiplier,
                          crossings=crossings, emits=emits, window_s=window,
                          cache_bytes=cache_bytes, commit_ms=args.commit_interval_ms,
                          evictions=evictions, load1=load1, group_ok=group_ok,
                          group_state=group_state, log_append=log_append, emit_band=emit_band)
    verdict = "ok" if not problems else "INVALID (" + "; ".join(problems) + ")"
    print(f"  arm={arm} records={records:>7,} keys={keys:>5,} cache={cache_bytes:>11,} "
          f"window={window:7.2f}s rec/s={result.rate:8,.0f} "
          f"crossings/rec={result.crossings_per_record:6.2f} emits={emits:>9,} "
          f"evict={'-' if evictions is None else format(evictions, ',')} "
          f"load1={load1:.2f} {verdict}")
    if problems:
        raise RuntimeError(f"run invalid, stopping rather than averaging over it: {verdict}")
    return result


@dataclasses.dataclass(frozen=True)
class HostRun:
    """One arm-H run: the single-threaded reimplementation whose rate defines F2."""

    spec: str                 # "tumbling" or "hopping-12"
    records: int
    keys: int
    updates: int              # dict updates; must equal multiplier x records
    window_s: float           # wall clock, first message to last - H produces nothing to stamp

    @property
    def rate(self) -> float:
        return self.records / self.window_s if self.window_s > 0 else 0.0


def _window_starts(timestamp_ms: int, size_ms: int, advance_ms: int) -> list[int]:
    """``TimeWindows.windowsFor``'s arithmetic: every start s with s <= t < s+size, s a multiple
    of the advance, clamped at zero exactly as Kafka clamps it."""
    starts = []
    start = timestamp_ms - timestamp_ms % advance_ms
    lower = timestamp_ms - size_ms
    while start > lower:
        if start >= 0:
            starts.append(start)
        start -= advance_ms
    return starts


def measure_host(args: argparse.Namespace, topic: str, records: int, keys: int,
                 window: TimeWindow, spec_label: str) -> HostRun:
    """Arm H: consume the same input single-threaded and aggregate into a dict.

    Deliberately stateless and non-durable - no store, no changelog, no rebalance recovery, no
    late-record handling - so its rate is an UPPER bound on a real reimplementation, and every
    F2 verdict says so. It pays the multiplier too: the same ``windowsFor`` arithmetic, one dict
    update per overlapping window per record. Batch consume, because a reimplementer would.

    Runs while the engine is idle (the lab tears each sidecar down before any H run starts).
    Timed on this process's wall clock from first message to last - H produces nothing, so there
    is no log-append record of its progress; the consume loop IS the reimplementation.
    """
    load1 = wait_for_quiet(args.load_limit)
    multiplier = -(-window.size_ms // window.advance_ms)
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": f"pc-wlab-h-{time.time_ns()}",
        "enable.auto.commit": False,
    })
    state: dict[tuple[bytes, int], bytes] = {}
    updates = 0
    seen = 0
    started: float | None = None
    ended = 0.0
    try:
        consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING)
                         for p in range(args.partitions)])
        deadline = time.monotonic() + 120 + records * 1e-3
        while seen < records and time.monotonic() < deadline:
            batch = consumer.consume(num_messages=1000, timeout=1.0)
            if not batch:
                continue
            if started is None:
                started = time.monotonic()
            for message in batch:
                if message.error():
                    continue
                key = message.key()
                if key is None or not key.startswith(_KEY_PREFIX.encode()):
                    continue
                seen += 1
                value = message.value() or b""
                _kind, timestamp_ms = message.timestamp()
                for start in _window_starts(timestamp_ms, window.size_ms, window.advance_ms):
                    state[(key, start)] = value
                    updates += 1
            ended = time.monotonic()
    finally:
        consumer.close()
    window_s = (ended - started) if started is not None else 0.0
    result = HostRun(spec=spec_label, records=seen, keys=keys, updates=updates,
                     window_s=window_s)
    ok = seen == records and updates == multiplier * records
    print(f"  arm=H {spec_label:<10s} records={seen:>7,} keys={keys:>5,} "
          f"window={window_s:7.2f}s rec/s={result.rate:8,.0f} dict-updates={updates:>9,} "
          f"load1={load1:.2f} {'ok' if ok else 'INVALID'}")
    if not ok:
        raise RuntimeError(f"arm H invalid: saw {seen:,}/{records:,} records, "
                           f"{updates:,} updates against {multiplier * records:,} expected")
    return result


def _band(low: float, high: float, floor_low: float, floor_high: float) -> str:
    """F2-first 'clears' semantics: rate minus spread at or above the floor plus ITS spread.

    min/max observed stand in for rate-minus-spread and rate-plus-spread. Returns 'clears',
    'fails', or 'straddles' - a straddle routes to the resweep action, never a verdict branch.
    """
    if low >= floor_high:
        return "clears"
    if high < floor_low:
        return "fails"
    return "straddles"


def _verdict(spec_label: str, arm_label: str, rates: list[float], h_rates: list[float]) -> str:
    """One per-specification verdict, F2 evaluated FIRST (the pre-registered lattice)."""
    low, high, mean = min(rates), max(rates), statistics.mean(rates)
    h_low, h_high, h_mean = min(h_rates), max(h_rates), statistics.mean(h_rates)
    f2 = _band(low, high, h_low, h_high)
    f1 = _band(low, high, _F1, _F1)
    lines = [f"  {spec_label}: best arm {arm_label} {mean:,.0f} rec/s "
             f"(min {low:,.0f}, max {high:,.0f}, n={len(rates)})",
             f"    F2 = arm H at this specification: {h_mean:,.0f} rec/s "
             f"(min {h_low:,.0f}, max {h_high:,.0f}) - H is a NON-DURABLE single-threaded "
             "reimplementation (no store, changelog, recovery or late handling), so F2 is an "
             "upper bound on a real one",
             f"    F2 first: arm-low {low:,.0f} vs H-high {h_high:,.0f} -> {f2}",
             f"    F1 = {_F1:,.0f} rec/s: arm-low {low:,.0f} -> {f1}"]
    if f2 == "fails":
        verdict = ("BET OFF - loses on rate to the non-durable single-threaded "
                   "reimplementation; not viable across this boundary at any placement "
                   "measured here")
    elif f2 == "straddles":
        verdict = "F2 STRADDLED - resweep required before any verdict (three failures = unsettled)"
    elif f1 == "clears":
        verdict = "VIABLE - clears F2 and F1"
    elif f1 == "fails":
        verdict = "MARGINAL - clears F2, misses F1; the specification is recorded as NOT offered"
    else:
        verdict = "F1 STRADDLED - resweep required before any verdict"
    lines.append(f"    VERDICT ({spec_label}, over the current single-session transport): "
                 f"{verdict}")
    return "\n".join(lines)


def _seed_host_topic(args: argparse.Namespace, records: int, keys: int, label: str) -> str:
    topic = f"pc-wlab-h-{label}-{time.time_ns()}"
    # retention.ms=-1 is load-bearing: this topic is REUSED across reps, so it sits well past
    # the broker's 5-minute retention check while carrying 2025-dated records (see
    # measure_placement) - without it, arm H reads an empty topic mid-session.
    ensure_topic(args.bootstrap, topic, args.partitions, config={"retention.ms": "-1"})
    seed_keyed(args.bootstrap, topic, records, keys, args.payload_bytes,
               timestamp_ms=_EVENT_TIME_MS)
    return topic


def run_host_reimpl(args: argparse.Namespace) -> int:
    """U6 arm H standalone: the reimplementation floor, per window specification."""
    records = max(int(n) for n in args.crossings_sweep.split(","))
    print(f"host-reimpl (U6 arm H): {records:,} records, {args.keys:,} keys, "
          f"{args.payload_bytes} B payloads, single-threaded confluent_kafka, engine idle")
    topic = _seed_host_topic(args, records, args.keys, "shared")
    try:
        for _ in range(args.reps):
            measure_host(args, topic, records, args.keys, _TUMBLE, "tumbling")
            measure_host(args, topic, records, args.keys, _HOP5, "hopping-12")
    finally:
        if not args.keep_topics:
            delete_run_topics(AdminClient({"bootstrap.servers": args.bootstrap}), [topic])
    return 0


def _shared_phase(args: argparse.Namespace, sweep: list[int]) -> tuple[
        list[PlacementRun], list[HostRun], tuple[PlacementRun, PlacementRun] | None]:
    """Arms A, B, C, D and H at the shared load, interleaved, swept in crossings."""
    print("shared-load phase - arms A (P1 tumbling), B (P1 hop 1h/5m), C (P1 hop 1h/30m, "
          "linearity), D (crossing-free control, LAST_BYTES), H (reimplementation floor):")
    print(f"  commit.interval.ms      {args.commit_interval_ms} (set explicitly)")
    print("  statestore.cache.bytes  0 (set explicitly; every put forwards, emit counts exact)")
    print(f"  quiescence              {args.quiescence_intervals} commit intervals "
          f"({args.quiescence_intervals * args.commit_interval_ms / 1000:.1f}s) with no new "
          "sink record; each break confirmed against sink end offsets after a further 2x, "
          "and the engine group must have committed the whole seeded backlog")
    print(f"  keys                    {args.keys:,} over {args.partitions} partitions, "
          f"{args.stream_threads} stream threads, {args.payload_bytes} B payloads")
    print(f"  event time              constant {_EVENT_TIME_MS} ms for every record")
    print(f"  crossings sweep         {', '.join(f'{n:,}' for n in sweep)} x {args.reps} reps; "
          "arm records = crossings / multiplier; D (zero crossings) runs at arm A's record "
          "counts so its windows are measurable")
    print(f"  JAVA_TOOL_OPTIONS       {os.environ.get('JAVA_TOOL_OPTIONS', '(unset)')}")
    print()

    h_records = max(sweep)
    h_topic = _seed_host_topic(args, h_records, args.keys, "shared")
    runs: list[PlacementRun] = []
    h_runs: list[HostRun] = []
    scenario4: tuple[PlacementRun, PlacementRun] | None = None
    try:
        for rep in range(args.reps):
            # H first in each rep: the engine is idle here (no sidecar is up between runs), and
            # a broken H arm surfaces before the rep's engine runs instead of after them.
            h_runs.append(measure_host(args, h_topic, h_records, args.keys, _TUMBLE,
                                       "tumbling"))
            h_runs.append(measure_host(args, h_topic, h_records, args.keys, _HOP5,
                                       "hopping-12"))
            for point in sweep:
                first = rep == 0 and point == sweep[0]
                runs.append(measure_placement(args, "A", point, args.keys, 0,
                                              show_topology=first))
                runs.append(measure_placement(args, "B", point // 12, args.keys, 0,
                                              show_topology=first))
                runs.append(measure_placement(args, "C", point // 2, args.keys, 0))
                runs.append(measure_placement(args, "D", point, args.keys, 0,
                                              show_topology=first))
        # Scenario 4: arm B with caching on and off - call counts identical, emits reduced.
        # Few keys and many records per key, because dedup needs a key dirtied twice within one
        # commit interval: at the shared 8,000 keys the smallest sweep point has fewer records
        # than keys, every put is a first put, and the comparison would be vacuous.
        print("  scenario 4 (arm B, 20 keys, cache off vs 64 MB): calls must not move, emits "
              "must fall:")
        scenario4 = (measure_placement(args, "B", 480, 20, 0),
                     measure_placement(args, "B", 480, 20, 64 * 1024 * 1024))
    finally:
        if not args.keep_topics:
            delete_run_topics(AdminClient({"bootstrap.servers": args.bootstrap}), [h_topic])
    return runs, h_runs, scenario4


def _fit_and_report_shared(args: argparse.Namespace, sweep: list[int],
                           runs: list[PlacementRun], h_runs: list[HostRun],
                           scenario4: tuple[PlacementRun, PlacementRun] | None) -> None:
    print()
    print("Shared-load summary (rates in RECORDS per second - the floors' unit):")
    fitted: dict[str, float] = {}
    for arm in ("A", "B", "C", "D"):
        arm_runs = [r for r in runs if r.arm == arm]
        for records in sorted({r.records for r in arm_runs}):
            rates = [r.rate for r in arm_runs if r.records == records]
            crossings = statistics.mean(
                r.crossings_per_record for r in arm_runs if r.records == records)
            print(f"  arm {arm} records={records:>7,}: rec/s {_spread(rates)}; "
                  f"crossings/record {crossings:.2f}")
        slope, intercept = fit_slope([(r.records, r.window_s) for r in arm_runs])
        fitted[arm] = 1 / slope if slope > 0 else float("inf")
        print(f"  arm {arm} fitted {slope * 1e6:.0f}us/record "
              f"(steady-state {fitted[arm]:,.0f} rec/s), intercept {intercept:.2f}s, "
              f"n={len(arm_runs)}")
    for spec_label in ("tumbling", "hopping-12"):
        rates = [h.rate for h in h_runs if h.spec == spec_label]
        print(f"  arm H {spec_label}: rec/s {_spread(rates)} "
              f"(single-threaded, non-durable; this IS F2 for {spec_label})")

    if scenario4 is not None:
        cache_off, cache_on = scenario4
        print(f"  scenario 4 (B, 20 keys, 480 records): calls {cache_off.crossings:,} (cache "
              f"off) vs {cache_on.crossings:,} (cache on) - identical is expected; emits "
              f"{cache_off.emits:,} vs {cache_on.emits:,} - fewer is expected (caching dedups "
              "emits, never aggregator calls)")

    # Attribution: B against D, the only pair that isolates the boundary (R5).
    biggest = max(sweep)
    b_rates = [r.rate for r in runs if r.arm == "B" and r.records == biggest // 12]
    d_rates = [r.rate for r in runs if r.arm == "D" and r.records == biggest]
    ratio = statistics.mean(b_rates) / statistics.mean(d_rates)
    fitted_ratio = fitted["B"] / fitted["D"]
    if ratio > 1 / 2:
        band = "REFUTES the multiplier reaching the crossing (above 1/2)"
    elif 1 / 16 <= ratio <= 1 / 9:
        band = "CONFIRMS the multiplier reaches the crossing at P1 (within 1/16..1/9)"
    else:
        band = ("partial/anomalous - the pre-registered mechanism neither confirmed nor "
                "refuted (outside 1/16..1/9 but not above 1/2); reported as the ratio itself")
    print()
    print(f"  attribution, B against D: ratio {ratio:.4f} at the largest point "
          f"(fitted {fitted_ratio:.4f}) -> {band}")

    # Instrument check (R4): the crossing counter must read zero in D and two per record in C.
    c_runs = [r for r in runs if r.arm == "C"]
    d_runs = [r for r in runs if r.arm == "D"]
    c_ok = all(r.crossings == 2 * r.records for r in c_runs)
    d_ok = all(r.crossings == 0 for r in d_runs)
    print(f"  instrument check (R4): arm C counter reads two per record on every run: "
          f"{'PASS' if c_ok else 'FAIL'}; arm D reads zero on every run: "
          f"{'PASS' if d_ok else 'FAIL'} (D registers no function, so any engine invocation "
          "would error an unregistered token and fail the run - the zero is measured, not "
          "assumed)")

    # The fitted-multiplier deliverable: A, B, C are three points on rate-vs-multiplier.
    print()
    print("Fitted multiplier (per-record seconds against multiplier, arms A, C, B):")
    big_runs = {arm: [r for r in runs if r.arm == arm
                      and r.records == max(x.records for x in runs if x.arm == arm)]
                for arm in ("A", "C", "B")}
    per_rep: list[list[tuple[int, float]]] = [
        [(m, big_runs[arm][rep].sec_per_record) for arm, m in (("A", 1), ("C", 2), ("B", 12))]
        for rep in range(args.reps)]
    all_pts = [(m, s) for rep in per_rep for m, s in rep]
    cost_slope, cost_base = fit_slope([(m, s) for m, s in all_pts])
    print(f"  t(m) = {cost_base * 1e6:.0f}us + m x {cost_slope * 1e6:.0f}us "
          f"(fit over {len(all_pts)} points, largest sweep point, all reps)")
    for floor_name, floor, h_spec in (("F1", _F1, None),
                                      ("F2-tumbling", None, "tumbling"),
                                      ("F2-hopping-12", None, "hopping-12")):
        f = floor if floor is not None else statistics.mean(
            h.rate for h in h_runs if h.spec == h_spec)
        crossing_ms = [(1 / f - base) / slope_
                       for slope_, base in (fit_slope(rep) for rep in per_rep)]
        central = (1 / f - cost_base) / cost_slope
        print(f"  fitted rate crosses {floor_name} ({f:,.0f} rec/s) at multiplier "
              f"{central:.2f} (per-rep fits span {min(crossing_ms):.2f}.."
              f"{max(crossing_ms):.2f})")
        if h_spec is not None:
            print(f"    (F2 is arm H's own rate at {h_spec}; H pays the multiplier too, so "
                  "this crossing reads the wrapper's fit against H at that one specification)")

    # The verdicts, one per window specification, F2 first.
    print()
    print("VERDICTS (per window specification, F2 evaluated first, scoped to the current "
          "single-session transport):")
    a_biggest = [r.rate for r in runs if r.arm == "A" and r.records == biggest]
    print(_verdict("tumbling", "A (P1, host at the aggregator)", a_biggest,
                   [h.rate for h in h_runs if h.spec == "tumbling"]))
    if _band(min(a_biggest), max(a_biggest), _F1, _F1) != "clears":
        print("    NOTE: arm A misses/straddles F1, so the plan's contingent tumbling-P2 arm "
              "must be defined and run under arm E's own-load discipline before a tumbling "
              "bet-off could be declared.")
    print(_verdict("hopping-12", "B (P1, host at the aggregator, shared load)", b_rates,
                   [h.rate for h in h_runs if h.spec == "hopping-12"]))
    print("    (arm B at the shared load carries the unconditional hopping verdict; arm E's "
          "observations are scoped to E's own operating condition and reported with its sweep)")


def _emit_phase(args: argparse.Namespace) -> None:
    """Arm E at its own load, with arm B and arm H re-run at each sweep point."""
    rpk = args.emit_records_per_key
    keys_sweep = [int(k) for k in args.emit_keys_sweep.split(",")]
    accumulator_cap = rpk * args.payload_bytes
    print()
    print("emit phase - arm E (P2: APPEND_BYTES, host at the emit) at its OWN load, with arm B "
          "and arm H re-run at each point so no cross-load comparison is ever a result:")
    print(f"  records per key         {rpk} (caps the APPEND accumulator at "
          f"{accumulator_cap:,} B per entry - the plan's low-tens-of-KB bound)")
    print(f"  key sweep               {', '.join(f'{k:,}' for k in keys_sweep)}")
    print("  cache sizing            2 x keys x 12 x (records-per-key x payload) - the "
          "end-of-run accumulator bytes, doubled for entry overhead; printed per point")
    print("  eviction assertion      ThreadCache trace counters; a point with evictions FAILS "
          "the arm at that point (retried once at 4x cache)")
    print(f"  JVM heap                bounded by JAVA_TOOL_OPTIONS "
          f"{os.environ.get('JAVA_TOOL_OPTIONS', '(unset)')} - MaxRAM 48g at 20% is ~9.8 GB")
    print()

    # Instrument check first: the eviction reader must be able to show a non-zero.
    print("  eviction-instrument check (R4): an undersized cache (1 MB) must report "
          "evictions:")
    check = measure_placement(args, "E", 200 * 8, 200, 1024 * 1024, trace_cache=True,
                              tolerate_evictions=True)
    if not check.evictions:
        raise RuntimeError("the undersized-cache run reported zero evictions - the eviction "
                           "instrument cannot see the thing it asserts, so every zero it "
                           "produces is meaningless")
    print(f"    instrument PASS: {check.evictions:,} evictions seen where evictions were "
          "forced")

    results: dict[int, dict[str, list[object]]] = {
        k: {"E": [], "B": [], "H": []} for k in keys_sweep}
    for keys in keys_sweep:
        records = keys * rpk
        cache = 2 * keys * 12 * rpk * args.payload_bytes
        b_records = min(records, args.emit_b_records_cap)
        print(f"    point keys={keys:,}: cache {cache:,} B, E records {records:,}, "
              f"B records {b_records:,} (capped - B's per-record cost does not depend on "
              "records-per-key), arms E, B, H interleaved per rep")
        h_topic = _seed_host_topic(args, records, keys, f"e{keys}")
        try:
            for _rep in range(args.reps):
                results[keys]["E"].append(_run_with_cache_retry(args, "E", records, keys,
                                                                cache))
                results[keys]["B"].append(_run_with_cache_retry(args, "B", b_records, keys,
                                                                cache))
                results[keys]["H"].append(measure_host(args, h_topic, records, keys, _HOP5,
                                                       "hopping-12"))
        finally:
            if not args.keep_topics:
                delete_run_topics(AdminClient({"bootstrap.servers": args.bootstrap}),
                                  [h_topic])

    print()
    print("E ratio curve (crossings per record, E vs the MATCHED B; rho = records-per-key-per-"
          "commit-interval from the achieved rate):")
    for keys in keys_sweep:
        e_runs = [r for r in results[keys]["E"] if isinstance(r, PlacementRun)]
        b_runs = [r for r in results[keys]["B"] if isinstance(r, PlacementRun)]
        h_rates = [h.rate for h in results[keys]["H"] if isinstance(h, HostRun)]
        e_cpr = [r.crossings_per_record for r in e_runs]
        rho = [r.rate * args.commit_interval_ms / 1000.0 / keys for r in e_runs]
        predicted = [12 * min(1.0, 1.0 / r) if r > 0 else 12.0 for r in rho]
        exceeds = [r.crossings_per_record for r in e_runs
                   if any(r.crossings_per_record > b.crossings_per_record for b in b_runs)]
        print(f"  keys={keys:>5,}: E crossings/record {_spread_f(e_cpr)}; B's 12.00 exact; "
              f"rho {_spread_f(rho)} -> predicted 12/max(rho,1) = "
              f"{statistics.mean(predicted):.2f}")
        print(f"    rates at this condition: E {_spread([r.rate for r in e_runs])} rec/s, "
              f"B {_spread([r.rate for r in b_runs])} rec/s, H {_spread(h_rates)} rec/s; "
              f"E evictions {[r.evictions for r in e_runs]}")
        if exceeds:
            print("    PREDICTION 7 VIOLATED: E exceeded B's crossings per record - treat as "
                  "an instrument fault and investigate before believing anything above")
    print("  (E-carried observations are scoped to these operating conditions - key count, "
        "rate, commit interval, records-per-key-per-flush - and never to F1's unstated "
        "cardinality)")


def _run_with_cache_retry(args: argparse.Namespace, arm: str, records: int, keys: int,
                          cache: int) -> PlacementRun:
    """The plan's eviction consequence: a point with evictions fails the ARM at that point -
    rerun with a bigger cache rather than reporting its ratio."""
    try:
        return measure_placement(args, arm, records, keys, cache, trace_cache=True)
    except RuntimeError as failed:
        if "evictions" not in str(failed):
            raise
        print(f"    point failed its zero-evictions assertion; retrying once at 4x cache "
              f"({4 * cache:,} B)")
        return measure_placement(args, arm, records, keys, 4 * cache, trace_cache=True)


def run_placement(args: argparse.Namespace) -> int:
    """U6: the decisive placement comparison."""
    sweep = sorted(int(n) for n in args.crossings_sweep.split(","))
    below = [n for n in sweep if n < 32_000]
    if below:
        raise SystemExit(f"sweep points {below} are below the 32,000-crossing warm-up line")
    print("placement experiment (plan U6) - the decisive arms; rates in RECORDS per second")
    if args.phase in ("all", "shared"):
        runs, h_runs, scenario4 = _shared_phase(args, sweep)
        _fit_and_report_shared(args, sweep, runs, h_runs, scenario4)
    if args.phase in ("all", "emit"):
        _emit_phase(args)
    return 0


EXPERIMENTS = {
    "hot-key": run_hot_key,
    "placement": run_placement,
    "host-reimpl": run_host_reimpl,
}


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_args(argv)
    if not args.bootstrap:
        raise SystemExit("--bootstrap is required (or PC_DEMO_BOOTSTRAP; demo/run.sh shows how "
                         "the compose broker starts)")
    return EXPERIMENTS[args.experiment](args)


if __name__ == "__main__":
    sys.exit(main())
