# The harness measures throughput only, and latency is the number that matters

<!-- inflight-type: next -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement -->

**Antony, 2026-08-22: "we've only been looking at throughput. our harness needs to include latency
measurements."** Correct, and there is a reason it does not, which has to be fixed before any latency
figure is trustworthy.

## Why latency cannot simply be added

**`bench/run-bisect.sh` produces the whole dataset ONCE, before any arm runs.** That is deliberate and
it is what makes the comparison fair - every arm re-reads the same bytes, and container startup and
several hundred thousand produces are outside the measured window.

But it means **every record has been waiting since before the run began**, so "arrival time" is the
same instant for all of them and end-to-end latency is not defined. Any per-record figure computed
this way measures queue position, not delay: record 40,000 "waited" longer than record 1 for reasons
that have nothing to do with the engine.

**So a latency column added to the current harness would be a plausible-looking number that means
nothing** - which is worse than not having one, and is the same failure mode as the blocking-callee
rows that nearly went out as an engine comparison.

## What it needs instead

**Produce DURING the run, at a configured arrival rate.** Then each record has a real arrival time,
`ConsumerRecord.timestamp()` carries it, and latency is `completion - timestamp` - genuinely
end-to-end, including every queue it sat in.

- `BENCH_ARRIVAL_RATE` - records per second fed in during the measured window.
- Report **p50, p99, p99.9 and max**, not a mean. The mean is what a serial engine hides behind.
- `HdrHistogram` is already on the harness classpath, so the recording is free.

**Below saturation is the whole point.** A latency measurement at 100% utilisation measures the
backlog; at 60-70% of measured throughput it measures the engine. Arrival rate must be a swept axis,
not a constant.

## And PC can model the serial engine itself - no Streams arm needed

Discovered by accident on 2026-08-22 when `PARTITION` ordering timed out: **`PARTITION` ordering on a
single partition is one record in flight at a time**, which is exactly Kafka Streams' execution model -
one thread per task, strictly serial within a partition.

So the head-of-line experiment needs no Streams integration at all:

| Arm | Models |
|---|---|
| `PARTITION` ordering, N partitions | Kafka Streams - serial per partition |
| `KEY` ordering, same N partitions | PC - serial per key, concurrent across keys |

**Same engine, same broker, same records, same handler.** The only variable is the ordering mode, and
the gap between those two rows *is* the cost of head-of-line blocking. Run it with the tailed work
model below and it is the strongest single measurement this project could produce.

## The work model this needs, which now exists

`bench/Bench.java.template` gained a configurable work model on 2026-08-22, off by default so every
earlier figure still means what it meant:

- `BENCH_DELAY_STDDEV` - normal distribution around the delay
- `BENCH_DELAY_P99` - one record in a hundred takes this long instead. The blunt, honest model of the
  thing that actually hurts
- `BENCH_FAILURE_RATE` - fraction of invocations that throw, after doing the work. PC retries them; a
  serial engine stalls its partition for the retry too
- `BENCH_WORK_SEED` - deterministic, so every arm in a sweep sees the same durations and the same
  failing offsets. A comparison across arms is otherwise merely similar rather than fair

**A constant delay is the best possible case for a serial engine** - with no variance there is no head
of line to block. Every figure this project has taken so far uses one.

