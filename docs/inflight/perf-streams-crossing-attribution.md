# The Streams round trip attributed: the crossing is ~150us and essentially all of the marginal cost

<!-- inflight-type: register -->


The Kafka Streams PoC on `research/kafka-streams-foreign-wrappers` (PR astubbs#334, umbrella
astubbs#242) published a per-invocation round trip of roughly 400-450us and said, correctly, that
it was a total nobody had decomposed into (a) the boundary crossing versus (b) Kafka Streams' and
Kafka's own per-record work. This note records the experiment that decomposed it. The demo flag it
added, `--no-transform`, and the sink-window instrument are documented in the Python demo's
[`README.md`](../../parallel-consumer-proxy-clients/parallel-consumer-proxy-client-python/demo/README.md),
section "The control arm".

## The prediction, stated before any run

Written down before the first run, verbatim in intent: the round trip is dominated by the
crossing, not by engine work. Quantified: the control arm would show engine-side per-record cost
in the low tens of microseconds, making the crossing's share roughly 90% or more. After a
shakedown run refuted the first instrument (below), the prediction was restated in slope terms
before the measurement matrix ran: control-arm slope in the tens of microseconds per record,
treatment slope several hundred, share 85% or more.

## Method, and why the control is valid

Two arms of the same demo (`streams_demo.py`), same machine, same broker, same seeded records,
same partitions (4), same key space (1000), same `num.stream.threads=1`, same properties,
at-least-once, state store cache off, `commit.interval.ms=200`:

- **Arm A (control):** source -> groupByKey -> count -> sink. The `mapValues` node is left out,
  so nothing ever crosses the boundary. Zero invocations, confirmed per run by the demo printing
  `Python invocations 0`.
- **Arm B (treatment):** the existing source -> mapValues -> groupByKey -> count -> sink.

One term changes: whether the `mapValues` node exists. `mapValues` preserves the key, so removing
it cannot move a repartition or change the grouping. Verified on every run's printed
`Topology.describe()` output: both arms are one sub-topology with the same store and no
repartition topic; they differ by exactly the `KSTREAM-MAPVALUES` node.

**Instrument:** both arms report a sink window measured on the broker's log-append clock (the
sink topic is created with `message.timestamp.type=LogAppendTime`): first count's append to the
last's. This clock exists in both arms, is stamped by the broker as the engine produces, and is
immune to how fast the verifier reads. In arm B it is cross-checked against the demo's
independent invocation-window clock; the two agreed within a few percent on every run (at
N=64000: 158 vs 159us per record; large-N slopes 146 vs 144us).

**The instrument correction the shakedown forced:** a window at ONE record count cannot attribute
anything. Both arms carry a fixed component (task start ramp, poll cadence, producer linger,
commit interval, JIT warm-up) of roughly 0.5-0.9s, and at the demo's default 2000 records that
fixed part dominates. The sound comparison is the slope of window against record count, which
cancels the fixed part. The per-record cost below is that slope.

## Runs and raw numbers

Seventeen runs per arm, all on 2026-08-24: one shakedown at N=500 plus a matrix over
N = {2000, 4000, 8000, 16000, 32000, 64000}, three reps at most sizes, arms interleaved B,A
within each rep so shared load drift lands on both alike. Every one of the 34 runs exited 0 with
every key's count exactly right, and the engine's consumer group read STABLE/1 on every sample
after joining in all of them - no rebalances contaminate any window.

Conditions, honestly: one Apple Silicon macOS developer machine, broker in Docker (compose,
loopback), engine on Temurin JDK 17.0.18, client on Python 3.14, gRPC over loopback, and
**another agent session was active on the same machine throughout** - absolute numbers carry that
load; the A/B difference and the interleaving are what the conclusion rests on.

Sink windows (seconds, per rep):

| N | arm B | arm A |
|---|---|---|
| 2000 | 0.89, 0.95, 0.76 | 0.51, 0.50, 0.50 |
| 4000 | 1.34, 1.37, 0.90 | 0.56, 0.65, 0.57 |
| 8000 | 2.01, 1.66, 2.11 | 0.70, 0.71, 0.70 |
| 16000 | 2.94, 3.05, 2.94 | 0.78, 0.77, 0.80 |
| 32000 | 5.60, 5.47, 5.46 | 0.98, 0.94, 0.91 |
| 64000 | 10.12 | 0.57 |

Least-squares slope over N >= 8000 (ten points per arm):

- **Arm B: 148us per record** (pairwise large-N slopes span roughly 141-159us; intercept 0.70s)
- **Arm A: -0.3us per record**, statistically zero (windows are flat-to-noisy, 0.57-0.98s across
  an 8x range of N; the largest positive pairwise slope seen was ~13us; intercept 0.79s)

Per-update figures at the demo's default N=2000, for continuity with the published number:
arm B 382-474us (the PoC's 400-450us reproduces), arm A 250-257us.

## Attribution

- **The boundary crossing costs roughly 140-160us per record at steady state, and it is
  essentially all of the marginal per-record cost** - arm A's engine-plus-Kafka share is at most
  ~15us per record and indistinguishable from zero at this instrument's resolution.
- **The published 400-450us total is real at the demo's default scale but is not the crossing's
  price.** At N=2000, an arm with no crossing at all shows ~250us per record of the same total,
  all of it fixed warm-up and cadence amortised over too few records. The steady-state
  single-thread ceiling is therefore ~6,500-7,000 invocations/sec on this machine, not the
  ~2,400 the PoC's window implied.

## What it means for C transport versus batching

The question this experiment existed to answer: if (b) had dominated, the shared C transport
would buy little. **It does not dominate - the crossing does, overwhelmingly - so attacking the
crossing remains the right target, by either lever or both.** Two corrections to how that work
should be costed:

- The prize per record is ~150us, not ~450us. The C transport plan's kill-criterion arithmetic
  (see [`../plans/2026-08-22-001-feat-shared-c-transport-plan.md`](../plans/2026-08-22-001-feat-shared-c-transport-plan.md))
  should use the smaller number: an FFI hop that lands at, say, 20us saves ~130us per record, not
  ~430us.
- Batching amortises the same ~150us, so its orders-of-magnitude argument survives unchanged -
  100 records per hop turns ~150us into ~1.5us plus the per-record engine cost, which this
  experiment measured as noise. The two levers still compose, as
  [`../language-bindings.md`](../language-bindings.md) argues.

## What refuted the prediction, reported as prominently as what held

- **The direction held**: crossing share >= 90% at steady state, as predicted.
- **The first instrument was refuted by its own shakedown.** At a single N=500, arm A read
  1000us per update against arm B's 1265us, which would have said the boundary was a mere ~20%
  of the total - a confident, wrong attribution that a single-run design would have published.
  The fixed-cadence component produces exactly that signature at small N.
- **"Low tens of microseconds" for the engine held only as a slope.** Arm A's small-N per-update
  figures (250us at N=2000) are dominated by the same fixed component; anyone quoting a
  per-record engine cost from a single small run will overstate it by an order of magnitude.
- **Unpredicted finding:** arm B's per-invocation cost falls with run length (471 -> 354 -> 259
  -> 190 -> 176 -> 159us across the N sweep) and had not converged until tens of thousands of
  invocations - JVM and gRPC warm-up on the crossing path. Any future crossing benchmark needs a
  warm-up discard or a slope, or it measures the ramp.

## Reproduction

`demo/run.sh --streams --native` for arm B, plus `--no-transform` for arm A, `--records <N>`
sweeping N; or `streams_demo.py` directly against any broker. Both arms print the sink window and
per-update figure; compare slopes across N, not single runs. 34/34 runs verified exact counts
under the conditions above; no failed, corrupt, or rebalanced run was observed at any N.
