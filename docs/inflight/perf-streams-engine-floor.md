# The engine floor: where the microseconds go when NOTHING crosses the boundary

<!-- inflight-type: register -->

The pre-registration and results record for the engine-floor spike. **Everything above the Results
line was written and committed before any arm ran** - the discipline inherited whole from
[`perf-streams-windowing-multiplier.md`](perf-streams-windowing-multiplier.md) (U6) and
[`perf-crossing-cost-ladder.md`](perf-crossing-cost-ladder.md): predictions in the tree before runs,
one term toggled per arm, an instrument check that can move, the broker's log-append clock, and the
record basis proven per run rather than assumed.

## The question

Two spikes converged on the same next question
([`core-compiled-function-seam-design.md`](core-compiled-function-seam-design.md), "Reassessment,
2026-08-25"): with the crossing removed, Kafka Streams still costs **~50us/record** (U6 arm D, this
class of box), **~132us/record** (spike A's control) and **~250us/record** (spike B, embedded), and
the reimplementation floor is **89k-723k rec/s** (arm H). The crossing is solved; this floor is the
whole remaining 4.5x-36x term. **Is it Kafka-Streams-intrinsic, or is it configuration and harness?**

Three candidate explanations are on the table before any arm runs, and the first is the reason this
spike exists at all:

1. **U6 ran the cache OFF** (`statestore.cache.max.bytes=0`, stated in U6's conditions and chosen
   there so emit counts would be exact). Cache off makes **every** put forward downstream, so arm D
   produced 12 changelog writes and 12 sink records per input record. That is a measurement choice,
   not a property of Kafka Streams.
2. **U6 ran `commit.interval.ms=200`** - 25x under the Kafka Streams default of 5,000ms - which
   forces a producer flush and an offset commit five times a second per thread.
3. **Arm D is a hopping-1h/5m topology, multiplier 12.** Its 50us/record is *twelve* window updates,
   so the per-(record x window) figure is ~4.2us and the per-record floor at multiplier 1 is a
   different number nobody has measured crossing-free.

## The unit that matters, stated before the numbers

**us per RECORD is the floors' unit** (U6's R6), but the budget below is built in **us per
(record x window)** wherever a term scales with the multiplier, because arm D's headline conflates
the two. A term that is once-per-record (poll, fetch, deserialise, source-offset bookkeeping) and a
term that is once-per-window-update (store put, changelog write, downstream forward, sink produce)
are different products, and the 4.5x-36x gap is only interpretable once they are separated.

## Pre-registered budget: where I think the 50us/record goes

Honest guesses, written before any arm ran, for U6 arm D's shape (hopping 1h/5m, multiplier 12,
cache off, `commit.interval.ms=200`, 8 threads, 8 partitions, 1 KB values, in-memory window store,
sink through `to_stream`).

| Term | Scales with | Predicted us/record (of ~50) | Reasoning |
|---|---|---|---|
| Sink produce | multiplier | **~16** (1.3 per update) | 12 x 1 KB records/record produced, serialised, batched, acked |
| Changelog produce | multiplier | **~16** (1.3 per update) | same volume again - the changelog is a second full copy of every forward |
| Window store put + forward | multiplier | **~10** (0.8 per update) | in-memory store, byte[] serde, `KTableSuppress`-free forward down the processor chain |
| Consumer poll / fetch / deserialise | record | **~4** | one 1 KB record decoded, `StreamTask` bookkeeping, punctuation checks |
| Commit (offsets + producer flush) | wall clock | **~3** | 5/s/thread x 8 threads, each flushing a partly-full producer batch |
| Cache | - | **0 as run** | it is OFF; the prediction is that turning it ON removes a large slice of the two produce terms |
| Demo/harness artefacts | - | **~1** | the verifier consuming 1.5M sink records on the same box |

**The load-bearing structural prediction: roughly two thirds of arm D's 50us is produce volume that
the cache-off choice manufactured, and roughly 42 of the 50us scales with the multiplier rather than
with the record.**

## Pre-registered predictions

| # | Prediction | Predicted effect | Why |
|---|---|---|---|
| 1 | Baseline replica of U6 arm D reproduces its rate on this box within a factor of ~1.5 | 13k-30k rec/s | same box class, same lab, ambient load differs |
| 2 | **Cache generous (64 MB) vs 0 is the single biggest toggle** | **>= 2x rate** | it collapses 12 forwards/record into ~one per (key,window) per flush, deleting most of BOTH produce terms |
| 3 | Changelog disabled vs enabled | **~25-30% faster** (12-16us/rec) | removes one of the two full-volume produce paths |
| 4 | `commit.interval.ms` 5,000 vs 200 | **~5-12% faster** | fewer flushes, fuller producer batches; real but second-order |
| 5 | No-sink vs sink | **~25-35% faster** (12-18us/rec) | removes the other full-volume produce path |
| 6 | `num.stream.threads` 1 vs 8 | per-thread rate within ~1.3x of 1/8th of the 8-thread rate | the engine floor is per-thread work, not a shared serialised resource - the crossing was the serialised thing, and it is gone |
| 7 | **Crossing-free TUMBLING (multiplier 1) is 5-15us/record, i.e. 65k-200k rec/s** | ~8-10x the hopping-12 rate, not 12x | most of arm D's cost scales with the multiplier; U6 arm A's 137us/rec minus its ~135us crossing leaves single-digit us for the engine at multiplier 1 |
| 8 | Instrument check: +100us/record injected through the wire path moves the per-record figure by 80-130us | delta ~= injected | anything else means the harness is not measuring what it claims |

**Prediction 7 is the one that would change the strategic reading**, and it is registered here
precisely because it is the uncomfortable one: if it holds, U6's "50us engine floor" is mostly the
*window multiplier*, not Kafka Streams' per-record economics, and the 4.5x-36x gap is
specification-dependent rather than intrinsic.

## Arms (each toggles exactly ONE term against the baseline)

All arms are **crossing-free**: `combine=LAST_BYTES` engine-side, **no host function registered at
all**, so the zero crossings are a measurement (an engine-side invocation would name an unregistered
token and fail the run) rather than an assumption - U6's rule, inherited.

| Arm | Toggle against baseline | Baseline value |
|---|---|---|
| **D0** baseline | none - U6 arm D's shape | hop 1h/5m, cache 0, commit 200ms, sink on, changelog on, 8 threads |
| **D-cache** | `statestore.cache.max.bytes` 0 -> 64 MB | |
| **D-nolog** | changelog on -> off (`withLoggingDisabled`, env-gated) | |
| **D-commit** | `commit.interval.ms` 200 -> 5,000 | |
| **D-nosink** | sink on -> off (no `sink()` call; the store is the terminus) | |
| **D-t1** | `num.stream.threads` 8 -> 1 | |
| **T0** tumbling | window hop 1h/5m -> tumbling 1h (multiplier 12 -> 1) | |
| **I0 / I100** instrument check | host function on the wire path, delay 0 vs 0.1ms | tumbling, so the delta is per-record not per-window |

**Clocks.** Sink-bearing arms use the sink topic's **broker log-append clock** and quiescence, as
U6 did. `D-nosink` has no sink, so it and a matched sink-bearing companion are BOTH measured on a
second clock - the engine group's **committed source offsets**, sampled to completion - so the
no-sink comparison is never made across two different clocks. Every sink-bearing arm reports both
clocks, which is what makes the second one believable.

**Method, inherited and binding.** 5,000+ records per arm (the runs below use far more), 3 reps,
arms interleaved within a rep so machine drift lands on all of them, 1-minute load recorded beside
every run, the engine's committed source offsets required to cover the whole seeded backlog before
a rate is believed, and a quiescence break confirmed against advancing sink end offsets before it is
trusted. Rates are reported as median and range over reps, never a single number.

**The env-gated changelog toggle is measurement-only.** `PC_STREAMS_MEASURE_DISABLE_CHANGELOG` in
`TopologyAssembler` (`windowedAggregate`) is off by default and is not on the protocol; a real
capability would be an additive field on `Aggregate`, which this spike deliberately does not add.

---

## Results

Appended as the arms run, each beside the prediction it confirms or refutes. Nothing above this line
changes after the first run; corrections land here as dated entries.

### The decomposition, measured 2026-08-25

Harness: `streams_windowing_lab.py`, experiment `engine-floor` (arms `D0`, `D-cache`, `D-nolog`,
`D-commit`, `D-nosink`, `D-t1`, `T0`, `T0-cache`; instrument pair `I0`/`I100`/`I1000`). Engine on
Temurin 17.0.20+8 under the box's ambient `JAVA_TOOL_OPTIONS` (`MaxRAM=48g`, `MaxRAMPercentage=20`,
`ActiveProcessorCount=8`), compose broker `confluentinc/cp-kafka:7.9.0` on loopback
(`127.0.0.1:19097`), Python 3.13.5, 32-core Linux box, 1 KB payloads, 8 partitions, in-memory window
store, constant event time past the epoch clamp. Every arm crossing-free unless its row says
otherwise, with the zero crossings measured client-side rather than assumed.

**Deviations from the pre-registration, named rather than glossed:**

- **1,000 keys, not U6's 8,000.** At 8,000 keys the hopping working set is 8,000 x 12 x ~1 KB = ~96 MB
  and a 64 MB cache would have measured eviction thrash rather than caching - the cache arm would
  have answered a different question. Every arm shares the key count, so the comparisons hold.
- **64,000 records per arm, flat, no sweep.** The arms are crossing-free, so there is no invocation
  count to normalise on; the term under test is the record.
- **Quiet-machine gate raised to 1-minute load 40** - the box carried ambient 1-20 from other agent
  sessions throughout. Per-run load is recorded beside every run (0.96-30.29). Arms are interleaved
  within each rep, so drift lands on all of them; absolute rates are biased low.
- **3 reps for the pre-registered arms; 2 for `T0-cache` and `I1000`**, both added after the first
  sweep in response to what it showed (stated below, with why).
- **`D-commit`'s committed-offset clock is void by construction** at a 5,000 ms commit interval - its
  first sample already sees the whole backlog and it reads 630k rec/s. Its sink clock is the figure,
  as the pre-registration required.

#### Per-arm table

Medians over reps, min-max beside; rates in RECORDS per second on the sink's broker log-append clock
except `D-nosink`, which has no sink and is read on the committed-source-offset clock **against
`D0`'s figure on that same clock** (16,739 rec/s, 59.8 us/rec - within 0.2 percent of `D0`'s sink
clock, which is what makes the second clock believable).

| Arm | One term moved | rec/s (min-max) | us/rec | us per (rec x window) | emits | vs D0 |
|---|---|---|---|---|---|---|
| **D0** | - (U6 arm D's shape) | 16,758 (15,960-17,544) | **59.7** | 5.0 | 768,000 | 1.00x |
| **D-cache** | cache 0 -> 64 MB | 67,797 (54,514-67,941) | **14.8** | 1.2 | 45,432 | **4.05x** |
| **D-nolog** | changelog on -> off | 20,215 (17,582-20,460) | 49.5 | 4.1 | 768,000 | 1.21x |
| **D-commit** | commit 200 -> 5,000 ms | 18,064 (15,733-19,698) | 55.4 | 4.6 | 768,000 | 1.08x |
| **D-nosink** | sink on -> off | 27,779 (18,290-29,137) | 36.0 | 3.0 | 0 | **1.66x** |
| **D-t1** | threads 8 -> 1 | 11,709 (9,357-13,101) | 85.4 | 7.1 | 768,000 | 0.70x |
| **T0** | hopping-12 -> tumbling (multiplier 12 -> 1) | 81,946 (81,841-90,652) | **12.2** | 12.2 | 64,000 | **4.89x** |
| **T0-cache** | tumbling AND cache 64 MB | 194,927 (190,476-199,377) | **5.1** | 5.1 | 2,000 | **11.63x** |
| I0 | instrument control: + one host crossing/record | 5,392-6,589 | 151.8-185.5 | - | 64,000 | 0.39x |
| I100 | I0 + 0.1 ms/record injected host-side | 5,399 (4,742-7,144) | 185.2 | - | 64,000 | - |
| I1000 | I0 + 1 ms/record injected host-side | 4,137 (3,580-4,694) | 241.7 | - | 64,000 | - |

#### The budget: which term owns how many microseconds

**The multiplier split, fitted from the one pair that moves only the window specification.** With
`us/rec = F + m x P`, `T0` (m = 1, 12.2 us) and `D0` (m = 12, 59.7 us) give

> **P = 4.3 us per (record x window-update), F = 7.9 us per record.**

So of U6 arm D's headline, **51.8 us of 59.7 (87 percent) is the multiplier**, and only 7.9 us is
once-per-record engine cost. The "50 us engine floor" was mostly twelve of something small.

**Where the per-window-update 4.3 us goes**, read off the single-term toggles against `D0`:

| Term | Predicted (of ~50) | **Measured** (of 59.7 us/rec) | Share |
|---|---|---|---|
| Sink produce | ~16 us | **23.8 us** (`D-nosink`, matched clock) | 40% |
| Changelog produce | ~16 us | **10.2 us** (`D-nolog`) | 17% |
| Commit (offsets + producer flush) | ~3 us | **4.3 us** (`D-commit`) | 7% |
| Consumer poll / fetch / deserialise + store put + forward | ~14 us | **~21 us** (the residual, and it is where the fixed 7.9 us/record sits) | 36% |
| **Cache, as a single toggle** | 0 as run; "large slice" of the produce terms | **45.0 us** (`D-cache`) - it subsumes sink, changelog and store-forward work at once by removing **94 percent of the emits** (768,000 -> 45,432) | **75%** |
| Demo/harness artefacts | ~1 us | not separately measured; the verifier's own consume runs on the same box and is inside every arm equally | - |

The terms are not additive: sink 23.8 + changelog 10.2 + commit 4.3 = 38.3 us, while the cache toggle
alone buys 45.0 us, because a cached put never reaches either produce path *or* the downstream
processor chain.

**The volume reading, which is the finding underneath all of the above.** `D0` writes 768,000 sink
records plus 768,000 changelog records of ~1 KB each - about **1.5 GB of broker writes per 64,000
input records**, in 3.8 s, or **~400 MB/s** into a single-container loopback broker. `T0-cache`
writes 2 MB for the same 64,000 records. The two arms differ by a factor of 750 in bytes produced and
a factor of 12 in rate.

#### The JFR capture (baseline arm, one run)

async-profiler is not on this box; JFR ships with the JDK, so the capture is JFR execution samples
(`settings=profile`), one `D0` run, 1,234 samples of which **1,219 are on `StreamThread` threads**.
Categories overlap by construction (a produce sample is reached *through* the processor chain), so
these are containment shares, not a partition: **71.6 percent of stream-thread samples carry a
producer frame** (`RecordCollectorImpl` / `KafkaProducer` / `RecordAccumulator`), 35.1 percent a
processor-chain frame, 21.2 percent a window-store frame, 3.7 percent a consumer fetch frame. The top
leaf frames are `Bytes$LexicographicByteArrayComparator.compare`, `HashMap.getNode`,
`ByteUtils.writeVarint`, `KafkaProducer$AppendCallbacks.onCompletion` and the metrics `Sensor` path -
serialising and accounting for records on the way out. The profile and the toggles agree: **the
produce path dominates.**

#### Predictions, confirmed and refuted

| # | Prediction | Outcome |
|---|---|---|
| 1 | Baseline reproduces U6 arm D within ~1.5x | **confirmed** - 16,758 rec/s here vs 20,062 fitted there, 1.20x, on a box carrying more ambient load |
| 2 | Cache is the single biggest toggle, >= 2x | **confirmed, and by more than predicted** - 4.05x, the largest single term by a factor of three over the next one |
| 3 | Changelog disabled ~25-30% faster | **refuted, low** - +21% (10.2 us of 59.7). Direction right, magnitude below the band: the changelog write is cheaper than the sink write on the same volume |
| 4 | `commit.interval.ms` 5,000 vs 200 ~5-12% faster | **confirmed** - +7.8% |
| 5 | No-sink ~25-35% faster | **refuted, high** - +66% on the matched clock. The sink produce path is the largest *individually removable* term |
| 6 | Per-thread rate within ~1.3x of 1/8th of the 8-thread rate | **REFUTED, badly, and it reframes the floor** - one thread delivers 11,709 rec/s where eight deliver 16,758. Eight threads buy **1.43x**, not 8x; per thread, one thread is **5.6x** more efficient than each of eight. The crossing-free engine does not scale with threads here |
| 7 | Crossing-free tumbling is 5-15 us/record, 65k-200k rec/s | **confirmed, mid-band** - 12.2 us/record, 81,946 rec/s |
| 8 | +100 us/record injected moves the per-record figure by 80-130 us | **REFUTED** - +33 us at 0.1 ms and +56 us at 1 ms (both medians). Mechanism established below; a valid substitute check is recorded in its place |

#### The instrument check, refuted as designed - and what replaced it

**The host-side injection does not move the figure by what it injects, and the reason is
structural rather than instrumental.** The Python client dispatches invocations onto a thread pool,
so a per-record sleep on the host side is absorbed by that concurrency whenever the keys are spread
across partitions: at 1 ms injected the figure moved 56 us, roughly the injected delay divided by the
pool width. U2's version of this check moved 1,091 us against 1,000 us injected precisely because
**its arm A ran a single key** - one partition, one stream thread, a strictly serial chain with no
concurrency to hide in. Recorded as a property of the harness: *the demo's wire path cannot inject a
calibrated per-record cost unless the arm is single-key.*

**The check that does hold, and it is the stronger one because its magnitude is known
independently:** `T0` and `I0` are the same topology differing by exactly one term - a registered
host function, one crossing per record. The per-record figure moves **12.2 us -> 151.8-185.5 us, a
delta of 140-173 us**, against U6's independently fitted **135 us per crossing** from a different
session, a different arm family and a different sweep. The instrument moves, on the exact quantity
under test, by a constant nobody tuned it to hit.

#### Contextualised against the two floors

| Figure | rec/s | us/rec | Source |
|---|---|---|---|
| U6 arm D (crossing-free, cache off, hopping-12) | 20,062 | 50 | `perf-streams-windowing-multiplier.md` |
| **D0 here** (same shape, busier box) | 16,758 | 59.7 | this note |
| **Best crossing-free hopping-12** (`D-cache`) | **67,797** | 14.8 | this note |
| **Best crossing-free tumbling** (`T0-cache`) | **194,927** | 5.1 | this note |
| Arm H, reimplementation floor, hopping-12 | 89,821 | 11.1 | U6 |
| Arm H, reimplementation floor, tumbling | 723,265 | 1.4 | U6 |

**What the wrapper's best case becomes when the biggest toggled term is eliminated** - that is, when
the cache is simply left at a sane value instead of the zero the measurement chose:

- **hopping-12: the gap to the reimplementation floor falls from 4.5x to 1.32x** (67,797 vs 89,821);
- **tumbling: from 36x to 3.7x** (194,927 vs 723,265).

Both against a floor that is explicitly stateless and non-durable - no store, no changelog, no
rebalance recovery, no late-record handling - so at hopping-12 a crossing-free wrapper is inside
noise of a reimplementation that gives all of that up.

#### Strategic reading: configuration and harness, not Kafka Streams

**The floor is not Kafka-Streams-intrinsic.** Three arms say so independently and they agree:
87 percent of U6 arm D's 59.7 us/record is the window multiplier rather than per-record engine cost
(`T0`); 75 percent of it is deleted by turning the state-store cache on (`D-cache`), a setting U6 set
to zero for a measurement reason - exact emit counts - and not for a product reason; and 71.6 percent
of profiled stream-thread samples sit in the produce path, writing the ~1.5 GB per 64,000 records
that the cache-off choice manufactures. The refuted thread-scaling prediction closes the argument:
eight threads buy 1.43x, which is what a **shared write path** looks like, not what per-record CPU
looks like. **So the 4.5x-36x that the compiled-function design named as untouchable engine cost is
mostly a measurement artefact plus a window specification** - with the cache on, hopping-12 lands at
1.32x of the non-durable reimplementation floor and tumbling at 3.7x.

**Against `STRATEGY.md`'s reopening condition**, which reads *"a transport that cuts the per-crossing
cost by roughly two orders of magnitude - the embedded/FFI direction"*: both fast-path spikes met it
(GraalWasm 747 ns staged; the embedded engine deleting gRPC's ~165 us outright), and this spike
removes the reason to believe the engine floor blocks the consequence. The windowed-aggregation
verdict was taken as *"the fitted cost model puts that floor out of reach at any window multiplier"*;
with the crossing gone and the cache on, the wrapper is within 1.32x at hopping-12. That is a
falsification of the strategy text's live claim, not a caveat to it, and the file's own rule - work
that falsifies a claim must update it - now applies. **What is NOT settled and must not be smuggled
in:** the cache-on arms have different emit semantics (94-97 percent of emits are deduplicated by the
flush), so a specification that genuinely needs every intermediate update is not covered by this
result; the F2 comparison against arm H has not been re-run in-session at cache-on; and the whole
decomposition ran on one 32-core box against a single-container broker whose write bandwidth is
visibly the binding constraint on the cache-off arms.

**Next, in order:** re-run U6's decisive placement arms with the cache on and arm H interleaved, so
the F2 verdict is retaken in-session rather than inferred across notes; then decide whether
`STRATEGY.md`'s windowed-aggregation paragraph is rewritten or annotated.

### The F2 comparison, retaken in-session 2026-08-25

The section above closed by naming this run as the next thing to do, and it was right to: its F2
reading paired **this note's** cache-on arms against **U6's** arm-H figures, measured in a different
session. The project's pre-registered discipline forbids exactly that - the authoritative baseline
is the control arm measured in the same session as its treatment arm, never a cited constant (the
plan's KTD18) - so the 1.32x and 3.7x above were inferred across notes rather than measured. This
section retakes them with arm H and the cache-on arms in one session, interleaved within each
repetition. **It does not confirm them.**

Harness: `streams_windowing_lab.py`, new experiment `f2-rerun` (`run_f2_rerun`), which reuses the
engine-floor arms through `_run_floor_arm` rather than restating their toggles. Arms within a
repetition, in order: arm H tumbling, arm H hopping-12, `T0-cache`, `D-cache`, `D0`, `T0`, then the
instrument pair `I0`/`I1000` (`_F2_ENGINE_ORDER`). Arm H goes first while no sidecar is up, the rule
inherited from `_shared_phase`.

**Conditions.** Engine on Temurin 17.0.20+8 (resolved through `mise`) under the box's ambient
`JAVA_TOOL_OPTIONS` (`MaxRAM=48g`, `MaxRAMPercentage=20`, `ActiveProcessorCount=8`), Kafka Streams
3.9.2, compose broker `confluentinc/cp-kafka:7.9.0` on loopback `127.0.0.1:19098` (compose project
`pc-f2rerun`, started and torn down by this run alone), Python 3.13.5 with `confluent-kafka` 2.15.0,
32-core Linux box. 1 KB payloads, 8 partitions, 8 stream threads, `commit.interval.ms` 200,
in-memory window store, constant event time past the epoch clamp, **1,000 keys**, **64,000 records
per arm**, quiescence at 15 commit intervals with each break confirmed against sink end offsets after
a further 2x, and the engine group required to have committed the whole seeded backlog. **3 reps per
pass, two passes, so n=6 per engine arm.** Every engine arm registers no host function and reported
`crossings/rec=0.00` **measured** client-side on all six runs; `I0`/`I1000` reported exactly 1.00.
1-minute load was read and recorded beside every one of the 60 runs: **2.07-13.65, median 3.22**,
against a limit of 40, so no run ever waited.

**Deviations from the plan, named rather than glossed:**

- **Two 3-rep passes, pooled to n=6, rather than one.** The first pass ran before the arm-H
  key-count control below existed; rather than discard three reps of the six arms it shares, both
  passes are pooled and the pooling is named here. The passes agree - `D0` medians 19,759 and 20,480,
  1.04x apart - and the wider min-max columns are pass 2 running under a heavier ambient load
  (2.20-13.65) than pass 1 (2.07-4.71).
- **An arm the plan did not ask for: arm H at 8,000 keys** (`--f2-host-control-keys`, default U6's
  8,000), in-session, one term moved. The 1,000-key choice was made to protect the *cache* arms from
  eviction thrash, but it lands on arm H too, and U6's arm-H figures were taken at 8,000 - without
  this control the disagreement below would have been *explained* rather than attributed. It refuted
  the explanation it was added to test.
- **A third measurement outside the interleave**: arm H standalone at U6's exact conditions (8,000
  keys, **128,000** records, engine idle, 3 reps), run after both passes through the existing
  `host-reimpl` experiment. It carries no ratio - it exists to test one cross-session figure.
- **The record count is reconciled to `--floor-records` on both sides.** `run_host_reimpl` derives
  its count from `max(--crossings-sweep)` and `run_engine_floor` from `--floor-records`; a comparison
  whose two sides ran at different loads is void, so `f2-rerun` drives arm H from `--floor-records`
  too. Every arm here ran at 64,000 records.
- **The instrument check was run, not argued for.** `I0`/`I1000`, both halves, inside each rep.
  `I100` was deliberately not re-run - refuted above as too small for the client's thread pool to
  expose.
- **Quiet-machine gate at 1-minute load 40**, as above, not the harness default of 8.
- **Broker on port 19098, not the 19097 the decomposition used.** A leftover container from a
  concluded spike holds 19096; a fresh port and a fresh compose project keep the two independent, and
  only this run's broker was torn down.
- **A smoke pass at 8,000 records was abandoned rather than accommodated.** At that size the whole
  backlog commits between two 50 ms samples of the committed-offset clock, which then reports a zero
  window and fails the run's own validity gate. That is a floor on the harness's record count, not a
  fault; **no gate was relaxed to get past it** - the same check is live at 64,000 and passes.
- **The engine classpath was the one already built in this worktree**
  (`parallel-consumer-proxy-streams/target/classes`, with `pcStreams.measure.disableChangelog`
  present in the compiled `TopologyAssembler`), not rebuilt: a rebuild mid-session would have
  contended with the measurement it was for.

#### Per-arm table

Medians over 6 reps, min-max beside; rates in RECORDS per second on the sink's broker log-append
clock. The committed-source-offset clock is sampled on every arm and agrees: `D0` reads 19,994 rec/s
on the sink clock and 19,732 on the committed clock, 1.3 percent apart, which is what makes the
second clock believable where it stands alone.

| Arm | One term moved | rec/s (min-max) | us/rec | us per (rec x window) | emits | vs D0 |
|---|---|---|---|---|---|---|
| **D0** | - (U6 arm D's shape, cache off) | 19,994 (12,230-21,433) | **50.0** | 4.2 | 768,000 | 1.00x |
| **D-cache** | cache 0 -> 64 MB | 69,265 (65,641-88,398) | **14.4** | 1.2 | 46,998 | **3.46x** |
| **T0** | hopping-12 -> tumbling | 90,724 (80,706-98,613) | **11.0** | 11.0 | 64,000 | **4.54x** |
| **T0-cache** | tumbling AND cache 64 MB | 169,748 (113,879-246,154) | **5.9** | 5.9 | 2,264 | **8.49x** |
| I0 | instrument control: + one host crossing/record | 8,114 (6,565-8,622) | 123.2 | 123.2 | 64,000 | 0.41x |
| I1000 | I0 + 1 ms/record injected host-side | 5,816 (4,985-5,867) | 171.9 | 171.9 | 64,000 | 0.29x |

**Arm H, this session, at the matched condition** (1,000 keys, 64,000 records, single-threaded,
non-durable - no store, no changelog, no rebalance recovery, no late-record handling, so it is an
upper bound on a real reimplementation), n=6:

| Arm H specification | rec/s (min-max) | us/rec |
|---|---|---|
| tumbling | 797,338 (630,271-909,065) | 1.3 |
| hopping-12 | 460,026 (390,388-487,071) | 2.2 |

#### The F2 verdict, retaken in-session

This is the number the whole re-run exists to produce. Wrapper best case against arm H **at the same
specification, in the same session, interleaved**:

| Specification | Wrapper best case | Arm H (F2) | **H / wrapper** | The cross-session figure above |
|---|---|---|---|---|
| tumbling | `T0-cache` 169,748 rec/s | 797,338 rec/s | **4.70x** | 3.7x |
| hopping-12 | `D-cache` 69,265 rec/s | 460,026 rec/s | **6.64x** | **1.32x** |

**At tumbling the in-session figure is close to the inferred one** (4.70x against 3.7x, the
difference inside the arms' own spread). **At hopping-12 it is not: 6.64x against 1.32x, a factor of
five.** Under the pre-registered F2-first band semantics - wrapper-low against H-high - both
specifications read **fails**: `T0-cache`'s 113,879 against H's 909,065, `D-cache`'s 65,641 against
H's 487,071. The wrapper does not reach the reimplementation floor at either specification, cache on.

The wrapper side is not the disagreement. `D-cache` reads 69,265 here against 67,797 above, within
2 percent. **The entire discrepancy is arm H**, which read 89,821 rec/s at hopping-12 in U6's session
and 460,026 here.

#### The anchors, and what they say about the box

| Arm | This session | 2026-08-25 | Ratio |
|---|---|---|---|
| `D0` | 19,994 rec/s | 16,758 rec/s | 1.19x |
| `T0` | 90,724 rec/s | 81,946 rec/s | 1.11x |

**Both anchors read high, in the same direction, within 8 points of each other.** That is a
box-condition offset of roughly 10-20 percent rather than an arm effect: this session's ambient load
was 2.07-13.65 (median 3.22) against the decomposition's recorded 0.96-30.29. So the two sessions'
*engine* figures are comparable after a uniform ~15 percent, and the interleave makes even that
harmless for every ratio reported here. **The anchors reproduce. Arm H does not** - which is exactly
what having anchors is for: it localises the disagreement to one arm instead of leaving it as
"different day".

#### Where arm H's 5x went: the key count is refuted, and the figure is bimodal

The obvious suspect was the key count - the engine arms run at 1,000 keys and U6's arm H at 8,000 -
so it was moved as a control arm, in-session, with nothing else changed (n=3):

| Arm H specification | 1,000 keys | 8,000 keys | Key count is worth |
|---|---|---|---|
| tumbling | 797,338 rec/s | 763,716 (690,192-800,705) | 1.04x |
| hopping-12 | 460,026 rec/s | 372,571 (280,538-420,417) | 1.23x |

**Refuted.** A 1.23x cannot account for a 5.1x. So arm H was re-run at U6's *exact* conditions -
8,000 keys, 128,000 records, engine idle, standalone, 3 reps - and the result is the finding:

- **tumbling: 751,163 rec/s (677,567-760,137) against U6's 723,265. Reproduces.**
- **hopping-12: 423,267 / 92,254 / 417,230 rec/s. Bimodal.** One rep of three landed at 92,254 -
  within 3 percent of U6's 89,821 (88,484-91,619, n=4) - and the other two at ~420,000.

Across all twelve arm-H hopping-12 runs this session, at every key and record condition, the samples
are 92,254 / 280,538 / 372,571 / 390,388 / 413,867 / 417,230 / 420,417 / 423,267 / 445,698 /
474,355 / 478,637 / 487,071. **One of twelve sits at U6's value and a second is halfway down;
the remaining ten span 372,571-487,071. All four of U6's reps sat in the slow mode.**

**Reported as a contradiction, not tuned into agreement.** The consequence is stated rather than
resolved: *F2 at hopping-12 is not a stable quantity on this harness*, so the 1.32x above rests on an
arm-H figure that this session reproduces one time in twelve. The leading hypothesis is CPython's
cyclic collector - hopping-12 allocates a `(key, start)` tuple twelve times per record, 768,000 to
1,536,000 per run against tumbling's twelfth of that, and whether a generation-2 collection lands
inside the timed window is close to a coin toss - which also explains why only the hopping arm is
bimodal while tumbling is stable across every condition tried. **It is a hypothesis with no control
arm behind it and must not be cited as a cause.** It was deliberately not tested here, and the reason
is worth recording: the slow mode appeared once in twelve, so a three-rep paired arm toggling
`gc.disable()` would with high probability show both sides fast and prove nothing. **The follow-up
needs a design that can make the slow mode appear on demand** - many more reps, or a forced
generation-2 collection inside the timed window - before a paired control is worth running at all.

#### The instrument check

Both halves ran inside each rep, and the stronger one is the crossing:

- **Crossing:** `T0` -> `I0` adds exactly one registered host function to the same tumbling topology
  and moves the per-record figure **11.0 -> 123.2 us/rec, a delta of 112 us**, against U6's
  independently fitted **135 us per crossing** from a different session and a different arm family.
  The instrument moves, on the quantity under test, by a constant nobody tuned it to hit.
- **Injected:** `I0` -> `I1000` adds 1,000 us/record host-side and moves the figure **123.2 -> 171.9
  us/rec, a delta of 49 us**. That is the harness property already recorded above - the client
  dispatches invocations onto a thread pool, which absorbs a per-record sleep unless the arm is
  single-key - reproduced here rather than a new result. It is why the crossing delta, not this one,
  is the check that counts.

#### Two caveats carried forward

- **The cache-on arms deduplicate almost all of their emits, and that is a different specification.**
  `D-cache` emits 46,998 where `D0` emits 768,000 (**93.9 percent deduplicated**); `T0-cache` emits
  2,264 against 64,000 (**96.5 percent**). A specification that genuinely needs every intermediate
  update is **not** covered by any figure in this section.
- **Every prior bet-off verdict remains valid for its pre-registered conditions.** Nothing here says
  a previous measurement was wrong; it says a condition of one of them - the state-store cache set to
  zero - was instrumental rather than a product choice, and that the F2 side of the comparison had
  never been taken in the same session as the arms it was being compared against.

#### What this changes above, and what is next

The preceding section's closing claim - *"hopping-12: the gap to the reimplementation floor falls
from 4.5x to 1.32x"* - **does not survive an in-session retake**: the in-session figure is 6.64x, and
the difference is entirely arm H, whose hopping-12 rate this session reproduces U6's value in one run
of twelve. The tumbling claim (3.7x) survives as 4.70x. The cache finding itself is untouched -
`D-cache` and `T0-cache` reproduce within 2 percent and 13 percent respectively - so the decomposition
above stands; what does not stand is the consequence it drew for F2 at hopping-12.

**Next, in order:** (1) settle arm H's bimodality with a single-term control arm on the collector,
because until it is settled F2-hopping has no median worth quoting; (2) only then decide whether
`STRATEGY.md`'s windowed-aggregation paragraph is rewritten or annotated - on today's evidence the
falsification claimed above is **not** established at hopping-12, and annotating it with this
section is the conservative reading.

### Why arm H's hopping-12 rate is bimodal, settled 2026-08-25

The section above closed by naming this as the first of two things to do, because until it is
settled *F2 at hopping-12 has no median worth quoting*. It also named the leading suspect - CPython's
cyclic collector - and, in the same breath, said it was **a hypothesis with no control arm behind
it**. So this section does not start there. It pre-registers five candidates, runs an observational
pass that can show each one's signature *before* any toggle exists, and only then toggles.

**Everything from here to the `#### Measured results` marker was written into the tree before the
first arm ran.** The harness change (`host-bimodal`, `run_host_bimodal`, `_H_ARMS`) was written first, because
the instrumentation is part of the pre-registration: what gets recorded beside each rate is itself a
claim about what could be responsible.

#### The condition is chosen to make the slow mode COMMON, not rare

The previous round declined to run a paired toggle and gave the reason: the slow mode appeared once
in twelve, so three reps would show both sides fast and prove nothing. That is a statement about a
*pooled* frequency across every key and record condition tried. Split by record count, the twelve
samples are not one population:

- **64,000 records** (the `f2-rerun` interleave, both key counts): 9 samples, **none** below
  280,538 rec/s;
- **128,000 records** (U6's four reps, plus this fork's standalone three at U6's exact conditions):
  7 samples, **five** at ~90,000 rec/s.

So the slow mode is not 1-in-12 everywhere; at U6's exact arm-H conditions it is roughly 5-in-7, and
64,000 records is where it hides. **Every arm below therefore runs at 128,000 records and 8,000
keys** - U6's conditions, the condition under which every slow sample so far was taken. Choosing the
condition that makes the mode common is what buys the power the previous round did not have; the
record count is itself pre-registered as a term (H3 predicts it is *the* term).

#### Pre-registered hypotheses and what each predicts

| # | Hypothesis | Predicted signature in the observational pass | Arm that would refute it |
|---|---|---|---|
| **H1** | **CPython's cyclic collector.** Hopping-12 allocates a `(key, start)` tuple twelve times per record - 1,536,000 per run against tumbling's twelfth - so whether a generation-2 pass lands inside the timed window is close to a coin toss | Slow runs carry >= 1 gen-2 pass inside the window and fast runs do not; **measured collector pause accounts for most of the ~1.1 s excess**; `cpu/wall` stays near 1 (the process is busy, not waiting) | `H-gcoff` (`gc.disable()`, one term) shows no slow runs, paired against `H-base` in the same rep; and `H-gcforce` prices a forced gen-2 pass |
| **H2** | **Cold read / per-topic first-read state.** The arm-H topic is REUSED across reps (`_seed_host_topic` sets `retention.ms=-1` for exactly that reason), so the first read of a topic is cold and later ones may be served from page cache | Slow runs cluster at low `rep`; the excess sits in `consume`, not `fold` | `H-fresh` (a topic seeded for this rep and never read) is slow every rep, or is not slow at all |
| **H3** | **Fetch-path stall.** The loop batch-consumes with `timeout=1.0`; librdkafka's local queue defaults to `queued.max.messages.kbytes=65536`, about 64,000 1 KB records - so at 128,000 records the loop MUST outrun the fetcher and can take an empty poll, which the timed window charges at up to a full second | Slow runs show >= 1 empty poll after the window opened, or one very large `max gap`; **the excess is quantised in ~1.0 s units**; `cpu/wall` collapses (the process is waiting); the fold-only rate is unaffected | `H-queue` (local queue raised past the whole backlog) shows no slow runs; `H-starve` (queue shrunk) produces the signature on demand |
| **H4** | **Box contention.** The box carries other agent sessions; a ~0.3 s single-threaded burst descheduled or landed on a busy core reads as a slow run | Slow runs correlate with the per-run **pure-CPU calibration** and with 1-minute load; the excess is spread continuously through `fold`, not quantised; `cpu/wall` stays near 1 while the calibration is proportionally slower | The calibration is flat across fast and slow runs |
| **H5** | **The metric charges wait to the rate.** `records / (ended - started)` counts every second between the first batch and the last, including seconds in which no record was processed. Whatever causes a wait, arm H's "rate" is then a property of the harness's polling rather than of the reimplementation | The **fold-only rate** (`records / fold_s`) is unimodal and stable across every run, while the wall-clock rate is bimodal | The fold-only rate is bimodal too - in which case the slow mode is real work, not accounting |

H1 and H4 predict the process is **busy**; H2, H3 and H5 predict it is **waiting**. `cpu/wall` and
the fold/consume split separate those two families on the observational pass alone, before a single
term is toggled - which is the point of running it first.

**H1 can also be refuted on magnitude without any toggle**, and that is the cheapest result
available: the live dict holds 8,000 x 12 = 96,000 entries plus their tuples, so a full generation-2
pass traverses a few hundred thousand objects. If `H-gcforce` prices that at milliseconds, no number
of them accounts for 1.1 s, and H1 is dead whatever the correlation says.

#### Arms

Each moves exactly one term against `H-base`, the untouched loop (`_H_ARMS` in
`streams_windowing_lab.py`). Three phases, in order, and the order is the method.

| Phase | Arms | What it is for |
|---|---|---|
| `observe` | `H-base` (hopping-12), `T-base` (tumbling) | The untouched loop, many reps, **nothing toggled**. `T-base` is the in-session stability control - tumbling has reproduced in every session, so an explanation that would also make tumbling bimodal is wrong |
| `toggle` | `H-base`, `H-gcoff`, `H-queue`, `H-fresh` | Paired single-term arms, interleaved within each rep, so a rep in which `H-base` is slow and its partner is fast is a *discordant pair* rather than a between-group difference |
| `positive` | `H-base`, `H-gcforce`, `H-starve` | Arms that make each candidate mechanism happen **on demand**, so it is priced instead of argued about. This is what the previous round said the follow-up needed |

**Power, stated before the runs.** For a toggle arm showing zero slow runs to mean anything, the
slow mode must be common in its partner. At the pooled `p = 1/12` the previous round quoted, 12 reps
of a clean toggle arm carry `(1-p)^n = 0.35` - a third of the time you see that by luck, which is why
it declined to run one. At this section's condition the historical rate is `5/7 = 0.71`, where 12
reps carry `4e-7`. The paired design is stronger still: with `k` discordant reps all in one
direction, the exact two-sided sign test is `2^(1-k)`, so **six discordant pairs settle a toggle**.
`H-gcforce` and `H-starve` need no power argument at all - they do not wait for the mode, they cause
it.

**Conditions, fixed for every arm.** 128,000 records, 8,000 keys, 8 partitions, 1 KB payloads,
constant event time past the epoch clamp, single-threaded `confluent_kafka`, **no engine, no sidecar
and no classpath** (arm H needs none). Compose broker `confluentinc/cp-kafka:7.9.0`, its own port
and compose project. 1-minute load read and recorded beside every run; every run also carries a
fixed pure-CPU calibration taken immediately before its window, the window split into fold time and
consume time, empty-poll count and duration, largest single consume gap, process CPU time across the
window, and the collector's passes and **measured pause time** inside the window.

**Nothing above this line changes after the first run; corrections land below as dated entries.**

#### Measured results

**Conditions.** Harness `streams_windowing_lab.py`, new experiment `host-bimodal`
(`run_host_bimodal`, arms in `_H_ARMS`, phases in `_H_PHASES`). Python 3.13.5 with
`confluent-kafka` 2.15.0 (librdkafka 2.15.0), compose broker `confluentinc/cp-kafka:7.9.0` on
loopback `127.0.0.1:19099` (compose project `pc-hbimodal`, started and torn down by this run
alone), 32-core Linux box, 48 GB RAM. 128,000 records, 8,000 keys, 8 partitions, 1 KB payloads,
constant event time past the epoch clamp, unless a row says otherwise. **No engine, no sidecar and
no classpath** - arm H needs none, which is why this ran in minutes rather than hours. 1-minute
load recorded beside every one of the 178 runs: **1.27-8.15**, against a limit of 40, so no run
ever waited.

#### What settled it, in one line

**The stall is librdkafka's `fetch.queue.backoff.ms`.** When the consumer's local queue passes
`queued.max.messages.kbytes` (64 MB by default, about 85,000 of these records) librdkafka stops
fetching and postpones the next fetch by **1,000 ms**. Arm H's aggregation loop then drains the
queue, arrives at an empty one, and blocks inside `consume()` for the remainder of that timer -
**one 0.57-0.66 s wait, at a fixed position in the stream, charged in full to a window that
otherwise takes 0.26 s.** Nothing in arm H is slow; arm H is *waiting*, and
`records / (ended - started)` counts the wait.

#### Phase `observe` - the untouched loop, nothing toggled

`H-base` hopping-12, n=20, and `T-base` tumbling, n=20, interleaved within each rep.

| Arm | median rec/s | min-max | fold-only rec/s (median) | polls > 100 ms | gen-2 passes | collector pause | cpu/wall |
|---|---|---|---|---|---|---|---|
| `H-base` hopping-12 | **94,333** | 93,182-95,088 | **495,980** | **3 in every run** | 0-1 | <= 0.008 s | 0.27-0.31 |
| `T-base` tumbling | 1,082,531 | 738,540-1,195,233 | 1,802,586 | **0 in every run** | 0 | <= 0.001 s | 1.56-1.65 |

`H-base`'s twenty wall-clock samples: 93,182 / 93,812 / 93,885 / 93,990 / 94,060 / 94,088 / 94,094
/ 94,213 / 94,306 / 94,310 / 94,355 / 94,405 / 94,411 / 94,420 / 94,492 / 94,557 / 94,621 / 94,647
/ 94,698 / 95,088.

**Three things are visible before any term is toggled.** The spread is **1.02x** - at 128,000
records the "slow mode" is not a mode at all, it is the only outcome. Every run's largest wait is
**0.61-0.66 s and lands at the same record index** (92,545 in 19 of 20). And `cpu/wall` is
**0.27-0.31**: the process is idle for three quarters of its own measured window, which rules out
every hypothesis that predicts work.

#### Phase `order` - the confound the smoke pass exposed, refuted

A 2-rep smoke ran before the arms were final and showed `H-base` slow in both reps - but `H-base`
ran *first* in each rep, where `f2-rerun` runs tumbling first. Read position is a term the
pre-registration did not name, so it got an arm before anything else was toggled (n=12 each,
interleaved as `H-first`, `T-base`, `H-second` on one shared topic).

| Arm | median rec/s | min-max |
|---|---|---|
| `H-first` (first read of the rep) | 93,438 | 87,517-94,804 |
| `H-second` (after `T-base` read the same topic) | 93,927 | 91,456-94,842 |

**Read order is worth 1.005x. Refuted** - and with it the "an early rep is disproportionately
likely to be slow" reading of the historical samples.

#### Phase `toggle` - the pre-registered paired arms

n=12 each, interleaved within every rep, at 1-minute load 5.31-7.43.

| Arm | One term moved | median rec/s | min-max | vs `H-base` | polls > 100 ms |
|---|---|---|---|---|---|
| `H-base` | - | 91,871 | 79,817-94,440 | 1.00x | 3 in every run |
| `H-gcoff` | `gc.disable()` for the window | 93,120 | 86,333-94,242 | **1.01x** | 3 in every run |
| `H-fresh` | topic seeded this rep, never read | 91,371 | 51,480-93,372 | **0.99x** | 3 in every run |
| `H-queue` | local queue raised past the whole backlog | **355,205** | 190,638-437,165 | **3.87x** | **0 in every run** |

**`H-gcoff` verified itself**: collector pause was exactly `0.000 s` on all twelve runs, so the
toggle demonstrably reached the run - and the rate did not move. **`H-fresh`** carries the one
outlier of the whole session (51,480 rec/s, a 1.734 s wait) and it is the same signature, not a
different one.

#### Phase `backoff` - the arm that names the mechanism

n=12 each, interleaved, one term: librdkafka's `fetch.queue.backoff.ms`.

| Arm | `fetch.queue.backoff.ms` | median rec/s | min-max | fold-only (median) | polls > 100 ms | cpu/wall |
|---|---|---|---|---|---|---|
| `H-base` | 1,000 (librdkafka's default) | 94,650 | 93,370-95,186 | 510,030 | 3 | 0.26-0.30 |
| `H-backoff100` | 100 | **436,569** | 424,129-447,529 | 513,786 | **0** | 1.21-1.29 |
| `H-backoff10` | 10 | **430,678** | 407,176-449,563 | 514,542 | **0** | 1.21-1.29 |

**Non-overlapping** - `H-base`'s maximum is 95,186 and `H-backoff100`'s minimum is 424,129 - and
**12 of 12 discordant pairs in the same direction**, exact two-sided sign test `2^-11 = 4.9e-4`.
The three arms' **fold-only** rates are identical (510,030 / 513,786 / 514,542): the toggle changes
nothing about the aggregation, only about the waiting.

**One prediction inside this arm was refuted.** The ladder was registered expecting the stall's
*length* to track the setting - 100 ms in, 100 ms of stall out. It does not: at 100 ms the stall
disappears entirely rather than shrinking. The reason is mechanical and is itself a confirmation -
the timer only bites if the consumer empties the queue before it expires, and draining 64 MB at
~510,000 rec/s takes ~125 ms, which is longer than 100 ms and far shorter than 1,000 ms. **The
response is a threshold, not a proportion.**

#### Phase `positive` - the two arms that make each mechanism happen on demand

n=3 each (the reduced n is named below). These need no power argument: they do not wait for a mode,
they cause one.

| Arm | median rec/s | window | polls > 100 ms | What it prices |
|---|---|---|---|---|
| `H-base` | 94,080 | 1.36 s | 3 | - |
| `H-gcforce` (one forced `gc.collect(2)` mid-window) | **94,453** | 1.36 s | 3 | **a full generation-2 collection over this working set costs 7-12 ms** |
| `H-starve` (local queue shrunk to ~1 MB) | **947** | 135 s | **135, each of them 1.001-1.002 s** | **`fetch.queue.backoff.ms` read straight off the clock, 135 times per run** |

`H-starve` is the decisive one: with the queue shrunk so that it refills and re-fills constantly,
the consume loop takes **135 waits of exactly 1.002 s** - at intervals of ~956 records, the shrunken
queue's capacity - while its **fold-only rate is unchanged at 318,310 rec/s** and `cpu/wall` reads
`0.00`. The mechanism is not inferred from a correlation; it is reproduced on demand with the timer's
own value on it.

#### Phase `ladder` - the record count is the term, and tumbling is not immune

3 reps at each count, untouched loop, default fetch config.

| Records | hopping-12 rec/s | stalls in the hopping window | tumbling rec/s | stalls in the tumbling window |
|---|---|---|---|---|
| 32,000 | 155,113-318,145 | **none** | 705,517-958,089 | none |
| 48,000 | 227,199-320,364 | **none** | 392,445-1,003,259 | none |
| 64,000 | 375,775-398,053 | **none** | 890,508-1,077,448 | none |
| 80,000 | 163,154-324,199 | **none** | 487,834-1,050,101 | none |
| 96,000 | 71,061-73,329 | **3/3 runs**, ~0.5 s at record ~84,000-92,000 | 602,690-1,054,509 | none |
| 128,000 | 91,597-94,395 | **3/3 runs**, ~0.6 s at record ~84,000-92,000 | 1,017,067-1,070,303 | none |
| 192,000 | 80,281 | **two episodes**, at records ~85,000 **and** ~177,000 | **143,810** | **yes** - 0.742 s at record 177,209 |

**The switch is between 80,000 and 96,000 records**, which is where the backlog first exceeds the
64 MB local queue by enough for the consumer to drain it before the 1,000 ms timer expires. This is
the whole of the historical "bimodality": **64,000-record runs sat below the threshold and 128,000-
record runs above it.** And at 192,000 records **tumbling stalls too** - it is not a property of the
hopping specification, it is a race between the fetcher's supply rate and the loop's drain rate, and
tumbling drains fast enough to keep the queue below the cap for longer, not forever.

#### Hypotheses, confirmed and refuted

| # | Hypothesis | Outcome | The arm that settled it |
|---|---|---|---|
| **H1** | CPython's cyclic collector | **REFUTED, three ways** | (i) magnitude, no toggle needed: measured collector pause inside a slow window is **2-15 ms** against a **1.1 s** excess, and gen-2 passes are 0 or 1; (ii) `H-gcforce` prices a *forced* full gen-2 pass at **7-12 ms** - you would need ~90 of them; (iii) `H-gcoff`, the paired toggle, moves the rate by **1.01x** with the collector demonstrably off (pause exactly 0.000 s, 12/12) |
| **H2** | Cold read / per-topic first-read state | **REFUTED, twice** | `H-second` reads a topic another arm read seconds earlier in the same rep and is **1.005x** of `H-first`; `H-fresh` seeds its own topic every rep and is **0.99x** of `H-base` |
| **H3** | Fetch-path stall | **CONFIRMED, and named exactly** | Three independent arms, each moving one term: `H-backoff100`/`H-backoff10` (the timer) **4.6x**; `H-queue` (the capacity) **3.87x**; `H-starve` (the positive control) reproduces the wait 135 times at its literal 1.002 s. The ladder adds the fourth: the stall switches on between 80,000 and 96,000 records, where the backlog crosses the queue's 64 MB |
| **H4** | Box contention | **REFUTED** | The per-run pure-CPU calibration is **13.1-26.4 ms across all 178 runs** and flat between fast and slow ones; the slowest and fastest `H-base` runs of the observational pass differ by 1.02x while their calibrations differ by 1.19x in the *wrong* direction. `cpu/wall` at 0.27-0.31 says the process is idle, not contended |
| **H5** | The metric charges wait to the rate | **CONFIRMED, and it is the reason the quantity looked bimodal** | The **fold-only** rate is 445,501-518,212 rec/s in the observational pass (spread 1.16x) while the wall-clock rate of the *same runs* is 93,182-95,088. Across the toggled arms the fold-only rate is 510,030 / 513,786 / 514,542 - unchanged by every term that moves the wall-clock rate 4.6x |

The two families the pre-registration named separated on the observational pass exactly as it said
they would: **`cpu/wall` was 0.27-0.31, so the process was waiting**, which killed H1 and H4 before
a single arm was toggled.

#### The distribution, which is the finding

**98 runs of the untouched loop at 128,000 records** (arms `H-base`, `H-first`, `H-second`,
`H-gcoff`, `H-fresh`, `H-gcforce` - none of which touches the fetch path), across five phases and
1-minute loads from 1.27 to 8.15: **every single one between 51,480 and 95,186 rec/s**, 96 of them
between 79,817 and 95,186. **36 runs with one fetch-path term moved** (`H-backoff100`,
`H-backoff10`, `H-queue`): **every single one between 190,638 and 449,563 rec/s.** The two
populations do not overlap and nothing in between was ever observed.

So the quantity was never bimodal in the sense of a coin toss. It was **two deterministic regimes
selected by the record count**, and the earlier sessions sampled both without recording the term
that chose between them.

#### Power, as reasoned rather than as hoped

The pre-registration argued from a pooled `p = 1/12` (useless at n=12) and a conditioned
`p = 5/7` at U6's exact conditions (`(1-p)^12 = 4e-7`). **The measured `p` at 128,000 records is
1.00 - 98 of 98.** That makes the paired toggles far stronger than planned: `H-base` against
`H-backoff100` is 12 discordant pairs out of 12, `2^-11 = 4.9e-4`; `H-base` against `H-queue` the
same. The refutations are equally powered in the other direction - `H-gcoff` produces **zero**
discordant pairs in 12, against a partner that stalls every time.

**And none of it was load-bearing**, because `H-starve` and `H-gcforce` do not sample a rate: one
reproduces the mechanism on demand and the other prices it. That is what the previous section meant
by "a design that can make the slow mode appear on demand", and it is why this settled in one
session where a paired 3-rep toggle would not have.

#### The guard, and its negative control

**A measurement that could silently become 4.7x wrong now fails instead.** `measure_host` raises
when its timed window contains a `consume()` call over 100 ms, naming the mechanism, the position
and the lever; the bimodality arms whose whole purpose is to exhibit the stall carry
`expect_stall=True` and stand down. Verified in both directions, and the negative control is the
exact configuration that produced the disputed number:

- `host-reimpl` at U6's conditions (128,000 records, 8,000 keys) - the run that reported **89,821
  rec/s** in U6 and **92,254** in the section above - now **fails** with
  `arm H invalid: 3 consume() call(s) over 100ms inside the timed window ... 81% of this window was
  fetch wait, not aggregation`;
- the same run with `--host-fetch-queue-backoff-ms 100`, one term moved, **passes**, and reads
  **393,855 and 433,285 rec/s** at hopping-12 (and 1,118,959 / 1,156,428 at tumbling);
- the guard then caught a case nobody predicted: **`T-base` at 192,000 records**, where tumbling
  stalls too, printed `STALLED` rather than a rate.

#### What F2 at hopping-12 should now be quoted as

**The in-session 6.64x stands, and the cross-session 1.32x is now dead for a stated reason rather
than merely contradicted.**

- The `f2-rerun` retake ran arm H at **64,000 records**, which the ladder places **below the stall
  threshold**. Its arm-H hopping-12 figure (460,026 rec/s, 390,388-487,071) sits squarely in this
  session's un-stalled population (190,638-449,563 at 128,000 records; 375,775-398,053 at 64,000).
  **That comparison was never contaminated, so `D-cache` 69,265 against arm H 460,026 = 6.64x is
  the figure to quote.**
- U6's **89,821 rec/s was the artefact**: taken at 128,000 records with librdkafka's default
  backoff, it is 78-81 percent fetch wait. Corrected at U6's own conditions, through U6's own
  `host-reimpl` experiment, with one term moved, arm H reads **393,855-433,285 rec/s** - **4.4-4.8x
  higher**. The 1.32x was a ratio whose denominator was a stalled consumer.
- **Arm H's hopping-12 rate is a stable quantity after all**, once the term that was never recorded
  is fixed: **~430,000-440,000 rec/s** on the wall clock at 128,000 records with the fetch queue not
  starved, and **~510,000 rec/s** on the fold-only clock, which is the figure that does not depend on
  the harness's polling at all. It was never a coin toss and there was never a second mode.
- **What this section does NOT license.** No engine ran in this session, so **no new F2 ratio is
  taken here** - KTD18 forbids pairing these arm-H figures against the wrapper arms measured in the
  `f2-rerun` session, which is the exact error this whole line of work exists to correct. The
  hopping-12 verdict remains `f2-rerun`'s 6.64x; what changes is that the 1.32x has a cause, and
  that arm H now has a defensible number to re-take a ratio against when engine arms next run.

Against the section above, its "Next, in order" item (1) is discharged: **arm H's bimodality is
settled, F2-hopping does have a median worth quoting, and it is 6.64x.** Item (2) - whether
`STRATEGY.md`'s windowed-aggregation paragraph is rewritten or annotated - is untouched by this
work and, on today's evidence, still reads as annotate: the wrapper does not reach the
reimplementation floor at hopping-12.

#### Deviations, named rather than glossed

- **A 2-rep smoke pass ran before the arm set was final**, and its numbers are reported rather than
  discarded (92,501 / 91,968 rec/s, both with the 0.57-0.63 s stall, gen-2 pauses of 2-3 ms). It is
  what exposed the read-order confound and motivated `H-first`/`H-second` and the
  gap-position instrument. It is counted in the 98.
- **Two arms and one instrument were added after the pre-registration**, both named at the point
  they appear above: the `order` phase (read position - a term the pre-registration did not name),
  and the `backoff` ladder plus the record-count `ladder` (added once the observational pass had
  localised the stall). The pre-registered arms were all run regardless, including the two the
  observational pass had already made unlikely.
- **The `positive` phase ran at n=3, not 12.** `H-starve` takes **135 seconds per run** by
  construction; three runs of it produce 405 waits of 1.002 s, which is not a quantity more reps
  would sharpen. A first attempt at n=8 was cut off by a wall-clock limit after 3 reps and its two
  completed `H-starve` runs agree with these (153 s, 154 waits of 1.001-1.002 s).
- **The `ladder` phase ran at n=3 per record count**, under a rising ambient load (4.42-8.15). The
  hopping rates below the threshold vary continuously with that load (155,113-398,053) and are not
  claimed as anything but "no stall"; the threshold itself is the result and it is 3/3 at every
  count.
- **`H-queue`'s spread is wide** (190,638-437,165). A queue raised past the whole backlog makes
  librdkafka buffer 140 MB eagerly, and its fetch threads then compete with the fold for CPU on a
  box at load 5-7. It moves the outcome decisively in the right direction; its *median* is not a
  clean figure for anything and is not used as one.
- **The instrumentation is inside the loop under test.** Two `time.monotonic()` calls per batch
  (about 130 per run) plus a `gc.callbacks` entry that runs only when the collector does. The check
  that it is harmless is that `H-base` reproduces the pre-existing figure: 94,333 here against
  92,254 in the section above and 89,821 in U6.
- **`--load-limit 40`, not the harness default of 8**, as in both sections above; the box carried
  other agent sessions throughout and the per-run load is recorded beside every figure.
- **The broker is on port 19099 in compose project `pc-hbimodal`**, torn down by this run alone. The
  leftover `pcnumba-broker-1` on 19096 was left untouched, as was `pc-f2rerun`'s.
- **No engine arm was run and none was needed.** Every hypothesis on the table was about the
  reimplementation's own consume loop, and arm H requires no engine, no sidecar and no classpath -
  which is why 178 measured runs fitted in one session.

#### What is not settled

**Why the previous session's three standalone 128,000-record arm-H runs came out 2/3 fast**
(423,267 / 92,254 / 417,230) when this session's 98 untouched runs at the same record count are
98/98 slow. The mechanism explains how that can happen - the stall only arms if the *fetcher*
outruns the *consumer* far enough to fill 64 MB, so anything that depresses the fetcher (a busier
broker, a busier box) removes it - and the tumbling arm demonstrates that race in-session from the
other side, stalling at 192,000 records where it does not at 128,000. But this session never
reproduced a fast untouched 128,000-record run, at loads from 1.27 to 8.15, so the fetcher-side
condition is named rather than measured. **What would settle it:** the same ladder run against a
broker under concurrent read load, with the fetcher's delivery rate recorded per run rather than
inferred from the consumer's.
