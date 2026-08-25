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
