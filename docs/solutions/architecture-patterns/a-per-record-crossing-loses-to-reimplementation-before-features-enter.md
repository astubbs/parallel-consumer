---
title: "A per-record boundary crossing loses to in-process reimplementation before any feature enters - measure the crossing against the user's honest alternative, not against nothing"
date: 2026-08-25
category: architecture-patterns
module: parallel-consumer-proxy-streams
problem_type: architecture_pattern
component: language_boundary
severity: critical
applies_when:
  - A design routes per-record work across a process or language boundary
  - The justifying comparison is "the capability does not otherwise exist in that language"
  - The user could reimplement the specific capability with the primitives they already have
tags: [falsification-spike, pre-registration, throughput-floor, kafka-streams, language-proxy]
---

# A per-record crossing loses to reimplementation before features enter

## The settled result

The windowed-aggregation falsification spike
(`docs/plans/2026-08-25-001-feat-streams-windowed-aggregation-plan.md`, results in
`docs/inflight/perf-streams-windowing-multiplier.md` while astubbs#334 is open) set its hard floor
F2 as *whatever a stateless single-threaded reimplementation measures* - a Python
`confluent_kafka` consumer folding into a dictionary - and evaluated verdicts F2-first. Both
window specifications came out **bet off**, and not marginally: the reimplementation ran **69x**
the wrapper's tumbling rate and **122x** its hopping rate, with non-overlapping spreads, on the
same box under the same load.

The fitted cost model is the durable finding: per-record cost across the boundary fits
**t(m) = 33us + m x 135us** (m = the window multiplier, `ceil(size/advance)`), over the
single-session gRPC transport. That crosses the parity floor (1,000 rec/s) at m ~ 7 - and **never
reaches either reimplementation floor at any m >= 0**: the 33us intercept alone caps the model at
~30,300 rec/s, an order of magnitude short of both floors, and the measured single-crossing arm
fails both directly. The window multiplier was real, linear and exactly as predicted (12.0 and 2.0
crossings per record, measured), and it was not the losing term: the crossing-free control ran
**2.7x above** the single-crossing tumbling arm, so *one* crossing per record already costs more
than an entire native hopping topology.

## The pattern

- **"The capability does not otherwise exist" is a claim with a boundary, and the boundary is
  measurable.** Where reimplementation is genuinely hard (topology plumbing, joins,
  repartitioning, fault-tolerant state), comparing against nothing is honest. Where the user's
  honest alternative is a dictionary over the client they already have, the comparison is against
  that dictionary - and a synchronous per-record crossing at ~135us marginal cost loses to it by
  orders of magnitude. Draw the floor from the alternative's *measured* rate, never from an argued
  number: the argued 100 rec/s floor was off by three orders of magnitude.
- **Judge a floor F2-first, and pre-register the lattice before any number exists.** The spike's
  verdict bands originally assumed the measured floor would land below the argued parity floor;
  five reviewers independently caught that an unbounded measured floor can land anywhere, and the
  F2-first rewrite made the verdicts decidable whichever way it fell. It fell 600x above.
- **A placement or batching redesign cannot rescue a per-crossing-cost loss.** Moving the host
  from the aggregator to the emit genuinely collapsed crossings (1.50 vs 12.00 per record where
  measured) and was still two orders of magnitude short. The reopening condition is a transport
  that changes the per-crossing constant itself (embedded/FFI), not a design above it.

## Method findings that outlive the verdict

- **Constant synthetic event times plus broker time-based retention silently empty a reused
  topic** - records timestamped in the past age out immediately; the run reads as data loss. Labs
  set `retention.ms=-1` on their topics.
- **Kafka Streams 3.9.2 exposes no cache-eviction metric.** The zero-evictions assertion that
  validates an emit-ratio measurement has to read `ThreadCache`'s TRACE counters; the instrument
  was proven able to report a non-zero (19,080 forced evictions on an undersized cache) before its
  zero was believed.
- **A quiet-machine gate that reads whole-box load starves on its own decaying bursts** and on
  ambient neighbours. Interleaved arms protect every in-session ratio; record per-run load as a
  condition and reserve the gate for absolute-rate claims near a floor.
- **`TopologyTestDriver` over-counts cached emissions** (commits - and so flushes - per record);
  broker-vs-TTD emit counts only agree under close-driven emit rules (suppression,
  `EmitStrategy.onWindowClose()`).

## Correction, 2026-08-25 (second): the 122x was a stalled consumer, and the floor itself was mis-specified

Two later rounds changed what the numbers above are worth. Neither overturns the pattern; both
sharpen what it may be cited for. Full workings in
`docs/inflight/perf-streams-engine-floor.md` and the second dated correction in
`docs/inflight/perf-streams-windowing-multiplier.md`.

**The hopping figure was an instrument artefact.** `122x` divided by an arm-H hopping rate of
89,821 rec/s that was not measuring the reimplementation at all: at 128,000 records the consumer's
local queue passes `queued.max.messages.kbytes`, librdkafka stops fetching and postpones the next
fetch by `fetch.queue.backoff.ms` (1,000 ms), and 78-81 percent of the timed window became fetch
wait charged to the rate. Corrected, arm H reads 393,855-433,285 rec/s and the margin widens to
roughly **540x**. **The tumbling 69x is unaffected** - that arm sits on the winning side of the
same fetcher race at this record count. Cited as a method finding: **an arm whose rate is a
division by elapsed time will silently price a stall as throughput**, and the harness now raises
rather than averaging - `measure_host` fails any window containing a `consume()` over 100 ms.

**The larger correction is to the floor, not the figure.** F2 was defined as *whatever a stateless
single-threaded reimplementation measures* - no store, no changelog, no restore, no rebalance
recovery, no exactly-once. That is the floor for a product Kafka Streams is not in the business of
being, so the comparison answers *"can a toy beat an engine at toy work"*, which it can at any
transport speed. Removing the crossing entirely does **not** invert it: with sub-microsecond
crossings available (747ns GraalWasm, 19.9ns Numba `@cfunc`) and the engine's own state-store cache
on, the wrapper reaches 69,265 / 169,748 rec/s and still loses 6.64x / 4.70x in-session.

**So this write-up's title is the durable claim and its numbers are not the argument for it.** The
question that decides the design is the crossover named in the title - *how many of the features a
user actually came for can be added back to the reimplementation before hand-rolling becomes the
worse choice* - and no figure here measures it. That measurement is under way, one feature at a
time, starting with durability.
