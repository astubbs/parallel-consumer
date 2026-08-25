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
single-session gRPC transport. That crosses the parity floor (1,000 rec/s) at m ~ 7 - but crosses
both reimplementation floors **below m = 1**. The window multiplier was real, linear and exactly
as predicted (12.0 and 2.0 crossings per record, measured), and it was not the losing term: the
crossing-free control ran **2.7x above** the single-crossing tumbling arm, so *one* crossing per
record already costs more than an entire native hopping topology.

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
