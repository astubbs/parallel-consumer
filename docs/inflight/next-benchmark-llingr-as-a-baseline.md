# Next: add llingr to the benchmark harness as a standing baseline

<!-- inflight-type: feature -->
<!-- inflight-impact: coordination -->

Owner's idea, 2026-08-21. Not primarily a competitive exercise - **it answers a question this project
cannot currently answer about itself.**

## The question it settles

> How much does the performance of the underlying engine actually matter, compared with the cost of
> processing the records?

Right now that is argued rather than known, and the argument is always the same shape: *"Go is
faster"*, *"virtual threads are faster"*, *"a JVM engine can't compete"*. Those claims are probably
true **and probably irrelevant**, but this project has no way to say so with a number - so they win by
default whenever someone raises them.

A llingr arm turns the engine's contribution into a **measured constant**. llingr's engine is Go, on the
same processing model, with a published per-message overhead of ~1.26µs. Ours is the JVM. Put both
against the same workload at a range of per-record delays and the answer falls out:

- At a **0ms** simulated delay, the engines are the whole result and the gap is maximal.
- At **2ms**, this session already measured PC's own configuration moving the same build 1.9x - which
  is larger than the entire five-year engine regression.
- At **20ms, 100ms** - a database write, an HTTP call, anything real - the prediction is that the
  engine difference disappears into the noise.

**If that prediction holds, it is one of the most useful things this project could publish about
itself** - and it reframes every "but Go is faster" conversation into "by how much, at your latency?".
**If it fails, that is more important still**, and we would want to know before shipping a comparison
demo whose headline is throughput.

## Why it is worth keeping permanently, not running once

- **It keeps us honest.** A standing external baseline catches drift that an internal-only benchmark
  cannot: this session's 35% regression was invisible for five years precisely because everything was
  measured against ourselves.
- **It prices roadmap items against each other.** With the engine's contribution known, the value of
  adaptive concurrency, micro-batching or virtual threads can be compared on one axis instead of
  argued.
- **It is the honest input to the virtual-threads decision.** `virtual-threads` is a roadmap entry
  (next-0x) partly justified by performance. A baseline says how much is available to win *before*
  the work is done.

## How it fits the existing harness

`bench/run-bisect.sh` already resolves each arm as a Maven coordinate, pins logging, runs a
no-PC-code control arm, prepares broker and dataset once, and measures peak in-flight at the stub.
A llingr arm is a different shape - a Go binary or a JVM artifact rather than a Maven coordinate - so
the harness needs an arm abstraction it does not have yet. llingr's published benchmarks
(`github.com/llingr/llingr-demux/benchmarks`) are re-runnable and are the natural starting point,
which also removes any argument about whether we configured their product fairly.

**Sweep the per-record delay as the primary axis**, not throughput at one delay. The delay axis is the
whole finding; a single-latency comparison is the thing this note exists to replace.

## Including llingr in the multi-language demo app - a conflict to resolve first

Raised 2026-08-21: should llingr be an arm in the per-language comparison demo
([`branch-classic-comparison-demo.md`](branch-classic-comparison-demo.md))?

**This collides with a standing decision and the collision should be settled deliberately.** The demo
is a user-facing marketing artifact - the whole point is that visitors run it and see the numbers. An
llingr arm inside it **is** a public comparison, which the owner ruled out on 2026-08-21: **no public comment on llingr anywhere - not in issues, not in
docs, not in marketing.**

The two cannot both hold. The options, with what each costs:

1. **Private benchmarking only** (the current decision). llingr stays in `bench/`, never in the demo.
   The demo keeps its native-client-vs-PC arms. Costs nothing, changes nothing, and the engine-cost
   question is still answered internally - which was the actual goal.
2. **Include it, and reverse the no-public-comparison rule.** Buys a striking demo, and takes on
   everything the fairness charter demands: configuration parity, publishing the case we expect to
   lose, and a named competitor who has asked this project for feedback and not received it.
3. **A third arm that is unnamed** - "another engine" - which is worse than either. It is still a
   public comparison, and an unnamed competitor reads as evasive rather than discreet.

**DECIDED (owner, 2026-08-21): option 1.** llingr is included in **internal** performance analysis and
kept out of anything user-facing - no arm in the click-to-run demo, no named comparison in marketing
or docs. The benchmark harness answers the engine-cost question privately, which is what it is for. The demo's job is to show PC working in the visitor's language; it does not need a
competitor to make that point, and adding one changes the artifact from a demonstration into an
argument. The benchmark harness already answers the engine question privately, which is what it was
for.

**One thing worth taking from llingr into the demo regardless, and it is not a comparison:** its
benchmarks sweep *simulated latency* as an axis (10ms, 35ms, 50ms, 100ms) rather than reporting one
number. The comparison demo should do the same with its delay dial - the shape of the curve is the
insight, and it is the same reason this note exists.

## Constraints

- **Private, for research.** Owner's decision: no public comparison, no naming them in marketing. See
  [`market-analysis-llingr.md`](market-analysis-llingr.md).
- **Licensing.** The Go engine is AGPL-3.0 and public; the JVM build sits behind a licence-key Maven
  repository. Benchmark the Go engine and be careful about what running the JVM build would require.
- **Configuration fairness.** Their `ConcurrentKeys` and our `maxConcurrency` are the same dial with
  different names, and this session showed a 1.9x swing from that dial alone. Sweep both, or the
  result measures configuration rather than engines.

## Related

- [`perf-throughput-regression-since-0-3.md`](perf-throughput-regression-since-0-3.md) - the harness
  and what it already controls for.
- [`next-performance-regression-testing.md`](next-performance-regression-testing.md) - making the
  internal half a gate.
- [`market-analysis-llingr.md`](market-analysis-llingr.md) - what llingr is and the rules of engagement.
