# A research program: publish the measurements, including the failures

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - needs a product decision -->

From the Codex strategy review of 2026-08-22/23 (breakdown in
[`core-engine-thesis.md`](core-engine-thesis.md)). The observation: the project is already
experimentally probing questions the Kafka world hand-waves about, and **the results are valuable
even where the ambitious features fail** - provided negative results are published rather than
buried. This extends the evidence-before-claims discipline the strategy already runs on, and it is
the credibility mechanism [`next-reclaim-the-category.md`](next-reclaim-the-category.md) says the
project needs: publishing "we predicted FFI would win and it did not" is what makes "here are
14,000 tests saying otherwise" believable.

## The questions, each already adjacent to existing work

1. **How much exploitable concurrency exists inside a real Kafka partition?** Not theoretical -
   measured under real key distributions: available key parallelism over time, effective
   parallelism after ordering constraints, parallelism lost to hot keys / retries / downstream
   saturation. Five numbers (partitions, active keys, theoretical / useful / sustainable
   parallelism) tell the whole story. No one publishes this quantity.
2. **How badly do real key distributions punish partition-serial execution?** The Zipf/hot-key
   experiment. The interesting result is predicted to be tail latency for *innocent* keys trapped
   behind a hot key sharing their partition - the review coined **collateral head-of-line
   blocking** for it, a phrase worth keeping.
   [`next-the-tail-experiment.md`](next-the-tail-experiment.md) and the tailed work model
   (`BENCH_DELAY_P99`) are the harness for exactly this.
3. **Can useful concurrency be inferred online, without being told the workload?** Run a schedule
   the controller has not seen (downstream latency shifts, hot-key burst, traffic doubling) and
   watch where it settles. This is the adaptive controller's (astubbs#227) evaluation protocol as
   much as a publication.
4. **Which Kafka Streams assumptions actually prevent intra-partition concurrency?** Even if
   Streams-on-PC (astubbs#255) hits a semantic wall, the output is a tested taxonomy - stateless
   ops / stateful key-local / joins / stream-time / punctuation / EOS / restoration / rebalance,
   each with a verdict and a counterexample - instead of folklore. Failure is publishable here.
5. **What does it cost to cross from a JVM engine into foreign user code?** The FFI-vs-sidecar
   measurements across ~ten languages (astubbs#242 work) are a dataset with an audience well
   beyond Kafka.

## Mechanism

A `/research` directory (or equivalent): reproducible workloads, raw results, scripts, conclusions,
**including refuted hypotheses**. Runnable by a reader who disagrees - reproducibility is the
promotion. Overlaps to reconcile before creating it:
[`next-benchmark-a-model-of-work-not-work.md`](next-benchmark-a-model-of-work-not-work.md) and the
perf note family own the harness; the publication surface is what is new. Written-up results feed
[`docs-content-series.md`](docs-content-series.md).
