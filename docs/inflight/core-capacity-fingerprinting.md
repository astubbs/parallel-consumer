# Capacity fingerprinting: remember what the controller learned

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - needs the controller in production first; persistence home decided 2026-08-31: Kafka Streams state -->

From the follow-up Codex strategy conversation, weekend of 2026-08-29/30 (breakdown root:
[`core-engine-thesis.md`](core-engine-thesis.md)) - its author's favourite of the batch, and it
names a recurring project pattern: *PC learns things transiently because execution needs them; the
cheap feature is remembering.*

A function's runtime fingerprint - parallelism source, useful-concurrency range, handler profile,
retry amplification, scale-out response, the p99 knee - is what the adaptive controller
(astubbs#333) discovers and discards on every restart. Persist it and three things fall out:

- **Warm starts.** A restart (or a deploy to a new environment) bootstraps near the learned
  operating region and verifies experimentally, instead of ramping from ignorance - directly
  shortening the post-rebalance cooldown windows [`core-auto-scaling.md`](core-auto-scaling.md)
  already designs around.
- **Empirical workload models** - observed, not configured; "CPU-heavy" as a measurement rather
  than a label.
- **Semantic regression detection.** "useful concurrency fell 210 -> 85 after release 4.8, and
  downstream latency sensitivity doubled" - production itself reports that the application's
  execution characteristics changed, no benchmark run required.

**The open question answered (owner, 2026-08-31): the fingerprint lives in Kafka Streams state.**
A Streams-backed store gets durability, failover and - naturally - the *record of performance over
time* that regression detection needs, for free. Three requirements added with it: a fingerprint
without its **environment** is uninterpretable (stamp the instance count and topology it was
measured under), it should link a **snapshot of the resource's own configuration** (the Postgres
that measured this was running two read replicas), and **central resource schedulers subscribe to
all fingerprints and reduce to their own resource** - the fingerprint stream is an input to the
allocator, not only a diagnostic. Still true regardless: a prior must expire - a fingerprint from
last month's key distribution can be worse than ignorance, so the verify step is mandatory, not
an optimisation.
