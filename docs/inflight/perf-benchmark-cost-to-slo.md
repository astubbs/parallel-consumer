# Benchmark dollars-to-SLO, not throughput - the economics angle

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - needs the adaptive controller and the tail harness first -->

From the Codex strategy review of 2026-08-22/23 (breakdown in
[`core-engine-thesis.md`](core-engine-thesis.md)). Especially relevant after 2026-08-22 showed the
raw-throughput story is not the ground to fight on
([`next-what-survives-share-groups.md`](next-what-survives-share-groups.md),
[`next-reclaim-the-category.md`](next-reclaim-the-category.md): engine microseconds are the
smallest term in the goal).

## The reframe

Fix the workload and the SLO - *process 10,000 msg/s with p99 completion under 2s* - and measure
**what it costs to meet it**: partitions required, instances required, CPU, memory, total
compute-hours, downstream pressure, recovery behaviour. Arms: plain consumer, PC fixed
concurrency, PC adaptive, PC adaptive + external scaling, stock Kafka Streams, PC-backed Streams.

"Same workload, same SLO, 3 pods instead of 18" is understood instantly by people who will never
read a latency histogram. "17x faster" is not.

## Why PC changes the cost curve, in two mechanisms

- **Partitions stop being the way you buy parallelism.** `24 partitions + 100,000 keys + 600
  concurrent operations` instead of `600 partitions`, with everything partitions cost (broker
  metadata, rebalance time, recovery, defensive over-partitioning years in advance) unbought.
- **Internal concurrency absorbs load before instances do.** The adaptive controller's first
  response to load is a dimension autoscalers do not have: `unused safe key parallelism? -> yes:
  concurrency up / no: instances up` ([`core-auto-scaling.md`](core-auto-scaling.md)). A workload
  can sit on two pods all day and burst internally, scaling out only when capacity is genuinely
  exhausted.

Second-order effect worth one line in any writeup: polyglot Streams removes a *platform* cost, not
a CPU cost - the team that keeps its Python application instead of adopting Flink or maintaining a
Java service solely because Streams is JVM-only. Do not make simplistic vendor-billing claims
(Confluent Cloud does not bill per partition); the defensible claim is about the user's
infrastructure, not the vendor's invoice.

## Dependencies

Needs the adaptive controller (astubbs#227) for the interesting arms, the tailed work model for
realistic load, and honest sub-saturation latency measurement - the known gap STRATEGY.md's key
metrics section already names (the harness drains a backlog, so at 100% utilisation residence time
measures the backlog, not the engine).
