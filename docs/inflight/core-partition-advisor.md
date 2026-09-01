# Partition advisor: say when partition count is ACTUALLY the constraint, with evidence

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - composes dimension 2's cap logic with bottleneck attribution; both must exist first -->

From the follow-up Codex strategy conversation, weekend of 2026-08-29/30 (first review's breakdown:
[`core-engine-thesis.md`](core-engine-thesis.md)).

## The claim

Partition count is Kafka's oldest piece of architectural fortune-telling because one number is
asked to answer two unrelated questions: how Kafka should distribute the data, and how much
application parallelism might exist someday. PC's thesis already splits those; this feature closes
the loop by telling the operator, from discovered evidence, when partition count has *actually*
become the processing constraint - and, just as valuably, when adding partitions would buy
nothing. Not "6 consumers, 3 partitions, add partitions", but: local concurrency saturated,
exploitable key parallelism remains, process capacity available elsewhere, and ownership cannot
spread the work further - partitions are now limiting scale. Conversely: 48 partitions, 17 active
ordering domains, downstream saturated - more partitions provide zero processing benefit.

The decision tree is the escalation ladder the arbitration and auto-scaling notes already carry,
extended one rung: PC exploits parallelism locally (do nothing) -> local machine exhausted but another owner
has capacity (scale processes, [`core-auto-scaling.md`](core-auto-scaling.md) dimension 2) ->
machines exhausted AND ownership cannot distribute further (partitions are finally the
constraint - this note).

**Positioning lines that come with it**: *choose partitions for Kafka; let PC choose parallelism
for your application* - and the conference-slide version: *"how many partitions will you need in
three years?" is the wrong question*. Filed in [`docs-content-series.md`](docs-content-series.md)
with the partition-myths group.

## Why it is nearly free once its parents exist

Dimension 2 already caps the instance recommendation at partition count, because instances beyond
it are idle by construction. This advisor is that cap's contrapositive: **when the cap binds while
profitable parallelism remains, the cap itself is the finding.** Every input is something the
controller or the attribution taxonomy ([`core-bottleneck-attribution.md`](core-bottleneck-attribution.md))
already measures - it is a report, not a new control loop. The share-groups earmark in
[`core-auto-scaling.md`](core-auto-scaling.md) is the same boundary from the protocol side:
KIP-932 relieves the cap without a repartition, so the advisor's "partitions are limiting"
recommendation eventually gains a second remedy besides adding partitions.

## The caveat to keep with it

**Scope the advice to processing capacity, and warn about the key-mapping cost.** Partition count
also answers questions PC cannot see - storage distribution, broker balance, replication - so the
advisor must say "partitions are/are not your *processing* constraint" and no more. And adding
partitions to a live topic changes the key->partition mapping: per-key ordering has a transition
window and Streams state locality breaks. An advisor that recommends repartitioning without
naming that cost would cause the incident it exists to prevent; the honest form presents
alternatives with their prices (add partitions and pay the remap / wait for share-groups support /
accept the ceiling).
