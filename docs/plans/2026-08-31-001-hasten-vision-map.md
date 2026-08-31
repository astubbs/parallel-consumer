# The Hasten vision map: how the notes string together

Dated binder, 2026-08-31. **Thin on purpose**: the spine, then each beat of the vision in a
sentence or two with the notes that own it - no content of its own, so it cannot drift from the
notes. Point-in-time per `docs/plans/` convention; the file-refs gate forces whoever moves a
linked note to update this map. The preserved primary sources are the handoff documents
([`2026-08-29-hasten-compound-engineering-handoff.md`](../ideation/2026-08-29-hasten-compound-engineering-handoff.md),
[`2026-08-30-hasten-handoff-supplement.md`](../ideation/2026-08-30-hasten-handoff-supplement.md))
and the vision fiction
([`2026-08-29-the-story-of-hasten.md`](../ideation/2026-08-29-the-story-of-hasten.md));
[`SOUND_BITES.md`](../../SOUND_BITES.md) carries the compressed intent.

## The spine

One question at successively larger scheduling domains:

```
Kafka          which machine owns this partition?
PC             which record inside that partition may run?
local engine   which runnable record is most useful to run?
fleet          which application should receive scarce capacity?
infrastructure which resource should receive additional capacity?
economics      where should the next dollar go?
```

## The beats

**1. The thesis.** Kafka ownership and execution are independent; PC proved it at the key level,
and following the observation to its conclusion makes execution itself programmable. Waiting is a
scheduling state, not an execution state - every mechanism that makes work wait is one eligibility
model. Work has identity, position and incarnation, and everything else is a projection of one
opportunity model.
[`core-engine-thesis.md`](../inflight/core-engine-thesis.md) ·
[`core-admission-scheduling-model.md`](../inflight/core-admission-scheduling-model.md) ·
[`core-execution-opportunity-model.md`](../inflight/core-execution-opportunity-model.md) ·
[`core-work-identity-model.md`](../inflight/core-work-identity-model.md)

**2. The local engine learns.** The controller discovers useful concurrency experimentally
(astubbs#333 implements it), classifies *why* more stops helping, proves what could be removed,
and holds a declared objective instead of a configured number.
[`core-auto-scaling.md`](../inflight/core-auto-scaling.md) ·
[`core-bottleneck-attribution.md`](../inflight/core-bottleneck-attribution.md) ·
[`core-scale-in-proof.md`](../inflight/core-scale-in-proof.md) ·
[`core-slo-objective-api.md`](../inflight/core-slo-objective-api.md)

**3. The future becomes legible.** The committed backlog is indexed by execution meaning
(an inverted index, not a cache), demand and capacity get horizons, queue disciplines become
policy over one primitive, and the causal past is the same graph pointed backward.
[`core-prescience-and-spice.md`](../inflight/core-prescience-and-spice.md) ·
[`core-temporal-horizons.md`](../inflight/core-temporal-horizons.md) ·
[`core-queue-disciplines.md`](../inflight/core-queue-disciplines.md) ·
[`core-decision-lineage.md`](../inflight/core-decision-lineage.md) ·
[`core-record-semantic-tracing.md`](../inflight/core-record-semantic-tracing.md)

**4. Capacity becomes shared.** Named resources own capacity; renewable pieces are delegated and
spent locally; per-function arbitration, tenant quotas, priorities and the partition advisor fall
out as policy. This is the buildable centre - the navigator micro-MVP and the twenty-instance
conservation test live here.
[`core-shared-execution-resources.md`](../inflight/core-shared-execution-resources.md) ·
[`core-distributed-throttling.md`](../inflight/core-distributed-throttling.md) ·
[`core-per-function-capacity-arbitration.md`](../inflight/core-per-function-capacity-arbitration.md) ·
[`core-partition-advisor.md`](../inflight/core-partition-advisor.md)

**5. The fleet, without a cluster.** Coordination rides Kafka; frontier agreements make drains,
deployments and topology evolution boring; partitions, records and topics virtualize; scheduled
intent generalises what an obligation is; and the boundary with specialist substrates stays
explicit.
[`core-fleet-capacity-coordination.md`](../inflight/core-fleet-capacity-coordination.md) ·
[`core-frontier-handover.md`](../inflight/core-frontier-handover.md) ·
[`core-partition-virtualization.md`](../inflight/core-partition-virtualization.md) ·
[`core-scheduled-intent.md`](../inflight/core-scheduled-intent.md) ·
[`core-nile-boundary.md`](../inflight/core-nile-boundary.md)

**6. Many faces, one engine.** Facades, ecosystem adapters, runtime services and compatibility
APIs are the adoption surface; internal machinery becomes product through the polyglot
multiplier; the manifest stays on the right side of the platform line.
[`core-alternate-api-facades.md`](../inflight/core-alternate-api-facades.md) ·
[`core-ecosystem-adapters.md`](../inflight/core-ecosystem-adapters.md) ·
[`core-spring-kafka-integration.md`](../inflight/core-spring-kafka-integration.md) ·
[`core-runtime-services-and-compat.md`](../inflight/core-runtime-services-and-compat.md) ·
[`core-internal-machinery-as-features.md`](../inflight/core-internal-machinery-as-features.md) ·
[`core-function-manifest.md`](../inflight/core-function-manifest.md) ·
[`release-certified-execution-semantics.md`](../inflight/release-certified-execution-semantics.md)

**7. Seeing and steering.** Observe/Explain/Act with expiring interventions; the cheap instruments
(gap explainer, hot keys, retry economics, true lag); fingerprints remembered over time; replay
and canarying as safe experimentation.
[`web-control-plane.md`](../inflight/web-control-plane.md) ·
[`web-gui-observability-ideas.md`](../inflight/web-gui-observability-ideas.md) ·
[`core-retry-economics.md`](../inflight/core-retry-economics.md) ·
[`core-ordering-profiler.md`](../inflight/core-ordering-profiler.md) ·
[`core-capacity-fingerprinting.md`](../inflight/core-capacity-fingerprinting.md) ·
[`perf-workload-replay-simulator.md`](../inflight/perf-workload-replay-simulator.md) ·
[`core-scheduler-canarying.md`](../inflight/core-scheduler-canarying.md)

**8. Proving and telling.** The lighthouse exists to falsify; one staged application feeds every
presentation and demo; measurements publish including the refuted ones; the archaeology grounds
it; the cost model says where attention goes.
[`core-lighthouse-mvp.md`](../inflight/core-lighthouse-mvp.md) ·
[`docs-executable-progression.md`](../inflight/docs-executable-progression.md) ·
[`web-three-reveal-demo.md`](../inflight/web-three-reveal-demo.md) ·
[`docs-research-program.md`](../inflight/docs-research-program.md) ·
[`docs-content-series.md`](../inflight/docs-content-series.md) ·
[`perf-benchmark-cost-to-slo.md`](../inflight/perf-benchmark-cost-to-slo.md) ·
[`process-agentic-cost-model.md`](../inflight/process-agentic-cost-model.md) ·
[`process-csid-repo-archaeology.md`](../inflight/process-csid-repo-archaeology.md)

## Sequencing, in one line each

v6 and the open PR stack are untouched by all of the above. The first built thing is the
navigator micro-MVP (beat 4), then the lighthouse spike (beat 8), which exists to attempt the
four falsifiers before the vision earns more investment. STRATEGY.md adoption waits for the
owner's triage and the ce-strategy run.

## Open decisions, all the owner's

Product name (trademark clearance first) and the W2/Voice codename question; the OSS/enterprise
split hypothesis; STRATEGY.md adoption; PC-inside-Streams timing (ruled post-lighthouse);
"Merge 367" disposition. Each is recorded where it arose - this map only lists them.
