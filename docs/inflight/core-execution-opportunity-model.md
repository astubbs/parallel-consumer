# The execution-opportunity model: one internal model, every feature a projection of it

<!-- inflight-type: task -->
<!-- inflight-impact: process -->
<!-- inflight-state: deferred - an architectural constraint on future work, not a work item; binds when any two of its projections are built -->

From the follow-up Codex strategy conversation, weekend of 2026-08-29/30 (first review's breakdown:
[`core-engine-thesis.md`](core-engine-thesis.md)). The conversation's own flag on this one: *an
architectural insight worth protecting*. Filed as a task because it is a constraint on how the
feature notes around it get built, not a feature itself.

**Matured 2026-08-30:** the conversation's final exchange generalised this into the admission
model - [`core-admission-scheduling-model.md`](core-admission-scheduling-model.md) **now owns the
conceptual model** (waiting as a scheduling state, eligibility vs selection, the KNOWN ->
ADMISSIBLE -> ADMITTED -> RUNNING states). This note keeps the features-as-projections table below
and the original gate-ladder form.

## The insight

Nearly every feature this conversation produced is the same knowledge asked a different question:

| Feature | The question it asks of the model |
|---|---|
| Adaptive concurrency (astubbs#333) | how much should I execute? |
| Autoscaling ([`core-auto-scaling.md`](core-auto-scaling.md)) | would another machine help? |
| Bottleneck attribution ([`core-bottleneck-attribution.md`](core-bottleneck-attribution.md)) | where did opportunity disappear? |
| Partition advisor ([`core-partition-advisor.md`](core-partition-advisor.md)) | is ownership preventing execution? |
| Rate-limit governance ([`core-distributed-throttling.md`](core-distributed-throttling.md)) | which opportunities are forbidden? |
| SLO control ([`core-slo-objective-api.md`](core-slo-objective-api.md)) | which opportunities should I exploit? |
| The GUI ([`web-control-plane.md`](web-control-plane.md)) | what is happening, and why? |

So the constraint: **do not implement these as separate clever features, each re-deriving state.**
Give the engine an explicit concept of *execution opportunity* and a gate ladder that preserves the
reason work stops at each gate:

```
available work
  -> ordering permits?        (shard/key structures)
  -> admission permits?       (astubbs#333's admission target)
  -> policy permits?          (declared contracts, SLO ceilings, operator overrides)
  -> downstream permits?      (probe evidence)
  -> local resources permit?  (CPU/memory)
  -> EXECUTE
```

Capture that model once and half the control-plane features become queries against it. Miss it and
each feature grows its own Map of the same information in a different shape - which is this repo's
own recorded bug-recurrence pattern (the engine's `AGENTS.md` keeps ledgers of shared state for
exactly that reason, and the owner's standing rule is *collapse parallel state when bugs recur*).

## Why the ground is unusually ready for it

The conservation-accounting work (astubbs#336, merged) already establishes the property the model
needs most: **every owned record is somewhere, and the numbers reconcile.** The "what is PC doing
right now" breakdown in [`web-control-plane.md`](web-control-plane.md) is that property made
legible; the concurrency-gap explainer is the same ladder read as arithmetic. The model is largely
a naming-and-preserving exercise over accounting the engine already keeps for correctness.

## The reframe, candidate thesis material

Most infrastructure observes *resources* - CPU, lag, thread counts. This engine observes
*opportunity*: "another 6,000 independent operations are available, and executing them would not
raise throughput, because this downstream has saturated" is a statement no resource metric can
make. The deeper abstraction is not adaptive concurrency; it is **continuously measuring the gap
between the work an application could execute and the work it should execute, and understanding
why the two differ**. Adaptive concurrency is merely the first consumer of that knowledge.

## The standing task this implies

Systematically inventory what PC uniquely knows at the boundary where Kafka semantics meet
application execution - Kafka does not know what the handler is doing, Kubernetes does not
understand keys and ordering, OpenTelemetry does not understand the commit frontier, language
runtimes do not understand Kafka ownership; PC sees all four. The 2026-08-29/30 batch mined that
seam ad hoc and found a dozen features; the inventory is how the next dozen get found on purpose
rather than by conversation.
