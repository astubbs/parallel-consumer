# Future Frontier Agreement: drains, deployments, and why Kafka groups are not execution groups

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - the operational protocol the admission model implies; the deepest engineering idea of the 2026-08-30 exchange -->

From the 2026-08-30 exchange (model root:
[`core-admission-scheduling-model.md`](core-admission-scheduling-model.md)).

## Drain becomes a precise, scoped protocol

Because the engine knows the exact boundary between outstanding and admitted work, "drain" stops
meaning "stop polling and hope": **fence admission for target T -> wait until T has zero admitted
executions -> change something -> reopen admission.** Kafka ownership stays intact, Prescience
stays intact, work keeps accumulating, no rebalance is required - and T can be one stage, one
resource, one tenant, one function version. Don't move the work; change whether it may cross the
boundary.

## Versions are capabilities, and deployment becomes scheduling

`handler/payment@v7` and `@v8` are resources: v8 appears as capacity, new work admits against it,
v7 stops receiving admissions and quiesces, remove it. A rolling deployment is capacity moving
between execution capabilities - without the runtime becoming Kubernetes.

## The protocol: prepare early, commit early, reach the boundary later

Propose a future frontier F comfortably ahead of everyone (a crude estimate is fine - correctness
never depends on it). Every participant atomically checks `highestAdmitted < F`; if true, install
the fence and ACK; any NACK cancels the proposal and a later F is tried, with the incumbent
authoritative throughout. All ACKs -> **commit**: `v7 owns < F, v8 owns >= F` is now fact, agreed
while F is still far away. When execution reaches F, nothing operational happens - offset F-1
runs on v7, offset F runs on v8, no rebalance, no pause, no state hydration on the critical path.
Two rules make it safe: **the successor speculatively PREPARES (fetch, index, warm) but never
speculatively EXECUTES**, and **a proposal never changes authority - only the commit does**. The
signature move of the whole architecture repeated: coordination latency hidden underneath work
that was going to execute anyway. Generalises far beyond deployment - routing epochs, policy
changes, schema versions, key migration, regional authority: *"by causal frontier F, rule R
becomes true."*

**The CAP-honest footnote, kept prominently:** under a control-plane partition at exactly the
wrong moment, a participant that cannot learn whether the commit happened must stop at F -
no-duplicates, no-loss and no-pause cannot all be guaranteed under arbitrary coordination loss.
Committing far ahead makes the window vanishingly small; it does not make it zero.

## Frontier barriers are the underlying primitive - and the bridge already built one

The csid-jms-bridge's startup does exactly this in production shape: write an epoch marker into
its Kafka WAL and refuse to activate until the materialized projection has processed past it. The
generalisation is stronger than "wait for offset N": **"this materialized semantic view is
causally caught up to frontier X"** - needed before failover activation, before restored state is
serving-ready, before a routing epoch activates, at drain completion, and before a snapshot is
declared consistent. First-class primitive, not a per-feature reimplementation; the Future
Frontier Agreement above is one user of it. And the accompanying design smell test: **whenever an
authoritative distributed decision can later be superseded, ask what fences the stale knowledge**
- epochs are not optional decoration, and even Prescience needs one (100% coverage is meaningless
except relative to a captured committed frontier).

## Open design question (owner, 2026-08-31): why negotiate F out-of-band at all?

Alternative: write a synchronization-boundary record into the topic itself and make *reading it*
the trigger - the csid-jms-bridge's epoch-marker move, applied to handover - or carry the boundary
in producer-written control headers to avoid polluting the log with control records. Trade-offs to
settle before building: the in-log marker needs no offset prediction and is causally positioned by
construction (the boundary IS where it sits in the log), but requires either producer cooperation
or a control producer injecting records into application topics; per-record control headers spread
the same requirement across every producer; the out-of-band F-agreement touches the log not at
all and works when producers are not runtime-controlled, at the cost of the prediction/ACK round.
Likely resolution is by ownership: in-log markers where the runtime owns the produce side
(internal/generated topics), F-agreement where it does not. Unresolved - record, do not assume.

## The separation the protocol forces, and it is thesis-grade

Kafka's group coordinator cannot express "v7 owns this partition until offset F, then v8" - so
both generations run as **separate Kafka consumer groups** (an ephemeral shadow acquisition group
for the handover window), both fetching the same partitions, with the engine deciding who may
*admit* on which side of F. The cost is duplicated fetching during overlap; the payoff is the
conceptual split: **a Kafka consumer group is a mechanism for acquiring portions of a log; an
engine execution group is fault-tolerant authority over outstanding obligations.** The original
thesis extended one more step - partition assignment stops meaning execution ownership and
becomes merely log-acquisition ownership.
