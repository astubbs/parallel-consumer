# Queue disciplines as projections: what else becomes a queue service on this substrate

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - a catalogue of policy projections over the admission primitive, not a build list -->

From the 2026-08-30 exchange (model root:
[`core-admission-scheduling-model.md`](core-admission-scheduling-model.md)). If the engine plus
full Prescience turns partitions into sharded semantic queues, the classic queue-service feature
set (SQS delay/visibility/FIFO-groups/DLQ, Cloud Tasks scheduling/dispatch-rate/dedup) becomes a
set of *projections* over one primitive - durable work + eligibility predicates + selection
policy - plus several disciplines no queue product offers:

- **Priority / EDF-deadline / delayed** - physical position stops being dequeue policy; no
  topic-per-priority-class, ordering constraints still hold; `notBefore` is just a predicate.
- **Fair queues, hierarchical** - company -> tenant -> workload -> ordering domain, with
  reservations and borrowing; thousands of logical queues over one physical stream
  (the multi-tenancy of [`core-shared-execution-resources.md`](core-shared-execution-resources.md)).
- **Semaphore / atomic-admission queues** - admit at most N against a semantic resource; admit
  {A,B,C} bundles all-or-nothing; no worker-level distributed deadlock.
- **Single-flight** - 500 outstanding equivalent requests, one canonical execution, the rest wait
  on its completion or coalesce: a queue that prevents redundant execution rather than ordering it.
- **Batch-forming** - wait briefly to form the best batch by endpoint/DB-partition/GPU/tenant,
  with Prescience seeing candidates far beyond physical order.
- **Dependency / condition queues** - admissible when prerequisites complete, or when a
  table-materialised predicate flips (status=approved); the point where queueing shades into
  workflow while the primitive stays the same.
- **Recovery / maintenance / cost / SLO-slack queues** - repair work joins the same buffet with
  its own QoS; fenced work keeps its causal position; budgets are capacity functions; selection
  by remaining slack rather than static priority.
- **Opportunity queues** - the most substrate-specific: rank by what completion *unlocks* (the
  tiny record freeing an ordering shard holding 100k valuable records outranks a large isolated
  one). Needs Prescience plus causal knowledge; no conventional discipline can express it.
- **Capacity-shaped queues** - the dequeue asks "what combination of admissible work best fits
  current capacity?" - DB-heavy work while Salesforce is saturated, pivot on replenishment. The
  strongest departure from every classic queue product, and the demand-shaping idea from
  [`core-prescience-and-spice.md`](core-prescience-and-spice.md) worn as a queue.

Adoption tie: these are rungs for the descending-commitment ladder in
[`core-alternate-api-facades.md`](core-alternate-api-facades.md) - "Kafka is already a work
queue" (the kwq argument) becomes "and it has queue disciplines SQS does not", without a third
system.
