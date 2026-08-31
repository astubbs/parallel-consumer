# The admission model: waiting is a scheduling state, not an execution state

<!-- inflight-type: task -->
<!-- inflight-impact: process -->
<!-- inflight-state: deferred - the conceptual root of the 2026-08-30 exchange; binds how the resource/scheduler features get designed, not scheduled work itself -->

From the largest exchange of the follow-up Codex conversation (2026-08-30, ~12:56am onward). This
note owns the conceptual model; [`core-execution-opportunity-model.md`](core-execution-opportunity-model.md)
keeps the features-as-projections table this model matured out of, and
[`core-shared-execution-resources.md`](core-shared-execution-resources.md) owns the resource
design built on it.

## Working codenames (Dune-themed, from the conversation - all uncommitted, like the product name)

**Prescience** - deep lookahead over the committed backlog. **Spice** - the compact per-record
scheduling metadata. **Mentat** - the local scheduler/selector. **Voice** - the global
coordination/resource plane. **Golden Path** - the selection objective. **Why Wait?** - the
explainability principle. Notes use them for fidelity to the transcript, nothing more.

## The hinge

Conventional rate limiting dispatches work, then blocks inside execution: a thread now represents
something that is not runnable, and the scheduler has already spent its decision. The leap:
**move the decision before dispatch** - a record waiting for Salesforce capacity is not "running
but blocked", it is simply *not currently eligible*, and the engine picks another independent
record instead. Rate limiting stops being special: `WAIT(ordering)`, `WAIT(resource)`,
`WAIT(retry-after)`, `WAIT(dependency)`, `WAIT(maintenance)`, `WAIT(not-before)` are all the same
thing - known work with an unsatisfied eligibility predicate. The kernel, in three sentences the
conversation itself ranked as the root of the design:

1. **Waiting is a scheduling state, not an execution state.**
2. **Never dispatch work already known unable to make useful progress.**
3. **Among admissible work, dispatch what best advances the system.**

Two distinct stages fall out - **eligibility** (may this execute: ordering, resources,
maintenance, time) and **selection** (should this execute next: priority, deadline, cost,
fairness) - and four states, not two: **KNOWN -> ADMISSIBLE -> ADMITTED -> RUNNING**. The
admissible/admitted distinction matters: 500,000 runnable records against capacity for 200 are
"awaiting selection", not "blocked", and honest observability must say which.

## Why blocking in user code is information destruction

Dispatch 1,000 functions and let 700 block on a saturated dependency: the scheduler sees 1,000
"executions" of which 300 are real. Concurrency metrics lie, demand and execution conflate,
workers hold non-work, substitution is impossible, and the autoscaler concludes "saturated - add
workers" when workers accomplish nothing. Keeping the record outside execution keeps the truth:
*outstanding work, not running work*. This redefines concurrency itself: not "how many functions
can run" but **how much useful work can currently proceed** - which is the quantity the adaptive
controller (astubbs#333) actually wants. It also splits backpressure into two kinds with opposite
remedies: **capacity** backpressure (maybe scale) and **causal** backpressure (scaling is
useless) - the distinction no queue-depth autoscaler can make.

## Late admission, and the failure-cost boundary

Prescience tempts early arrangement; the discipline is **know early, commit late, execute
immediately**. Before admission, failure costs nothing - the work is still durable intent in
Kafka. Admission itself is a tiny internal transaction (reserve the complete resource bundle or
none - *partial admission must never be externally observable*), and only after a side effect
begins does failure enter hard distributed-systems territory. Late admission is therefore not just
utilisation - it postpones the moment failure becomes expensive. A failed record needs no DLQ
transfer and no retry queue: **failure just changes its eligibility** (astubbs#149's brainstorm
should absorb this framing).

## Candidate engineering invariants

- **No blocked worker, for known constraints**: no work is dispatched merely to wait on a
  condition the engine already knew was unsatisfied. `dispatch -> acquire -> sleep` appearing
  anywhere is an architecture violation - the conversation called this one of the best review
  tests the project could have.
- **No unexplained waiting**: every WAIT points at machine-readable predicates (predecessor
  unresolved / capacity exhausted / not-before T / fence), which is what makes the
  [`web-control-plane.md`](web-control-plane.md) Explain layer *correctness*, not polish - and
  enables the next questions: what would make this PROCEED, and what would that cost.

## Prior art, stated before anyone over-claims

Pre-execution admission control is not novel: CockroachDB explicitly moves queuing out of the Go
scheduler into admission queues; Impala queues queries against cluster limits; Kubernetes will not
start an unschedulable pod. What they own is the request/query/workload. What this engine owns is
different: **a record that already durably exists in somebody else's log, before it becomes
application execution** - with the nasty part already solved (not dispatching record A does not
stop the partition, because PC decoupled those years ago). The unusual thing is the substrate
combination: durable intent + predeclared requirements + deep lookahead + true causal ordering +
ownership of the dispatch boundary. The deepest internal noun may be **obligation** rather than
work: a record is the durable representation of intent, and the scheduler grants *permission for
effects to begin* - which is also why records turn out to be one source of obligations among
several ([`core-scheduled-intent.md`](core-scheduled-intent.md)).

## The lineage, for the record

Adaptive concurrency -> "more concurrency does not help, the downstream is saturated" -> model the
downstream as an admission predicate -> schedule around the bottleneck -> globalise the capacity ->
look ahead at queued demand -> Prescience. Global rate limiting was not the goal; it was the
problem that exposed the mutation. And the name earns itself: the scheduler's fundamental question
about every known piece of work is *why is this not executing?* - if no binding reason exists,
PROCEED.
