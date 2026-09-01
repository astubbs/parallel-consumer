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
The supplement expands the set: **Guild / Navigator** - the higher-level placement/topology
optimizer (where work, ownership and capacity should live); **No-record / No-ship** - explicit
markers of semantic opacity ([`core-prescience-and-spice.md`](core-prescience-and-spice.md));
**Kwisatz Haderach** - a playful alias for complete semantic Prescience, easter-egg only.
**Naming, partially resolved:** the supplement confirms **Voice** as the authoritative control
plane - the thing the earlier handoff called **W2** - while the owner's separate use of W2 as a
project-level codename stands on its own. Public API names must work for people who have never
read Dune; these stay internal/demo vocabulary.

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

## Refinements from the JMS archaeology (2026-08-31)

The csid-jms-bridge deep-dive ([`process-csid-repo-archaeology.md`](process-csid-repo-archaeology.md))
sharpened five things:

- **Some "missing broker features" are scheduler features, not storage features.** Priority,
  delay, selectors, expiry and arguably routing were forced into physical topic structure only
  because the consumer lacked knowledge and scheduling authority. The principle: **do not encode
  execution semantics into physical transport unless the transport genuinely must enforce them.**
- **Semantic position virtualisation** - the deepest form of the thesis: Kafka's
  (topic, partition, offset) stays the beautiful immutable physical coordinate, but *physical log
  position is provenance; semantic position determines execution* - position in an ordering
  domain, in causal history, relative to a deadline, in a workflow, in the completion frontier.
  PC proved offset order is not execution order; this generalises it.
- **The DLQ formulation, final form**: the bridge faced the forced choice "block progress or
  remove the failed thing"; the sparse frontier adds the third option - leave it exactly where it
  is and progress everything it does not constrain. **Do not DLQ on failure; DLQ only when the
  original execution position is deliberately abandoned** (a compatibility projection, not a
  mechanism). The supplement adds: the head-of-shard block is sometimes a *desirable correctness
  property*, not a cost; the operator verb is **Proceed**, not Retry (make the original execution
  eligible again); and Skip writes durable skipped-state to a compact store with an optional
  metadata-only DLQ projection. Feeds astubbs#149's requirements work.
- **The effect frontier.** The bridge's documented crash windows are the generic problem: three
  frontiers per execution - source durable / effect performed / completion authoritative - and
  recovery reconciles them via stable effect identity, execute, authoritative check
  (PRESENT/ABSENT/UNKNOWN), optional compensation. (The "recoverable side-effect boundary" this
  names comes from uncaptured weekend exchanges - same flag as in
  [`core-decision-lineage.md`](core-decision-lineage.md).)
- **Compatibility layers as a falsification method.** For each foreign model (JMS, Orleans/Akka,
  Temporal, Celery, cron): can its execution semantics be expressed as combinations of existing
  primitives *without a bespoke distributed subsystem*? JMS compresses remarkably well
  (priority->selection, expiry->validity deadline, selector->eligibility, request/reply->addressed
  work + logical continuation identity, redelivery->incarnation, HA->ownership, DLQ->abandoned
  position). If most models reduce this way, that is evidence; if each needs new machinery, the
  architecture is less general than claimed. The adapter test: if a JMS layer still builds its own
  HA, priority, retries and DLQ, the substrate failed.

The past/present/future symmetry that closes it: **lineage asks what caused this; Why Wait asks
what prevents this; Prescience and the scheduler ask what should happen next** - three tenses of
one graph.

## The lineage, for the record

Adaptive concurrency -> "more concurrency does not help, the downstream is saturated" -> model the
downstream as an admission predicate -> schedule around the bottleneck -> globalise the capacity ->
look ahead at queued demand -> Prescience. Global rate limiting was not the goal; it was the
problem that exposed the mutation. And the name earns itself: the scheduler's fundamental question
about every known piece of work is *why is this not executing?* - if no binding reason exists,
PROCEED.
