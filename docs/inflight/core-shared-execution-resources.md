# Shared execution resources: one new abstraction, a ridiculous number of features fall out

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - the concrete candidate design for the distributed-throttling track; the gating decisions in that note remain the owner's -->

From the follow-up Codex strategy conversation, 2026-08-29 ~1:20pm - the exchange where, in the
owner's words, "the scheduler dawned on me". This is the most concrete feature design either
weekend produced. [`core-distributed-throttling.md`](core-distributed-throttling.md) owns the
track and its open decisions; this note records the design the follow-up converged on.

## The primitive

**A named resource owns capacity; the system delegates renewable pieces of that capacity to the
execution engines that can currently use it.** Handlers declare what they consume
(`customer-enrich -> salesforce-prod`), and the limit follows the *resource* - across topics,
functions, languages and applications. Three handlers in three languages accidentally allowing 60
concurrent calls against a 50-call service becomes `salesforce-prod = 50`, globally. Waiting for a
permit never blocks a worker thread - it is an admission constraint, the same enforcement seam the
ideation doc's ideas 2/3 already describe. The execution predicate becomes:

```
ordering permits it        (may it execute)
adaptive target admits it  (should it execute)
resource permits held      (is it allowed to execute)
   -> runnable
```

Same admission point PC already owns. Applies to database connection budgets, vendor APIs, GPU
pools, mainframes, per-customer SaaS quotas, model inference - anything finite and shared.

## What falls out as policy over the same primitive

- **Adaptive global rate limiting** - not "may this request execute" but *"of everyone who could
  consume the next unit, where does it produce the most useful work?"* The budget flows to
  demonstrated convertible demand and reflows as circumstances change - no static quota carving.
- **The global envelope is itself adaptive.** Two numbers per resource, kept separate: the
  contractual hard ceiling ("never exceed 500") and the discovered sustainable envelope ("312
  today, 410 tomorrow, 120 during failover") - the astubbs#333 experiment run one level up, with
  `targetP99` on the *resource* instead of `maxConcurrency` on each application.
- **It fixes controller interference.** Independent adaptive controllers hill-climbing one shared
  database see each other's probes as noise and fight. Attach them to one named resource and the
  adaptive boundary moves outward: one envelope loop per resource, local controllers divide the
  allocation. Hierarchical adaptive concurrency - cleaner control theory, and the collective-probe
  scene in [`core-fleet-capacity-coordination.md`](core-fleet-capacity-coordination.md) made
  concrete.
- **429/Retry-After become fleet-wide control signals.** A Python workload's 429 is evidence about
  *the resource*, so Java and Go back off before ever receiving one. Materially different from
  every application independently discovering the same limit by hitting it.
- **Multi-tenancy.** Resource names can be dynamic - `tenant/{id}`, `stripe/account/{id}` - with
  policy templates (`tenant/*` defaults, per-tenant overrides), state materialised on first
  reference and TTL-expired. Per-tenant *execution* quotas: Kafka quotas govern broker resources;
  this governs application execution. Noisy-neighbour protection nothing else in the Kafka
  ecosystem offers.
- **Priorities, guarantees, graceful degradation** - critical/background, guaranteed minimums with
  borrowing, weighted fairness: all allocation policy over the same credits, no second scheduler.
- **The execution-resource graph.** The allocator ends up holding both halves: all the work that
  could execute, and the scarce resources it would consume - an adaptive admission-control plane,
  not a rate limiter.

## The implementation shape that makes it tractable

**Do not build a distributed token bucket everyone mutates. Delegate pieces of a budget.** That
converts "how do 50 processes synchronise every request?" into "how do we periodically divide
10,000 permits between 50 processes?" - coordination at 2-10 Hz, execution at local speed, nothing
distributed on the record path. The load-bearing line: **synchronise ownership of capacity, then
spend capacity locally.**

- **Rate = finite consumable credits per quantum.** Mint at most the quantum's worth; spent is
  gone; next quantum mints afresh. A conservation law rather than a coordination algorithm, with
  the failure bias you want: **failure wastes capacity, never violates the constraint** (a dead
  instance strands its credits; a superseded coordinator must not re-mint interval N). Window
  boundaries mean the contract is rate *plus burst*, which is what token buckets mean anyway; for
  undocumented sliding windows, leave headroom and treat 429 as feedback.
- **Concurrency is honestly harder than rate** - permits must come back, and a dead instance's
  downstream operations may still be running. Leased disjoint slot ranges plus expiry-and-grace
  covers bounded-duration operations; a truly strict "never ever exceed N" contract with unbounded
  durations needs stronger coordination. Model resources as **hard vs adaptive** explicitly
  rather than pretending the semantics are identical.
- **Kafka is the coordination plane** - the discipline that killed an earlier idea in the same
  conversation ("Kafka already gives you replay; I was inventing infrastructure around a native
  property") applied positively: hash resource names onto partitions of an internal control topic;
  the partition's owner is the authority for those resources; sharding, failure detection,
  authority movement and the fencing vocabulary (generations, epochs) all come from machinery
  Kafka already has. No Redis, no etcd, no server.
- **Report USEFUL demand, not queue size.** PC knows that payments "wants 5,000 Stripe permits"
  but only 800 are executable because Postgres binds them - so Stripe capacity flows to refunds
  instead of sitting allocated-and-unusable. A work-conserving allocator over *executable
  opportunity* ([`core-execution-opportunity-model.md`](core-execution-opportunity-model.md)) is
  what makes this qualitatively better than any generic distributed limiter. Not needed for v1;
  the abstraction leaves room for it.
- **Multi-resource records need no 2PC**: a record consuming `tenant/acme` + `stripe-prod` +
  `postgres-orders` runs when the *local* slices of all three have a permit - deterministic
  acquisition order, release on partial failure, all local synchronisation. Fits PC exactly
  because the engine already lives with "semantically executable but not selected right now".

## The v1 the conversation itself insisted on

One hard distributed **rate** resource. Kafka-elected owner mints finite short-lived credits;
instances spend locally; unspent credits expire; instance death loses capacity, never creates it;
a replacement coordinator cannot re-mint an interval. Even cheaper first rung: **divide the budget
by active instance count** - membership already comes from Kafka, wasteful under uneven demand but
safe, and no new distributed algorithm at all. The acceptance demonstration: twenty instances
hammering one fake API, aggregate rate never exceeding the contract while instances join, leave
and are killed. Explicitly deferred from v1: mid-window reclamation, multi-resource optimisation,
the adaptive global envelope, hard global concurrency. Every later feature is then policy or
optimisation over a proven primitive, not correctness.
