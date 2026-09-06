# Shared execution resources: one new abstraction, a ridiculous number of features fall out

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - the in-process and partition-share rungs have landed; what is still open here is the controller rung, designed below and not built -->

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

## The micro-MVP before even that (owner's checklist, 2026-08-31)

Smaller than the twenty-instance test, and the first observable moment: a user function registers
a 2-tokens-per-second rate-limited service, two instances spawn, the observer reports the
bottleneck as *rate limit* (not CPU, not keys), and each instance is seen firing at 1Hz. One
resource, two nodes, the constraint visibly shared and correctly attributed. The owner's working
name for the branch: the navigator module. Then the v1 below is the scale-up of the same thing.

### The in-process rung landed (2026-09-01)

<!-- post-merge: checked -->
Landed by astubbs/parallel-consumer#392. The checklist above is now
demonstrated, in-process: the seams exist and hold their contracts (`ResourceContract`
registration, per-instance resource tags on the options, ONE application-supplied allocator
shared across instances behind the `ResourceAllocator` interface the distributed plane will
implement); the soft-credit system works as designed (quantum-indexed lazy minting, equal-share
division with membership at lifecycle anchors, spend-after-claim with overdraft absorbing the
races, a conservation ledger whose identity closes at every observation point); attribution
answers "why wait" from the admission subsystem's own logs, `pc.navigator.*` meters and the
read-only `PollContext` view; and the observable moment itself ran wall-clock -
`NavigatorRateShareTest`: two tagged instances at ~1Hz each against a real broker, an untagged
bystander untouched and attribution-free, kill-one convergence to ~2Hz, the aggregate inside the
rate-plus-burst bound. The wall-clock lane earned its keep on the way in: it caught a
control-loop ordering bug (quantum pull after work distribution starved a purely-throttled
instance to a standstill) that the virtual-clock lane structurally could not see.

What the next rung inherits: the Kafka coordination plane behind the SAME `ResourceAllocator`
seam - swap transport, not the seam; useful-demand signals feeding the division (equal share is
the placeholder); and the successor/epoch no-re-mint proof - v1's no-re-mint is determinism plus
ledgers inside one allocator instance, and the epoch-fenced successor-cannot-re-mint-an-interval
guarantee is deliberately still owed by the distributed rung.

### The partition-share rung landed (2026-09-06)

<!-- post-merge: checked -->
Landed with the partition-share rung of astubbs#228, planned in
[`docs/plans/2026-09-05-1046-feat-navigator-partition-share-plan.md`](../plans/2026-09-05-1046-feat-navigator-partition-share-plan.md).
The micro-MVP's promise now holds across JVM boundaries, still with no control plane: the consumer
group's own assignment is the division, and the group's own rebalance re-divides it.

What landed:

- **The allocation strategy is an explicit menu and nothing is chosen silently.** `PARTITION_SHARE`
  is the default, `IN_PROCESS` selects the shared in-JVM stub, `CUSTOM` takes an application
  allocator. An allocator instance supplied without a strategy that uses it fails validation naming
  both fields, so the in-process rung's applications migrate by one deliberate line rather than
  being switched under.
- **A second allocator behind the UNCHANGED `ResourceAllocator` seam.** An instance's share of a
  tagged resource is the fraction of its subscription's partitions it holds, minted locally per
  quantum from the same remainder rotation the in-process allocator uses. The seam's stub rule held:
  the transport changed, the seam did not.
- **The rotation slot is the partition's fleet-stable ORDINAL, never the bare partition index.** The
  ordinal is the cumulative partition count of the subscribed topics sorted by name before the
  partition's topic, plus its own index. The bare index collides across topics, so partition 0 of
  two topics would share slot 0 and the high slots would never be held. The plan review caught this
  before it was built, and a mutation that reverts it is killed by the two-topic test.
- **The assignment crosses the poll and control threads as one immutable snapshot** (held partitions
  plus per-topic totals, captured together) published lock-free from the rebalance callbacks. A
  quantum mints from the newest snapshot published before its start, so a mid-quantum publish
  affects the next index and no index is minted twice on one clock.
- **An unresolved total is declined strictly, never papered over with the previous one.** Any
  timeout, exception, empty or null result from the metadata read publishes an unresolved snapshot,
  which mints nothing. Keeping a stale total at exactly the rebalance that changed it is the one
  case that over-mints; undershoot with loud deferral attribution stays inside the bound.
- **The proof runs across real process boundaries.** A child-JVM harness in the shared integration
  utilities, an asserted two-JVM storyline, and a churn ladder that publishes the overshoot bound as
  a measured curve under both assignment protocols with injected clock skew. The record is written
  by the run and committed under [`docs/test-hardening/`](../test-hardening/) from the CI artifact,
  with the run URL that produced it. [`docs/testing.md`](../testing.md) owns the lane inventory.

The lesson worth carrying, from building the ladder's fleet identity: **a conservation check must
compare against the EXACT per-index entitlement, not against a rotation-averaged share.** The share
gauge a user reads is deliberately the rotation average, because "why am I at 1Hz" wants an average;
summing that average per index and calling it the ceiling is off by the rotation's phase, by up to
half the slots a contiguous block holds. The allocator grew a pure `entitledCredits` read for the
harness to sum instead, and the child samples it once more after its processor stops so the last
minted index is always in the sum.

### The controller rung, designed and not built (R15)

The remaining rung, and the one that reaches astubbs#228's cross-application half. Its allocation
model, in the owner's own words: **clients continuously report the capacity they want; the
partition-owned controller sums the reports and assigns capacity unevenly by request; the total is
the maximum reached over time** - the discovered envelope, rather than a number anybody configures.

What the partition-share rung settled for it:

- **Announcements are keyed by app id, and each app assigns only its own partition.** The announce
  topic's key is the application's identity, so an application writes only where it owns the key and
  the controller reads the fleet from one place.
- **Membership is lease-TTL**, not the consumer group's, because the controller's members are
  applications rather than one group's instances.
- **The fenced-writer question is a `transactional.id` cost question**, not a correctness unknown:
  Kafka does not fence a plain produce by consumer generation, so fencing means a transactional
  writer per control partition, and what is undecided is whether that cardinality is affordable.
- **Policy is even-spread first, demand-weighted second.** Even spread is the placeholder that makes
  the transport provable; the demand weighting is what the model above is for, and it is why
  partition-share's idle-share waste is the controller's problem to solve rather than this rung's.
- **The cross-application case is a global controllers' group**, one level up from the per-app
  group, with the same shape as the per-app one.
- **Pacing derives from the slot ordinal (KD7).** Staggered division was deferred off the
  partition-share rung with the contract left at name, rate, quantum and burst; the ordinal already
  gives every partition a stable phase, so a paced grant is a phase per slot rather than a new
  contract field.
- **Kafka Streams and assignor-carried grants were considered and rejected (KD8).** A compacted
  topic read by a plain consumer is a KTable minus the runtime, and KS would add a dependency, a
  second group and a competing threading model to buy standby state the controller must not need.
  Carrying grants in a custom assignor's user-data looked like the controller for free, but KIP-848
  moves assignment server-side and drops client-side assignors.

Still to decide at that rung: whether the per-control-partition `transactional.id` cardinality is
affordable, and the controller's fallback mode when its owner is unreachable.

## The v1 the conversation itself insisted on

One hard distributed **rate** resource. Kafka-elected owner mints finite short-lived credits;
instances spend locally; unspent credits expire; instance death loses capacity, never creates it;
a replacement coordinator cannot re-mint an interval. Even cheaper first rung: **divide the budget
by active instance count** - membership already comes from Kafka, wasteful under uneven demand but
safe, and no new distributed algorithm at all. The acceptance demonstration: twenty instances
hammering one fake API under churn - **corrected by the cross-model adversarial review
(2026-08-31, finding 1): the honest promise is BOUNDED OVERSHOOT,
not a hard ceiling.** Kafka ownership fences Kafka operations, not the external calls a
GC-paused or partitioned old holder makes with credits it still holds after a successor minted
new ones - and the external service validates no fencing token. So the design needs a durable
issuance ledger and overlap-free epoch transitions (a successor waits for or accounts for every
prior-epoch credit), the contract must say WHICH limit semantics it promises (fixed window /
sliding window / token-bucket average plus burst), and the churn test measures overshoot bounds
under long pauses, partitions, delayed grants, coordinator replacement and clock skew at quantum
boundaries. A true hard ceiling exists only where the downstream itself validates fencing
tokens. Explicitly deferred from v1: mid-window reclamation, multi-resource optimisation,
the adaptive global envelope, hard global concurrency. Every later feature is then policy or
optimisation over a proven primitive, not correctness.

## Additions from the 2026-08-30 exchange - authority, contracts, and stolen theory

The final exchange of the weekend rebuilt this design's foundations on the admission model
([`core-admission-scheduling-model.md`](core-admission-scheduling-model.md)). What follows is the
delta.

### Every waiting primitive collapses into capacity

An exclusive lock is capacity 1; a semaphore is capacity N; a rate limit is capacity that
replenishes; a quota is capacity scoped to a tenant or window; an open circuit breaker is capacity
dynamically zero; a maintenance fence is capacity held at zero until cleared. **Different capacity
functions feeding one admission system** - so the planned KS-backed locks/token-buckets did not
disappear, they moved: build Kafka-backed resource authority as scheduler internals first, and
expose conventional `acquire(lock)` APIs later as thin projections of the same substrate
([`core-internal-machinery-as-features.md`](core-internal-machinery-as-features.md)) for the
things that are not engine-dispatched work.

### Declare on your function, not in your function

The registration API is an **execution contract**: ordering domain, required resources and
quantities (dynamic identities derived from the record - `tenant/{id}`), atomicity requirements,
not-before/deadline, QoS. The engine compiles it into an admission plan before user code runs;
"acquire A, then B, maybe wait" leaves the function body entirely. Contracts are versioned
runtime artifacts (v2 that stops calling Salesforce changes future-demand arithmetic for v2
records), KS topology stages declare them per stage (astubbs#271's API grows this), and waiting
records are indexed by the fact that could make them runnable - a capacity change re-evaluates
the affected candidates, never a scan of the waiting population. The model leaves room for
alternative bundles ((GPU-A or GPU-B) and DB; provider A or cheaper-B) so fallback becomes
proactive admission planning rather than failure handling - explicitly not MVP.

### Knowledge is global, authority is sharded, execution is local

**Wording correction from the supplement, so nobody misreads "local hot path" as "no
scheduler":** there is a scheduler on every execution path - what the design avoids is
*centralized* scheduling. The scheduler is **sharded with the work it schedules**: Kafka
ownership decides where embedded scheduler authority lives, and *scheduler failover follows work
failover*. The compact form: **your applications are the scheduler.**

The owner's correction that reshaped the design: a partition-local scheduler cannot inspect "is
lock X free?" - it only sees its delegated slice. Two resource classes follow: **delegatable**
(token buckets, quotas, pools - the owner hands out chunks, the hot path stays local) and
**authoritative** (N=1 mutexes, tiny-N semaphores - a grant needs request/response with the
sharded owner, but the round trip happens while the work is still WAITing, never inside user
code). Three levels of state: **replicated fact** (global tables: resource definitions,
contracts, fences, epochs, dictionaries), **delegated authority** (local leases, spend without
coordination), **authoritative decision** (the control-shard owner). Control traffic gets
dedicated compacted topics and a hard reserved capacity class - if the estate is on fire, the
messages that reallocate capacity must still flow. One rejected design recorded so it stays
rejected: the offset-race lock (write a claim, win if your record is the key's next) - retries
amplify exactly under contention, and the sharded in-memory owner with a changelog does the same
job without the weirdness.

### Prefetch authority, and let completion release it

Request scarce authority at the *last responsible moment* that still allows immediate dispatch -
lead the request by the measured control-plane RTT so the grant lands as the final predicate
satisfies. Prescience makes lease *placement* schedulable too ("A has 70 future users of X, B has
2 - keep the lease at A"): **authority locality**, cache locality where the cached thing is
authority. Lock lifetime attaches to the work, not the process - PC's sparse completion frontier
is richer release evidence than any committed offset - with lease expiry and fencing epochs as
the failure path, and renewal driven by the engine's own liveness view of the executing work
rather than user code calling renew.

### The two schedulers, cleanly separated (the owner's decomposition)

The multidimensional optimisation lives at the **resource owner**: divide capacity C among demand
streams to maximise aggregate utility subject to QoS/fairness - utility curves *measured* by the
adaptive machinery (astubbs#333's probes answer "when this workload got 20 more units, how much
useful throughput appeared?"). Record selection is **downstream and local**: spend the allocation
on the best admissible work. The loop: allocate -> execute -> measure marginal utility ->
reallocate. And v1 needs no solver - ranked first-fit over requirement vectors
(`requirements <= available`, component-wise; reserve all-or-nothing; skip and try the next
candidate) is the whole MVP scheduler. Steal packing algorithms only if measurement shows greedy
leaving real capacity unused.

### The stub rule and the seam inventory (from the handoff document)

**Architectural stubs preserve future seams: an in-memory implementation may stand in for a
distributed one only if it obeys the same semantic contract** - the handoff's critical rule, and
the licence for building v1 locally without painting over the distributed future. The handoff's
section 24 carries the candidate seam inventory (WorkSource, WorkEnvelope, OrderingDomain,
AdmissionController, ResourceContract, DemandSignal, CapacityLease, ResourceAllocator,
CapacityProvider, Actuator, Decision/Explanation, ActorAddress) - the vocabulary implementation
should start from rather than invent
([`docs/ideation/2026-08-29-hasten-compound-engineering-handoff.md`](../ideation/2026-08-29-hasten-compound-engineering-handoff.md)).

### Theory to steal, not reinvent (the literature dive's verdicts)

- **Conservative 2PL / preclaiming**: declare the full resource set before execution, acquire
  all-or-nothing, no hold-and-wait, no deadlock. Its historical weakness - you must know the full
  set in advance - is exactly this design's natural state: contracts declare it, Kafka holds the
  work before execution, Prescience sees millions of declarations ahead. *The old CS says "if
  only we knew"; this system says "we do."*
- **Calvin**: deterministic conflict ordering validates the shape - but only order where claims
  intersect (a partial order), not globally. A single-partition Kafka topic is a gloriously
  boring durable sequencer, used **only** for conjunctive multi-resource claims; single-resource
  work never needs a global order. With a common claim sequence, every resource queue sorts
  identically and A-before-B holds everywhere they conflict - deadlock-free ORDERING without
  negotiation, which (per the GitHub Codex review, 2026-08-31) is not the whole protocol: atomically reserving
  capacity across separately owned resources, and deciding release-vs-commit after an owner
  fails, still needs a grant/commit/fencing exchange. The sequence removes the ordering
  negotiation, not the agreement protocol.
- **C/D-RAS** (conjunctive resource allocation) for the safety/liveness mathematics; **advance
  reservation / co-allocation** (grid) for the Capacity Horizon; **DRF** for multi-resource
  fairness. Separation of concerns: preclaiming gives safety, DRF gives fairness, the objective
  ("which feasible allocation best advances useful future work") is the part that is ours.
- The design objective all of it serves: **do all distributed arbitration before the work becomes
  runnable, so the final admission decision is local** - coordination latency hidden under
  waiting the work was doing anyway, the scheduling analogue of CPU prefetching.
- **These are analogies, not transferred guarantees** (cross-model review 2026-08-31, finding 8): preclaiming's "we do know the full set" assumes declarations are complete -
  dynamic application dependencies and preclaiming's utilization/starvation costs do not vanish;
  Calvin's guarantees rest on deterministic locking, replication and known access sets that a
  single sequencer topic alone does not supply (grants, authority failure, cancellation and
  atomic release remain ours); DRF equalizes dominant shares - it does not optimize marginal
  utility, replenishment, priorities, alternative bundles or temporal reservations; and
  CockroachDB's admission control already covers more of this policy surface (slots, tokens,
  dynamic adjustment, priorities, multitenancy) than the earlier characterisation acknowledged.
  Anything not proved under leases, failures, dynamic declarations and indivisible work is
  original research, and should be written as such.

## Implicit detected dependencies - the scaler as dependency prober (owner idea, 2026-09-01)

Undeclared resource dependencies can be **discovered experimentally by the auto-scaler**. If
function A declares no resources, but scaling A out slows function B - and B *does* declare
resource X, and X's observed rate degrades as A scales - the system can infer that A carries an
implicit dependency on X. The navigator micro-MVP already ships the instruments this needs: the
per-resource rates and conservation counters are the signal, and the scaler's deliberate
parallelism changes are the controlled perturbation (the same control-arm discipline
`docs/investigating.md` demands of humans, run by the machine).

Modelling rules, as stated by the owner:

- An inferred dependency is a **separate class from a declared one** - "implicit detected", never
  silently promoted to explicit - but it **participates in optimisation calculations exactly as
  if declared**, so the allocator can stop A's scale-out from starving B's contract on X.
- It **expires**: an implicit dependency has a TTL and must keep being rediscovered, so a deploy
  that removes the hidden coupling is forgotten rather than haunting the model.
- Before acting on one at higher stakes (isolation decisions), the probing scaler can **re-test
  it** - perturb A deliberately and confirm X still responds - so the inference is re-verified at
  the moment it matters, not just remembered.

This directly narrows the closing caveat above: preclaiming's "declarations are complete"
assumption becomes "declarations are complete *or detectable*". It also gives the declaration
surface a graceful adoption story - functions that never declare anything still get modelled,
with declared contracts remaining the stronger, cheaper, always-current path. Fits with
[`core-capacity-fingerprinting.md`](core-capacity-fingerprinting.md) (an inferred dependency is a
fingerprint fact worth persisting, stamped with the environment it was measured under and its
expiry) and [`core-auto-scaling.md`](core-auto-scaling.md) (the prober is the same controller
that already runs scale-out experiments).
