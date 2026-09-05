---
title: Navigator Partition-Share - Plan
type: feat
date: 2026-09-05
topic: navigator-partition-share
artifact_contract: ce-unified-plan/v1
artifact_readiness: requirements-only
product_contract_source: ce-brainstorm
execution: code
---

# Navigator Partition-Share - Plan

## Goal Capsule

- **Objective:** Every instance of one application collectively stays within a tagged resource's declared rate and burst across process boundaries - the overshoot bounded and priced, never asserted - with nothing new to run and nothing new to configure. This is astubbs#228's ask, delivered for one application: the navigator micro-MVP's promise, kept across JVMs.
- **Means:** Each instance takes its share of a tagged resource from the fraction of the subscription's partitions it holds, mints that share locally per quantum exactly as the in-process allocator mints from membership today, and lets the consumer group's own rebalance re-divide the rate when instances come and go. No control plane.
- **Product authority:** This plan, then `docs/inflight/core-shared-execution-resources.md` for the design it instantiates and `docs/plans/2026-08-31-2029-feat-navigator-micro-mvp-plan.md` for the seams and settled decisions it inherits. The controller rung (across-app sharing, demand-weighted allocation, pacing) is not active scope; its design is recorded in How This Work Fits Together and in the owning notes, not built.
- **Stop conditions:** Stop and surface rather than work around if computing the instance's partition fraction needs a broker round trip on the quantum or record path (one metadata read per rebalance, on the poll thread, is the budget - see Dependencies; anything per quantum breaks the "nothing distributed on the record path" line), or if the multi-process lane cannot be made to hold its bound under the no-retry CI policy - do not loosen the bound to go green (see R12 and `docs/testing.md`).
- **Open blockers:** None.

---

## Product Contract

### Summary

Global rate limiting across every instance of one application, with no control plane: share by partition fraction, mint locally, re-divide on rebalance. Proven across real JVM boundaries by a kill-one storyline and a churn ladder that publishes the overshoot bound as a measured curve, including the clock-skew term the in-process rung never had.

### Problem Frame

The in-process rung (astubbs/parallel-consumer#392) proved the seams and the credit model inside one JVM: one application-supplied allocator, shared by reference. astubbs#228's ask - two independent upstream reports behind it - is N processes respecting one limit, and today each of those processes either divides the rate by hand and gets it wrong the first time an instance dies, or drags in a shared cache to coordinate. The design record's own "even cheaper first rung" was to divide the budget by membership Kafka already provides. The consumer group already tells every member exactly what it holds; nothing yet turns that into a share of a resource.

### Key Decisions

- KD1. **Across one application first; across applications deferred.** One application means one consumer group, and the group's assignment is the membership fact. (session-settled: user-directed - chosen over building the cross-application version now: it is the same shape plus a global controllers' group and per-app announce partitions, and it waits on the controller rung.) Governs R1, R15.
- KD2. **Partition-share finishes the MVP; no controller, no control topic.** The group coordinator already divides the work, so it divides the rate for free; demand-weighting waits for the rung that needs a controller anyway. (session-settled: user-approved - chosen over building the controller with even-spread as its first policy, and over staging both in one plan: skateboard first, and the second half would be planned before the first had taught anything.) Governs R2, R3, R4.
- KD3. **Share is the partition fraction of the subscription, not the instance count.** A crude demand proxy - an instance holding three partitions has roughly three partitions' worth of work - and the only number the assignment hands every member without a round trip. (session-settled: user-approved - chosen over dividing by live member count, which needs a group-describe round trip for a number the assignment already implies.) Governs R2, R5, AE3.
- KD4. **The proof is two JVMs plus a churn ladder that prices the bound.** Kill-one convergence across real process boundaries, then N processes joining and leaving under both rebalance protocols while the aggregate overshoot is measured and published. (session-settled: user-directed - chosen over the kill-one storyline alone: the bound gets a number, and the register's "instance count the acceptance tests use" item is answered by a ladder instead of a constant.) Governs R11, R12, R13.
- KD5. **Clock skew is priced, not assumed away.** Partition-share mints from each JVM's own clock, so a partition moving at a quantum boundary between skewed clocks can be minted twice for one quantum index; the ladder injects skew and the stated bound carries the term. (session-settled: user-approved - chosen over documenting a synchronised-clocks precondition, and over designing skew out, which is a planning-stage investigation with this as its fallback.) Governs R8, R12.
- KD6. **An explicit strategy menu, defaulting to the most sophisticated strategy available.** The idea-5 menu is visible from day one - partition-share, the in-process stub, a custom allocator - and its default on this rung is partition-share, so tags alone work out of the box while choosing the stub or a custom allocator is a deliberate act. (session-settled: user-directed - chosen over tags-only zero-config with no menu, and over construct-and-pass as today: nothing is chosen silently, and nothing has to be configured by default.) Governs R6, R7.
- KD7. **Staggered division ships with the controller rung, not here.** The contract stays `name`, `ratePerSecond`, `quantum`, `burst`, and this rung states what those three promise rather than adding a pacing field. Partition-share would give a phase per shard for free (slot = partition index) and that derivation is recorded for the next rung. (session-settled: user-directed - chosen over shipping pacing as a per-resource option now, or on by default for unknown window shapes: smallest surface, and the contract schema question is answered by stating the existing promise.) Governs R8.
- KD8. **No Kafka Streams, and no assignor-carried grants - on this rung or the next.** A compacted topic read by a plain consumer is a KTable minus the runtime; KS would add a dependency, a second group, and a competing threading model to buy standby state the controller must not need. Carrying grants in a custom partition assignor's user-data looked like the controller for free (the leader computes, the generation fences) but KIP-848 moves assignment server-side and drops client-side assignors. (session-settled: user-approved - the owner asked whether KS was overkill and agreed it is.) A framing decision for the deferred design; no governed R.
- KD9. **Two gating decisions are recorded as taken, because the code took them.** The enforcement fork was settled by the micro-MVP's KD1 (admission predicate with `availableAt` deferral), and standalone-first was settled by the owner's 2026-09-01 ruling that the finished navigator is the rate-limiting feature; `docs/inflight/core-distributed-throttling.md` says so in this rung rather than after. Governs R15.

### Actors

- A1. **Application developer** - registers the resource contract, tags the instance, and picks (or accepts the default) allocation strategy.
- A2. **Operator** - runs N instances of the application, watches the resource's aggregate rate and each instance's share, adds and removes instances.
- A3. **Group coordinator** - the broker-side authority whose assignment decides which partitions, and therefore what share, each instance holds. A system actor; nothing in this rung talks to it beyond what the consumer already does.

### Requirements

**Share and minting**

- R1. A resource tagged by instances of one consumer group is shared among exactly those instances; instances in other groups tagging the same name are independent on this rung.
- R2. An instance's share of a resource for a quantum is the resource's rate over that quantum, scaled by the fraction of the subscription's partitions the instance holds - assigned partitions over total partitions, summed across every subscribed topic. The same fraction scales the instance's burst budget, rounded up so a holder of at least one partition keeps a budget of at least one credit: burst is a fleet-wide budget divided by share, never a per-process budget. Instances that tag the same resource subscribe to an identical topic set, and a mismatch fails validation before any share is minted; untagged members of the group may subscribe to anything.
- R3. Over any window starting at a quantum boundary under stable assignment, the fleet's minted credits never exceed the resource's rate over that window; across a rebalance, R8's bound governs instead. A share below one whole credit per quantum is minted by the partition-indexed remainder rotation - each partition's slot in the in-process allocator's rotation, taken over the subscription's total partitions - so instances holding fractional shares never coincide above the grant, and no holder of at least one partition starves indefinitely.
- R4. Share follows the assignment: a revoked partition's share stops being spent from the moment of revocation, and a newly assigned partition's share is first minted at the next quantum boundary after assignment - the same next-quantum rule the in-process allocator applies to a joining member.
- R5. An instance holding no partitions has no share; any tagged work it somehow holds is deferred with the existing resource attribution, not admitted.

**Configuration surface**

- R6. The options carry an allocation-strategy choice with three values - partition-share, the in-process allocator, a custom allocator instance - and an absent choice resolves to partition-share; selecting the in-process or custom strategy without the allocator instance it needs fails validation naming the omission. The fleet-wide guarantee - rate, burst, and the R11 and R12 proof - is scoped to a uniform partition-share configuration across the group; the in-process and custom strategies are local opt-out modes on this rung and provide only what their allocators implement. The options also carry the resource contracts to register (name, rate, quantum, burst): under partition-share the engine registers them against the allocator it builds, applying R7's fail-fast rules at validation; under the in-process or custom strategy a contract supplied here and one already registered on the instance must be identical, or validation fails naming the collision.
- R7. Contract registration and its fail-fast rules (unknown tag, conflicting policy under one name, unusable policy) apply per instance exactly as in the in-process rung; two JVMs registering different policies under one name is not detected on this rung and the documentation says so.
- R8. The contract's promise is stated where the contract is documented: credits per quantum equal rate times quantum, burst is the fleet-wide overdraft budget divided by share per R2, R3's rule holds under stable assignment, and across a rebalance the aggregate may exceed rate by at most burst plus one quantum's credits plus a clock-skew term - the term the ladder publishes per R12. The bound names its supported process-count and clock-skew domain and the deterministic rule by which R12's measurements become the number a user reads; outside that domain no bound is claimed. The documentation also states the utilisation property: an idle partition's share expires unspent, so under uneven demand the fleet runs below rate by the idle fraction, and demand-weighted allocation (the controller rung) is the remedy. The documentation also states the scope: a resource is shared among the instances of one consumer group only; a second group tagging the same name receives the full rate again, so the effective aggregate multiplies by the number of groups; cross-application sharing is the controller rung's scope.

**Attribution and observability**

- R9. Every existing resource-wait explanation (decision reasons, deferral log, `pc.navigator.*` meters, the read-only context view) holds unchanged, and an instance's current share of each tagged resource - fraction and credits per quantum - is observable alongside them, so "why am I at 1Hz" is answerable from one instance.
- R10. The conservation identity holds per instance as today; the aggregate identity across instances - total minted across the fleet never exceeds the fleet's summed shares - is asserted by the multi-process lane rather than by any instance.

**Proof**

- R11. The asserted twin runs two separate JVM processes against one broker: two tagged instances on a 2-credits-per-second resource each fire at about 1Hz, an untagged bystander drains unthrottled with no navigator interaction, one process is killed and the survivor converges toward 2Hz through the group's rebalance, and the aggregate stays inside the R8 bound throughout. CI-gated, under the no-retry policy. The acceptance envelope - rate tolerance, observation window, minimum sample count, and a convergence deadline after the rebalance - is predeclared before implementation and shared by this requirement and the acceptance examples, so "about 1Hz" has one meaning fixed before any result is seen.
- R12. A churn ladder runs N processes (a ladder of N) joining and leaving repeatedly under both the cooperative and the eager assignment protocol, with a test-only clock offset injected per process, and publishes measured aggregate overshoot against N and against skew; the R8 bound holds at every rung, and the published curve - not a constant - is the record of the bound. Aggregate emission is timestamped by the observing harness on its own clock (or by broker log-append time), never by the emitting process's offset clock, so the published overshoot measures the mechanism and not the injected offset.
- R13. `bin/demo-navigator.sh` runs the R11 storyline across two JVMs as a per-second dashboard, under the rules in `docs/demos.md`: the demo is the eyes-optimised twin of R11 and never the evidence.
- R14. The machinery that launches, observes and kills PC processes for R11 and R12 lands in the shared test utilities as reusable infrastructure, not inside one test class.

**Record-keeping**

- R15. The controller rung's design is recorded in `docs/inflight/core-shared-execution-resources.md` as the next rung, with what this rung settled: announcements keyed by app id with each app assigning only its own partition, lease-TTL membership, the fenced-writer question as a `transactional.id` cost question, even-spread then demand-weighted policy, a global controllers' group for the cross-application case, slot = partition index as the pacing derivation, and KS and assignor-carried grants as considered and rejected. The controller rung's allocation model is recorded in the owner's words: clients continuously report the capacity they want; the partition-owned controller sums the reports and assigns capacity unevenly by request; the total is the maximum reached over time, the discovered envelope. `docs/inflight/core-distributed-throttling.md` records the two KD9 decisions as taken.

### Key Flows

- F1. **Steady state**
  - **Trigger:** An instance's control loop ticks.
  - **Actors:** A3 (implicitly, through the assignment already held)
  - **Steps:** The allocator reads the instance's partition fraction from the assignment the engine holds; the quantum pull mints the share for the current quantum index once (R2, R3); claims spend locally and defer to the next credit time as today; the share is visible on the view and meters (R9).
  - **Outcome:** N instances collectively emit at about the resource's rate, each at its fraction.
- F2. **An instance leaves**
  - **Trigger:** A process is killed, or closes.
  - **Actors:** A2, A3
  - **Steps:** The coordinator rebalances; survivors receive the departed partitions in `onPartitionsAssigned`; their fractions rise and their shares are minted at the next quantum boundary (R4); the departed instance's unspent credits die with it.
  - **Outcome:** The aggregate converges to the rate within one rebalance plus one quantum, overshoot inside the R8 bound.
- F3. **An instance joins**
  - **Trigger:** A new process subscribes.
  - **Actors:** A2, A3
  - **Steps:** The coordinator rebalances; the holders of the partitions it takes stop spending those partitions' share at revocation (R4); the joiner first mints at the next quantum boundary after its assignment.
  - **Outcome:** The aggregate never exceeds the rate by more than the R8 bound; the joiner reaches its fraction within one quantum of assignment.
- F4. **Startup with the default strategy**
  - **Trigger:** A1 tags a resource and sets no strategy.
  - **Actors:** A1
  - **Steps:** Options validation resolves the strategy to partition-share (R6); the engine builds the allocator against its own consumer and registers the options-supplied contracts against it under R7's rules.
  - **Outcome:** Global rate limiting with no allocator code in the application.

### Acceptance Examples

- AE1. **Two JVMs share the rate.** **Covers R1, R2, R11.** Given two processes in one group, one topic of two partitions, both tagging `api-x` at 2/s with quantum 1s and burst 2, and an untagged bystander in the same group consuming its own topic (the in-process demo's shape) so the tagged pair are the only members tagging the resource, when both tagged processes are consuming a backlog, then each fires at about 1Hz and the bystander is unthrottled with zero navigator interaction.
- AE2. **Kill one, the survivor inherits.** **Covers R4, R11.** Given AE1 in steady state, when one process is killed, then after the rebalance the survivor fires at about 2Hz and the aggregate over the rebalance window stays inside rate plus burst plus one quantum plus the skew term.
- AE3. **Uneven partitions, uneven shares.** **Covers R2, R3.** Given four partitions assigned three-to-one, when both instances have backlog, then they fire at about 1.5Hz and 0.5Hz - the half-credit share accruing to one credit every other quantum, never rounding to zero.
- AE4. **Skew is inside the bound.** **Covers R8, R12.** Given the ladder with a positive and a negative clock offset injected on two processes, when a partition moves between them at a quantum boundary, then the measured aggregate overshoot exceeds the skew-free bound by no more than the stated skew term.
- AE5. **No partitions, no share.** **Covers R5.** Given more instances than partitions, when an instance holds nothing, then its share is zero, it mints nothing, and it emits no resource-wait attribution because it holds no work.
- AE6. **The default is partition-share.** **Covers R6.** Given tags and no strategy, when the options validate, then partition-share is selected; given the in-process strategy and no allocator instance, then validation fails naming the missing instance.
- AE7. **Conservation across the fleet.** **Covers R10, R12.** Given any rung of the ladder, when the run ends, then the sum of every process's minted credits is at most the sum of their shares over the run, and each process's own identity balances as it does today.
- AE8. **Idle share expires unspent.** **Covers R8.** Given four partitions assigned two-to-two, when only one instance's partitions have backlog, then the fleet fires at about half the rate and the other instance's credits expire unspent - asserted, not merely tolerated.

### Success Criteria

- The overshoot bound exists as a published curve from R12 - against N and against skew - and the number a user reads in the contract's documentation is traceable to that curve.
- The multi-process lane is green under the no-retry policy on the hosted runner across the ladder, with no loosened assertion and no quarantine entry.
- `bin/demo-navigator.sh` shows the R11 storyline across two JVMs in about the time the in-process demo takes today.
- astubbs#228 can record its one-application half as shipped; the cross-application half stays open and points at the controller rung.
- A representative one-application deployment removes manual rate division and shared-cache coordination, and stays within the bound through instance joins and leaves with no operator reconfiguration - the workaround the Problem Frame names is gone, not just measured around.

### Scope Boundaries

**Deferred for later rungs**

- The controller: metrics topic, announce topic, lease-TTL membership, the fenced writer, even-spread and then demand-weighted allocation, and cross-application sharing through a global controllers' group.
- Staggered division (a paced grant with a phase), the downstream 429 signal, hard global concurrency, non-Kafka participants and the credit-vending endpoint, a safe-capacity fallback when a vendor is unreachable (no vendor exists on this rung), and the twenty-instance conservation test - the ladder is its CI-scale form.
- Detecting a contract-policy mismatch across JVMs - a replicated-fact problem that belongs with the controller.

**Outside this product's identity**

- A shared cache or a second coordination service as the limiter's substrate. The Kafka client is the only dependency; a limiter that needs Redis is the thing astubbs#228's mirror warned against.

<!-- ce-section: work-relationships -->
### How This Work Fits Together

This plan owns the partition-share rung only; the breakdown below is the current understanding, not a committed roadmap.

- **Depends on** astubbs/parallel-consumer#392, the in-process rung, for every seam it reuses - the allocator interface, the quantum pull, spend-after-claim, attribution, the context view, the two test lanes.
- **Depends on** astubbs/parallel-consumer#367 for the design record it cites and extends (R15); the corpus is read from that branch, not imported here.
- **Enables** the controller rung, which replaces the assignment fraction with an announced allocation behind the same allocator interface and inherits the multi-process lane (R14) as its harness.
- **Shares** the admission seam with the adaptive concurrency controller (astubbs/parallel-consumer#333); the two still compose by conjunction, and the min-composition of a declared ceiling with a discovered one remains the strategy-menu shape of idea 5.
- **Can proceed independently of** the non-Kafka participants and standalone-deployment work; nothing here shapes the vending surface.
- **Still to decide**, at the controller rung: whether per-control-partition `transactional.id` cardinality is affordable (the fencing correction), and the controller's fallback mode when its owner is unreachable.

### Dependencies / Assumptions

- Base is `feats/hasten-micro-mvp` at astubbs/parallel-consumer#392's tip; the stack's master catch-up stays deferred, and this branch does not merge master on its own.
- The engine holds R2's numerator - the assigned-partition count, maintained in the rebalance callbacks (`numberOfAssignedPartitions` in `AbstractParallelEoSStreamProcessor`) - but not its denominator: nothing in the engine reads a topic's partition count today. The total per subscribed topic is read from the consumer's cached topic metadata (`partitionsFor`) on the poll thread inside the rebalance callbacks, once per rebalance and never on the quantum or record path, and refreshed at every assignment so a partition expansion re-divides the rate at the rebalance it triggers; for a pattern subscription it is the partition total of the topics matched at that refresh. Group metadata is exposed through `ConsumerManager`; manual `assign()` is not supported anywhere in the engine, so "the assignment" always means a consumer group's.
- The rebalance callbacks run on the poll thread and the quantum pull runs on the control thread, so the share crosses the two-thread boundary that `docs/solutions/runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md` is written against (its master sibling on the commit seam, "two threads, one consumer", is not on this branch yet); planning reads it before choosing how the assignment reaches the allocator, and the engine's `@GuardedBy` rule applies to whatever holds it.
- No test today launches a second JVM running Parallel Consumer, and nothing in the main code creates topics or uses an admin client - R14's harness and R12's ladder are new infrastructure.
- The partition-share allocator keeps no clock of its own: every navigator call takes its instant from the engine module's clock, which the existing admission tests already override through a module subclass - that is how R12 skews a process, by launching it through a module subclass whose clock carries the offset. The virtual-clock lane's shared clock is `org.threeten.extra.MutableClock`, a dependency rather than a repo class, and the in-process allocator's constructor clock remains that lane's hook only.
- The build runs on kafka-clients 3.9.2 and nothing references the KIP-848 group protocol yet; partition-share depends on nothing the new protocol removes.
- Production clocks are assumed NTP-class; the R8 skew term states what happens when they are not, rather than requiring that they are.
- The partition proxy assumes tagged demand is spread roughly evenly across the subscription's partitions; a subscription mixing a busy tagged topic with quiet untagged ones under-uses the rate, and demand-weighting is the controller rung's answer.

### Outstanding Questions

**Resolve Before Planning**

- None.

**Deferred to Planning**

- How the allocator learns the assignment - the rebalance listener the engine already registers, wired through the module - and the exact shape of R6's strategy option.
- How R14 launches and kills processes (a process builder against the test classpath, or containers), and where R12 publishes its curve so it outlives a CI log.
- Whether the R9 share is one gauge per resource or folded into the existing ledger meters.
- The values of R11's acceptance envelope, calibrated on the target hardware as the predecessor plan rules, and the supported domain and measurement-to-bound rule R8 requires R12 to publish.

### Sources / Research

- `docs/inflight/core-shared-execution-resources.md` - the design this rung instantiates, including its own "even cheaper first rung: divide the budget by active instance count".
- `docs/inflight/core-distributed-throttling.md` - the gating decisions and idea 5's strategy menu.
- `docs/plans/2026-08-31-2029-feat-navigator-micro-mvp-plan.md` - the seams, KD1 through KD11 and KTD3, KTD4, KTD7, all inherited.
- `docs/plans/2026-09-01-001-handoff-navigator-session-two-rate-limiting.md` - what the in-process rung hands over, including the proof obligations.
- `docs/ideation/2026-08-17-distributed-throttling-ideation.html` - idea 5 (min-composition of ceilings), the prior-art autopsy ("nobody in the Kafka consumer-framework space divides a downstream budget using the consumer group itself"), and the deferral of a custom assignor as adoption friction.
- `docs/demos.md` - the asserted-twin and simulated-work rules R13 runs under.
- astubbs/parallel-consumer#367, branch `docs/codex-strategy-conversation`, read from the ref: the fencing correction (Kafka does not fence a plain produce by consumer generation), the open-research register's "the resource contract has no schema" item, staggered division and its prior art (RLQS and Doorman divide quantity, never time), Doorman's lease and fallback modes as the free design review for the controller rung, and uForwarder as the closest comparator - which divides per topic-and-group, leaving the named resource shared across unrelated workloads as the surviving edge.
<!-- file-refs: N/A - the register, the sweep and the corrected notes live on astubbs/parallel-consumer#367's branch, not on this one; cite them there -->

---

## Deferred / Open Questions

### From 2026-09-05 review

- **An allocator passed without a strategy is silently thrown away** — R6 (the allocation-strategy option) and AE6 (P1, product-lens, adversarial, confidence 100)

  Every application written against the in-process rung passes an allocator and no strategy - that was the only valid shape - and R6's default resolves that configuration to partition-share, so the allocator the application built is never consulted and nothing says so. That is the silent choice KD6 (the explicit strategy menu) forbids, and the branch's own multi-instance tests are written that way. The proposed remedy was to fail validation on the pairing, naming both fields; inferring the custom strategy from the instance's presence is the other reading, and it is itself a silent choice.
