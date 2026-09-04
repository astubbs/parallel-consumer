# Hasten adjacent systems: which claims are occupied, and by whom

<!-- inflight-type: register -->
<!-- inflight-impact: misdirection -->

The living view over what already exists in the engine's space. Sibling of
[`ci-inflight-adjacent-systems-register.md`](ci-inflight-adjacent-systems-register.md), same
contract and the same three evidence marks, different question set - because the questions that
discriminate here are about scheduling, not about knowledge.

**It is a register because it is consulted, never completed.** The first sweep ran 2026-09-05 and
[`docs/plans/2026-09-05-001-investigate-hasten-prior-art-sweep.md`](../plans/2026-09-05-001-investigate-hasten-prior-art-sweep.md)
**owns those findings** verbatim, including the ten claims that failed verification and the two
citation defects that must be fixed before anything is published. This note owns what changes from
here, and never restates a finding that document holds.

## Why it exists, and what it cost to not have it

[`core-engine-thesis.md`](core-engine-thesis.md) already carried the instruction - a single external
model's novelty search, *"recorded as a lead and not a survey; do the prior-art sweep before any
public 'first' claim"* - and it went unexecuted while the corpus grew. `docs/w2-vision.md`'s risks
register names the same hazard from the other side as **frictionless-derivation bias**: a medium with
no adversarial pressure, where the derived half accumulates detail and typographic confidence without
a falsifier ever running. The sweep is that falsifier, and it landed on the derived half.

## The evidence rule

Identical to the InFlight register's, and it is the reason a row can be trusted:

- **verified** - run, or read from primary source, by someone here, with the document cited.
- **surveyed** - read from the system's own published sources by a sweep, but not run.
- **claimed** - asserted and not checked anywhere. A lead.

The 2026-09-05 sweep additionally used **adversarial 3-vote verification**: each extracted claim went
to three independent verifiers and needed two refutals to be killed. Ten were killed. Rows below
carry the vote where one exists, because a 2-1 is not a 3-0.

## The discriminating clause, tested once and answered

**Envoy answers NO, and that is the most useful result the register holds.** Researched 2026-09-05:
its adaptive concurrency is adaptive but **strictly local**; RLQS is global but its upward signal is
**demand, never performance**; and **the two subsystems never touch** - the minRTT one discovers is
published nowhere and the other never reads it. Nothing in Envoy closes the loop from *measured
service time* to *global allocation*. It defines an interface where one could live and implements
none of it.

That is the first time the clause has been put to a system properly, and it survived. It does not
generalise - Arktos, IBM's LLA and Hadar are unexamined and are exactly the rows most likely to
answer yes - but it means the clause **discriminates** rather than merely sounding good, which is
what a discriminating clause has to prove before it can carry weight.

## The falsifier

**This register had no falsifier until 2026-09-05**, which is an odd omission given its InFlight
sibling has carried one throughout, and it meant the question set below had nothing to serve. From a
handoff document dated 2026-09-04:

> **Find a system that can sit transparently under an existing Kafka application, separate partition
> ownership from semantic ordering and execution, schedule record/key/order-domain work locally,
> adapt concurrency, coordinate named shared resources through delegated authority, retain unresolved
> work in causal position while unrelated work proceeds, look ahead across the committed backlog
> using semantic execution knowledge, explain every wait, and fail scheduler authority over with work
> ownership - without requiring a separate execution cluster or a new application programming model.**

**If such a system exists and is mature, study or join it before rebuilding it** - the same posture
the InFlight register takes, and for the same reason. Refine the falsifier as the claims sharpen; it
is a live instrument, not a slogan.

**Run against uForwarder, 2026-09-05 - it passes more clauses than anything else found.** It
separates ownership from execution, schedules record work with adaptive concurrency, coordinates
divided capacity spent locally, and explains every wait. **It fails three clauses, and they are the
ones to lead with:** it does not sit transparently under an existing Kafka application (the app is
rewritten into a gRPC server), it requires a separate execution cluster (controller plus workers plus
ZooKeeper), and it has **no semantic ordering at all** - the falsifier's *separate partition ownership
from semantic ordering* clause cannot be satisfied by a system that surrendered ordering to get
concurrency. That is the falsifier working: it discriminated on a real system rather than a
hypothetical one.

## Five standing falsifiers for the architecture itself

Distinct from the prior-art falsifier above: these are the ways the design could be *wrong* rather
than *already built*. Recorded together because a register that only asks "does this exist" misses
the other half of the risk.

1. Shared scheduler primitives do not actually simplify the derived features - each needs its own
   parallel subsystem after all. **This is the one the composition test in
   [`../w2-vision.md`](../w2-vision.md) is designed to detect early.**
2. Deep Prescience provides little value over a modest lookahead buffer.
3. Transparency collapses, and users end up redesigning around a framework after all - which would
   break *keep your code, replace the runtime underneath it*.
4. Kafka's physical abstractions leak so badly that virtualisation becomes Kafka-squared.
5. An existing system already solves the same architectural problem well enough that the right move
   is to join or build on it. **That is what this register exists to detect**, and the first sweep
   made it less hypothetical rather than more.

## The question set - ask every system the same eight

1. **What does it schedule** - requests, queries, jobs, records, packets, invocations?
2. **What does it own**, and what does it merely observe?
3. **Where does it sit relative to the dispatch boundary** - above it, at it, or under it?
4. **Does the application have to be rewritten** into its model?
5. **Is there a cluster, a server, or a database in the per-call hot path?**
6. **Is capacity delegated, borrowed, or arbitrated centrally** - and on what substrate?
7. **What happens when the global half is unreachable** - stall, degrade, or over-admit?
8. **Does it reason about work it has not dispatched yet**, and from declared requirements or
   reactive signals?

Question 5 is the one that has so far separated this design from every durable-execution runtime;
question 8 is the one nothing found answers the way this design proposes to.

## Read this before the table below

**Nothing here was ever a claim.** This corpus explores product space: *X would be good - does anyone
do it, how, and if not why not?* The sweep was run in adversarial mode because that is what makes a
language model search hard rather than agreeably; **"refuted" and "disproved" are instructions to the
instrument, not conclusions about what we believed.** A row reading REFUTED means *this question is
already answered, and here is who answered it* - not *we thought we invented this and were wrong*.
That distinction is the difference between a useful register and a false history, and the dated
sweep's own verdict language is preserved in
[`docs/plans/2026-09-05-001-investigate-hasten-prior-art-sweep.md`](../plans/2026-09-05-001-investigate-hasten-prior-art-sweep.md)
as the instrument's output rather than as ours.

## Why idea-novelty is close to irrelevant here

The thing that matters is **whether the implementation is novel and useful in its placement**, and
placement is the whole argument. Two audiences, and they are not equally important:

- **Primary: teams already running Kafka**, who are not shopping for a scheduler at all. For them
  this arrives as capabilities they get *for free* - no new cluster, no second system to operate, no
  application rewrite - because the runtime is already sitting where their work is dispatched.
  Doorman having invented lease-vending in no way helps that team: they were never going to adopt
  Doorman, deploy its server tree and rewrite their call sites.
- **Byproduct: teams already on a scheduler or orchestrator.** Becoming attractive to them is a
  consequence of being good, not the goal. **Pulling people off an existing scheduler is explicitly
  not the strategy**, which is why "someone else already implements this mechanism" costs far less
  than it appears to.

So the useful question a row answers is not *is the idea new* but **is anyone delivering this to
somebody who already has Kafka and did not ask for a scheduler.** Every occupied row so far answers
no, and each one requires the user to adopt a whole new system to get the capability.

## The search families - eighteen, replacing the first sweep's five angles

The first sweep's angles were mechanism-shaped and Kafka-blind, which is how uForwarder was missed.
From the same 2026-09-04 handoff, and to be used as the family list for the next sweep:

Kafka consumer proxies and decoupled-consumption systems · Kafka queue and work-distribution systems
· record- and key-level schedulers · adaptive concurrency and admission-control systems · distributed
shared rate and resource controllers · embedded workflow, actor and RPC runtimes · Kafka-backed
workflow systems · Kafka Streams alternative runtimes · dataflow schedulers · durable execution
systems · Orleans/Akka/Temporal-style actor and workflow runtimes · distributed admission control and
lease systems · work-conserving schedulers · look-ahead schedulers · semantic indexes over queues and
logs · virtual partition and hot-key extraction systems · transparent Kafka client replacements and
proxies · systems deriving coordination primitives from Kafka ownership or log state.

**For every candidate, classify rather than score**: competitor, substrate, integration, historical
prior art, or something to join and build upon. A row that only carries a verdict has thrown away the
useful half.

## The state per question, after one sweep

| Question explored | Already answered? | By whom, and what it costs their user |
|---|---|---|
| **1. Admission is a scheduling state, not an execution state** | **Answered** (3-0 x4) | Kueue, CockroachDB, Impala, Restate. Only the scoping clause survives: none admit records already durable in an external log somebody else owns |
| **2. Delegated capacity leases, no cluster, no per-call permit server** | **Answered in part** | Doorman occupies the mechanism outright - renewable time-bounded leases to an embedded client that decides in-process. Residual: **the durable log as coordination substrate**, which Doorman (server tree + etcd), DRL (UDP gossip), Kueue (API server) and DBOS (Postgres) each miss on a different axis |
| **3. Global intelligence, local execution** | **Answered, most emphatically of the set** | Impala, Doorman, and SIGCOMM 2007 Distributed Rate Limiting - three decades, three layers |
| **Conservation-law safety bias** | **Explored space, stricter corner unoccupied** | Doorman makes it a per-client configuration option and ships an explicitly contract-violating *optimistic* mode; DRL's shipped designs bias the other way |
| **4. Prescience - backlog indexed by declared requirements** | **No answer found** | Nothing found. But requirement *declaration* is heavily occupied (Slurm GRES, AWS Batch consumable resources, Kueue requests, US 7,813,276) - the unoccupied part is only the **index over the committed backlog** plus horizon and feasibility reasoning |
| **5. The composite** | **No answer found**, medium confidence | Pieces occupied at different layers with incompatible topologies; synthesized rather than directly verified |
| **No rewrite, nothing in the per-call hot path** | **Confirmed as a real difference** (3-0 x3) | Restate and DBOS both fail it - Restate pushes rules cluster-side, DBOS hits Postgres per dequeue |

## Substrate neutrality

**The unit of comparison is the execution architecture, not the transport.** A system that places
admission or scheduling beneath an existing API belongs in this register whether it runs over Kafka,
RPC, actors, work queues, a database or generic async work. The 2026-09-05 sweep's angles were
implicitly Kafka-shaped, which narrowed it; from 2026-09-05 the briefs are substrate-neutral and
[`process-prior-art-research-targets.md`](process-prior-art-research-targets.md) states the rule in
full. The discriminating clause, stated without a substrate: **does it drive that decision from
adaptive global optimisation over measured performance?** Many systems have a limiter, a quota or a
scheduler; far fewer close the loop from observed behaviour back into a global allocation.

## uForwarder - the closest comparator found, and it disproves four claims

**Searched 2026-09-05 against its source, its IDL, its issues and Uber's two blog posts.** It is a
citation, not a threat - but the "nobody has done adaptive concurrency, sparse frontiers or admission
control for Kafka" framing does not survive it, and that framing appears in this corpus.

**Four properties are no longer distinctive, each disproved from source:**

| Property | What uForwarder has |
|---|---|
| Adaptive concurrency discovered from measured service time | `VegasAdaptiveInflightLimiter` wrapping Netflix `concurrency-limits` - TCP Vegas, inferring the limit from latency drift. It even runs a static and a shadow adaptive limiter side by side and switches between them |
| A sparse completion frontier | `AckTrackingQueue` - per-offset `UNSET -> NACKED/CANCELED/ACKED`, committing only the contiguous prefix |
| Admission as a state before execution | Three stacked gates before dispatch |
| Every wait attributable to a binding constraint | `KafkaPipelineIssue` is a literal enum of *which* limiter is binding - message rate, inflight count, and so on |

It also delegates capacity and spends it locally: the controller divides a job group's rate and
inflight quota by partition count and each worker enforces its slice. **So delegation is prior art
too** - what is missing is the *named shared resource*: the unit is always
`(cluster, topic, consumer_group)`, there is no way to say three workloads share one database's
capacity, and there is no renegotiation, only a re-divide when partition count changes.

**Two things survive cleanly, and they are the ones to lead with.**

- **Ordering, and it is the largest single gap.** uForwarder advertises *"Out of order Message
  delivery"* as a **property, not a limitation**, and there is no key ordering anywhere in its source
  - the record key is used only for tracing and DLQ metadata. **It buys concurrency by surrendering
  ordering entirely; Parallel Consumer buys it while keeping key order.** That is structural, not a
  gap they might close.
- **Position, and the axis was confirmed rather than assumed.** A separate operated controller and
  worker cluster, with **ZooKeeper mandatory** - the maintainer states it is required for leader
  election and job metadata and is unrelated to Kafka dropping ZK. Exactly two Spring profiles,
  **no embedded or library mode anywhere**, and its own capacity-planning problem solved by an
  operator-run control loop with a configured per-worker capacity. Workers do not even join a Kafka
  consumer group - they `assign()` from an assignment the controller holds in ZooKeeper, replacing
  the rebalance protocol wholesale. Uber runs it at a scale that is itself a fleet to size.

**And the application is rewritten.** It stops being a Kafka consumer and becomes a gRPC server; the
routing target is a URI the control plane holds, not app config. Migration is a rewrite of the
consumption path plus an operator ticket - the opposite of *keep your code, replace the runtime
underneath it*.

**One difference of kind worth keeping precise.** The completion frontier is novel here **in
durability, not in kind**: Parallel Consumer encodes it into commit metadata so it survives restart
and rebalance, while uForwarder's lives in worker memory and, when it fills, the escape hatch is not
a wider frontier but eviction to a dead-letter queue on head-of-line-blocking detection.

**Health, which matters if anyone proposes depending on it rather than citing it:** the README claims
Apache 2.0 but the repository has **no LICENSE file** and the PR adding one is still open; there is
no SASL/PLAIN in the OSS build; internals are documented only on an external wiki; and activity is
largely *merge changes from internal repo* - a monorepo mirror rather than a community project.

**Honest positioning, and it replaces a sentence this corpus currently uses:** *uForwarder is the
proxy-cluster answer to this problem; this is the embedded answer, and it keeps key ordering.* Never
*nobody has done this.*

**Seeds for the next sweep, from its own citations:** Confluent's Kafka REST Proxy and Kafka Connect
- both explicitly considered and rejected in Uber's 2021 write-up, Connect over rebalance latency
against a hard end-to-end latency requirement - and Netflix `concurrency-limits` as the direct
algorithmic ancestor. Notably it never mentions SQS, RabbitMQ, Pulsar shared subscriptions, or
Parallel Consumer.

**Why the earlier sweep missed it** is the durable lesson: its five angles were organised by
*mechanism*, and a consumption proxy is not a mechanism. The eighteen-family list below exists
because of that miss.

## Rows added 2026-09-05, all `claimed` and none checked here## Rows added 2026-09-05, all `claimed` and none checked here

Named by the owner from reading, recorded so they are not lost, and **not verified in this
repository** - the evidence rule applies to them exactly as to anything else.

| System | Why it matters | Evidence |
|---|---|---|
| **Envoy adaptive concurrency filter** | A gradient controller descended from TCP Vegas via Netflix's `concurrency-limits`: probes an ideal round-trip time by deliberately underloading, then scales a concurrency limit by the ratio of that to the current window's sampled latency. **Strictly local** - one controller per process, no cross-instance channel; the fleet-level `jitter` field means *do not all probe at once*, not *share what you learned*. **It implements Netflix's Gradient, the minRTT-probing variant - not Gradient2**, which this engine already ports and which exists precisely because minimum-latency measurement drifts. So the earlier *study rather than reinvent* direction inverts on the algorithm and holds on everything else: the mitigation set and the shipped stability constants are the expensive part to rediscover. [`core-auto-scaling.md`](core-auto-scaling.md) and [`core-envoy-is-the-other-half.md`](core-envoy-is-the-other-half.md) carry the detail. | **surveyed** 2026-09-05 from Envoy docs, protos and source |
| **Envoy RLQS** (rate limit quota service) | **Fair independent validation of the delegated-credit shape**: advance assignment, local spend, no per-request permit call, periodic usage report, TTL, explicit degraded-mode behaviour - a different team on a different substrate reaching the same conclusion that the per-request permit call is the thing to eliminate. Three differences that matter: it delegates a **rate**, not a stock of credit; its bucket is *requests matching matchers*, not a named external resource with real capacity shared by unrelated participants; and the number vended is **human-configured, never discovered**. Its usage report carries requests allowed, denied and time elapsed - **no performance signal at all**, so a server cannot do performance-driven allocation over it. Partition behaviour is configuration-selected rather than solved, with no lease return, fencing token or spend reconciliation. **Envoy ships no RLQS server**, so its allocation policy is out of tree and unreadable - *"RLQS does global optimisation"* is unproven either way. | **surveyed** 2026-09-05; the server side is **unknowable** from public sources |
| **Arktos Global Scheduler** | Built around a global view across clusters and data centres, with application-aware scaling and migration from observed input-flow behaviour and multidimensional optimisation. Non-Kafka, cluster-scale, and squarely on the discriminating clause. | **claimed** |
| **IBM LLA** | Continuously adapts distributed CPU and network allocation to workload and resource variation, maximising **aggregate utility from end-to-end latency**. A stronger objective formulation than anything this corpus has written down. | **claimed** |
| **Hadar** | Online scheduling of task placement across heterogeneous accelerators from measured or modelled workload performance. Shows how far measured-performance-driven placement has been taken mathematically. | **claimed** |

**The academic rows are the uncomfortable ones**, and that is why they are here: they go further
mathematically than the design does, on the exact clause claimed as discriminating. Reading them is
more likely to improve the design than to threaten it, but neither outcome should be assumed before
somebody looks.

## Where the answers already exist - say so first, always

Not a prohibition, a courtesy and a credibility move: on each of these, somebody has a shipped
answer, so name theirs before describing ours. Being told "that's Doorman" by someone who knows the
field costs more than the sentence ever bought.

*Pre-execution admission control. Queueing work before it starts. Globally coordinated capacity with
locally decided dispatch. Graceful degradation when the coordinator is unreachable. Renewable
capacity leases delegated to an embedded client library. Divisible or borrowable quota across named
pools. Predeclared resource requirements. Embedded-library-not-cluster packaging.*

**Added 2026-09-05 by the uForwarder pass, and these are the ones that hurt**, because they were the
Kafka-specific claims: *adaptive concurrency discovered from measured service time · a sparse
completion frontier over unresolved work · admission as a state before execution, for Kafka records ·
every wait attributable to a named binding constraint · dividing a quota and letting each participant
spend its slice locally.* All five are in uForwarder's source today. The surviving forms are narrower
and stated in the row above: **key-ordered** concurrent execution, a frontier that is **durable
because it is encoded into the commit**, and capacity attached to a **named resource shared across
unrelated workloads** rather than to one topic-and-group.

The register's real use is the opposite of a warning list: each of these is a **design already
reviewed by somebody else**, free to read. Doorman's design document settles lease expiry semantics,
refresh intervals and unreachable-vendor fallbacks; Impala settles what decentralised admission with
shared counters actually costs; DRL quantifies the degrade-versus-over-admit tradeoff.

## What is unanswered, and what the arrangement buys - stated narrowly enough to be attacked

1. **The substrate combination** - divisible capacity leases carried on a *durable log* to embedded
   instances, with no coordination cluster and nothing per-call in the hot path. A combination
   claim, not a mechanism claim, and the mechanism half is Doorman's.
2. **Prescience** - an inverted index over the committed backlog keyed by producer-declared execution
   requirements, with demand and capacity horizons, admission debt and feasibility.
3. **The composite**, resting on integration and on the two residuals above, never on any individual
   mechanism - every mechanism examined turned out to be occupied.
4. **Under the existing dispatch boundary, no application rewrite, nothing in the hot path** - the
   only difference the sweep positively confirmed rather than merely failed to find a counterexample
   to. Given the primary audience above, this is also the most commercially load-bearing of the four,
   and the uForwarder pass confirmed the axis independently: that project is a separate operated
   ZooKeeper-backed cluster with no library mode, and adopting it rewrites the application's
   consumption path.
5. **Key-ordered concurrency, promoted 2026-09-05 to the front of this list.** The closest comparator
   in the field advertises out-of-order delivery as a *property* and has no key-level scheduling
   primitive at all. Concurrency beyond partition count *while keeping key order* is the one place
   the comparison is not close, and it is structural rather than a gap somebody might close. It had
   been treated as background because it is Parallel Consumer's existing behaviour rather than a new
   claim - which is exactly how a real differentiator goes unmentioned.

**And the honest form of the conclusion is not "nobody does this".** It is: *nobody does it from this
position, and the position is what makes the rest cheap.* Sitting between a log somebody already runs
and execution they already own is what turns admission, capacity governance, attribution and scaling
advice from four products into feature arms of one architecture - each arriving without a cluster,
a rewrite, or a second thing to operate. That is a claim about **arrangement and placement**, which
is testable, rather than about **invention**, which the table above shows is mostly not the point.

## The caveat that governs how much of this to believe

**The sweep's coverage is uneven, and the hole is in the nearest neighbourhood.** Angles (a)
admission control and (b) distributed rate limiting are well covered by primary sources. Angle (c)
durable execution covered only Restate and DBOS - Temporal, Cadence, Conductor, Inngest and Azure
Durable Functions produced no surviving verified claims. **Angle (d), stream-processing elasticity,
produced nothing at all**: no verified claim touches Flink's reactive or adaptive scheduler or its
autoscaler, Kafka Streams, Beam/Dataflow, Pulsar Functions, KEDA, or Kafka Share Groups (KIP-932).
Angle (e) produced no research literature and no commercial capacity-governance hits.

Since (d) is the closest adjacent domain to a Kafka-resident runtime, **treating claims 4 and 5 as
"gaps" is premature - they are "not found", which is a much weaker statement.** That distinction is
the same one
[`../solutions/workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md`](../solutions/workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md)
owns, and it applies to this register's own output.

## The next sweep

**The queue lives in [`process-prior-art-research-targets.md`](process-prior-art-research-targets.md)**, which owns every outstanding target across both projects and what each would settle. Repeated in outline here only:


In priority order, and the first is not optional before any public claim:

1. **Angle (d), properly** - Flink adaptive scheduler and autoscaler, Kafka Streams, Beam/Dataflow,
   KEDA, Pulsar Functions, KIP-932 Share Groups. Does anything there decouple ordering from
   concurrency, or schedule against a committed backlog? This is where claims 4 and 5 live or die.
2. **The remaining durable-execution runtimes** - Temporal, Cadence, Conductor, Inngest, Azure
   Durable Functions - against question 5, since that is the confirmed differentiator and it is
   currently confirmed against two systems out of seven.
3. **Log-transported quota** - Kafka's own quota and throttling internals, service-mesh rate-limit
   services with log-backed state, Raft-backed token brokers. The residual of claim 2 is a substrate
   choice and only four alternatives were checked.
4. **Scheduling-theory literature** on backlog-aware and requirement-aware lookahead, which was never
   searched.
5. **Gubernator** - all three claims about it failed verification, so the register holds no reliable
   position on the closest *distributed rate limiter without a central service* comparator.
6. **Escrow and reservation protocols, distributed-counter literature** - for whether the
   conservation-law contract has a formal precedent. Both DRL and Doorman leave the strict-safety
   corner explicitly open.

## What this changes about the thesis

- **2026-09-05, first sweep.** Two claims dropped as novelty arguments and one narrowed to its
  substrate. **The engineering is unharmed and arguably validated** - four independent teams
  converging on the same shape is evidence the shape is right - but the *pitch* has to change, and
  Doorman's design document is now available as a free design review of claim 2, including the lease
  expiry semantics, refresh intervals and unreachable-server fallbacks it already settled. Two notes
  carry claims this contradicts and are corrected in the same change as this register lands:
  [`core-admission-scheduling-model.md`](core-admission-scheduling-model.md) and
  [`core-distributed-throttling.md`](core-distributed-throttling.md). `docs/w2-vision.md`'s law 3
  gains a pointer rather than a rewrite, because the law is still the right law - it is only no
  longer *ours*.
