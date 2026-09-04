<!-- issue-refs: exempt-file - preserved verbatim external document; its #N tokens are markdown TOC anchors and section numbers, not issue references -->
> Provenance: an agent-written compound-engineering handoff produced at the owner's request at the
> end of the 2026-08-29/30 strategy weekend (document self-dated 29 August 2026 - it predates the
> 30th's exchanges, so it uses "W2" for the resource/coordination plane where the later transcript
> used "Voice", and none of the later Dune-themed codenames appear). Preserved verbatim as the
> companion reference to the `docs/inflight/` breakdown; where this document and a note disagree,
> flag it - do not silently reconcile. The tracked deltas it surfaced are in
> `docs/inflight/core-partition-virtualization.md` and
> `docs/inflight/core-runtime-services-and-compat.md`. Part 1 of 2; a second handover document and
> a notes checklist follow it.

# HASTEN Compound Engineering Handoff

**Architecture • Product thesis • Reference system • Design laws • Implementation seams • Stories • Language**

> Kafka provides the log. Hasten provides the runtime.

>
> **Hasten — Why wait?**

*Working handoff • 29 August 2026*

## Contents

- [0. Purpose](#0-purpose)
- [1. The origin and the zoom](#1-the-origin-and-the-zoom)
- [2. Architectural laws](#2-architectural-laws)
- [3. The four decisions](#3-the-four-decisions)
- [4. Canonical reference system: global logistics](#4-canonical-reference-system-global-logistics)
- [5. Core execution concepts](#5-core-execution-concepts)
- [6. Adaptive execution and useful demand](#6-adaptive-execution-and-useful-demand)
- [7. Shared resources and W2](#7-shared-resources-and-w2)
- [8. Coordination plane](#8-coordination-plane)
- [9. QoS](#9-qos)
- [10. Observability is scheduler state](#10-observability-is-scheduler-state)
- [11. Transparent envelope, tracing and opportunity graph](#11-transparent-envelope-tracing-and-opportunity-graph)
- [12. Service contracts and resource discovery](#12-service-contracts-and-resource-discovery)
- [13. Polyglot architecture and language leverage](#13-polyglot-architecture-and-language-leverage)
- [14. Internal machinery becomes product](#14-internal-machinery-becomes-product)
- [15. Kafka Streams-backed actors](#15-kafka-streams-backed-actors)
- [16. Workflow systems and future knowledge](#16-workflow-systems-and-future-knowledge)
- [17. Kafka Streams relationship](#17-kafka-streams-relationship)
- [18. Infrastructure and optimizer composition](#18-infrastructure-and-optimizer-composition)
- [19. Economic optimization](#19-economic-optimization)
- [20. Broker participation and self-hosting](#20-broker-participation-and-self-hosting)
- [21. Adoption staircase](#21-adoption-staircase)
- [22. Product/company shape](#22-productcompany-shape)
- [23. Competitive framing](#23-competitive-framing)
- [24. Candidate engineering seams](#24-candidate-engineering-seams)
- [25. Agentic engineering rules](#25-agentic-engineering-rules)
- [26. The story that explains it](#26-the-story-that-explains-it)
- [27. Language worth preserving](#27-language-worth-preserving)
- [28. Concept glossary](#28-concept-glossary)
- [29. What not to do](#29-what-not-to-do)
- [30. Near-term sequence](#30-near-term-sequence)
- [31. Open research backlog](#31-open-research-backlog)
- [32. External grounding](#32-external-grounding)
- [33. Final synthesis](#33-final-synthesis)
- [34. Runtime services at the processing edge](#34-runtime-services-at-the-processing-edge)
- [35. Maintenance as resource fencing](#35-maintenance-as-resource-fencing)
- [36. Native unresolved-work / DLQ model](#36-native-unresolved-work-dlq-model)
- [37. Cron, scheduled and batch work](#37-cron-scheduled-and-batch-work)
- [38. Embedded compatibility services](#38-embedded-compatibility-services)
- [39. Runtime wrappers as transparent augmentation seams](#39-runtime-wrappers-as-transparent-augmentation-seams)
- [40. Lineage is carried, not reconstructed](#40-lineage-is-carried-not-reconstructed)
- [41. CDC to automatically served distributed read models](#41-cdc-to-automatically-served-distributed-read-models)
- [42. New product/design rules](#42-new-productdesign-rules)
- [43. Partition virtualization: attack Kafka partition rigidity from both sides](#43-partition-virtualization-attack-kafka-partition-rigidity-from-both-sides)
- [44. Virtual records and transparent large-message support](#44-virtual-records-and-transparent-large-message-support)
- [45. Architectural synthesis: make partition decisions less architectural](#45-architectural-synthesis-make-partition-decisions-less-architectural)
- [46. Product story: Hasten — Why wait?](#46-product-story-hasten-why-wait)

# 0. Purpose

This is a compound engineering handoff: not merely a feature list, but the accumulated reasoning behind Hasten. It exists so engineering, documentation and agentic work can continue without one person retaining the entire architecture in working memory. Treat it as an architectural constitution. New work should fit these semantics, deliberately amend them, or identify the contradiction. Do not silently turn Hasten into a bag of unrelated features.

> Independent work shouldn't wait unnecessarily.

## Status vocabulary

Use the maturity labels **Implemented**, **Experimental**, **Designed**, and **Hypothesized** consistently. See [Reference tables](#reference-tables) for definitions.

## One-sentence thesis

> Hasten is an embedded, polyglot execution runtime that coordinates work globally while executing locally, removing unnecessary waiting without forcing users into a new programming model or runtime cluster.

# 1. The origin and the zoom

Parallel Consumer began with a small observation: Kafka partition ownership is coarser than the true dependency structure of application work. A partition can contain unrelated keys. Unrelated work should not wait merely because it shares a partition.

- Kafka/broker → where partition work is owned.
- Parallel Consumer → which keys/order domains may execute independently inside that ownership.
- Hasten local runtime → which runnable work is most useful next.
- Hasten resource plane → which workload receives scarce shared capacity.
- Infrastructure integration → where additional capacity would actually help.
- Economic optimization → where the next unit of spend creates the greatest useful improvement.

> Kafka brokers orchestrate the log. Hasten orchestrates the work that emerges from it.

> Ordering → Opportunity → Capacity → Allocation → Optimization.

# 2. Architectural laws

## Ownership and execution are independent.

Partitions answer where work is owned; they do not fully answer what may execute concurrently.

## Programming model: yours. Execution model: Hasten.

Prefer insertion at the execution seam. Keep Kafka Streams, plain Consumer, Spring, actors and workflows recognizable.

## Global intelligence, local execution.

Coordinate constraints and capacity globally; make hot-path dispatch locally from delegated state whenever possible.

## One implementation of intelligence. Many implementations of ergonomics.

Distributed correctness belongs in the shared runtime/control plane; language SDKs should be native surfaces over it.

## Discover what is safe; declare what is unknowable; delegate control explicitly.

Infer mechanics and observations; require declarations for business intent; require explicit authority for external control.

## No unexplained waiting.

Every prevented execution should have a binding constraint and a reason describing what would let it proceed.

## The scheduler's decision state is the observability.

Preserve scheduler reasons instead of reconstructing them later from generic telemetry.

## Internal machinery should become public capability when reusable.

If Hasten already needs a primitive, prefer exposing it rather than deploying a separate subsystem.

## Architectural stubs preserve future seams.

Designed capabilities can begin with local/in-memory implementations only if they obey eventual distributed semantics.

## The canonical example must compose.

Every major capability extends the same logistics application. If it requires a new conceptual universe, challenge the scope.

# 3. The four decisions

Hasten separates four decisions that Kafka applications often conflate. See [Reference tables](#reference-tables) for the ownership table.

# 4. Canonical reference system: global logistics

Use one fictional global logistics company across documentation, tests, demos, generated tutorials, UI screenshots and talks. Logistics is familiar, morally neutral, naturally full of queues and constraints, and scales from one parcel to a planetary network.

> Two parcels arrive at the depot. They're unrelated. Why should either wait for the other?

- Parcel/shipment event → record/work.
- Shipment ID → ordering domain.
- Depot → application/process boundary.
- Sorter → constrained resource.
- Customs → globally shared service/resource.
- Carrier/route/warehouse → dynamic resource namespace.
- Medical shipment → reserved/high-priority QoS workload.
- Ordinary parcel → borrowable best-effort workload.
- Truck manifest → known future work.
- Christmas forecast → predicted future.
- Shipment journey → workflow/DAG.
- Shipment entity → actor with ordered mailbox and state.
- Operations console → Why wait? / Why this? / resource graph / decision telemetry.

## Composable tutorial stack

01. Consumer — Same partition, different shipment IDs: preserve necessary ordering while removing unnecessary waiting.
02. Streams — Validation → routing → processing topology. Kafka Streams remains the programming model; Hasten becomes the execution model.
03. Buffet / horizon — Expose owned work and show how shard depth buys deterministic future visibility.
04. Named resources — Parcels require sorter, customs, carrier or storage resources.
05. Delegated semaphore — Multiple depots share customs capacity globally while spending permits locally.
06. QoS — Medical shipments receive reserved capacity; ordinary freight borrows unused reserve.
07. Observability — Add OpenTelemetry plus decision telemetry. Click any parcel and ask Why wait?
08. Scaling — Same backlog, different key distribution: one case merits SCALE OUT; the hot-key case does not.
09. Workflow — Shipment DAG exposes structurally known future beyond the immediate buffet.
10. Actors — Shipment becomes an addressed persistent entity with ordered Streams-backed mailbox/state.
11. Infrastructure — Kubernetes/other actuator consumes horizontal/vertical capacity recommendations.
12. Economics — Compare marginal value across app compute, databases and shared services.
13. Global system — Multiple languages, clusters, depots, workflows and resources form one coordinated execution graph. Engineering rule: each example should import/depend on the previous stage and add only the new capability. Adjacent diffs become executable tutorial source material.

> Change your import. We'll discover everything we safely can. Tell us only what we cannot know. Give us control only when you're ready.

# 5. Core execution concepts

## Execution buffet

The execution buffet is the set of owned, ready or potentially-ready work from which Hasten may choose the next useful unit instead of being forced to process only the next arrival.

> Where is X's buffet?

## Ordering domain

Ordering is a constraint, not the scheduler. A shipment ID can serialize its own events while unrelated shipments remain independently executable.

## Execution horizon

The execution horizon is work Hasten can reason about before it becomes the single immediately runnable item. Immediate buffet, topology/DAG structure, schedules and forecasts create progressively different horizons.

> Memory buys future visibility.

## Known future vs predicted future

- Immediate future: owned buffet.
- Structural future: topology/DAG.
- Scheduled future: manifests/deadlines.
- Predicted future: statistical expectation.

# 6. Adaptive execution and useful demand

The local controller should discover sustainable useful concurrency, probe upward to escape local minima, and back off when latency/errors show saturation.

- Internal loop: discover useful local concurrency.
- External loop: SCALE_OUT / HOLD / SCALE_IN with an explicit reason. Scale-out should depend on exploitable independent work and downstream headroom, not lag alone.

> It knows when the bottleneck is infrastructure — and when it's the keys.

- Opportunity ceiling
- Local capacity ceiling
- Distributed application ceiling
- Shared-resource ceiling
- Policy/QoS ceiling

# 7. Shared resources and W2

Named shared execution resources are the bridge from a fast Kafka client to a globally coordinated runtime. Work may require customs/nz, warehouse/auckland/sorter, carrier/dhl, database/orders, tenant/acme, or combinations.

> Ordering? Local capacity? Customs? Database? Tenant? Execute.

## Delegated capacity

Do not put a globally mutable semaphore/token bucket on the record hot path. Coordinate ownership globally, delegate renewable capacity slices, and spend locally. Failure should lose capacity before it loses safety.

> Globally coordinated, locally consumed.

## Multiple resources

Acquire locally in deterministic order; roll back tentative permits when another required resource is unavailable; do not burn consumable rate credit until actual dispatch.

## Dynamic resources

Resource IDs may derive from records and reach very high cardinality. Use templates/policies, lazy materialization, TTL and batched demand/lease traffic.

# 8. Coordination plane

A Hasten process can participate in a separate resource-plane group alongside the application consumer group. A resource hashes to an internal control-topic partition; ownership of that partition determines allocator authority.

> Consumer groups coordinate ownership of work; resource groups coordinate ownership of capacity.

- ResourceContract → capacity/objectives/policy.
- DemandSignal → useful demand, excluding work blocked elsewhere.
- ExecutionObservation → latency/errors/consumption/outcome.
- CapacityLease → amount/slots + epoch + expiry + owner.
- Decision/Explanation → binding constraint + alternatives + counterfactual.

## Adaptive control cadence

Coordination frequency should adapt: faster during churn, degradation, rebalance, probing and near-expiry; slower while stable; effectively asleep for idle namespaces.

## Control reserve

W2 coordination traffic should have hard reserved broker capacity. Ordinary work may borrow unused reserve, but overload must not starve the control mechanism required to recover.

# 9. QoS

Global QoS is broader than distributed rate limiting: guarantees, maxima, priorities, latency objectives, borrowing, reclamation and degradation policy over shared resources. Canonical story: medical shipments have guaranteed airfreight/customs capacity. Ordinary freight borrows it while unused; Hasten reclaims it when medical demand returns.

# 10. Observability is scheduler state

> The scheduler's decision state is the observability.

Traditional telemetry tells us what executed. Hasten additionally knows what could have executed but did not, which constraint bound the choice, what alternative work was selected, and what change would have altered the decision.

- Why wait? — Why isn't this work executing?
- Why this? — Why was this work selected?
- Why here? — Why this process/region/resource?
- Why scale? — Why would more infrastructure help?
- Why shrink? — Why can capacity disappear safely?
- Why spend? — Why is this the cheapest safe capacity plan?

## Execution provenance

Retain origin, ordering identity, resources, wait reasons, selection reason, downstream calls, retries, completion and causal descendants as machine-readable lifecycle data.

> No unexplained waiting.

# 11. Transparent envelope, tracing and opportunity graph

Hasten can propagate standard trace context while maintaining a richer internal execution envelope: execution identity, ordering identity, trace context, QoS, resource requirements, causal ancestry, workflow identity, timing and retry lineage. Use OpenTelemetry/W3C propagation for interoperability. Hasten's distinctive contribution is the pre-execution state: what could execute but did not.

## Opportunity graph

Static architecture says what can call what. Tracing says what did call what. Hasten can additionally say what would profitably execute if capacity moved.

# 12. Service contracts and resource discovery

> Service owners publish capacity and objectives. App owners publish demand and priority. Hasten continuously clears the market.

Contracts may express hard rate/concurrency, latency/error targets, burst, QoS reserve, adaptive bounds and recovery behavior. Instrumentation can discover candidate dependencies; unknown dependencies should appear as candidates rather than silently becoming hard policy. This permits backpressure to propagate across applications when the evidence and contracts justify it.

# 13. Polyglot architecture and language leverage

Potential surfaces include Java/Kotlin/Scala, Go, Python, TypeScript, Rust, Ruby, C#, C/C++ and Swift. The exact list can change; the architectural rule should not.

> One implementation of intelligence. Many implementations of ergonomics.

The shared runtime owns leases, allocation, failover, QoS, resource graph, adaptive controllers and optimization. SDKs own idiomatic declarations, callbacks, observations and protocol glue.

## Language leverage

Once a primitive exists in the shared runtime, every supported language can expose it without reimplementing distributed correctness. A Python user who installed Hasten for Kafka can later use the same global rate limiter, semaphore, actor bus, QoS policy or feature flag as Go, Rust, Swift or Java.

> Build one primitive × expose through N language bindings = N product surfaces without N distributed implementations.

# 14. Internal machinery becomes product

- **Delegated semaphore** — Globally bounded capacity partitioned into renewable local leases.
- **Global rate limiter** — Delegated consumable budget with interval/credit semantics.
- **QoS allocator** — Guarantees, borrowing, reclaim, priority and fairness.
- **Circuit breaker** — Fleet-wide resource health suppressing harmful admission.
- **Feature flags / dynamic config** — W2-distributed configuration already required to control Hasten.
- **Persistent actor mailbox** — Addressed ordered work + durable state.
- **Global work queue** — Kafka-derived scheduling exposed as a generic execution surface.
- **Durable timers** — Potential composition of durable state + scheduled future work.
- **Service/resource registry** — Named resources/contracts as distributed registry.
- **Decision telemetry** — Scheduler reasoning directly exposed.

**Litmus test:** if a new 'product' needs an entirely new distributed coordination mechanism, first ask whether the abstraction is wrong.

# 15. Kafka Streams-backed actors

> An actor is an ordering domain with state and an address.

Do not clone an actor ecosystem merely to own the programming model. A natural Hasten feature is a persistent actor/mailbox substrate backed by Kafka Streams while preserving familiar actor semantics where useful. Possible mapping: actor address → key; mailbox → Kafka-backed ordered work; state → Streams store; actor serialization → ordering domain; concurrency across actors → Hasten; resource admission → W2; ownership movement → Kafka rebalance. Because the runtime is polyglot, the actor bus can become polyglot too.

# 16. Workflow systems and future knowledge

Workflow engines already contain valuable causal structure. Prefer importing that knowledge and taking the execution seam rather than replacing their programming model. Shipment workflows make this obvious: collection → scan → customs → sort → transport → destination depot → delivery. A DAG increases Hasten's execution horizon even when later tasks are not runnable yet.

# 17. Kafka Streams relationship

> Kafka Streams is the programming model. Hasten is the execution model.

Kafka Streams already provides topology, state, joins, windows, fault tolerance and exactly-once processing. Hasten should not recreate these merely to feel self-contained. The thesis is to preserve Streams semantics while changing the execution scheduling beneath them.

> Just change your import.

Treat that as severe design pressure, not an absolute promise. Business policy cannot always be inferred and may need a few explicit declarations.

# 18. Infrastructure and optimizer composition

> Hasten decides what capacity would help. Infrastructure systems decide how to provision it.

Adapters can translate semantic demand into Kubernetes HPA/VPA/KEDA or specialist optimizer inputs. Hasten need not outbuild every provisioning/cost engine.

## Optimizer composition

Hasten can act as a semantic demand oracle: useful executable demand, marginal benefit of another replica/core, downstream binding point, shock reserve and safe shrink. Ship a simple built-in optimizer; let sophisticated users delegate to specialists.

# 19. Economic optimization

> Cloud cost tools know what infrastructure costs. Hasten knows what additional infrastructure would actually accomplish.

Long-horizon hypothesis: compare marginal value across app compute, databases, broker capacity, GPUs, vendor tiers, regions and substitutable resources. Minimize cost subject to SLOs, hard contracts, availability, QoS and reserve.

> For every unit of useful work, what is the cheapest safe way to execute it right now?

# 20. Broker participation and self-hosting

Kafka itself can be a resource graph node: broker CPU, disk, network, request handling, leadership and replication. Keep this replaceable and avoid requiring invasive broker modifications initially. The recursion is useful: Kafka carries the Hasten control plane while broker capacity itself becomes observable/optimizable. Control reserve prevents self-starvation.

# 21. Adoption staircase

1. Parallel Consumer for key concurrency.
1. Language-native Kafka-compatible Hasten client.
1. Kafka Streams wrapper.
1. GUI/decision telemetry in observe-only mode.
1. Named resources/global coordinated limits.
1. Horizontal scaling recommendations.
1. Vertical scaling recommendations.
1. Delegate scaling to Kubernetes/specialist optimizer.
1. Coordinate shared infrastructure across workloads.
1. Global cost/SLO optimization across the dependency graph.

## Progressive declaration

- **Zero declaration** — Discover keys, records, lag, timings, concurrency, dependency/trace information and local opportunity where safe.
- **Small declaration** — Declare unknowable business semantics: resource identity, medical priority, reserve, deadline.
- **Explicit integration** — Grant authority to enforce global policy or control infrastructure.

## Observe → Recommend → Shadow → Enforce

Use this progression for consequential control so users can inspect Hasten's model before delegating authority.

# 22. Product/company shape

> A globally coordinated runtime with no runtime cluster.

> The optimizer is distributed through the applications it optimizes.

> Locally: a library. Globally: the company's execution operating system.

Potential OSS center: key scheduling, Streams integration, polyglot clients, adaptive concurrency, named resources/W2, global limiting primitives and basic decision telemetry. Potential enterprise center: company-wide resource graph, governance/RBAC, global QoS/contracts, cross-cluster control, SLO/history, cloud inventory/pricing, scaling/cost optimization, audit and automation.

# 23. Competitive framing

Be precise. Other schedulers know queued work; workflow engines know DAG future; infrastructure optimizers predict demand. The potentially distinctive composition is key-aware executable opportunity at the processing edge + delegated shared-resource admission + cross-app/language coordination + known-future topology/workflow signals + adaptive resource envelopes + optimizer composition.

> Flink: bring your computation to our distributed execution system. Hasten: keep your computation where it is; we make local runtimes behave like one globally optimized execution system.

> Ray distributes compute across resources it owns. Hasten coordinates execution against resources the whole organization shares.

These are positioning hypotheses, not claims that Hasten already replaces the full semantics of Flink, Ray, workflow engines or cloud optimizers.

# 24. Candidate engineering seams

- **WorkSource** — Owned work/buffet.
- **WorkEnvelope** — Normalized execution metadata without destroying the user's record.
- **OrderingDomain** — Serialization constraint.
- **AdmissionController** — Whether work may cross the execution boundary.
- **Resource / ResourceId** — Named constrained capacity.
- **ResourceContract** — Capacity/objective/policy.
- **DemandSignal** — Useful demand for capacity.
- **ExecutionObservation** — Measured latency/errors/consumption/outcome.
- **CapacityLease** — Delegated authority with epoch/expiry.
- **ResourceAllocator** — Demand/contracts/observations → leases.
- **CapacityProvider** — Available/proposed infrastructure capacity.
- **CapacityOptimizer** — Useful marginal capacity evaluation.
- **Actuator** — External system authorized to change capacity.
- **DependencyGraph / OpportunityGraph** — Work-resource relationships.
- **ControlTransport** — W2 state/event distribution.
- **CoordinatorOwnership** — Partitioned authority/failover.
- **Decision / Explanation** — Reason + counterfactual.
- **ActorAddress / Mailbox / EntityState** — Actor projection over work + ordering + state.

> **Critical rule:** in-memory implementations must obey the same semantic contracts as eventual distributed implementations.

# 25. Agentic engineering rules

- Read this architecture/manual before planning a major capability.
- Label work implemented, experimental, designed or hypothesized.
- Prefer extending an existing semantic seam over creating a parallel subsystem.
- Extend the canonical logistics app as acceptance criteria.
- Add Why wait?/Why this? explanation for new scheduler constraints.
- Preserve decision reasons as data, not only logs.
- When adding a primitive, design its polyglot surface.
- Keep global coordination off the record hot path when delegated enforcement works.
- Use standards such as OpenTelemetry for interoperability.
- Do not recreate Kafka Streams topology/state/window/join semantics.
- Protect execution semantics with the full correctness/test harness.
- If a feature cannot be a small logistics extension, challenge its scope.

# 26. The story that explains it

## The birthday card and the hot sauce

A birthday card going three streets away waits behind a parcel with a customs problem involving twelve bottles of artisanal hot sauce. The architect asks why. There is no good answer. That is the seed. Then customs becomes a resource; twenty depots require global coordination; medical shipments introduce QoS; scheduler reasons become observability; Christmas backlog proves lag is insufficient; truck manifests introduce known future; workflows extend the horizon; polyglot clients preserve user code; global optimization compares where capacity actually helps.

> Find the things that genuinely have to wait. And everything else? Hasten.

## Explanation rule

For non-specialists, begin with parcels and waiting, not Kafka, leases or distributed control theory. Zoom out only after the previous layer feels obvious. The strongest explanation makes the listener repeatedly think, 'well, of course that is the next thing.'

# 27. Language worth preserving

> Hasten — Why wait?

> Kafka can linger. Hasten doesn't.

> Kafka provides the log. Hasten provides the runtime.

> Kafka Streams defines the topology. Hasten runs the work.

> Write the function. Hasten runs it.

> Write the topology. Hasten runs it.

> Stop using your data architecture as your thread pool.

> Programming model: yours. Execution model: Hasten.

> One implementation of intelligence. Many implementations of ergonomics.

> Globally coordinated, locally consumed.

> Global intelligence, local execution.

> Consumer groups coordinate ownership of work; resource groups coordinate ownership of capacity.

> The scheduler's decision state is the observability.

> No unexplained waiting.

> Most infrastructure observes resources; Hasten observes opportunity.

> It knows when the bottleneck is infrastructure — and when it's the keys.

> Hasten decides what capacity would help; infrastructure systems decide how to provision it.

> Cloud cost tools know what infrastructure costs. Hasten knows what additional infrastructure would actually accomplish.

> A globally coordinated runtime with no runtime cluster.

> The optimizer is distributed through the applications it optimizes.

> Locally: a library. Globally: the company's execution operating system.

> Change your import. We'll discover everything we safely can. Tell us only what we cannot know. Give us control only when you're ready.

> Build one primitive × expose through N language bindings = N product surfaces without N distributed implementations.

> Find the things that genuinely have to wait. And everything else? Hasten.

# 28. Concept glossary

- **Execution seam** — Replaceable boundary between work becoming available and executing.
- **Execution boundary** — Point where available work crosses into execution.
- **Processing edge** — Local boundary immediately before application execution.
- **Execution buffet** — Owned work from which the scheduler can choose.
- **Execution horizon** — Future work Hasten can reason about.
- **Look-ahead depth** — Amount of owned future visible to a shard.
- **Shared execution resource** — Named capacity constraining one or more workloads.
- **Delegated semaphore** — Globally bounded capacity partitioned into renewable local leases.
- **Useful demand** — Work that could profitably execute if the relevant capacity existed.
- **Decision telemetry** — Choice, alternatives, binding constraint and counterfactual.
- **Execution provenance** — Machine-readable scheduling/execution lifecycle.
- **Opportunity graph** — Live graph connecting executable work to resources/dependencies.
- **Progressive declaration** — Discover safe mechanics; ask only for unknowable intent; delegate control explicitly.
- **Adaptive control cadence** — Coordination frequency varies with volatility, urgency and lease safety.
- **Control reserve** — Reserved capacity protecting Hasten's own coordination traffic.
- **Optimizer composition** — Hasten contributes semantic demand to specialist optimizers.
- **Architectural stubs** — Stable future interfaces with honest simple implementations.
- **Language leverage** — One runtime primitive becomes available through every language binding.

# 29. What not to do

- Do not turn Hasten into a mandatory programming DSL.
- Do not require a dedicated Hasten execution cluster merely because distributed systems traditionally have one.
- Do not centralize every permit acquisition on a remote coordinator.
- Do not equate lag/backlog with useful parallelism.
- Do not infer business priority that must be declared.
- Do not market hypothesized optimization as implemented.
- Do not duplicate specialist systems when Hasten can compose with them.
- Do not implement the control plane independently per language.
- Do not discard scheduler reasoning and reconstruct it from logs later.
- Do not let the logistics app become decorative; it must compile and evolve with the architecture.

# 30. Near-term sequence

1. Stabilize Parallel Consumer execution/correctness and the Kafka Streams test-harness strategy.
1. Make the manual/concepts structure authoritative and use explicit maturity labels.
1. Create the smallest canonical logistics reference app.
1. Define WorkEnvelope / OrderingDomain / Explanation seams.
1. Finish/validate adaptive concurrency and reason-bearing scale recommendations.
1. Define ResourceId, ResourceContract, DemandSignal, CapacityLease and local AdmissionController semantics.
1. Implement one named resource locally, then delegated globally, with strong safety tests.
1. Expose Why wait? before complexity makes explanation expensive to retrofit.
1. Add QoS reserve/borrowing/reclamation.
1. Make W2 control traffic consume declared broker capacity and add control reserve.
1. Propagate standard trace context and build opportunity/dependency views.
1. Expose the first non-Kafka primitive through several language SDKs to prove language leverage.
1. Prototype Kafka Streams-backed persistent actor/mailbox semantics.
1. Add observe/recommend infrastructure adapters before autonomous actuation.
1. Deepen global economic optimization only after the substrate produces trustworthy signals.

# 31. Open research backlog

- Exact W2 wire/versioning/compatibility strategy.
- Lease expiry and epoch semantics during allocator ownership transfer.
- Strict concurrency semantics when downstream work outlives a failed lease holder.
- Fairness evolution: equal share → demand-aware → priority/QoS → hierarchical/DRF-like.
- Compact useful-demand representation at very high dynamic-resource cardinality.
- Header vs internal-state boundary for execution-envelope metadata.
- Security/trust boundaries for propagated metadata and cross-cluster control.
- Actor delivery/state semantics and desired Akka/Pekko compatibility.
- Highest-value workflow integrations for execution-horizon knowledge.
- Safe production perturbation for marginal throughput/cost learning.
- Uncertainty/exploration budget in adaptive capacity experiments.
- Resource substitutability, region choice, time/deadlines and demand shaping.
- Trademark/domain clearance for Hasten before major brand investment.

# 32. External grounding

Kafka Streams remains a strong programming-model substrate because it already supplies processor topologies, state stores, fault tolerance and exactly-once processing semantics. Hasten's Streams work should therefore focus on the execution seam rather than reimplementing those semantics. OpenTelemetry context propagation is the interoperability baseline for distributed tracing. Hasten can enrich execution state while propagating standard context across messaging and service boundaries. Grounding references consulted: Apache Kafka Streams documentation (kafka.apache.org, 2026); OpenTelemetry context propagation and messaging semantic conventions (opentelemetry.io, 2026).

# 33. Final synthesis

Hasten is easiest to understand not as a pile of products but as a reusable execution kernel. Work has identity and ordering. Work forms a buffet. The scheduler sees opportunity. Resources constrain execution. Capacity is coordinated globally and consumed locally. Policy decides who deserves capacity. The scheduler retains its reasons. Future knowledge extends the horizon. Infrastructure receives semantic demand. The same primitives surface through every supported language. That is why Kafka concurrency, Streams execution, distributed semaphores, rate limiting, QoS, actors, feature flags, observability, autoscaling and economic optimization can plausibly compound rather than fragment. The engineering challenge is to keep the primitive set small and the semantics honest.

> Demand × Opportunity × Capacity × Policy. Start with two unrelated parcels in one Kafka partition. Extend the same application one dimension at a time until it becomes a globally coordinated logistics estate. If the architecture is sound, the tutorial should feel less like a sequence of inventions and more like the inevitable answer to the same question asked at larger scales.

> Why wait?

## Reference tables

### Maturity status

| Status | Meaning | Treatment |
|---|---|---|
| **Implemented** | Exists and is expected to work now. | Protect with tests; document actual behavior. |
| **Experimental** | Code exists but semantics/API may move. | Test hard; avoid accidental promises. |
| **Designed** | Semantics are clear enough to create seams/stubs. | Create interfaces and honest local implementations. |
| **Hypothesized** | Promising direction, not proven. | Research/prototype; preserve optionality. |

### The four decisions

| Decision | Question | Owner |
|---|---|---|
| **Partitions** | Where is work owned? | Kafka/group ownership |
| **Keys/order domains** | What may execute independently? | Hasten scheduler |
| **Engine** | How much useful parallelism should run? | Adaptive local controller |
| **Infrastructure** | How much engine capacity should exist? | External actuator informed by Hasten |

# 34. Runtime services at the processing edge

Hasten should systematically absorb the distributed-systems quality-of-life work that Kafka and Kafka Streams make possible but leave applications to assemble themselves. The rule is not to replace the user's programming model or generate their business topology. Kafka Streams remains the programming model; Hasten supplies the missing runtime model around it.

> If Kafka Streams already knows the answer, the developer should not have to configure it again.

Prefer transparent augmentation derived from existing Kafka/Streams/runtime state. Require declarations only for business semantics that cannot be discovered safely.

## 34.1 Distributed Interactive Queries as dogfood

Kafka Streams materializes fault-tolerant local state but leaves remote Interactive Query routing and transport to the application. Hasten already requires discovery, ownership routing and polyglot RPC, so expose distributed IQ as a first-class runtime service.

- Local owner: execute the IQ locally.
- Remote owner: route directly to the owner when appropriate.
- Durable/controlled path: route through the Hasten/Kafka RPC backplane.
- Standby state: permit replica-aware reads when the requested consistency policy allows it.
- Polyglot clients: expose the same store/query surface through every Hasten language binding.
- UI: use the same API to browse application state; do not create a privileged UI-only backend.

> Distributed Interactive Queries for Kafka Streams, out of the box, in every Hasten language.

Hasten should also publish a live catalog of Streams applications, topologies, materialized stores, schemas, partitions, active/standby owners, restore state and query capabilities. This turns separately deployed Streams JVMs into one discoverable state fabric.

## 34.2 Kafka Streams runtime quality of life

Machine generation belongs around the user's topology, not in place of their business code. From an ordinary Streams application Hasten should derive and expose, where possible:

- topology and application inventory
- source/sink and store catalog
- distributed IQ routing
- active/standby ownership
- state restore progress and query readiness
- lifecycle-aware health/readiness/startup probes
- graceful drain endpoints
- topology and state lineage
- schema/catalog integration
- replay/backfill controls
- maintenance/fencing controls
- topology performance and binding-constraint diagnostics. The product goal is:

> You write a normal Kafka Streams application. Hasten turns it into an operationally complete distributed application.

## 34.3 Semantic health, not process health

Hasten health checks can inspect execution facts unavailable to generic frameworks. In particular, the runtime can inspect Parallel Consumer's encoded sparse completion/offset frontier and deadlock probes, in addition to Kafka/Streams lifecycle, task assignment, restore state, scheduler progress and dependency observations. Health should distinguish at least: alive, assigned, ready, restoring, draining, fenced, making progress, scheduler/deadlock failure, unresolved-frontier pressure and dependency degradation. Readiness should mean the instance can correctly serve the work/state it currently owns, not merely that the JVM responds to HTTP. Expose conventional health/readiness endpoints and Kubernetes-compatible probes as projections of this semantic state.

## 34.4 Drain as an exposed lifecycle primitive

Hasten already has a draining lifecycle stage. Expose it directly. A process entering drain stops accepting new eligible work, completes already-admitted work, cooperates with ownership movement, and reports when it is safe to terminate. This should remove bespoke shutdown/rebalance glue from applications.

# 35. Maintenance as resource fencing

Maintenance mode should not primarily be an application switch. A maintained dependency can be represented as a tagged shared resource whose currently available capacity is reduced or fenced. Any Hasten workload touching that resource dynamically adapts.

> Fence the resource, not the applications.

If `database/orders-primary` is fenced, work requiring that resource waits while unrelated work continues. Applications do not need to know every other application sharing the dependency. When the fence is removed, accumulated work becomes eligible and drains under normal QoS/resource/adaptive-concurrency controls rather than creating an uncontrolled thundering herd. This naturally supports:

- full capacity zero during hard maintenance
- degraded capacity during partial maintenance
- critical/recovery QoS while ordinary work remains fenced
- gradual restoration/ramp-up
- dynamic/tag-based fences such as region, tenant, dependency or resource class
- full topology backpressure where downstream work depends on the fenced execution path. Application/process maintenance is a specialization of the same primitive: fence new admission to that execution resource, allow admitted work to drain, then report quiescence.

> Maintenance is a temporary resource constraint.

# 36. Native unresolved-work / DLQ model

Parallel Consumer's sparse completion frontier enables a fundamentally different Kafka failure model. A failed record does not need to leave its original partition merely so later unrelated records can make durable progress. Suppose offset 100 fails permanently under the currently deployed application version. Offsets 101-105 belonging to independent ordering domains may complete and remain represented as completed in the PC frontier. Records sharing offset 100's ordering domain naturally remain lined up behind it. Offset 100 itself remains unresolved in its original causal and ordering position.

> The failure queue is the original log.

> A failed record never leaves its causal position.

This is not conventional DLQ-topic management. It eliminates the need to copy failed records into side topics merely to advance the partition, and therefore avoids reinsertion ordering, duplicate topic topology, loss of original execution context and reconstruction of ordering relationships. The operational surface becomes a projection over unresolved work:

- inspect unresolved records and their complete execution/decision history
- group/filter by application version, exception, key/order domain, resource or age
- retry one, a selection, or all eligible unresolved work
- explicitly resolve/skip under policy where permitted
- show dependent records currently waiting behind each unresolved ordering domain
- retain unrelated partition progress throughout.

After deploying a corrected application version, an operator can press **Drain failures**. The unresolved records become eligible again. If they now succeed, dependent records naturally become runnable. To the execution model this is indistinguishable from an extraordinarily late completion; unrelated work did not suffer partition-level head-of-line blocking in the meantime.

> A DLQ without a queue.

This should be treated as a signature Hasten/Parallel Consumer capability rather than an implementation detail. It arises specifically from the sparse completion/offset model.

# 37. Cron, scheduled and batch work

Cron and nightly batch execution fall directly out of durable eligibility. A scheduled job is ordinary work whose eligibility condition includes time or a recurrence rule. Once eligible it uses the same ordering, resource contracts, QoS, adaptive concurrency, recovery and decision telemetry as real-time work. This unifies cron, nightly batches, delayed work, Retry-After, maintenance jobs and long workflow waits without introducing a separate scheduler service. For Kafka applications, backfill and bounded replay should be exposed through the same runtime: select a topic/range/key/time/version scope, make the resulting work eligible, and let the normal scheduler execute it under production resource/QoS constraints.

> A timer is work whose eligibility condition is time.

# 38. Embedded compatibility services

When Hasten already materializes the authoritative state needed by an established infrastructure API, prefer exposing a compatibility facade rather than requiring another service deployment.

## 38.1 Schema Registry compatibility

Kafka-backed Schema Registry is established prior art: Confluent Schema Registry, Karapace and Apicurio already demonstrate storing/materializing schema state using Kafka. Hasten's opportunity is therefore not "put Schema Registry in Kafka". The useful inversion is ubiquitous embedded serving. Hasten should investigate embedding/reusing the important compatibility machinery from an appropriate implementation (subject to license and architectural review) so every runtime can maintain a high-speed local schema cache and, where useful, expose a Confluent Schema Registry-compatible API. The same internal catalog can additionally understand Hasten-specific contracts: RPC procedures, actor state, Streams stores, envelope versions and side-effect boundary contracts. Do not claim novelty for Kafka-backed schema storage; the value is removing another separately operated server tier and making schema/contract knowledge local to execution.

## 38.2 Consul-compatible discovery

Hasten already needs a distributed catalog of applications, procedures, actors, Streams stores, ownership, versions and health. A Consul-compatible discovery facade can project the subset conventional applications expect, while native Hasten clients use richer logical ownership-aware addressing.

## 38.3 Service-mesh compatibility

Istio/Envoy-style service meshes operate primarily on network traffic. Hasten sees logical work before it becomes network traffic: retry identity, causal context, side-effect state, QoS, resource contracts, deadlines and alternative runnable work.

> Istio meshes network connections. Hasten meshes execution.

Investigate standard Envoy/xDS-compatible projections where they let existing gateways/proxies consume Hasten discovery/routing/policy. Do not build a new proxy or security system merely for architectural symmetry; wrapping Kafka and common clients is the preferred low-complexity insertion point.

# 39. Runtime wrappers as transparent augmentation seams

Hasten is already resident in many applications through Kafka client wrapping and may gain additional visibility through carefully chosen HTTP/database/RPC client wrappers. These wrappers should remove application work rather than create a new mandatory framework. Useful automatically observed/derived services include:

- dependency discovery and live dependency health
- topology-aware service health
- resource identification/observations where safely inferable
- OpenTelemetry propagation and enrichment
- causal/data lineage metadata
- retry/deadline observations
- runtime catalog/discovery publication.

Avoid security-heavy or speculative "edge authority" abstractions unless a concrete requirement demands them. The insertion strategy should remain boring: wrap infrastructure developers already use, discover safe facts, and expose useful runtime services from those facts.

# 40. Lineage is carried, not reconstructed

Extend Hasten/OpenTelemetry envelopes with causal and data-lineage identifiers. Combine execution identity, parentage, retry lineage, RPC continuation, actor/workflow identity, Streams topology/store metadata and schema information. Where interoperable, project to OpenTelemetry and OpenLineage conventions. Traditional lineage systems often reconstruct relationships after execution from SQL, logs or traces. Hasten should preserve lineage while work moves through the execution fabric.

> Lineage is execution metadata carried with the work.

# 41. CDC to automatically served distributed read models

CDC plus Kafka Streams plus Hasten distributed IQ creates a straightforward runtime service: automatically maintained, queryable distributed read models of operational database state. Conceptual path: Database → CDC topics → schema discovery → generated/augmented Streams materialization → active/standby state stores → Hasten distributed IQ → polyglot typed clients. The target experience can be as simple as declaring which database tables/entities to mirror. Hasten then handles the operational apparatus around the materialization: discovered schemas, store creation/configuration where safe, ownership, restoration/readiness, replica-aware query routing, polyglot access, catalog/discovery, lineage and observability. Be precise in product language: these are Kafka Streams-backed distributed/materialized read models, not storage-engine database replicas. Their consistency semantics must be explicit. This is especially valuable because the resulting state is not isolated infrastructure. It lives inside the same execution fabric: it can be queried through IQ, associated with actors/entities, participate in lineage, and be observed alongside the work that updates or consumes it.

# 42. New product/design rules

The latest runtime-service work sharpens several rules:

1. **Build the missing runtime, not another programming framework.** Preserve normal Kafka Streams/business code and generate/augment the distributed-system apparatus around it.
2. **If Kafka/Streams already knows the answer, do not ask the developer to configure it again.**
3. **Prefer projections over duplicate subsystems.** Health, DLQ, discovery, lineage, schema serving and maintenance should expose state Hasten already needs rather than create parallel control mechanisms.
4. **Keep failed work in its causal position when possible.** PC's sparse frontier is a product primitive, not just offset bookkeeping.
5. **Fence dependencies, not fleets of applications.** Let the resource graph propagate maintenance/backpressure.
6. **Compatibility is a distribution strategy.** Where useful, pretend to be the infrastructure applications already understand: Confluent Schema Registry, Consul, standard health endpoints, Envoy/xDS, OpenTelemetry/OpenLineage.
7. **Transparent augmentation beats topology generation.** Machine-generate runtime services implied by the user's topology; do not infer or replace business topology semantics.

These reinforce the existing architectural law of compositional reinforcement: each new capability should preferably emerge as another projection of ownership, eligibility, resources, durable state, execution metadata and the sparse completion frontier rather than requiring an independent distributed system.

# 43. Partition virtualization: attack Kafka partition rigidity from both sides

Parallel Consumer already makes a Kafka partition internally virtual: PC shards/order domains let unrelated keys execute independently while preserving the ordering that actually matters. Hasten can now attack the same rigidity from the outside because it can control both production and consumption, including inside Kafka Streams integrations.

> PC attacks Kafka partition rigidity from both sides.

> Inside the partition: virtual shards remove unnecessary head-of-line coupling.

> Outside the partition: Hasten can optimize ownership, extract pathological keys, and evolve the physical topic underneath a stable logical stream.

The objective is not to pretend Kafka physical partitions do not exist. It is to make their count and historical hash placement matter far less to application performance, and to provide an evolution path when they genuinely become binding constraints.

## 43.1 Stable logical shards over mutable Kafka placement

PC shards are the natural virtual-partition primitive. A shard/order domain retains its logical identity while Hasten may change its physical backing at deterministic routing boundaries. Useful identities should distinguish:

- physical Kafka address: topic + partition + offset
- logical stream identity
- virtual shard/order-domain identity
- virtual/logical position where required
- routing/topology epoch
- physical provenance/generation.

> Stable logical shards over mutable Kafka placement.

Do not unnecessarily expose this virtualization to ordinary Kafka-compatible applications. Vanilla compatibility should retain normal Kafka semantics. More aggressive virtualization is appropriate when Hasten owns both endpoints or for Hasten/Streams-generated internal topics.

## 43.2 Globally coordinated routing state

A key primitive is a globally materialized routing map, naturally represented through Kafka Streams/KTables/control topics, whose changes are anchored to durable log positions and epochs. A routing entry may describe key/shard → physical topic generation + partition, together with the source offset/frontier and routing epoch from which that mapping becomes authoritative. Producers and consumers materialize the same routing state. Hasten-controlled producers must not emit work using a newer routing epoch than the routing state they can themselves resolve. Consumers use the epoch/frontier to gate execution and preserve ordering while routing state converges. This permits routing to be distributed/eventually propagated without turning stale routing knowledge into an ordering violation.

> Routing changes are data, anchored to durable execution frontiers.

The broader primitive is a deterministically versioned physical representation of a stable logical execution graph.

## 43.3 Scheduler-driven partition assignment

Before changing physical placement, Hasten should exploit Kafka's existing ownership seams. A Hasten partition/task assignor should use scheduler knowledge rather than treating equal partition counts as equal load. The optimizer can consider:

- actual and look-ahead key distribution
- predicted runnable/executable opportunity per partition
- hot-key concentration
- local adaptive-concurrency envelopes
- spare execution capacity on other runtimes
- state-store size and restoration/migration cost
- standby locality
- downstream/shared-resource constraints
- maintenance/resource fences
- unresolved work and recovery pressure. A consumer with two hot partitions should not retain both while another compatible consumer is mostly idle merely because the partition counts are numerically balanced.

> Balanced ownership means balanced predicted useful execution pressure, not balanced partition counts.

For Kafka Streams, prefer its task-assignment semantics so state, joins, stores and IQ ownership continue to move coherently with Streams tasks. Hasten distributed IQ and other ownership-aware services should follow the resulting assignment automatically.

## 43.4 Hierarchy of partition interventions

The scheduler should choose the cheapest intervention that actually releases useful work:

1. **Internal execution:** exploit independent PC shards inside the existing physical partition.
2. **MOVE_PARTITION / MOVE_TASK:** move a whole physical partition or Streams task to a less-loaded runtime.
3. **SPLIT_SHARD / key extraction:** when multiple hot independent keys are trapped behind the same physical ownership boundary, extract only the problematic logical shard/key.
4. **MERGE_SHARD:** collapse an extracted shard when the pathological distribution disappears.
5. **EVOLVE_TOPIC:** when the physical topic shape itself remains structurally poor, migrate logical shards to a new backing topic generation with a better partition layout.
6. **SCALE_RUNTIME / SCALE_RESOURCE:** add infrastructure only when placement/topology transformations cannot unlock the available opportunity.

> Do not scale machines because Kafka happened to hash badly.

This introduces a first-class **partition-placement ceiling**: useful parallelism and spare execution capacity exist, but Kafka's current physical placement prevents them from meeting. Unlike most ceilings, Hasten may be able to remove this one itself.

## 43.5 Predictive hot-key extraction and shadow routes

The graph optimizer can see longer-term key distribution across the owned execution horizon. If two or more sustained hot keys collide on one physical Kafka partition, Hasten can predict that the physical ownership boundary will become the bottleneck before simple lag/CPU signals make the problem obvious. Instead of repartitioning the user's visible topic, Hasten can create a virtual/shadow route for one key or shard. Future records for that logical domain are routed to a Hasten-controlled shadow topic/partition while the wrapped consumer subscribes to both physical sources and transparently merges them into the same logical shard set. PC ordering incorporates routing epochs/frontiers so old-route and new-route records remain correctly ordered. Independent keys continue without head-of-line blocking.

> A virtual partition is created when physical partitioning becomes an observable execution constraint.

The shadow can be removed later. Hot-key extraction is therefore elastic rather than a permanent topology decision.

## 43.6 Topic generations: reversible physical partition layouts

Kafka cannot transparently shrink an existing topic's physical partition count, and changing partition counts is operationally consequential because keyed placement changes. Hasten need not resize the physical topic in place. When both producer and consumer are Hasten-controlled, a stable logical topic can be backed by successive Kafka topic generations:

- generation N has one physical partition layout
- Hasten computes a better layout
- generation N+1 is created with the chosen physical partition count/configuration
- routing entries establish per-shard deterministic cutover frontiers/epochs
- producers begin sending post-frontier logical records to the new generation
- consumers temporarily subscribe to both generations
- PC merges them into stable logical shards and preserves ordering using routing epochs
- after all old-generation work is resolved, the old physical generation can be retired.

The application continues to see one logical stream.

> Hasten does not resize the Kafka topic. It replaces the physical representation underneath the logical topic.

This makes historical partition-count decisions increasingly reversible.

## 43.7 Partition-count optimization from real key distributions

Do not choose new partition counts by simplistic doubling or throughput arithmetic alone. Hasten has access to actual key-frequency distributions and a look-ahead execution horizon. For candidate physical partition counts/layouts, the optimizer can simulate the relevant partitioner and estimate:

- predicted load distribution
- maximum partition pressure
- variance/skew
- hot-key collisions
- useful consumer parallelism exposed
- expected assignment quality
- state migration/restoration cost
- downstream/resource consequences
- operational cost of additional physical partitions.

The correct recommendation may be an irregular count such as 19 rather than 16/32 because the actual keys hash materially better at that layout.

> Partition count becomes an optimization output derived from observed workload, not a capacity guess made years in advance.

Closed-loop operation is possible: predict a better layout, migrate to it, and measure whether the predicted execution improvement occurred.

## 43.8 Internal Streams topics are the safest virtualization target

Kafka Streams-generated repartition and changelog topics are especially attractive for early aggressive optimization because their physical representation is already runtime-generated rather than an external application contract. Investigate Hasten-controlled partition-count selection, generation migration, placement optimization and restoration-aware management for these topics before applying equivalent transformations to user-visible topics. Preserve Streams correctness requirements such as co-partitioning, joins and state/task affinity.

# 44. Virtual records and transparent large-message support

Because Hasten controls both producer and consumer boundaries, a logical record need not map one-to-one to a physical Kafka record. A normal logical record may remain one physical record. A very large logical record can transparently become multiple chunk records carrying logical record identity, ordering/shard identity, routing epoch, chunk index/count, integrity information and envelope metadata. The consumer reconstructs the logical record before exposing it to PC, Kafka Streams or user code. The application still produces and consumes one logical record. Chunk/reassembly state should participate in normal Hasten semantics:

- an incomplete logical record is ineligible rather than partially delivered
- completion/offset state does not consider the logical record complete until reconstruction semantics are satisfied
- ordering applies to the logical record
- tracing and lineage follow the logical identity
- unresolved/DLQ semantics operate on the logical message rather than arbitrary chunks
- resource/QoS controls can constrain expensive reconstruction
- health/Why wait? can explain missing/corrupt chunks.

For extremely large payloads, a future implementation may use externalized payload storage plus a Hasten manifest while retaining the same logical-record API, but do not require this for the basic chunking design.

> Kafka's physical message-size limits do not have to become application constraints.

This extends the virtualization stack:

- **virtual record** — one logical record over one or more physical representations
- **virtual offset/position** — logical execution position distinct from physical Kafka address where required
- **virtual shard / PC shard** — logical ordering/execution unit finer than a physical partition
- **virtual partition** — stable logical placement unit with mutable physical backing
- **virtual topic** — stable logical stream spanning physical topic generations
- **virtual topology generation** — deterministic evolution of physical representation behind stable application semantics.

# 45. Architectural synthesis: make partition decisions less architectural

Kafka partitioning historically leaks deeply into application architecture: ordering, consumer parallelism, state placement, joins, scaling and operational migration all depend on decisions that are difficult to change later. PC/Hasten should not promise that partitions cease to matter. Instead it progressively reduces the consequences of getting the physical layout wrong:

- PC shards make partition count matter less for execution concurrency
- scheduler-driven assignment makes equal partition counts unnecessary for balanced runtime load
- hot-key extraction removes pathological hash collisions
- graph look-ahead identifies placement problems before blind infrastructure scaling
- topic generations provide an evolution path when physical partition count really is wrong
- distributed IQ/state routing follows logical ownership changes
- routing epochs/frontiers make those changes deterministic and replayable.

> Inside: PC makes partitions matter less by virtualizing execution into shards.

> Outside: Hasten makes partition placement and count less permanent by optimizing ownership and evolving the physical backing.

The target outcome is not "partitionless Kafka." It is that users can choose a reasonable initial partition setup without treating it as a permanent performance architecture decision.

# 46. Product story: Hasten — Why wait?

The expanded product vision should still resolve to a simple meaning rather than an inventory of mechanisms. For non-native English speakers, **to hasten** means to make something happen sooner: to remove unnecessary delay or bring forward something that would otherwise happen later. It does not mean "make everything run at maximum speed." Waiting may be correct because ordering, capacity, safety, QoS or an unavailable dependency requires it. Hasten's invariant is therefore:

> Something was going to happen. Hasten helps it happen sooner when there is no good reason to wait.

The canonical logistics story now spans the expanded runtime:

- a parcel initially waits behind an unrelated parcel because Kafka's partition is coarser than the true dependency structure; PC shards let it proceed
- two hot customers collide on one conveyor; Hasten changes ownership or surgically extracts one logical shard rather than blindly adding machines
- if the physical conveyor layout is genuinely wrong, Hasten can evolve the backing topic generation while preserving the logical stream
- a database enters maintenance; the resource is fenced and only work requiring it waits
- a failed parcel remains in its original causal position rather than being copied into a DLQ, while unrelated work proceeds; after a deployment, retrying it is indistinguishable from very late completion
- midnight batch work becomes eligible and shares spare capacity with realtime work under the same QoS/resource scheduler
- distributed IQ finds state without the developer implementing ownership routing
- CDC can materialize operational database state into queryable Streams-backed read models
- the optimizer distinguishes key/placement constraints, downstream saturation and genuine compute shortage before recommending additional capacity
- every waiting unit can answer **Why wait?**

The story should culminate with:

> **Hasten** — make it happen sooner.

> **Why wait?**

This remains the brand-level compression of the entire architecture even as the runtime expands.
