<!-- issue-refs: exempt-file - preserved verbatim external document; its #N tokens are markdown TOC anchors and section numbers, not issue references -->
> Provenance: the second handoff document from the strategy weekend - the supplement covering
> everything after the main handoff
> ([`2026-08-29-hasten-compound-engineering-handoff.md`](2026-08-29-hasten-compound-engineering-handoff.md))
> was written, i.e. the 2026-08-30/31 exchanges. Preserved verbatim except for HTML-comment gate
> markers inserted where the text names files by their original download names. This document
> resolves several terms the earlier captures flagged as uncaptured (Guild/Navigator, No-record /
> No-ship, Sandtrout/Worm, the Nile relationship) and carries two sequencing rulings tracked in
> `docs/inflight/core-lighthouse-mvp.md`. Where it and a note disagree, flag it - do not silently
> reconcile.

# Hasten Compound Engineering Handoff — Supplemental Update

**Scope:** Only concepts and decisions discussed after the last update of `Hasten_Compound_Engineering_Handoff.md` (preserved here as `2026-08-29-hasten-compound-engineering-handoff.md`). This document is intended to be appended/merged into the main handoff later. It deliberately does not repeat the earlier architecture except where needed to explain a new refinement.

## 1. Scheduler placement correction: a scheduler on every execution path

An important wording correction was made to the earlier architecture. Hasten is **not** a design where the hot path avoids a scheduler. There is a scheduler on every execution path. The important property is that scheduling is not centralized.

The scheduler is **sharded with the work it schedules**. Kafka ownership determines where the relevant embedded scheduler authority lives, and when work ownership fails over, scheduler responsibility follows it. Global coordination can exist above this, but record-by-record execution decisions remain local to the execution path.

Strong formulations:

> The scheduler is sharded by the work itself.

> Scheduler failover follows work failover.

> Your applications are the scheduler.

Hasten can therefore be described as a transparent, fault-tolerant distributed scheduler embedded in every execution path, coordinating globally while scheduling locally and looking ahead as far as its available working knowledge permits.

## 2. Native unresolved-work / DLQ semantics refined

The existing PC sparse completion model already implements most of the useful semantics normally sought from a DLQ, without requiring failed records to leave their original log position.

### 2.1 Failure is not DLQ

A normally failed record remains unresolved in the PC completion frontier. Records in unrelated ordering domains continue. Records in the same ordering domain remain behind the failed head record. Continuous retry may continue automatically, or the record can be explicitly held.

This is not merely an implementation convenience. The head-of-shard block can be a **desirable correctness property**. If subsequent records must not execute until the failed record is correctly processed, preserving that block is exactly what the application needs.

> The failure queue is the original log.

> A failed record never leaves its causal position.

The missing product surface in PC was primarily **visibility and operator control**, not necessarily a side-topic DLQ.

### 2.2 Expose the DLQ architecture PC already has

Hasten/PC should expose unresolved work through API and GUI:

- browse blocked shards and unresolved records
- show failure signature, exception, attempts, age, app/version and original Kafka coordinates
- show how many same-shard records are waiting behind the head
- query unresolved work through the PC/Hasten API
- alert on old/stuck shards, repeated signatures and growing dependent queues
- explicitly Hold automatic retry where appropriate
- **Proceed** after a fix/redeployment, preserving the original causal position

`Proceed` is preferred to `Retry` because the semantic action is to make the original unresolved execution eligible again.

### 2.3 Skip/abort is the point at which DLQ state becomes necessary

Skipping is semantically different from failure or hold. An explicit Skip/Abort decision means Hasten deliberately releases the original causal position so later same-shard work can continue.

On Skip/Abort:

- PC/Kafka offset progress treats the record as acknowledged/completed for frontier purposes
- durable skipped/aborted state is written separately to a compact Kafka/Streams-backed state log/store
- the Hasten GUI and PC API can browse/query this state
- a conventional application DLQ topic can optionally be produced as a **compatibility projection**
- that projection may contain metadata/signature only, or metadata plus a copy of the original payload

Useful durable metadata includes logical execution ID, topic/partition/offset, key/order domain, application/version, abort reason/actor, failure signature, timestamps/attempts/lineage, schema identity and payload hash.

Strong distinction:

> Proceed preserves causal position.

> Skip sacrifices/releases that position so dependent work may continue.

> Don't DLQ on failure. DLQ only when you deliberately abandon the original execution position.

If skipped work is executed later after subsequent same-key records have run, that is **not** ordinary late completion and is no longer original-order preserving. It must be presented explicitly as reprocessing/recovery/compensation under a new execution identity, or the user must knowingly accept changed ordering semantics.

## 3. Hasten Logistics LLC: the lighthouse system

The canonical logistics example is now explicitly **Hasten Logistics LLC**.

A suitable tagline is:

> Hasten Logistics — Why wait? We deliver.

The logistics application should not be a collection of feature demos. It should be one coherent fictional company that continuously exercises the complete Hasten architecture.

Design rule:

> If Hasten claims it, Hasten Logistics uses it.

And, more strongly:

> No feature demo. One company.

The logistics estate becomes simultaneously:

- tutorial
- architectural integration test
- polyglot compatibility suite
- chaos/failover environment
- performance playground
- documentation source
- contributor onboarding path
- conference/product demonstration

New ideas should be forced through the question: **What does this mean inside Hasten Logistics?** If a purportedly fundamental Hasten feature cannot compose naturally into the same company/runtime, challenge whether it belongs in the execution kernel.

## 4. Every supported language gets a meaningful logistics component

Hasten Logistics should contain at least one component in every supported language, even when that component is not itself a novel tutorial step. The objective is to continuously prove that RPC, addressing, state, tracing, resources and execution semantics are genuinely transparent across language boundaries.

Each language should preferably perform work that naturally exploits that ecosystem rather than implementing Hello World repeatedly. Examples discussed:

- Java/Kotlin: Kafka Streams / PC reference execution and stateful processing
- Python: route optimization, forecasting or scientific computation using the Python numerical/scientific ecosystem
- Go: naturally concurrent/network-oriented customs or eligibility component
- Rust: CPU/concurrency-sensitive work
- TypeScript: operational/event-facing component
- C#: enterprise-oriented policy/rules component
- Swift: possible depot/operator client
- Haskell: deliberately exercise a truly functional programming model; Hasten RPC is naturally represented as an explicit effect/durable future rather than disguised as an ordinary local call
- Elixir: native-feeling Elixir/BEAM actor whose durable identity/mailbox/state are backed by a Hasten actor

Acceptance criterion:

> No language island.

Components should call across multiple language boundaries, not merely Java-to-everything. A Java → Python → Go path is more valuable than independent Java↔language examples because it proves language neutrality is architectural rather than hub-and-spoke binding glue.

## 5. CDC dogfood through an existing corporate policy database

Hasten Logistics does not need to invent an application database merely to exercise CDC. A more realistic scenario is a pre-existing corporate PostgreSQL system that Hasten emphatically does not own.

For example, it contains service/resource policies such as service, resource, max concurrency, rate limit, priority, maintenance state, region and effective time.

Flow:

**Existing PostgreSQL → CDC → Kafka → Hasten materialized read mirror → policy adapter → live Hasten resource policies**

This simultaneously exercises CDC, schema discovery, Streams materialization, embedded distributed IQ, policy/config distribution, dynamic ResourceContracts and polyglot state access.

Product story:

> You do not have to move your source of truth into Hasten. Hasten can continuously turn the source of truth you already have into execution policy.


## 7. MVP technical prototype spike: a miniature Hasten universe

Ignoring current PR/merge sequencing, the best prototype is a deliberately thin end-to-end logistics slice that exercises the maximum number of critical architectural dimensions with minimal implementation depth.

Suggested minimal deployment:

- one Kafka cluster
- Java/Kotlin Streams + PC execution kernel and shipment state
- Python route optimizer
- Go customs/rules component
- small web GUI
- Kafka-backed control/catalog/resource/skipped-state topics

Critical dimensions to prove:

1. transparent Kafka client substitution
2. record-by-record/key-ordered scheduling
3. two runtime instances so scheduler ownership/failover follows Kafka ownership
4. one globally named resource with tiny lease/delegation implementation
5. `Why wait?` / `Proceed` decision states
6. native unresolved-work semantics plus explicit Skip and skipped-state query
7. cross-language RPC
8. one Hasten actor, e.g. `ShipmentActor/{shipmentId}`
9. distributed IQ with local/remote owner routing
10. compact runtime catalog
11. one resource maintenance fence
12. scaling recommendation only, distinguishing true compute shortage from a downstream binding constraint

The prototype should intentionally leave sophisticated implementations hollow. Its purpose is to prove that these surfaces are projections of one execution kernel rather than unrelated systems glued together.

## 8. Additional distributed primitives that fall out of Kafka + Hasten

Several further mechanics appear to require only a thin Hasten ergonomic/semantic layer because Kafka/Streams already supply persistence, replication, ordering, ownership, failover and/or materialized state:

- leader election / singleton responsibility / leases
- durable futures and promises
- barriers, countdown latches and phasers
- durable conditions/watches
- idempotency / `once` gates for Kafka-contained effects
- distributed rendezvous/fan-in
- presence/membership/capability discovery
- durable causal cancellation
- single-flight/request coalescing
- application-level quorum decisions
- globally coordinated circuit breakers expressed as dynamically inferred resource policy
- Share Groups as an alternate work acquisition substrate for genuinely unordered/fungible jobs

A useful unification emerged:

> Known work is runnable when its eligibility predicates are satisfied.

Ordering wait, resource wait, timer wait, RPC future, barrier, condition and dependency are all forms of **known work with unsatisfied eligibility predicates**. `Why wait?` can therefore explain all of them through one model.

## 9. Kafka Share Groups / Kafka Queues and PC's durable role

Kafka's “Queues” work is implemented through **Share Groups / ShareConsumer** semantics. Multiple consumers can acquire individual records from the same partition concurrently using record-level acquisition locks/acknowledgements. This provides queue-like competing-consumer behavior by giving up ordinary partition processing order.

Useful conceptual comparison:

| Model | Distribution/execution unit | Processing ordering |
|---|---|---|
| Consumer Group | partition | partition order |
| Share Group | record | no general parallel processing order |
| PC/Hasten | ordering domain/key | per ordering domain |

Share Groups therefore clarify rather than eliminate PC's semantic niche. Hasten should eventually accept multiple acquisition engines beneath the same execution model:

- classic Consumer + PC sparse completion
- ShareConsumer for fungible work
- future Kafka primitives that may absorb more PC mechanics

This makes Hasten robust to Kafka eventually implementing PC's original core idea. If Kafka provides the lower-level mechanics natively, Hasten can delete code and use them. Hasten's durable abstraction remains application-aware execution scheduling.

Strong lineage:

> Parallel Consumer: ownership ≠ execution.

> Hasten: once those are separated, execution itself becomes programmable.

PC's original mechanisms become one small but foundational part of Hasten rather than the whole product thesis.

## 10. Unique injection point: between Kafka ownership and user execution

The architectural location PC occupies is more valuable than originally appreciated. Kafka sees durable records and ownership; application code sees business logic. Hasten sits at the handoff where both the durable work identity and the impending execution decision are visible.

This enables capabilities that are difficult for brokers, external schedulers, service meshes, APM systems or application code to implement as cleanly:

- semantic load balancing using ordering/resource/state/retry/deadline knowledge
- selective semantic backpressure rather than stopping an entire service
- semantic retry classification and failure-domain-aware retries
- record-level canaries and shadow execution while preserving ordering-domain semantics
- pre-execution observability: explain work that has not executed yet
- version-aware replay and repair
- cost-aware/deadline-aware scheduling
- dynamic batching and cross-record coalescing
- state/dependency prefetch and task warming
- placement based on data/cache/model locality
- priority inversion detection
- critical-path and unlock-value scheduling
- semantic draining/quiescence
- exact in-flight state rather than crude consumer lag
- execution-time schema/version/implementation routing
- causal debugging

The strongest new scheduler objective is not merely “what can run?” but:

> What should run next to unlock the most useful future?

## 11. Prescience: turn PC look-ahead into a first-class knowledge system

A major architectural discovery is that PC's existing buffer was optimized for **feeding workers**, but Hasten can use available memory/storage to **feed the scheduler knowledge**.

Instead of fetching only enough records to keep workers busy, Hasten can index as much of the currently committed backlog of its assigned partitions as resources permit.

This produces a new first-class concept:

> **Execution Horizon** — the committed future work currently visible to the scheduler.

> **Prescience** — the degree/percentage of currently committed outstanding work represented in that scheduling knowledge set.

At a captured set of Kafka high-watermarks, 100% Prescience means every currently committed outstanding record within the relevant ownership frontier is represented in the scheduler's knowledge. It does **not** claim knowledge of records that have not yet been produced.

Strong guarantee at 100% coverage, subject to sufficient semantic metadata:

> No currently committed eligible work in the owned backlog is hidden from the scheduling decision.

This converts queue jumping, priority, deadlines, hot-key analysis, resource demand and critical-path scheduling from small-buffer heuristics into potentially complete optimization over the known backlog.

## 12. Prescience Store is separate from PC execution shards

Do not make the existing PC execution buffer enormous. Separate two concerns:

### Prescience Store

A potentially complete, highly compressed/indexed representation of future work. It contains only enough information for scheduling decisions.

### Execution shards

The existing hot subset of complete/hydrated records actually selected for near-term execution.

Flow:

**Kafka log → Prescience Store → Mentat/scheduler selection → execution shards → workers**

The Prescience Store is derived/disposable state. Kafka remains authoritative, so the store can be rebuilt after loss.

It should tier automatically:

- compressed RAM/off-heap
- mmap/local NVMe
- embedded index/KV/LSM engine such as RocksDB or an equivalent implementation behind an abstraction

The exhaustive index can live on disk while RAM retains hot candidate structures, aggregates and actual execution shards.

## 13. Minimal Spice: key → offsets

For basic PC Prescience, the irreducible scheduling metadata can be astonishingly small:

> **ordering key/domain → outstanding offsets**

Ordered offset lists can be delta/varint encoded and compressed aggressively. This alone provides complete knowledge of which ordering domains exist, their depth, their heads, where important keys occur in the physical log and how much independent work exists.

Additional metadata such as priority, deadline, resources, cost, causal links and routing epoch enriches scheduling but is not required for the basic Prescience feature.

This makes **100% Prescience a plausible standalone Parallel Consumer feature today**, independent of the rest of Hasten.

At 100% Prescience, PC can legitimately act as a semantic queue over Kafka: arbitrary execution selection across independent ordering domains while preserving order inside each domain.

## 14. Prescience as an elastic runtime SLO

Prescience should become an explicit portable runtime policy, conceptually like:

`targetPrescience(100%)`

The exact API should be idiomatic in every language, but the contract is shared.

Hasten can dynamically maintain the target:

1. extend the Prescience Store into available local RAM/disk
2. if storage/index throughput/capacity becomes limiting, add application/runtime nodes
3. spread ownership so aggregate distributed Prescience capacity rises
4. if physical partition count prevents sufficient distribution, use virtual partitions/topic generations to increase logical/physical parallel placement
5. continue until the target is met or a declared budget/constraint is reached

This creates **elastic scheduler knowledge**. Additional nodes may be added not because CPU is exhausted, but because more distributed memory/storage is needed to see enough of the known future.

Useful runtime modes/policies include target percentage, fixed memory/storage budget, cost budget, or target time horizon.

The UI should expose both target and limiting factor, e.g. memory, local disk, partition count, or budget.

## 15. Physical versus semantic Prescience; No-records and No-ships

Indexing every committed record does not necessarily mean Hasten understands every scheduling consequence before execution.

Distinguish:

- **physical/committed coverage** — whether every committed outstanding record has been observed/indexed
- **semantic Prescience** — whether sufficient scheduling information is known to make informed decisions about that work

A **No-record** is visible as work but semantically opaque in some important scheduling dimension until execution or further inspection.

A **No-ship** is an opaque execution/system boundary behind which Hasten cannot see the queued future or internal constraints, for example a legacy external service.

This lets Hasten explicitly represent where its knowledge ends rather than allowing the optimizer to imply omniscience.

Possible UI:

- committed coverage: 100%
- semantic Prescience: 97.2%
- No-records: N
- No-ship boundaries: legacy ERP, external vendor, etc.

## 16. Dune-derived architecture vocabulary

The Dune vocabulary has become useful because several terms map cleanly to real architectural distinctions. Public API usage should remain understandable to people who do not know Dune; some names can remain internal/demo terminology.

### Prescience

Knowledge/coverage of the currently committed future work.

### Mentat

The optimizer that reasons over the Prescience set and produces local scheduling/optimization recommendations.

> Prescience tells Hasten what lies ahead. The Mentat decides what should proceed.

### Golden Path

The selected optimal execution trajectory given current Prescience, objectives and constraints. It can incorporate unlock value/critical path rather than simply selecting the next oldest runnable record.

### Guild / Guild Navigator

The higher-level placement/topology optimizer. Navigators use Mentat outputs and global information to determine where work/ownership/capacity should live.

### The Voice

The authoritative control plane. Mentats reason; Navigators plan; **The Voice communicates authoritative policy, leases, epochs and coordination decisions** to embedded runtimes.

### Kwisatz Haderach

An intentionally playful constant/name for complete semantic Prescience over the defined committed frontier. The normal numeric API should remain canonical, with a Dune alias/easter egg possible.

### No-record / No-ship

Explicit markers of semantic opacity as described above.

### Spice

**The scheduling metadata is the Spice.** Payload size is not what gives Hasten power; compact scheduler-legible metadata does.

A possible quantitative metric is **Spice density**: useful scheduling information per byte of Prescience storage.

## 17. Payload/control separation and Prescience density

The most important realization about 100% Prescience is that Hasten does **not** need to cache all business payloads. It needs to cache/index the **decision surface** of the work.

For large records, producer-side control creates an opportunity to separate compact scheduler-legible data from heavyweight payload.

Two implementation approaches remain open:

### Two-layer Prescience

Leave ordinary Kafka records unchanged and maintain a separate derived Prescience index containing only scheduling metadata. Payload remains in the original Kafka record and is fetched/hydrated when selected.

### Transparent head/body externalization

Because Hasten controls production and consumption, large fields/payloads can be transparently externalized into a Hasten shadow payload/data topic while the main/control record remains small and scheduler-legible. The consumer lazily joins/reconstructs the logical record only when execution needs it.

This gives existing transparent large-message support a second architectural purpose: **increase Prescience density** and make full semantic knowledge cheap even when logical messages contain very large blobs.

A fixed threshold such as 1 KB was suggested as an illustrative trigger, but the final policy should likely be adaptive/configurable rather than hard-coded. The actual question is whether externalizing the payload materially improves Prescience economics.

Strong synthesis:

> Prescience does not require caching Kafka's data. It requires caching Kafka work's decision surface.

For extreme cases, a two-layer design may be simpler and less invasive than automatically decapitating every message into head/body form. Both should remain architectural options until implementation experiments establish the simpler path.

## 18. Queue semantics: PC + Prescience becomes an explicit semantic queue

Kafka Share Groups implement a record-level competing-consumer queue by relaxing execution ordering. PC + Prescience can offer a different queue abstraction:

> A durable Kafka-backed queue in which physical log order remains unchanged, execution can jump arbitrarily far through the known backlog, and ordering is preserved exactly where declared ordering domains require it.

At 100% Prescience, priority scheduling can operate over the entire committed owned queue rather than the current PC buffer.

Potential scheduling policies include:

- strict priority
- weighted fairness
- deadline / earliest-deadline-first
- tenant reservation and borrowing
- aging/starvation prevention
- cheapest/shortest-job-first
- critical-path/unlock-value
- resource-aware packing
- priority inheritance

This gives PC a stronger independent product story even before the full Hasten runtime exists.

## 19. Kafka delayed delivery / KIP-1277 relationship

Kafka's delayed-delivery work is conceptually relevant because delaying one record while allowing later records to be delivered requires Kafka to relax physical offset delivery order and maintain an additional delivery-time index. This resembles a specialized broker-side out-of-order scheduler whose scheduling dimension is time.

If such broker functionality becomes available, Hasten should use it rather than recreate it. Time remains an eligibility predicate in Hasten, but Kafka can implement the efficient physical storage/delivery primitive.

General rule reinforced:

> As Kafka gains primitives, Hasten gets smaller, not less useful.

Hasten's value is composing lower-level Kafka capabilities into an application-aware execution model.

## 20. Folding space: topic-generation and placement transitions

The Dune “folding space” metaphor now has a concrete technical meaning.

**Space** is the physical Kafka placement/topology distance between where logical work currently resides and the placement that best serves execution.

A **fold** is a Hasten-controlled routing/ownership/topology transformation that removes that logical distance while preserving stable application-visible identity.

Possible fold classes:

- ownership fold — move partition/task ownership between runtime nodes
- shard fold — extract/reposition a hot logical shard/key
- topic fold — migrate a virtual topic to a new physical topic generation
- future cluster fold — migrate logical work between Kafka clusters while preserving logical addressing

The strongest current use is topic-generation migration. The Navigator can simulate candidate physical partition counts using known key distributions and Prescience, specifically seeking to avoid hot-key hash collisions. If, for example, 24 partitions collide important hot keys while 37 produces a materially better distribution, the Navigator selects the target geometry and Hasten folds the stable virtual topic onto the new physical generation.

### Guild ships / Heighliners

The **Guild** is the placement system, **Navigators** compute the desired fold, and a **Heighliner** can be the mechanism that executes a large ownership/topic-generation movement through Kafka's physical infrastructure. This is currently best treated as internal/demo vocabulary rather than a required public API.

## 21. Sandtrout and Worms

A tentative Dune mapping was identified for elastic virtual shards:

- **Sandtrout** — lightweight latent ordering domains/logical shards observed inside ordinary physical Kafka partitions
- **Worm** — a promoted independently routable virtual shard/partition once a logical domain becomes hot/important enough that physical placement is an observable execution constraint

This mirrors the existing design rule that virtual partitions should not necessarily be created eagerly. Hasten observes logical shards and promotes them only when independent mobility has measurable value. They may later collapse again when the hot condition disappears.

This vocabulary is fun and memorable but is not yet recommended as public API terminology.

## 22. Prescience changes partition elasticity into one control loop

Three ideas that previously appeared separate now form a single feedback system:

**Execution Horizon / Prescience → elastic runtime scaling → virtual partition/topic-generation elasticity**

Example:

1. backlog grows and Prescience falls below target
2. Hasten first extends the local tiered Prescience Store
3. if local storage is insufficient, Hasten adds runtime nodes to gain distributed Prescience capacity
4. if partition count prevents useful ownership distribution, Navigator detects a partition-placement ceiling
5. Hasten creates/extracts virtual shards or folds the virtual topic onto a better topic generation
6. ownership redistributes
7. Prescience rises toward target

Thus Kafka partition count becomes not only an execution scaling constraint but also a **knowledge-distribution constraint** that Hasten can eventually correct transparently.

## 23. PC's strategic role after Hasten

Hasten gives Parallel Consumer a durable role even if Kafka eventually absorbs PC's original core mechanics.

PC's original insight was that Kafka partition ownership and application execution do not have to be the same thing. Hasten takes the architectural position created by that separation and makes execution programmable.

PC mechanisms now map naturally into larger Hasten concepts:

- sparse completion frontier → unresolved-work / causal failure semantics
- key ordering → ordering-domain primitive
- PC shard → virtual partition / logical execution unit
- work queue → execution buffet
- retries → durable causal position
- adaptive concurrency → local admission control
- buffer/look-ahead → Prescience / Execution Horizon

If Kafka eventually provides record-level ordered acquisition/completion primitives that subsume more of PC, Hasten should adopt them. The higher-level scheduler, Prescience, resources, RPC, actors, state routing, placement, control and observability remain legitimate application-boundary responsibilities.

> Kafka implementing more PC is not an existential threat to Hasten; it is Kafka implementing more of Hasten's substrate for us.

## 24. Current strongest architectural synthesis

The latest architecture can be compressed as follows:

**Kafka** owns the authoritative durable log and lower-level distributed mechanics.

**Prescience Store** materializes as much scheduler-legible committed future as possible, potentially all of it.

**Spice** is the compact metadata that makes future work scheduler-legible.

**Mentats** reason over that future and identify execution opportunity.

**Navigators/Guild** optimize placement, ownership and physical topology.

**Golden Path** is the currently selected optimal execution/placement trajectory.

**The Voice** communicates authoritative global coordination/policy decisions.

**Embedded Hasten schedulers** live on every execution path, sharded with the work, and make local record-by-record execution decisions.

**PC execution shards** are the hot hydrated subset promoted from the much larger Prescience set.

**Proceed** is the positive admission/execution decision when no binding eligibility constraint remains.

At the project level:

> Parallel Consumer discovered that ownership and execution are independent.

> Hasten makes the resulting execution layer programmable.

And with Prescience:

> Hasten does not merely choose among the records it happened to fetch. It can know how much of its committed future it can see, elastically drive that knowledge toward 100%, and choose what should proceed from the entire scheduler-legible future.


## 25. Kafka Streams Lingual MVP is lighthouse-driven

Hasten Logistics should drive implementation of the multilingual Kafka Streams wrapper rather than treating multilingual Streams as an independent completeness project.

The MVP rule is:

> **Hasten Logistics defines the MVP surface area of multilingual Kafka Streams.**

Implement only the Kafka Streams functionality required to build the Hasten MVP/lighthouse. Once the logistics estate can express the required topologies, state/materialization and cross-language participation, stop expanding the wrapper until the rest of Hasten's core architecture is sufficiently complete and further Streams coverage becomes the next highest-value increment.

Important distinction:

- **Multilingual Kafka Streams** is enabling infrastructure for Hasten.
- **PC/Hasten execution inside Kafka Streams** is a later optimization and is not currently core to the Hasten MVP.

PC-inside-Streams is technically high-risk because Streams task/state/EOS/punctuation/join/restoration semantics can couple deeply to its execution model. A faster multilingual Kafka Streams is an attractive arm of Hasten, but it proves substantially less of the new Hasten thesis than Prescience, embedded scheduling, global resources, RPC, actors, unresolved-work semantics and decision telemetry.

Design rule:

> **No subsystem gets completed for its own sake. Hasten Logistics pulls through the minimum implementation necessary to prove the whole architecture.**

## 26. Hasten should be treated as a grounded architectural hypothesis

A devil's-advocate review identified real falsifiers rather than reasons not to start. Hasten is not a speculative architecture invented independently of experience. Its progression is grounded in years of Parallel Consumer implementation and observed Kafka constraints:

1. Kafka partitions unnecessarily couple ownership and execution for many workloads.
2. PC already demonstrates that finer ordering domains and sparse completion work in practice.
3. PC occupies the unusually valuable injection point between durable Kafka ownership and user execution.
4. Treating that position as an execution runtime causes many apparently separate distributed mechanisms to collapse onto common scheduling, ownership, state and admission primitives.
5. Prescience extends the same observation by turning Kafka's committed backlog into scheduler-visible future work.

The project should therefore be approached as a new architecture inferred from a working system and accumulated constraints, with a deliberately lean lighthouse used to falsify or validate the larger thesis.

Key things the lighthouse should attempt to falsify:

- shared scheduling primitives do not actually simplify the derived mechanisms
- deep Prescience does not materially improve decisions over a modest execution buffer
- transparency collapses and meaningful benefits require application redesign around a Hasten framework
- attempts to virtualize Kafka physical topology create more correctness complexity than the value they remove

If these survive implementation, the case for Hasten becomes substantially stronger.

## 27. Agentic programming changes the engineering cost model

Traditional estimates of implementation burden are increasingly misleading for Hasten. The expensive unit is no longer necessarily lines of code or number of language bindings. The expensive unit is increasingly **unresolved architectural and semantic decisions**.

A more appropriate cost pipeline is:

**architecture → specification → invariants → executable conformance tests/oracles → agent implementation → review → empirical validation**

Once semantics and protocols are precise, strong coding agents can absorb a large fraction of mechanical breadth:

- polyglot SDKs and bindings
- generated RPC surfaces
- compatibility façades
- repetitive Kafka Streams wrapper operators
- framework adapters
- Hasten Logistics services in many languages
- deployment/configuration examples
- dashboards and operational surfaces
- cross-language conformance matrices
- benchmarks and alternative implementation spikes
- documentation and examples

This changes the rational approach to experimentation as well. Throwaway implementations are dramatically cheaper, so competing designs can often be implemented and benchmarked rather than debated extensively in advance.

However, several costs remain fundamentally important:

- distributed correctness and failure semantics
- API/protocol decisions that become compatibility commitments
- production validation and real elapsed operational experience
- security boundaries
- performance claims requiring representative workloads
- complexity imposed on users

The primary planning heuristic should therefore be:

> **Do not ask first how much code a feature requires. Ask how much new conceptual complexity it introduces.**

A feature requiring large amounts of mechanical/generated implementation but reusing existing Hasten primitives may now be relatively cheap. A small feature introducing a new consistency model, user concept or failure mode can remain expensive.

This cost model particularly favors Hasten because much of the proposed breadth appears to reuse or collapse onto a relatively small set of hard core concepts: ownership, ordering domains, eligibility, Prescience, scheduling, resources, durable state, causal identity and control authority.

## 28. Project-centre implications

The primary objective is technical impact: advance the state of the art, publish/build the architecture openly, and make the canonical implementation useful enough that the industry can build on it.

This reinforces the importance of Hasten Logistics as both lighthouse and architectural integration test: it should make the project sufficiently tangible that collaborators can understand and evaluate the architecture from a running system rather than a large speculative design document.

## 29. Updated project-level framing

The current project framing is:

> **Parallel Consumer discovered that Kafka ownership and application execution do not have to be the same thing. Hasten follows that observation to its logical conclusion: once ownership and execution are separated, execution itself becomes programmable.**

Prescience strengthens that further:

> **Kafka owns the durable committed future. Hasten indexes its decision surface, knows how much of that future it can see, and schedules the best eligible work from it.**

The implementation strategy is correspondingly lighthouse-first and architecture-first:

- retain PC as a foundational execution mechanism, not the entire product thesis
- build only enough multilingual Streams to serve Hasten Logistics
- use agentic implementation aggressively for mechanical breadth
- concentrate scarce human attention on semantics, correctness, architecture and empirical validation
- allow Hasten Logistics to determine which subsystem slice is required next
- deliberately test whether supposedly separate capabilities truly collapse onto the same execution substrate

## Nile relationship to Hasten

### Nile and Hasten: primarily complementary, with a possible competitive edge

Nile and Hasten attack a similar architectural smell from opposite sides of the application boundary.

- **Nile:** make tenant identity first-class in the database substrate so logical tenants are not unnecessarily coupled to physical database placement.
- **Hasten:** make work identity, ordering, resources, causality and opportunity first-class in the execution substrate so logical work is not unnecessarily coupled to physical Kafka/execution placement.

A useful clean boundary is:

> **Nile owns data placement. Hasten owns work placement.**

This makes the systems more naturally complementary than competitive. Competition appears only if Nile expands upward into a general distributed application execution runtime, or Hasten expands downward into tenant-aware database/storage placement. Hasten should deliberately avoid replacing specialist systems when coordination with them is sufficient.

### Data placement + work placement

A deep Nile/Hasten integration could form a joint optimization loop:

- Nile knows where tenant state lives and how much database capacity is available.
- Hasten knows what tenant work exists, what is eligible, what resources it will consume, and—through Prescience—what committed future demand is approaching.
- Nile can expose tenant/database capacity, placement and constraint information as Hasten resource signals.
- Hasten can admit and place work according to those signals before database overload manifests as latency.
- Hasten can expose predicted future demand to Nile so Nile can make placement/scaling decisions before the workload reaches the database.

Strong formulation:

> **Nile controls supply. Hasten controls demand.**

This is an important test of the Hasten resource model: dependencies should not be treated only as dumb fixed-capacity resources. Hasten should be able to coordinate with sophisticated elastic substrates that themselves make placement and scaling decisions.

### Could Nile delete code by adopting Hasten?

Potentially yes, but the target is **generic distributed execution machinery around Nile**, not Nile's differentiated database implementation.

Hasten should not replace Nile's storage engine, database consistency, tenant isolation, query execution, data/page placement, Postgres gateway or other database-specific intelligence.

The potentially deletable class of code is machinery surrounding durable background/control-plane work, where a specialist system otherwise tends to build bespoke implementations of:

- durable jobs and ownership;
- retries and unresolved work;
- concurrency control;
- per-tenant ordering;
- resource/rate limits;
- failover;
- progress and quiescence;
- scheduling and admission;
- execution observability.

A Nile operation could instead become durable addressed work carrying rich scheduling Spice such as tenant identity, operation type, database resource, priority and ordering domain. Hasten would supply the generic execution substrate while Nile retains the domain-specific operation and database intelligence.

This yields a broader validation criterion for Hasten:

> **Don't replace the specialist system. Remove the generic distributed execution machinery it shouldn't have needed to build in the first place.**

If a sophisticated system such as Nile can retain its differentiated core while deleting meaningful bespoke orchestration/control-plane code in favor of Hasten, that would be unusually strong evidence for the Hasten thesis.
