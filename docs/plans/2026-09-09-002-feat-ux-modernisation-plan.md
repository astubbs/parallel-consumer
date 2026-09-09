---
title: Processor Definition UX Modernisation - Plan
type: feat
date: 2026-09-09
topic: ux-modernisation
artifact_contract: ce-unified-plan/v1
artifact_readiness: requirements-only
product_contract_source: ce-brainstorm
execution: code
---

# Processor Definition UX Modernisation - Plan

## Goal Capsule

- **Objective:** A developer who once used Parallel Consumer and is deciding whether to come back can define a working consumer from the README alone, and the README's own example proves it on every build: typed handling per topic, a retry limit, a dead-letter destination and filtering, in one screen of code (the budget is forty lines), without opening the javadoc.
- **Means:** A new, modern entry point that is a facade over today's engine, shipped beside the existing API as an equal. Its behaviours are specified as record outcomes so the engine can take each one over natively after the God-class decomposition, without the surface moving.
- **Product authority:** This document, for the surface and its behaviours. The work sits under STRATEGY.md's Flexibility track; shipping it requires that document's audience and Tracks sections to record the returning developer and the surface work. The engine-native implementation of each outcome is separately planned work that must honour the behaviours fixed here. The Kafka Streams work (astubbs#255) and the language proxy (astubbs#242) are constraints on this surface, not scope.
- **Open blockers:** None. Every open item is deferred to planning.

---

## Product Contract

### Summary

Add a modern way to define a Parallel Consumer: connection properties in, one typed route per topic with one processing function each, retry limit and dead-letter destination declared once per instance, a handle out. The existing options builder and processor stay as the classic API, an equal, undeprecated surface. Each behaviour the fluent API offers is specified as a record outcome, so that when the engine can own it, the facade implementation is replaced without changing what the user wrote.

### Problem Frame

Today a consumer is built by constructing a Kafka consumer and producer by hand, wiring both into one options builder beside unrelated settings, passing that to a static factory, subscribing, and then registering exactly one function for every subscribed topic. That function receives one key and value type for every topic and must produce records of the same types. A record whose function throws is retried forever; there is no way to say "give up", "skip", or "send this somewhere". A payload the configured deserialiser cannot read ends the broker-poll thread. Users carry the same three workarounds: produce to their own dead-letter topic and swallow the exception, switch on the topic name inside one handler, and consume raw bytes so they can deserialise inside the function.

The demand is recorded across the issue tracker and is the oldest open surface work in the project: separate consume and produce types (astubbs#243), per-topic functions (astubbs#254), the retry epic (astubbs#239), a dead-letter queue (astubbs#149), skip or stop reactions (astubbs#231), terminate processing from inside the function (astubbs#172, confluentinc#718), one bad record failing a whole batch (astubbs#189), and the deserialisation-failure cluster, which is the largest group of user asks with no design at all: astubbs#148 (confluentinc#304, stop a bad record killing the poll thread), astubbs#153 (confluentinc#391, a handler and policy API) and astubbs#163 (confluentinc#550, is there any exception handler). `docs/inflight/core-163-poll-path-has-no-error-seam.md` confirms there is no seam for it on the processing path, and records that the poll path already has a typed per-exception seam with two arms, so a deserialisation policy is a third arm rather than new architecture. Two of these clusters have design notes but none reached requirements. A comparable library in the same space now offers all of them in one chain, so a returning user meets the gap on first contact.

### Key Decisions

- KD1. **The fluent API coexists with the old as an equal.** Both are documented; nothing is deprecated; the API-compatibility gate (astubbs#315) must pass unchanged for the old surface. (session-settled: user-directed - chosen over deprecating the old surface in favour of the new one: no forced migration for existing users.) Governs R20, R21.
- KD2. **Facade first, engine second.** The fluent API ships as composition over today's primitives before any God-class cut lands; each behaviour is specified as an outcome so the engine can take it over later without the surface moving. (session-settled: user-directed - chosen over landing the outcome model in the engine first: the surface is the most user-facing change, it drives fixes the engine needs anyway, and the returning user should not wait for the decomposition.) Governs R7, R8, R9, R22, R23.
- KD3. **One callback per route, everything else is data.** A route carries its types and one processing function; retry limit, backoff, dead-letter destination, ordering and concurrency are data on the instance. The processing function reports its outcome; retry-versus-terminal is never expressed as a list of exception classes; the three-way decode result of R12 is the one classification the surface carries. Type declarations borrow the Kafka Streams shape, consumed and produced with serdes, because every Kafka Java developer already reads it; the Streams chain grammar is not borrowed across routes, because a route has one function and no topology; a short prelude on a route, filter, map and peek before process, is Java-binding sugar composed into that one function. (session-settled: user-directed - chosen over Java-rich hooks such as predicate filters and exception-class retry lists: every callback is a function that must exist in each foreign client of the language proxy, and data crosses the wire for free.) Governs R3, R5, R6, R10, R16, R18.
- KD4. **The reference surface is a guide and a floor, not a ceiling.** Every capability the comparable library offers gets a disposition here, and anything beyond it is welcome when it falls out of this library's own mechanisms cheaply: the offset map, the transactional produce path, per-route limits. (session-settled: user-directed - first chosen as a ceiling on 2026-09-09, "we do not need to go beyond what it offers"; lifted on 2026-09-10, "it is only a guide; if it is easy to do, we should do better". The two additions admitted under the ceiling, terminate processing (astubbs#172) and the classic-API deserialisation policy, stay; what the lift adds is park, export at capacity, the queryable parked set, scheduled retry as a park delay, direct park and export from the function, and per-route retry overrides.) Governs the Feature disposition section, R24, R25, R27, R28, R29.
- KD5. **The proxy mirroring decision is left open, and the surface is designed for both.** No construct in the fluent API may be one a wire contract could not carry. (session-settled: user-directed - chosen over committing the proxy clients to the modern surface now.) Governs R18.
- KD6. **Capacity across topics is work-conserving fair share, not reservation.** Recorded from the owner on 2026-08-21 in `docs/inflight/next-multi-topic-multi-function.md`. (session-settled: user-directed - chosen over per-topic capacity reservation: an idle topic must not waste its share.) Superseded for the fluent API on 2026-09-10, user-directed: on virtual threads (astubbs#360) threads are cheap, so each route gets its own concurrency limit, a copy of the instance default, and routes never compete for one shared limit; reservation's cost, idle capacity, is nil there, which removes the reason for sharing. A platform-thread user sets a lower per-route limit so the sum fits the pool. (chosen over work-conserving sharing of one instance limit: sharing needs a topic-aware engine, per-route limits need only the facade.) Governs R6, R23.
<!-- file-refs: N/A - the multi-topic note lives on unmerged branches; print it with bin/inflight.mjs docs show -->

- KD7. **The primary success signal is the README's first example, compiled in CI and run in the sandbox.** (session-settled: user-directed - first chosen on 2026-09-09 as a returning-user trial over the README example; superseded on 2026-09-10, "the trial is not a thing that is actually going to happen", so the signal is the one that runs on every build. Issue closure and workaround deletion remain secondary.) Governs Success Criteria, AE13, R33.
- KD8. **Full parity in one document.** The four independently plannable outcomes (entry point and routes, terminal outcomes, per-topic types, lifecycle) are specified here together. (session-settled: user-directed - chosen over owning one area and naming the rest as follow-ons.)
- KD9. **Deserialisation happens per route, inside the facade.** The fluent API consumes raw bytes and applies each route's deserialisers itself. A payload that cannot be read is a per-record failure (R12) rather than an error on the poll thread. Governs R4, R12.
- KD10. **An exported record travels the existing produce-many path.** Under the transactional commit mode it is in the same transaction as the offset commit; a failed export leaves the record parked. Governs R13, R14, R15.
- KD12. **Park in place is the dead-letter of first resort; a dead-letter topic is export.** The offset map already commits past an incomplete record, so a record that has exhausted its attempts can stay where it is, holding no worker, with the source topic as its store and the map as the index; copying it to a topic is needed only when the map's capacity or the topic's retention forces it. (session-settled: user-directed, 2026-09-10 - chosen over the topic-first design of round two: "we don't need a dead-letter queue like normal systems because of our offset encoding", with export at a capacity fraction, eighty percent by default, as the relief valve.) Governs R11, R13, R14, R15, R16, R27.
- KD14. **Lead with what only this architecture allows.** The fluent API's first screen and the README lead with park in place, which the offset map makes possible and a commit frontier cannot, then export at capacity and the queryable parked set; the entry-point and typed-route conveniences follow. (user-directed, 2026-09-10: "lean into features that only PC can do because of its architecture".) Governs R21, R27, R28, and the STRATEGY.md marketing line the Success Criteria require.
- KD13. **Milestones are cut by engine change, small then medium then large.** The surface ships first as composition, the seam changes next, the engine-native forms after the decomposition; a requirement's tier is where it lands, not how important it is. (session-settled: user-directed, 2026-09-10.) Governs R30.
- KD11. **Nothing is global except the commit mode; every other setting is a per-route value with an instance default; a topic has exactly one route.** (user-directed, 2026-09-10: "really not much should be global except defaults".) The commit mode is global because it is a property of the clients, not of the work: one consumer means one offset commit per group, and the transactional mode wraps that commit in one producer's transaction; a per-route commit mode would be two producers and two transactions over one consumer, which is two instances. That is the line: anything that needs another consumer or another transaction is another instance. Ordering is per route from the medium tier because its seam is in the engine. Two functions on one topic is not offered. Governs R2, R6.

<!-- ce-section: work-relationships -->
### How This Work Fits Together

This plan owns the modern surface and the behaviours it promises. The breakdown below is the current understanding, not a committed roadmap.

- **Depends on** nothing before planning for the small tier. The facade composes today's public primitives (KD2), with one engine item inside this plan: R25's third arm on the poll path's exception seam. Export under the transactional commit mode beyond today's terminate-on-failure (R14, medium tier) depends on the producer-recovery stack: astubbs#474, which puts an aborted transaction's work back instead of the instance dying, and astubbs#410, which replaces the invalidated producer; astubbs#426, already merged, is what lets R1 build the producer from properties today.
- **Enables** the engine-native takeover of each outcome, which is separately planned work sequenced after the God-class decomposition in `docs/inflight/core-decompose-abstract-parallel-eos-stream-processor.md` (astubbs#479).
- **Shares** the record-outcome vocabulary with the dead-letter brainstorm (astubbs#313, prior-art report `docs/plans/2026-08-18-001-investigate-dlq-prior-art-report.md`). This document answers that report's six open questions at product level (R7 to R15); the 2022 draft astubbs#8 remains the implementation seed for the engine-native form.
- **Shares** the per-topic design cluster with `docs/inflight/next-multi-topic-multi-function.md` (astubbs#254, astubbs#243, astubbs#236, astubbs#150, astubbs#245, astubbs#244). R2 to R4 settle the attachment shape; cross-topic key identity and topic priority stay with that note.
- **Can proceed independently of** the Kafka Streams work (astubbs#255), which gives a Streams topology the engine's concurrency and is the fluent API for stateful processing. The two APIs must not contradict each other about what an outcome means.
- **Can proceed independently of** the language proxy (astubbs#242), subject to R18.
- **Enables** per-function self-scaling: the per-route admission target (R6) is what the adaptive controller (astubbs#333, astubbs#392, astubbs#456) will move for each route individually; that work merges after this and must find admission per route, not per instance.
- **Shares** its vocabulary with the strategy-conversation notes (astubbs#367): the adoption ladder's share-consumer-shaped facade maps accept, release and reject onto succeeded, retry, and park or export, so R7's outcomes stay mappable to it; per-function capacity arbitration names the many-functions process this plan's routes are, and its per-function admission sub-targets are R6's per-route admission; the function manifest is a route declared as data (R18); the admission model classifies a parked record as known work with an unsatisfied eligibility predicate; the SLO-objective builder is a future per-route setting the route block leaves room for.
- **Still to decide:** whether the proxy clients mirror this surface (KD5); whether the health surface arrives through astubbs#226; whether the user function runs on virtual threads (astubbs#360) - neither changes this contract.
<!-- file-refs: N/A - the decomposition note, the dead-letter report and the multi-topic note live on unmerged branches, named by PR above; print any of them with bin/inflight.mjs docs show -->


### Actors

- A1. **Returning developer** - knows the classic API, left, is evaluating whether to come back. Reads the README first.
- A2. **Existing user** - runs the classic API in production with one or more of the three workarounds. Must not be broken (KD1).
- A3. **Foreign-client author** - implements a proxy client in another language against the wire contract (astubbs#242). Sees only what R18 permits.
- A4. **The engine** - today's processor behind the facade; later the native owner of each outcome.

### Requirements

**Entry point and routes**

- R1. A consumer is defined from connection properties and started to obtain a handle; the user constructs no Kafka client objects. Supplying pre-built clients remains possible through the classic API only.
- R2. A route binds one topic, or under R5 a set of topics, to one processing function; registering a second route for a topic already routed is refused at definition time.
- R3. A route declares its own key and value types for consumed records and, when it produces, separate key and value types for produced records. The processing function returns zero or more produced records, each naming its destination topic; zero records on a normal return is success (R7), and the filtered value (R8) carries no output. A route that has not declared produced types cannot return a produced record, and in the Java binding that is a compile error: declaring produced types changes the route's type so that only its function may return a producing outcome. On the wire, where types cannot help, the engine refuses a produced record from a non-producing route at definition time.
- R4. A route declares its deserialisers through the general form, consumed with a key and a value deserialiser, or through a format helper that resolves to the deserialiser already on the classpath for JSON, Avro or Protobuf, with the key defaulting to string; a format-named route, json, avro, protobuf or bytes with the topic, is sugar that desugars to the route form. A route's deserialisers are applied per record inside the facade and, when the route declares produced types, its serialisers are applied to produced records before they reach the produce path; the consumer and the producer are both configured for raw bytes by the facade, never by the user. Deserialiser settings supplied in the connection properties are refused at definition time with a message naming the setting and the route that supersedes it; the remaining properties are passed to each route deserialiser's configuration.
- R5. A set of topics that share one function and one type pair may be declared as a single route.
- R6. Only the commit mode is instance-wide, because the engine has one producer and one transaction; declaring it on a route is refused at definition time. Everything else is per route with an instance default: dead-letter destination, ordering mode, concurrency limit, which is the route's admission target (CONCEPTS.md), retry limit, retry delay, park policy and breaker are per route: each route takes a copy of the instance default unless it declares its own, and the surface names the two kinds apart, instance-wide settings plain and per-route defaults with a default prefix. Per-route ordering is an engine change on one seam, the shard key and the shard's head check, so it ships in the medium tier and the small tier accepts ordering only as the instance default. Cross-topic key identity, whether one key on two topics is one shard, stays with astubbs#150. The declared admission target is a starting point, not a constant: the self-scaling work (astubbs#333 and the navigator rungs astubbs#392, astubbs#456) makes admission adaptive per route at runtime, and merges after this. The export fraction of R27 is an instance default, eighty percent unless declared, which a park policy may override.

**Outcomes and policy**

- R7. Every record reaches exactly one terminal outcome: succeeded, filtered, parked, or exported; a retry is a step towards one of these, never an outcome of its own, and a parked record may later become exported (R27). Terminal exclusivity applies once a record leaves the retrying state; a record under the explicit unbounded limit may never leave it. A route that produces nothing reaches succeeded on a normal return.
- R8. The processing function reports filtered by returning an explicit filtered outcome value; a normal return is success on every route, producing or not. It may also return park or export directly, with a reason, for a record it already knows is hopeless, which skips the remaining attempts (R27). A filtered record completes and commits like a success and is counted separately.
- R9. The processing function reports retry by throwing; the existing retriable exception keeps its meaning; any other exception is also a retry. The distinction affects logging only.
- R10. Retry stops after the retry limit, counted as attempts after the first. The limit is optional: the default is ten attempts followed by park (R27), which also gives the inert failure-history option of ten a meaning at last, and an explicit unbounded value is opt-in, so the classic API's retry-forever behaviour is available on the fluent API only by asking for it. Park is what makes a default finite limit safe: an exhausted record costs offset-map capacity, not a worker, and the map's bound is visible (R28). The count is per assignment: a rebalance, restart or crash resets it, so the limit bounds attempts within one assignment, a record reassigned before exhaustion starts again, and a record may exceed the limit across assignments.
- R11. On exhaustion the record is parked in place (R27); a finite retry limit needs no destination. Under key and unordered processing the partition commits past a parked record. Under key ordering the parked record is the head of its key's shard, so later records with that key wait behind it: park holds the key, not the partition, and the parked view reports how many records each parked record holds (R28). Under partition ordering a parked record still holds its partition, so the documentation says park serves key and unordered processing and a partition-ordered instance should declare export.
- R12. A payload a route cannot deserialise never ends the poll thread. A route's decode step yields one of three results: a value, a permanent failure, or a transient failure. A permanent failure is parked immediately without consuming attempts (R27); a transient one is a failed attempt under R9 and R10. A plain Kafka deserialiser that throws yields a transient failure by default, since it cannot tell a corrupt payload from a registry outage; a route that needs the distinction wraps its deserialiser to say so. Under the explicit unbounded limit every decode failure is a transient attempt, so AE1's definition behaves as the classic API does.

**Park and export**

- R27. A parked record stays incomplete in the offset map, holds no worker, and is not re-attempted until its park delay elapses, which is how scheduled retry (astubbs#234) is delivered: a park policy may declare a delay after which the record is attempted again with its count reset, or no delay, meaning parked until resumed or exported. Otherwise it waits until an operator resumes it through the handle (R28), or a restart re-delivers it (R10's per-assignment rule). Parked records count against the partition's offset-map payload, never against the intake load gate. When a partition's payload reaches the declared fraction of Kafka's commit-metadata cap, eighty percent by default, its parked records are exported oldest-first to the declared dead-letter destination until the payload is below the fraction; with no destination declared, intake on that partition pauses at the cap's pressure threshold as it does today. A declared age bound exports a parked record before the topic's retention could delete it, and an instance may declare export-immediately, which is the classic dead-letter queue. The parked set is queryable and metered (R28), and the documentation states that consumer-group lag reads as stuck at the oldest parked record.
- R28. The parked set is queryable per route, retrieved from the handle by the route's name, with an instance-wide roll-up under a name of its own so the per-route accessor is never overloaded. A route's parked view spans every partition by default and answers, per partition on request: the parked count, the oldest parked record's age, the offset-map payload as a fraction of the cap, an estimated time to reach the export fraction from the current park rate and payload growth, and the count of unresolved records (parked, waiting and in flight), which is the honest lag figure beside the broker's offset distance; and it lists parked records with topic, partition, offset, key, attempt count, last failure, parked-since time, and the count of records held behind it under key ordering, which is the blast-radius figure the retry-economics note ranks by. Two commands act on a parked record or a partition's parked set: resume, which re-attempts now, and export, which sends to the dead-letter destination now. The same figures are published as metrics (R19): parked count, oldest parked age and estimated time to export per topic-partition as gauges, the payload fraction per partition as a gauge, and exported records as a counter. Queries and commands are data on the wire (R18), and the embedded dashboard (astubbs#268) consumes them for its blocked-frontier panel rather than reading engine state itself.
- R29. A route may declare a circuit breaker: a failure-rate threshold over a window of terminal outcomes, and an open duration. When the rate crosses the threshold the route opens: its records are withheld without counting an attempt for the open duration, then a declared number are let through half-open, and the route closes on their success or re-opens on failure. Other routes are unaffected; an instance-wide breaker is the same policy declared on the instance. Open, half-open and closed transitions are counted (R19) and the state is on the route's handle (R28). The policy is data. Retries and the breaker answer different failures: a retry is one record's transient failure, the breaker is a dependency that is down.
- R13. An exported record carries the original key bytes, value bytes and headers unchanged, plus provenance headers naming the source topic, partition, offset, timestamp, attempt count, the time of the last failure and the last failure's class and message. The header names take the 2022 draft's prefix and names (astubbs#8: `pc-failure-count`, `pc-last-failure-at`, `pc-last-failure-cause`, `pc-partition`, `pc-offset`) and add the source topic and timestamp the draft lacked; the draft's reaction enum maps onto this document's outcomes, SHUTDOWN to stop (R24), SKIP to filtered (R8), DLQ to export. A destination shared by several routes carries records from all of them, so its consumer reads raw bytes and dispatches on the source-topic provenance header; a route may declare its own. Provenance headers are appended after the copied user headers, their names are reserved, and where a user header shares a name the last occurrence is the framework's and is authoritative.
- R14. Under the transactional commit mode an export send is part of the transaction that commits the exported record's offset. An export send that fails inside that transaction aborts it, so no offset in it commits and every record in it is re-attempted; in today's engine the instance then terminates, and the producer-recovery work (astubbs#225) is what would change that. Until it lands, a persistently failing export under this mode terminates and restarts the instance with attempt counts reset (R10), so a parked record that cannot be exported makes no progress once the payload fraction is reached; Scope Boundaries records the limit.
- R15. Under the non-transactional commit modes, an export send that fails leaves the record parked with its attempt count kept; only the export is re-attempted after the retry delay, never the user function, and the instance does not stop. That holds within one assignment: after a rebalance, restart or crash the record is re-polled with no memory of its park, the user function runs again and the count restarts (R10). The attempt count the user sees is the facade's own count of user-function attempts per record, distinct from the engine's internal retry counter, which advances on every export re-attempt. A crash between a successful export and the offset commit replays the record, so the dead-letter topic is at-least-once.
- R16. Parking is observable once per record, after the last attempt, with the record, the last failure and the attempt count, as a Java-binding observer that is sugar over the outcome (KD3); it ships in the first cut. It fires once for a permanent decode failure too, with an attempt count of zero. Export is counted (R19), not observed. When decoding failed the observer receives a raw envelope of the original bytes and headers; typed values are present only when decoding succeeded.

**Lifecycle and observability**

- R17. The handle exposes a bounded graceful shutdown that drains in-flight work, is usable with try-with-resources, and a blocking wait for shutdown. The handle's close drains, bounded by the drain timeout, with the shutdown timeout bounding the close that follows; this differs from the classic API, whose plain close does not drain and is bounded by the shutdown timeout alone.
- R18. Every construct on the fluent API is either data a wire contract can carry or the one processing function per route; no second callback is required for correct operation, and any observer is optional sugar. From the wire's point of view a route's deserialisation is part of that one function: bytes cross the wire and a foreign client decodes them inside its function, so the per-route deserialisers of R4, and the three-way decode result of R12, are Java-binding sugar composed into the function, not a second construct the contract carries.
- R19. Outcome counts (succeeded, filtered, parked, exported) and the parked-set gauges of R28 are published through the existing metrics integration, tagged by topic and, for the gauges, by partition.

**Coexistence and sequencing**

- R20. The classic API's public surface is unchanged and the API-compatibility gate passes with no allowed-breakage entries added for this work; the classic API's existing unit and integration suites pass unchanged as the behavioural regression beside the gate.
- R30. The implementation plan is cut into milestones ordered by how much of the engine each needs to change, and every requirement is assigned to exactly one tier: **small** is facade-only, composed from today's public primitives with no engine edit; **medium** is a contained engine change on an existing seam, no God-class cut; **large** is engine-native work that waits for the decomposition. The first milestone is the smallest set that lets the README's first example run in the sandbox, and each milestone ships on its own. The expected tiering, for planning to confirm against the code rather than inherit:
  - Small: the entry point, routes and types (R1 to R6), the classic-API change list as a constraint (R34), the sandbox and generator over the shipped mock consumer (R33), instance-wide batch mode over the existing batch option (R32, size only), outcomes and the filter value (R7 to R9), the retry limit and per-route policy (R10, R11), the three-way decode result (R12), export on the produce-many path (R13 to R15), the park observer (R16), the handle and console sink (R17), the wire constraint (R18), park in place as the retry queue with the re-attempt withheld (R27), the per-route breaker (R29), stop through the existing close paths (R24), the API rule and README (R21, R26).
  - Medium: per-route ordering at the shard-key seam (R6); the classic API's poll-path policy as a third arm on the existing seam (R25); seek and runtime route changes as control-thread commands (R31); the batch defects (astubbs#311, astubbs#164) and the maximum-wait release (R32); the parked-set query and its gauges where they need an engine accessor, the payload fraction above all (R19, R28); the compatibility gate (R20).
  - Large: the engine-native form of each outcome (R22); per-route batching, same-key batches and per-record outcomes inside a batch (R32); parked state carried in commit metadata so a restart does not re-attempt; per-route admission inside the engine, which the self-scaling work owns (R6, R23).
- R31. The handle exposes the consumer operations users have asked for, as commands that are data on the wire (R18): seek a partition to an offset, to its beginning, or to its end (astubbs#174, astubbs#246; the safe-exposure ask of astubbs#158 is answered by these being the only consumer operations the handle offers), and add or remove a route while the instance runs (astubbs#245), under the same definition-time checks as at start. A seek runs on the control thread between polls: the partition's in-flight work is abandoned and those records are delivered again from the new position, and its offset map is reset. Removing a route drains its in-flight work first.
- R32. A route may declare batch mode: the function receives up to a declared number of records, released early when a declared maximum wait elapses (astubbs#165), and optionally only records sharing one key (astubbs#145). Each record in a batch reaches its own outcome, so one record's failure parks or retries that record alone (astubbs#189); the batch's produced records and filtered values are per record. The classic API's batch defects, the extra in-flight request and the unvalidated size (astubbs#311, astubbs#164), are fixed in the engine before batch mode ships on the fluent API.
- R33. A sandbox module runs any definition with no broker and no test environment: the same facade over the mock consumer that already ships in the main artefact, with a generator that produces records into the definition's source topics at a declared rate, hydrating each route's consumed type with realistic random data through a random-object filler, and a console sink by default. The definition does not change between sandbox and broker; only the start call does, and the generator can be replaced by hand-written records. The README example and the generator's default types use the parcel-logistics domain the core example already established, per the executable-progression note, so the sandbox is the first stage of that progression rather than a separate demo. This is also the broker-free test kit, since a test drives the same sandbox with its own records and asserts on outcomes and the parked set.
- R34. The classic API changes only by addition: no existing method changes shape or meaning, and the compatibility gate (R20) proves it. What is added: overloads of the produce methods that take separate produced key and value types, so a classic user gets astubbs#243 without migrating; the deserialisation-failure policy on the poll path (R25); the batch defects fixed (R32); and, once the engine owns park and export natively, a thrown terminal signal in the 2022 draft's shape (astubbs#8), so a classic user can park or export a record from the function they already have. Anything that would need an existing classic method to change belongs to the fluent API instead. The list is closed; a new addition to it is a decision recorded here, not a convenience.
- R26. The API rule: the classic API receives only fixes for failures that today end the poll thread (R25); every other behaviour in R7 to R16 and R24 lands on the fluent API, and the README's classic-API section says so. An issue in the Success Criteria cluster counts as closed when its capability is available on the fluent API; for an existing user the named residual is that it requires the fluent API. The README states beside its first example when to choose the classic API: when pre-built Kafka clients are required.
- R21. The README is rewritten for two APIs: its first example uses the fluent API; each API has its own section; the error-handling and skipping-records sections are rewritten around outcomes, park and export; a migration section maps each of the three documented workarounds (own dead-letter topic plus swallow, switch on topic name inside one handler, consume raw bytes to deserialise by hand) to its one-line fluent-API replacement.
- R22. The behaviours in R7 to R15 hold when the facade implements them over today's engine, and hold unchanged when the engine implements them natively; the same acceptance examples are the oracle for both. The parity promise binds the terminal outcomes, not the reset: R10's per-assignment counting is a facade-era floor the engine-native form may tighten to a durable per-record count.
- R24. The processing function may report a stop outcome (astubbs#172). The instance then takes no new work and closes through the drain-first or the dont-drain-first path, selected once per instance as data; the record that reported stop is left incomplete so it is delivered again after a restart, and in-flight work follows the chosen close path. Stop is a request about the instance, not a terminal outcome of the record under R7. The awaiting caller learns that the instance stopped by request rather than by close; the stopping record and the reason are recorded once; stops are counted beside the R19 outcome counters. An automatic restart re-delivers the stopping record and the function will stop again, so the definition's author owns breaking that loop.
- R25. On the classic API, a deserialisation failure thrown by the poll is handled by a policy declared once per instance as data: fail the instance, which is today's behaviour and the default, or skip and log the record, or dead-letter its raw bytes and headers under R13. The policy is a third arm of the poll path's existing typed per-exception seam, so it is contained work; the fluent API never reaches it because R4 keeps deserialisation off the poll path. Together R4, R12 and R25 close the deserialisation cluster (astubbs#148, astubbs#153, astubbs#163).
- R23. Routes do not compete for one shared limit: each route is bounded by its own admission target (R6), a route declared over a set of topics (R5) shares that one limit across them, and the engine's total admission is the sum of the route limits. The in-flight buffer and back-pressure remain the shared bounds. On platform threads a route waiting at its limit occupies a pool thread, so the documentation advises a lower per-route limit whose sum fits the pool; on virtual threads (astubbs#360) the sum is free.

### Illustrative surface

Illustrative, not binding: the names are placeholders and the compiled README example decides the syntax (Outstanding Questions). What the examples fix is the shape the requirements imply: a definition from properties, one statement per route ending in `process` or a sink, policy as data, a handle out. Type declarations borrow Kafka Streams' `Consumed.with` and `Produced.with` shape and its `Serdes` names, in this library's own package so no Streams dependency arrives; the chain grammar of the Streams DSL is deliberately not borrowed across routes (KD3). A route is called a route, not a stream, because a stream in Streams is the start of a topology and this is a topic bound to one function. Format helpers such as `json(Order.class)` resolve to the deserialiser already on the classpath, and the format-named routes `json`, `avro`, `protobuf` and `bytes` are sugar for `route(...).consumed(...)`; the same verb, route, defines one before start, adds one after start, and on the handle retrieves one. The one instance-wide setting, commit mode, is plain; per-route defaults carry the prefix `default`, and a route's own setting overrides its copy.

The shortest definition: one topic, nothing else declared. Failures retry ten times with the default delay, then park (R1, R2, R10, R11, R17):

```java
var pc = ParallelConsumer.define(props);
pc.json("orders", Order.class)
    .process(ctx -> { inventory.reserve(ctx.value()); return Outcome.succeeded(); });
try (var handle = pc.start()) {
    handle.awaitShutdown();
}
```

Two routes, one statement each, so a formatter cannot hide the boundary. Instance settings on the definition; per-route defaults prefixed; a route's own setting overrides (R3, R5, R6, R23):

```java
var pc = ParallelConsumer.define(props)
    .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)      // instance-wide: the engine has one
    .defaultOrdering(KEY)                            // per-route default: every route copies it
    .defaultConcurrency(100)
    .defaultRetryLimit(10);

pc.json("orders", Order.class)
    .produced(Produced.with(Serdes.String(), avro(OrderEvent.class)))   // now, and only now, process may produce
    .retryLimit(5)                                   // this route only
    .process(ctx -> Outcome.produce(
        new ProducerRecord<>("order-events", ctx.key(), OrderEvent.from(ctx.value()))));

pc.bytes(Set.of("audit", "audit-replay"))
    .ordering(PARTITION)                             // this route only, from the medium tier
    .concurrency(4)                                  // this route only; the self-scaling controller may move it later
    .toConsole();                                    // sink sugar: print the record and succeed

pc.route("legacy")                                   // the general form, for a non-string key or your own deserialiser
    .consumed(Consumed.with(Serdes.Long(), new LegacyDeserializer()))
    .process(ctx -> Outcome.succeeded());

try (var handle = pc.start()) {
    handle.awaitShutdown();
}
```

Retry and park policy, every call optional, as a per-route default on the definition or on one route (R10, R13, R27, R29). The export fraction has an instance-level default, eighty percent, itself configurable:

```java
pc.defaultAfterRetries(park()
        .exportTo("orders.dlq")                      // optional; without it park is bounded by the map alone
        .exportAtOffsetMapFraction(0.5)              // optional; overrides the instance default of 0.8
        .exportOlderThan(Duration.ofDays(2)))        // optional; before retention wins
  .exportAtOffsetMapFraction(0.8)                    // the instance default, if you want it explicit
  .defaultCircuitBreaker(failureRate(0.5).over(100).openFor(Duration.ofSeconds(30)).halfOpenProbes(5));

pc.json("payments", Payment.class)
    .afterRetries(exportImmediately("payments.dlq")) // this route: the classic queue
    .process(ctx -> ...);
```

Outcomes inside the one function (R8, R9, R24). `downstreamDatabase` is your own client, captured by the lambda; this library never sees it:

```java
ctx -> {
    if (ctx.value().customerId() == null) return Outcome.filtered();
    if (ctx.value().schemaVersion() > SUPPORTED) return Outcome.stop("unsupported schema; deploy needed");
    downstreamDatabase.write(ctx.value());       // your client; an exception here is a retry
    // Outcome.park("reason") or Outcome.export("reason") skip the retries for a record you know is hopeless
    return Outcome.succeeded();
}
```

A prelude on a route, sugar composed into the one function before it crosses the wire (KD3):

```java
pc.json("orders", Order.class)
    .filter(ctx -> ctx.value().customerId() != null)     // false is the filtered outcome
    .map(ctx -> ctx.value().withProcessedAt(now()))
    .peek(order -> metrics.seen(order))
    .process(order -> { downstreamDatabase.write(order); return Outcome.succeeded(); });
```

Telling permanent from transient at decode time, when a stock deserialiser cannot (R12):

```java
pc.route("orders")
    .consumed(Consumed.with(Serdes.String(),
        classifyDecodeFailures(avro(Order.class), e ->
            e instanceof RestClientException ? Decode.transientFailure(e) : Decode.permanentFailure(e))))
    .process(ctx -> ...);
```

Querying and acting on the parked set, per route, with an instance roll-up under its own name (R28). The default view spans every partition; one partition is the rare case:

```java
var parked = handle.route("orders").parked();   // this route's parked set, every partition
parked.records().stream()                        // offset, key, attempts, last failure, parked-since
      .filter(rec -> rec.attempts() > 5)
      .forEach(parked::resume);                  // or parked::export
parked.byPartition().forEach(p -> log.info("{} parked={} oldest={} payload={}% export in about {}",
        p.partition(), p.count(), p.oldestAge(), p.payloadFraction() * 100, p.estimatedTimeToExport()));
parked.partition(3).records();                   // one partition, the rare case
handle.parkedAllTopics().total();                // every route, named apart so parked() is never overloaded
```

Handle operations (R31) and a batch-mode route (R32). A route added after `start` is the same statement:

```java
handle.seek("orders", 3, Seek.beginning());          // one partition; in-flight work is delivered again
pc.json("refunds", Refund.class)                      // after start: a runtime route add
    .process(ctx -> { refunds.apply(ctx.value()); return Outcome.succeeded(); });
pc.removeRoute("audit");                              // drains first

pc.json("orders", Order.class)
    .batch(Batch.upTo(100).maxWait(Duration.ofSeconds(1)).sameKey())
    .processBatch(batch -> batch.map(ctx -> ledger.post(ctx.value()) ? Outcome.succeeded() : Outcome.filtered()));
```

The sandbox: the same definition, no broker, generated records at a rate (R33):

```java
try (var handle = pc.sandbox(Generate.into("orders", Order.class).perSecond(50))) {
    handle.awaitShutdown();                      // fields hydrated with realistic random data
}
```

The park observer, sugar over the outcome (R16), and the classic API's poll-path policy (R25):

```java
pc.json("orders", Order.class)
    .onParked((record, failure, attempts) -> log.warn("parked {} after {}", record.offset(), attempts, failure))
    .process(ctx -> ...);

ParallelConsumerOptions.builder()
    .consumer(consumer)
    .deserializationFailure(SKIP_AND_LOG)        // default FAIL_INSTANCE, today's behaviour
```

### Key Flows

- F1. Define and start
  - **Trigger:** A1 writes a definition from the README.
  - **Steps:** Declare properties; declare one route per topic with types and a function; declare instance policy; start; hold the handle in try-with-resources; await shutdown.
  - **Outcome:** A running consumer. Definition-time errors (R2, R6) surface before any poll.
  - **Covered by:** R1 to R6, R17.
- F2. A record fails, retries, parks, and is later exported
  - **Trigger:** The function throws for a record.
  - **Steps:** Attempt counted; retry delay applied; further attempts until the limit; on exhaustion the record is parked (R27): it stays incomplete, holds no worker, the observer fires once (R16), the parked count increments (R19), and the partition commits past it. Later, when the partition's payload reaches the fraction, or the age bound, or immediately when export-immediately is declared, the export record is built (R13) and sent on the produce-many path (R14); the record completes; the exported count increments.
  - **Outcome:** Offsets past the parked record commit throughout; after export the record is in the dead-letter topic with provenance and its own offset commits.
  - **Covered by:** R7, R9 to R11, R13, R14, R16, R19, R27.
- F3. A record is filtered
  - **Trigger:** The function returns the filtered outcome value.
  - **Outcome:** The record completes and commits as a success; the filtered count increments; nothing is produced.
  - **Covered by:** R8, R19.
- F4. A payload cannot be read
  - **Trigger:** A route's deserialiser throws on a record.
  - **Outcome:** That record alone takes the path of F2; the poll thread continues; other routes are unaffected.
  - **Covered by:** R4, R12.
- F6. The function asks the instance to stop
  - **Trigger:** The function reports the stop outcome for a record.
  - **Steps:** No further work is taken; the instance closes on the declared close path; the stopping record stays incomplete; the handle's awaiting caller returns.
  - **Outcome:** The instance is closed; after a restart the stopping record is delivered again.
  - **Covered by:** R24, R17.
- F5. The engine takes an outcome over natively
  - **Trigger:** A God-class cut lands and the engine can own park, export, filter or retry limit.
  - **Steps:** The facade's implementation of that outcome is removed; the engine's is wired behind the same definition; the acceptance examples below are re-run unchanged.
  - **Outcome:** No user definition changes.
  - **Covered by:** R22.

### Acceptance Examples

- AE1. **Covers R10, R11.** Given a definition declaring the explicit unbounded retry limit and no dead-letter destination, when a record's function always throws, then the record is retried indefinitely and no offset past it commits under partition ordering, exactly as the classic API behaves; and given a definition that declares no retry limit at all, then the record is attempted ten times and parked.
- AE2. **Covers R10, R11, R27.** Given a retry limit of two under key ordering, when a record's function throws three times, then the fourth attempt does not occur, the record is parked: its offset stays incomplete in the commit metadata, offsets past it on the same partition commit, it holds no worker, and the parked count for its topic is one.
- AE18. **Covers R13, R27.** Given AE2's definition with a dead-letter destination and the default fraction, when parked records push a partition's payload past eighty percent of the metadata cap, then the oldest parked records on that partition are exported until the payload is below the fraction, each holding the original bytes and headers plus provenance headers reporting its attempts, and each exported record's source offset commits.
- AE21. **Covers R29.** Given a route with a breaker of half the last hundred outcomes and a thirty-second open duration, when sixty of a hundred consecutive records fail terminally, then the route opens, its next records are withheld for thirty seconds with no attempt counted, other routes keep processing, the transition is counted, and after thirty seconds five probe records run and the route closes when they succeed.
- AE22. **Covers R31.** Given a running instance, when the handle seeks one partition to its beginning, then that partition's in-flight records are abandoned and delivered again from offset zero, its offset map is reset, other partitions are untouched; and when a route is added at runtime for a new topic, then its records are processed under its own declared policy without a restart, and adding a route for an already-routed topic is refused as at definition time.
- AE23. **Covers R32.** Given a route in batch mode with a size of one hundred and a maximum wait of one second, when forty records arrive and no more follow, then the function receives the forty after one second; and when one record in a batch throws, then that record alone is retried and later parked while the other records' outcomes stand.
- AE24. **Covers R33.** Given the README's first example started in the sandbox with a generator of fifty orders per second, when it runs for ten seconds with no broker reachable, then about five hundred generated orders with realistic field values have passed through the route, the console sink has printed them, and switching the start call to a broker changes nothing else in the definition.
- AE20. **Covers R28.** Given AE2's definition and three parked records on one partition, when the handle is queried, then it reports three parked on that partition with the oldest one's age and the partition's payload fraction, lists the three with offset, key, attempts, last failure and parked-since; and when resume is issued for one of them, then that record is attempted again at once and, on success, its offset commits and the parked count reads two.
- AE19. **Covers R27.** Given AE2's definition with a dead-letter destination and export-immediately declared, when a record exhausts its retries, then it is exported at once with provenance reporting three attempts and its source offset commits, which is the classic dead-letter queue.
- AE3. **Covers R14.** Given the transactional commit mode and AE19's definition, when a consumer reads the dead-letter topic with read-committed isolation and the source consumer group's committed offset is observed separately, then the exported record becomes visible only together with that committed source offset; and when the export send fails after the record was produced but before the offset committed, then no source offset for it is committed and no exported record becomes visible.
- AE4. **Covers R15.** Given a non-transactional commit mode, AE19's definition, and a dead-letter destination that is unreachable, when a record exhausts its retries, then the record stays parked, its offset does not commit, its attempt count is unchanged, the user function is not run again, the instance keeps processing other records, and the export is re-attempted after the retry delay; and when the instance is restarted before the export succeeds, the record is re-polled, the user function runs again and its attempt count starts from zero.
- AE5. **Covers R8, R19.** Given a route whose function returns the filtered outcome for records with a missing field, when a thousand records are consumed of which a hundred lack the field, then the succeeded count is nine hundred, the filtered count is one hundred, all offsets commit, and nothing is produced for the hundred.
- AE6. **Covers R4, R12.** Given two routes on two topics, when one topic carries a payload the route's deserialiser rejects, then that record, reported permanent by the deserialiser, is parked without consuming attempts, with the deserialisation error as its failure and the original bytes preserved for export, while a failure the deserialiser reports transient follows AE2; the other topic's records are unaffected, and the poll thread is alive throughout.
- AE7. **Covers R2, R4, R6, R10.** Given a definition that registers a second route for a topic already routed, sets a retry limit on a route, supplies deserialiser settings in the connection properties, or declares export-immediately or an age bound without a dead-letter destination, when the definition is built, then it is refused with a message naming the offending topic or setting, before any connection is opened.
- AE8. **Covers R3.** Given a route consuming string keys and JSON-typed values that produces long keys and Avro-typed values, when the function returns produced records, then they are typed by the route's produced types and the compiler accepts the definition without casts.
- AE9. **Covers R17.** Given a running handle inside try-with-resources, when the block exits with work in flight, then in-flight work drains up to the configured drain timeout before the consumer closes, and offsets for drained work commit.
- AE10. **Covers R20.** Given the classic API's public surface before this work, when the API-compatibility gate and the classic API's existing unit and integration suites run after it, then the gate passes with no new allowed-breakage entry and the suites pass unchanged.
- AE11. **Covers R22.** Given AE1 to AE9, AE12, AE18 and AE19 passing against the facade, when the engine implements park and export natively behind the same definition, then they pass unchanged.
- AE12. **Covers R16.** Given a park observer registered, when a record exhausts its retries and is parked, then the observer is invoked exactly once with that record, the last failure and the attempt count, after the last attempt and before the record's offset commits; and when the record's decoding had failed permanently, the observer fires once with an attempt count of zero and its record is the raw envelope of the original bytes and headers.
- AE17. **Covers R6, R23.** Given two routes with concurrency limits of ten and one hundred, both with backlogs larger than their limits, when the instance runs, then at most ten records of the first and one hundred of the second are in flight at once, and draining the first route's backlog does not change the second's throughput.
- AE16. **Covers R13.** Given an input record carrying a header named like a provenance header, when it is exported, then the exported record holds the user's header first and the framework's last, and a consumer reading the last occurrence gets the framework's value.
- AE14. **Covers R24.** Given an instance declaring the drain-first close path, when a record's function reports stop while other records are in flight, then no new record is started, the in-flight records complete and their offsets commit, the stopping record's offset does not commit, the instance closes, and after a restart the stopping record is delivered again.
- AE15. **Covers R25.** Given the classic API with the skip-and-log policy declared, when the poll returns a record its configured deserialiser rejects, then that record is logged and its offset advances, the poll thread stays alive, and the next record is processed; and given the default policy, the instance fails as it does today.
- AE13. **Covers the one-screen objective.** Given the README's first example, a two-route definition with JSON and Avro values, a filtered outcome on one route, a retry limit and a dead-letter destination, when it is compiled in CI, then it compiles and its definition fits within the forty-line budget named in the Goal Capsule.

### Success Criteria

- The README's first example, a definition with two routes of different value types, a filter on one of them, a retry limit and a dead-letter destination, compiles in CI and runs in the sandbox with no broker on every build, within the one-screen budget in the Goal Capsule, and a generated record that always fails is parked by the end of the run. Primary (KD7): it is the signal that runs, and it goes red when the surface drifts.
- STRATEGY.md records the returning-developer audience and the surface work under its Flexibility track when the fluent API ships, and its marketing section leads with park in place as the capability the offset map alone makes possible (KD14).
- Each of the three documented workarounds has a one-line first-class replacement shown in a migration section of the README.
- Each issue in the cluster (astubbs#243, astubbs#254, astubbs#239, astubbs#149, astubbs#148, astubbs#153, astubbs#163, astubbs#231, astubbs#172, astubbs#189, astubbs#141, astubbs#158, astubbs#174, astubbs#246, astubbs#245, astubbs#165, astubbs#145) is either closed by the shipped surface or reduced to a named residual on the issue.

### Feature disposition against the reference surface

Every capability the comparable library offers is listed with its disposition here (KD4). The survey was taken on 2026-09-09; its inventory is held outside the repository by owner decision, so this table is the authoritative copy and a later reader re-checks it against the library, not against a citation. "In scope" means this plan specifies it; "Deferred" means it fits the fluent API and is wanted later; "Excluded" means it contradicts what this library is; "Not this plan" means the capability belongs to separately tracked work that is orthogonal to the fluent API.

| Capability | Disposition | Reason |
|---|---|---|
| Properties-in entry point, no client construction | In scope (R1) | The first thing a returning user sees |
| One typed route per topic | In scope (R2, R3) | astubbs#254; the attachment shape the multi-topic note left open |
| Homogeneous topic set sharing one route | In scope (R5) | Today's multi-topic subscribe, typed |
| Separate produced key and value types | In scope (R3) | astubbs#243, non-breaking because it lives on the fluent API |
| Per-route deserialisation, any format | In scope (R4) | Kafka's own deserialiser interface covers JSON, Avro, Protobuf and schema registries; no format modules of our own |
| Format helpers and format-named routes | In scope (R4), Java-binding sugar | `json(Order.class)`, `avro(...)`, `protobuf(...)`, `string()`, `bytes()` resolve to the deserialiser already on the classpath, with optional compile-only dependencies and a definition-time failure naming the missing library; `json("orders", Order.class, r -> ...)` desugars to the route form |
| Filter | In scope (R8) | As an outcome of the one function, not a second callback (KD3) |
| Retry limit and delay | In scope (R6, R10), optional with a finite default | The delay function already exists; the limit is new |
| Console sink | In scope, Java-binding sugar | A route whose function prints the record and succeeds; the first thing a README example needs |
| Park in place on exhaustion, no topic needed | In scope (R11, R27) | The offset map commits past an incomplete record, so the source topic is the store and the map the index |
| Dead-letter topic in one declaration | In scope (R13, R27), as export at a capacity fraction or an age bound, or immediately by choice | astubbs#149, the most-demanded missing feature, delivered as the relief valve for park |
| Original bytes and headers preserved, provenance headers added | In scope (R13) | Matches the 2022 draft's header set |
| Failed dead-letter send leaves the offset uncommitted | In scope (R15) | The record stays parked; the library's existing correctness stance |
| Scheduled retry: attempt again after a declared delay | In scope (R27), a park with a delay | astubbs#234; the retry queue is already time-ordered, so this is the same structure with a longer horizon |
| Direct park or export from the function, skipping retries | In scope (R8, R27) | For a record the function already knows is hopeless; the outcome carries a reason |
| Per-route retry limit, delay and park policy | In scope (R6) | Data, not callbacks, so free on the wire; the facade already bounds each route |
| Parked set query, resume and export commands, and metrics | In scope (R28) | A store nobody can see is a leak; the control-plane note already designed the panel and asks for the engine API first |
| Exhaustion observer, once per record | In scope (R16) | As Java-binding sugar over the outcome |
| Ordering modes: unordered, key, partition | In scope (R6), per route from the medium tier | Already exist; the shard key already carries the topic, so per-route ordering is two engine reads at one seam; a "sequential" mode is partition ordering with concurrency one |
| Concurrency limit, the admission target | In scope (R6), per route with an instance default | Exists per instance today; the facade bounds each route |
| Work-conserving fair share of capacity across topics | Not needed (R23) | Per-route limits mean routes never compete; KD6's sharing is superseded for the fluent API |
| Commit mode: consumer commit or transactional producer | In scope (R6) | Exists; declared once per instance, and R14 and R15 differ by it |
| Back-pressure with pause and resume | In scope, unchanged | Exists in the engine; the fluent API exposes the existing settings |
| Handle: bounded graceful shutdown, try-with-resources, await | In scope (R17) | The existing close modes, given the idiom |
| Outcome counters by topic | In scope (R19) | Falls out of the outcome vocabulary |
| Produce results to another topic | In scope (R3) | The existing produce-many path, with separate output types |
| Poll timeout setting | Deferred | The long poll is hard-coded at two seconds today, so this is a new engine setting on either API, and it is orthogonal to the objective |
| Health snapshot on the handle | Deferred | astubbs#226 is the health surface; the fluent API adopts it when it lands |
| Per-key queue-depth diagnostics | Deferred | Belongs to the observability track and its GUI; the held-behind count on a parked record (R28) is the first slice |
| A declared service objective per route, in place of a concurrency number | Deferred | The SLO-objective note; the route block is where it would sit |
| Circuit breaker | In scope (R29), per route | A declarative pause of one route driven by its observed failure rate; the facade already bounds each route, so opening one is withholding its records |
| Trace-context propagation | Deferred | Owned by the record-tracing note; not asked for here |
| Transform composition on a route (filter, map, peek before process) | In scope, Java-binding sugar | Composed into the one function before it crosses the wire, so the engine and the proxy still see one callback; nothing across routes |
| Per-attempt failure observer | Deferred | A second callback with no data equivalent; R16 covers the once-per-record case |
| Batch sink with size and age flush and a coverage contract | Deferred | Batching on the produce side is separate work |
| Batch consumption: many records per call, max wait, same-key batches, per-record outcomes | In scope (R32), tiered | astubbs#165, astubbs#145, astubbs#189; the classic API's batch option is the seed and its two defects are fixed first |
| Seek a partition; add or remove a route at runtime | In scope (R31) | astubbs#174, astubbs#246, astubbs#245, and the only consumer operations the handle offers (astubbs#158) |
| Broker-free sandbox and test kit over the mock consumer, with a rate-driven generator of realistic random records | In scope (R33) | The mock consumer ships in the main artefact; the sandbox is the injection point R1 removed, plus the generator; try the whole fluent API with no broker and no test environment |
| Fixed-length wire-prefix stripping before deserialisation | Deferred | A route's deserialiser can do this; no fluent-API support needed |
| Terminate processing from inside the function | In scope (R24) | astubbs#172; the close paths already exist, the outcome names one |
| Deserialisation-failure policy on the classic API's poll path | In scope (R25) | astubbs#148, astubbs#153, astubbs#163; a third arm on an existing seam |
| Pluggable offset manager replacing the commit path | Excluded | The offset-map encoding is the product; replacing it is a different library |
| Best-effort multi-sink fan-out that commits despite a sink failure | Excluded | Contradicts exactly-once; the existing produce-many path is the durable form |
| Records for an unrouted topic dropped and committed | Excluded | A subscribed topic with no route is a definition error under R2; pattern subscriptions are an outstanding question |
| Virtual thread per record | Not this plan | astubbs#360 owns it; orthogonal to the surface |
| Build-time module descriptors and a bill of materials | Not this plan | Packaging; unaffected by the fluent API |

### Scope Boundaries

**Deferred for later**

- Everything marked Deferred in the disposition table.
- Null-key records processed unordered under key ordering (astubbs#244): named here so the route policy leaves room for it, out of scope for a later phase; it is a shard-assignment change in the engine, tier large, and the multi-topic note records the safe form, keying the unordered case by offset behind an option.
- A durable per-record attempt count, and progress on a poison record under the transactional mode with an unreachable dead-letter topic: both wait on engine work (R22's floor, and astubbs#225).
- A per-route commit mode: instance-wide only (R6); the engine has one producer and one transaction.
- Cross-topic key identity (astubbs#150) and topic priority (astubbs#236): stay with the multi-topic note.
- Engine-native implementation of each outcome: after the God-class decomposition.
- The poll-timeout setting: a new engine setting on either API, orthogonal to the objective.

**Tracked elsewhere**

- Everything marked Not this plan in the disposition table: virtual threads per record (astubbs#360), and build-time module descriptors with a bill of materials.

**Outside this work's identity**

- Everything marked Excluded in the disposition table.
- The Kafka Streams API (astubbs#255): stateful processing goes there; the fluent API is per-record.
- Deciding what the proxy clients mirror (KD5).

### Dependencies / Assumptions

- The facade can implement park, export, filter and retry limit over today's public primitives: today's retry queue already holds a failed record incomplete while the map commits past it, so park is that state with the re-attempt withheld; the produce-many path for the export send, a per-record user-function attempt count the facade keeps itself (the engine's exposed counter also advances on dead-letter send re-attempts, so it is not the number the user is promised), and success-on-return for filter. Confirmed against the code on 2026-09-09.
- The API-compatibility gate (astubbs#315) is the mechanism that proves R20; if it has not merged when this ships, R20 is proved by its check run on the branch.
- Consuming raw bytes on the fluent API and deserialising per route costs no extra copy for the common case: the consumer copies each record out of its fetch buffer into a byte array before any deserialiser runs, and the route's deserialiser reads that same array. The one exception is a deserialiser written against the buffer-view API, which a typed consumer can serve without the array and the fluent API cannot; planning measures the difference rather than assuming it.
- The transactional commit mode's produce path is atomic with the offset commit, as `docs/plans/2026-08-07-001-test-transactional-eos-battle-test-plan.md` proved; R14 rests on it.

### Outstanding Questions

**Resolve Before Planning**

- None.

**Deferred to Planning**

- Whether the fluent API is a new module or a package in the core module, and its name.
- The exact chain syntax and the handle's method names; a compiled README example is the arbiter, and a cheap sketch settles it.
- How the facade multiplexes routes into the engine's single function, and how it recovers the route for a record under a pattern subscription; a pattern subscription needs either a default route or a definition-time refusal.
- Metric names and tags for R19, consistent with the existing metrics, and whether the outcome counters extend the existing processed and failed record meters or sit beside them.
- Whether the health surface (astubbs#226) has landed, and how the handle adopts it.
- Which connection properties the facade owns outright and which pass through to route deserialisers, such as schema-registry settings.
- Whether parked state is carried in the commit metadata, through the opaque-rider work (astubbs#460), so that a restart does not re-attempt a parked record; without it R10's per-assignment rule applies.
- The measured per-record cost of the raw-bytes consumer against a typed one at zero processing time, on the client version PC targets (Dependencies).
- Metric names for the R28 gauges, beside the existing incomplete-offset gauges, and whether the parked list is served from a control-thread snapshot as the dashboard plan prescribes.
- Whether a stop (R24) also leaves the consumer group promptly on the consumer-commit modes, where today's close sends no leave-group request.

### Issues this work relates to

Fork numbers; each mirror links its upstream original. A row says how the issue relates, not that it closes: the Success Criteria closure list is the authority for what counts as closed.

| Issue | How it relates |
|---|---|
| astubbs#243 | Separate consume and produce types: each route declares its own, produced types separately (R3); on the classic API by new produce overloads with separate output types (R34) |
| astubbs#254 | Per-topic processing functions: one route per topic (R2) |
| astubbs#149 | Dead-letter queue: delivered as park in place plus export at capacity (R11, R13, R27) |
| astubbs#141 | Max retries with a callback: the retry limit and the park observer (R10, R16) |
| astubbs#239 | The retry epic: its two open children are the two rows above |
| astubbs#231 | Skip, dead-letter or shutdown reactions: the filtered, export and stop outcomes (R8, R24, R27) |
| astubbs#172 | Terminate processing from the function: the stop outcome (R24) |
| astubbs#148 | A bad record kills the poll thread: per-route deserialisation on the fluent API, a poll-path policy on the classic (R4, R12, R25) |
| astubbs#153 | Serialisation error handling API: the three-way decode result (R12) |
| astubbs#163 | Is there an exception handler: same cluster, same answer |
| astubbs#189 | One bad record fails the batch: per-record outcomes inside a batch (R32) |
| astubbs#234 | Scheduled retry: a park with a delay (R27) |
| astubbs#165 | Minimum batch size and maximum wait: batch mode (R32) |
| astubbs#145 | Same-key batches: batch mode (R32) |
| astubbs#164, astubbs#311 | Batch defects, fixed in the engine before batch mode ships (R32, R34) |
| astubbs#158 | Safe exposure of consumer APIs: the handle's seek is the only such operation (R31) |
| astubbs#174, astubbs#246 | Seek to an offset, to the beginning: handle operations (R31) |
| astubbs#245 | Change subscription after start: add or remove a route at runtime (R31) |
| astubbs#244 | Null-key records unordered under key ordering: named, deferred, tier large |
| astubbs#150, astubbs#236 | Cross-topic key identity and topic priority: out of scope, with the multi-topic note |
| astubbs#119 | The retry-forever intake stall: a parked record holds no worker, which is what removes it |
| astubbs#225 | Producer recovery: export under transactional commit beyond terminate-on-failure waits for its stack (R14) |
| astubbs#333, astubbs#392, astubbs#456 | Self-scaling: per-route admission is the knob it will move per function (R6, R23) |
| astubbs#255 | Kafka Streams on PC: the stateful API; a constraint on outcomes, not scope |
| astubbs#242 | The language proxy: one callback per route and policy as data exist for it (R18) |
| astubbs#226 | Health surface: the handle adopts it when it lands |
| astubbs#315 | API-compatibility gate: proves the classic API unchanged (R20) |
| astubbs#215, astubbs#216 | Dashboard and unbounded-buffer metrics: the parked-set query is the engine API their panel needs (R28) |
| astubbs#460 | The commit-metadata rider: the vehicle for parked state surviving a restart |
| astubbs#8, astubbs#313 | The 2022 dead-letter draft and the dead-letter brainstorm binder: read against, and superseded at the requirements stage |

### Sources / Research

- `docs/plans/2026-08-18-001-investigate-dlq-prior-art-report.md` - the demand ledger, the 2022 lineage and its three draft defects, the six open questions this document answers.
- `docs/inflight/next-multi-topic-multi-function.md` - the design cluster, the fair-share position the fluent API supersedes with per-route limits, the co-partitioning limit on user-supplied keys.
- `docs/inflight/core-decompose-abstract-parallel-eos-stream-processor.md` - the cut order and the PRs to merge first; why engine-native outcomes wait.
- `docs/inflight/branch-ks-streams-workstream.md` - the Streams API and its status.
- `docs/inflight/core-work-identity-model.md` - the disposition vocabulary this document narrows to the ceiling.
- `docs/plans/2026-08-07-001-test-transactional-eos-battle-test-plan.md` (on master) - the proof that the transactional produce path is atomic with the offset commit, which R14 rests on.
- `docs/inflight/next-select-retries-from-the-retry-queue.md` - the retry queue is already time-ordered with a hash index and nothing selects from it; park is that state with the re-attempt withheld.
- `docs/inflight/web-control-plane.md` - the blocked-frontier panel with retry-now and dead-letter actions, and the rule that each button is an engine API first; R28 is that API.
- `docs/plans/2026-08-07-002-feat-embedded-web-dashboard-plan.md` - the per-partition payload-budget view and the control-thread snapshot the dashboard serves from.
<!-- file-refs: N/A - the three sources above live on unmerged branches; print them with bin/inflight.mjs docs show -->
- `docs/inflight/core-163-poll-path-has-no-error-seam.md` (on master) - the deserialisation cluster's answer: no processing-path seam, and the poll path's existing typed seam that R25 extends.
- astubbs#8, the 2022 dead-letter draft (confluentinc#366): its header names and its reaction enum carry into R13; its user-function-runner extraction is the God-class seam the engine-native form lands in; its three draft defects are recorded in the prior-art report.
- Issues: astubbs#243, astubbs#254, astubbs#239, astubbs#149, astubbs#231, astubbs#163, astubbs#153, astubbs#172, astubbs#189, astubbs#255, astubbs#242, astubbs#226, astubbs#315, astubbs#360.
- The README anchors `skipping-records` and the circuit-breaker section, for today's documented behaviour and the DIY composition.
<!-- file-refs: N/A - the four docs/inflight and docs/plans sources above live on unmerged branches; print them with bin/inflight.mjs docs show, which searches every ref -->

