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

- **Objective:** A developer who once used Parallel Consumer and is deciding whether to come back can define a working consumer from the README alone: typed handling per topic, a retry limit, a dead-letter destination and filtering, in one screen of code, without opening the javadoc.
- **Means:** A new, modern entry point that is a facade over today's engine, shipped beside the existing API as an equal. Its behaviours are specified as record outcomes so the engine can take each one over natively after the God-class decomposition, without the surface moving.
- **Product authority:** This document, for the surface and its behaviours. The engine-native implementation of each outcome is separately planned work that must honour the behaviours fixed here. The Kafka Streams work (astubbs#255) and the language proxy (astubbs#242) are constraints on this surface, not scope.
- **Open blockers:** None before planning. Every open item is classified under Outstanding Questions.

---

## Product Contract

### Summary

Add a modern way to define a Parallel Consumer: connection properties in, one typed route per topic with one processing function each, retry limit and dead-letter destination declared once per instance, a handle out. The existing options builder and processor stay as an equal, undeprecated door. Each behaviour the new door offers is specified as a record outcome, so that when the engine can own it, the facade implementation is replaced without changing what the user wrote.

### Problem Frame

Today a consumer is built by constructing a Kafka consumer and producer by hand, wiring both into one options builder beside unrelated settings, passing that to a static factory, subscribing, and then registering exactly one function for every subscribed topic. That function receives one key and value type for every topic and must produce records of the same types. A record whose function throws is retried forever; there is no way to say "give up", "skip", or "send this somewhere". A payload the configured deserialiser cannot read ends the broker-poll thread. Users carry the same three workarounds: produce to their own dead-letter topic and swallow the exception, switch on the topic name inside one handler, and consume raw bytes so they can deserialise inside the function.

The demand is recorded across the issue tracker and is the oldest open surface work in the project: separate consume and produce types (astubbs#243), per-topic functions (astubbs#254), the retry epic (astubbs#239), a dead-letter queue (astubbs#149), skip or stop reactions (astubbs#231, astubbs#172), a deserialisation failure handler (astubbs#163, astubbs#153), and one bad record failing a whole batch (astubbs#189). Two of these have design notes but none reached requirements. A comparable library in the same space now offers all of them in one chain, so a returning user meets the gap on first contact.

### Key Decisions

- KD1. **The new door coexists with the old as an equal.** Both are documented; nothing is deprecated; the API-compatibility gate (astubbs#315) must pass unchanged for the old surface. (session-settled: user-directed - chosen over deprecating the old surface in favour of the new one: no forced migration for existing users.) Governs R20, R21.
- KD2. **Facade first, engine second.** The new door ships as composition over today's primitives before any God-class cut lands; each behaviour is specified as an outcome so the engine can take it over later without the surface moving. (session-settled: user-directed - chosen over landing the outcome model in the engine first: the surface is the most user-facing change, it drives fixes the engine needs anyway, and the returning user should not wait for the decomposition.) Governs R7, R8, R9, R22, R23.
- KD3. **One callback per route, everything else is data.** A route carries its types and one processing function; retry limit, backoff, dead-letter destination, ordering and concurrency are data on the instance. The processing function reports its outcome; retry-versus-terminal is never expressed as a list of exception classes. (session-settled: user-directed - chosen over Java-rich hooks such as predicate filters and exception-class retry lists: every callback is a function that must exist in each foreign client of the language proxy, and data crosses the wire for free.) Governs R3, R5, R6, R10, R18.
- KD4. **The feature ceiling is the reference surface.** The document addresses every capability the comparable library offers, and adds nothing beyond it. (session-settled: user-directed - chosen over a richer outcome vocabulary and extra hooks: "we do not need to go beyond what it offers".) Governs the Feature disposition section.
- KD5. **The proxy mirroring decision is left open, and the surface is designed for both.** No construct in the new door may be one a wire contract could not carry. (session-settled: user-directed - chosen over committing the proxy clients to the modern surface now.) Governs R18.
- KD6. **Capacity across topics is work-conserving fair share, not reservation.** Recorded from the owner on 2026-08-21 in `docs/inflight/next-multi-topic-multi-function.md`. (session-settled: user-directed - chosen over per-topic capacity reservation: an idle topic must not waste its share.) Governs R23.
<!-- file-refs: N/A - the multi-topic note lives on unmerged branches; print it with bin/inflight.mjs docs show -->

- KD7. **The primary success signal is a returning-user trial.** (session-settled: user-directed - chosen over the README example, issue closure, or workaround deletion as the primary signal; those remain secondary.) Governs Success Criteria.
- KD8. **Full parity in one document.** The four independently plannable outcomes (entry point and routes, terminal outcomes, per-topic types, lifecycle) are specified here together. (session-settled: user-directed - chosen over owning one area and naming the rest as follow-ons.)
- KD9. **Deserialisation happens per route, inside the facade.** The new door consumes raw bytes and applies each route's deserialisers itself. A payload that cannot be read becomes a failed outcome for that one record rather than an error on the poll thread. Governs R4, R12.
- KD10. **A dead-letter record travels the existing produce path.** Under the transactional commit mode it is in the same transaction as the offset commit; a failed dead-letter send leaves the record incomplete. Governs R13, R14, R15.
- KD11. **Policy is per instance; a topic has exactly one route.** Two functions on one topic is not offered. Governs R2, R6.

<!-- ce-section: work-relationships -->
### How This Work Fits Together

This plan owns the modern surface and the behaviours it promises. The breakdown below is the current understanding, not a committed roadmap.

- **Depends on** nothing before planning. The facade composes today's public primitives (KD2).
- **Enables** the engine-native takeover of each outcome, which is separately planned work sequenced after the God-class decomposition in `docs/inflight/core-decompose-abstract-parallel-eos-stream-processor.md` (astubbs#479). Fair share across topics (R23) waits for that engine work.
- **Shares** the record-outcome vocabulary with the dead-letter brainstorm (astubbs#313, prior-art report `docs/plans/2026-08-18-001-investigate-dlq-prior-art-report.md`). This document answers that report's six open questions at product level (R7 to R15); the 2022 draft astubbs#8 remains the implementation seed for the engine-native form.
- **Shares** the per-topic design cluster with `docs/inflight/next-multi-topic-multi-function.md` (astubbs#254, astubbs#243, astubbs#236, astubbs#150, astubbs#245, astubbs#244). R2 to R4 settle the attachment shape; cross-topic key identity and topic priority stay with that note.
- **Can proceed independently of** the Kafka Streams work (astubbs#255), which gives a Streams topology the engine's concurrency and is the door for stateful processing. The two doors must not contradict each other about what an outcome means.
- **Can proceed independently of** the language proxy (astubbs#242), subject to R18.
- **Still to decide:** whether the proxy clients mirror this surface (KD5); whether the health surface arrives through astubbs#226; whether the user function runs on virtual threads (astubbs#360) - neither changes this contract.
<!-- file-refs: N/A - the decomposition note, the dead-letter report and the multi-topic note live on unmerged branches, named by PR above; print any of them with bin/inflight.mjs docs show -->


### Actors

- A1. **Returning developer** - knows the old door, left, is evaluating whether to come back. Reads the README first.
- A2. **Existing user** - runs the old door in production with one or more of the three workarounds. Must not be broken (KD1).
- A3. **Foreign-client author** - implements a proxy client in another language against the wire contract (astubbs#242). Sees only what R18 permits.
- A4. **The engine** - today's processor behind the facade; later the native owner of each outcome.

### Requirements

**Entry point and routes**

- R1. A consumer is defined from connection properties and started to obtain a handle; the user constructs no Kafka client objects. Supplying pre-built clients remains possible through the old door only.
- R2. A route binds one topic to one processing function; registering a second route for the same topic is refused at definition time.
- R3. A route declares its own key and value types for consumed records and, when it produces, separate key and value types for produced records.
- R4. A route's deserialisers are applied per record inside the facade; the consumer itself is configured for raw bytes by the facade, never by the user.
- R5. A set of topics that share one function and one type pair may be declared as a single route.
- R6. Retry limit, retry delay, dead-letter destination, ordering mode and concurrency limit are declared once per instance and apply to every route; declaring any of them on a route is refused at definition time.

**Outcomes and policy**

- R7. Every record reaches exactly one terminal outcome: succeeded, filtered, dead-lettered, or exhausted with no destination; a retry is a step towards one of these, never an outcome of its own.
- R8. The processing function reports filtered by returning without a result and without throwing; a filtered record completes and commits like a success and is counted separately.
- R9. The processing function reports retry by throwing; the existing retriable exception keeps its meaning; any other exception is also a retry. The distinction affects logging only.
- R10. Retry stops after the configured limit, counted as attempts after the first; the default limit is unbounded so that the old door's behaviour is the default for a definition that sets nothing.
- R11. On exhaustion, the record is dead-lettered when a destination is declared; when none is declared it is logged and left incomplete, which is today's behaviour, so that a returning user who declares nothing loses nothing.
- R12. A payload a route cannot deserialise is a failed attempt for that record under R9 and R10; it never ends the poll thread.

**Dead-letter**

- R13. A dead-letter record carries the original key bytes, value bytes and headers unchanged, plus provenance headers naming the source topic, partition, offset, timestamp, attempt count and the last failure's class and message.
- R14. Under the transactional commit mode the dead-letter send is part of the transaction that commits the record's offset.
- R15. A dead-letter send that fails leaves the record incomplete, so its offset is not committed and it is attempted again under R10; the instance does not stop.
- R16. Exhaustion is observable once per record, after the last attempt, with the record, the last failure and the attempt count, as a Java-binding observer that is sugar over the outcome (KD3).

**Lifecycle and observability**

- R17. The handle exposes a bounded graceful shutdown that drains in-flight work, is usable with try-with-resources, and a blocking wait for shutdown; the existing drain and shutdown timeouts govern the bound.
- R18. Every construct on the new door is either data a wire contract can carry or the one processing function per route; no second callback is required for correct operation, and any observer is optional sugar.
- R19. Outcome counts (succeeded, filtered, dead-lettered, exhausted) are published through the existing metrics integration, tagged by topic.

**Coexistence and sequencing**

- R20. The old door's public surface is unchanged and the API-compatibility gate passes with no allowed-breakage entries added for this work.
- R21. The README's first example uses the new door; the old door keeps its own documented section.
- R22. The behaviours in R7 to R15 hold when the facade implements them over today's engine, and hold unchanged when the engine implements them natively; the same acceptance examples are the oracle for both.
- R23. When more than one route has work and the instance is at its concurrency limit, capacity is shared between routes on a work-conserving basis (KD6); until the engine is topic-aware this is not enforced, and the document says so at the new door.

### Key Flows

- F1. Define and start
  - **Trigger:** A1 writes a definition from the README.
  - **Steps:** Declare properties; declare one route per topic with types and a function; declare instance policy; start; hold the handle in try-with-resources; await shutdown.
  - **Outcome:** A running consumer. Definition-time errors (R2, R6) surface before any poll.
  - **Covered by:** R1 to R6, R17.
- F2. A record fails, retries, dead-letters
  - **Trigger:** The function throws for a record.
  - **Steps:** Attempt counted; retry delay applied; further attempts until the limit; on exhaustion the dead-letter record is built (R13) and sent on the produce path (R14); the record completes; the observer fires once (R16); the dead-lettered count increments (R19).
  - **Outcome:** The source offset commits; the record is in the dead-letter topic with provenance.
  - **Covered by:** R7, R9 to R11, R13, R14, R16, R19.
- F3. A record is filtered
  - **Trigger:** The function returns with no result.
  - **Outcome:** The record completes and commits as a success; the filtered count increments; nothing is produced.
  - **Covered by:** R8, R19.
- F4. A payload cannot be read
  - **Trigger:** A route's deserialiser throws on a record.
  - **Outcome:** That record alone takes the path of F2; the poll thread continues; other routes are unaffected.
  - **Covered by:** R4, R12.
- F5. The engine takes an outcome over natively
  - **Trigger:** A God-class cut lands and the engine can own dead-letter, filter or retry limit.
  - **Steps:** The facade's implementation of that outcome is removed; the engine's is wired behind the same definition; the acceptance examples below are re-run unchanged.
  - **Outcome:** No user definition changes.
  - **Covered by:** R22.

### Acceptance Examples

- AE1. **Covers R10, R11.** Given a definition with no retry limit and no dead-letter destination, when a record's function always throws, then the record is retried indefinitely and no offset past it commits under partition ordering, exactly as the old door behaves.
- AE2. **Covers R10, R11, R13.** Given a retry limit of two and a dead-letter destination, when a record's function throws three times, then the fourth attempt does not occur, the dead-letter topic holds one record with the original bytes and headers plus provenance headers reporting three attempts, and the source offset commits.
- AE3. **Covers R14.** Given the transactional commit mode and AE2's definition, when a consumer reads the dead-letter topic with read-committed isolation, then the dead-letter record becomes visible only together with the committed source offset.
- AE4. **Covers R15.** Given a dead-letter destination that is unreachable, when a record exhausts its retries, then the record remains incomplete, its offset does not commit, the instance keeps processing other records, and the record is attempted again after the retry delay.
- AE5. **Covers R8, R19.** Given a route whose function returns no result for records with a missing field, when a thousand records are consumed of which a hundred lack the field, then the succeeded count is nine hundred, the filtered count is one hundred, all offsets commit, and nothing is produced for the hundred.
- AE6. **Covers R4, R12.** Given two routes on two topics, when one topic carries a payload the route's deserialiser rejects, then that record follows AE2 with the deserialisation error as its failure, the other topic's records are unaffected, and the poll thread is alive throughout.
- AE7. **Covers R2, R6.** Given a definition that registers a second route for a topic already routed, or sets a retry limit on a route, when the definition is built, then it is refused with a message naming the topic or the instance-level setting, before any connection is opened.
- AE8. **Covers R3.** Given a route consuming string keys and JSON-typed values that produces long keys and Avro-typed values, when the function returns produced records, then they are typed by the route's produced types and the compiler accepts the definition without casts.
- AE9. **Covers R17.** Given a running handle inside try-with-resources, when the block exits with work in flight, then in-flight work drains up to the configured drain timeout before the consumer closes, and offsets for drained work commit.
- AE10. **Covers R20.** Given the old door's public surface before this work, when the API-compatibility gate runs after it, then the gate passes with no new allowed-breakage entry.
- AE11. **Covers R22.** Given AE1 to AE9 passing against the facade, when the engine implements dead-letter natively behind the same definition, then AE1 to AE9 pass unchanged.

### Success Criteria

- A returning user, or a reviewer standing in for one, builds a working consumer with a route per topic, a retry limit and a dead-letter destination from the README alone, without opening the javadoc. Primary (KD7).
- The README's first example is the new door and is compiled in CI so it cannot drift.
- Each of the three documented workarounds has a one-line first-class replacement shown in a migration section of the README.
- Each issue in the cluster (astubbs#243, astubbs#254, astubbs#239, astubbs#149, astubbs#163, astubbs#189) is either closed by the shipped surface or reduced to a named residual on the issue.

### Feature disposition against the reference surface

Every capability the comparable library offers is listed with its disposition here (KD4). "In scope" means this plan specifies it; "Deferred" means it fits the door and is wanted later; "Excluded" means it contradicts what this library is.

| Capability | Disposition | Reason |
|---|---|---|
| Properties-in entry point, no client construction | In scope (R1) | The first thing a returning user sees |
| One typed route per topic | In scope (R2, R3) | astubbs#254; the attachment shape the multi-topic note left open |
| Homogeneous topic set sharing one route | In scope (R5) | Today's multi-topic subscribe, typed |
| Separate produced key and value types | In scope (R3) | astubbs#243, non-breaking because it lives on the new door |
| Per-route deserialisation, any format | In scope (R4) | Kafka's own deserialiser interface covers JSON, Avro, Protobuf and schema registries; no format modules of our own |
| Filter | In scope (R8) | As an outcome of the one function, not a second callback (KD3) |
| Retry limit and delay | In scope (R6, R10) | The delay function already exists; the limit is new |
| Dead-letter destination in one declaration | In scope (R11, R13) | astubbs#149, the most-demanded missing feature |
| Original bytes and headers preserved, provenance headers added | In scope (R13) | Matches the 2022 draft's header set |
| Failed dead-letter send leaves the offset uncommitted | In scope (R15) | The library's existing correctness stance |
| Exhaustion observer, once per record | In scope (R16) | As Java-binding sugar over the outcome |
| Ordering modes: unordered, key, partition | In scope (R6) | Already exist; the door maps to them; a "sequential" mode is partition ordering with concurrency one |
| Concurrency limit | In scope (R6) | Exists |
| Back-pressure with pause and resume | In scope, unchanged | Exists in the engine; the door exposes the existing settings |
| Handle: bounded graceful shutdown, try-with-resources, await | In scope (R17) | The existing close modes, given the idiom |
| Outcome counters by topic | In scope (R19) | Falls out of the outcome vocabulary |
| Produce results to another topic | In scope (R3) | The existing produce path, with separate output types |
| Poll timeout setting | In scope, unchanged | Exists |
| Health snapshot on the handle | Deferred | astubbs#226 is the health surface; the door adopts it when it lands |
| Per-key queue-depth diagnostics | Deferred | Belongs to the observability track and its GUI |
| Circuit breaker | Deferred | A declarative form of pause driven by observed failure rate; the primitives exist and the README shows the DIY composition |
| Trace-context propagation | Deferred | Owned by the record-tracing note; not asked for here |
| Transform composition on a route (chaining, peek, conditional) | Deferred | A route has one function; composition is the user's code. Revisit if returning users ask |
| Per-attempt failure observer | Deferred | A second callback with no data equivalent; R16 covers the once-per-record case |
| Batch sink with size and age flush and a coverage contract | Deferred | Batch consumption exists on the old door; batching on the produce side is separate work |
| Broker-free test kit driving the real code path over a mock consumer | Deferred | The mock consumer already ships in the main artefact; the kit is a packaging and documentation task |
| Fixed-length wire-prefix stripping before deserialisation | Deferred | A route's deserialiser can do this; no door support needed |
| Pluggable offset manager replacing the commit path | Excluded | The offset-map encoding is the product; replacing it is a different library |
| Best-effort multi-sink fan-out that commits despite a sink failure | Excluded | Contradicts exactly-once; the existing produce-many path is the durable form |
| Records for an unrouted topic dropped and committed | Excluded | A subscribed topic with no route is a definition error under R2; pattern subscriptions are an outstanding question |
| Virtual thread per record | Not this plan | astubbs#360 owns it; orthogonal to the surface |
| Build-time module descriptors and a bill of materials | Not this plan | Packaging; unaffected by the door |

### Scope Boundaries

**Deferred for later**

- Everything marked Deferred in the disposition table.
- A stop-the-instance outcome (astubbs#172) and scheduled retry (astubbs#234): above the ceiling (KD4).
- Per-route policy (retry, ordering, concurrency per topic): refused for now (R6); the next issue after per-topic functions, and the multi-topic note keeps it.
- Cross-topic key identity (astubbs#150) and topic priority (astubbs#236): stay with the multi-topic note.
- Engine-native implementation of each outcome and topic-aware fair share (R23): after the God-class decomposition.

**Outside this work's identity**

- Everything marked Excluded in the disposition table.
- The Kafka Streams door (astubbs#255): stateful processing goes there; this door is per-record.
- Deciding what the proxy clients mirror (KD5).

### Dependencies / Assumptions

- The facade can implement dead-letter, filter and retry limit over today's public primitives: the produce-many path for the dead-letter send, the attempt count the record context already exposes, and success-on-return for filter. Confirmed against the code on 2026-09-09.
- The API-compatibility gate (astubbs#315) is the mechanism that proves R20; if it has not merged when this ships, R20 is proved by its check run on the branch.
- Consuming raw bytes on the new door and deserialising per route costs one extra copy per record relative to a typed consumer; accepted for the door's benefit, to be measured in planning.
- The transactional commit mode's produce path is atomic with the offset commit, as the battle-test plan proved; R14 rests on it.

### Outstanding Questions

**Resolve Before Planning**

- None.

**Deferred to Planning**

- Whether the new door is a new module or a package in the core module, and its name.
- The exact chain syntax and the handle's method names; a compiled README example is the arbiter, and a cheap sketch settles it.
- How the facade multiplexes routes into the engine's single function, and how it recovers the route for a record under a pattern subscription; a pattern subscription needs either a default route or a definition-time refusal.
- Whether the exhaustion observer (R16) is offered at all in the first cut, given it is sugar.
- Metric names and tags for R19, consistent with the existing metrics.
- Whether the health surface (astubbs#226) has landed, and how the handle adopts it.
- The measured cost of the extra deserialisation copy (Dependencies).

### Sources / Research

- `docs/plans/2026-08-18-001-investigate-dlq-prior-art-report.md` - the demand ledger, the 2022 lineage and its three draft defects, the six open questions this document answers.
- `docs/inflight/next-multi-topic-multi-function.md` - the design cluster, the fair-share position, the co-partitioning limit on user-supplied keys.
- `docs/inflight/core-decompose-abstract-parallel-eos-stream-processor.md` - the cut order and the PRs to merge first; why engine-native outcomes wait.
- `docs/inflight/branch-ks-streams-workstream.md` - the Streams door and its status.
- `docs/inflight/core-work-identity-model.md` - the disposition vocabulary this document narrows to the ceiling.
- Issues: astubbs#243, astubbs#254, astubbs#239, astubbs#149, astubbs#231, astubbs#163, astubbs#153, astubbs#172, astubbs#189, astubbs#185, astubbs#255, astubbs#242, astubbs#226, astubbs#315, astubbs#360.
- The README anchors `skipping-records` and the circuit-breaker section, for today's documented behaviour and the DIY composition.
<!-- file-refs: N/A - the four docs/inflight and docs/plans sources above live on unmerged branches; print them with bin/inflight.mjs docs show, which searches every ref -->

