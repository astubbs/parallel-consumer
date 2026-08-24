---
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
execution: code
type: feat
product_contract_source: ce-plan-bootstrap
created: 2026-08-24
---

# feat(streams): bundle foreign invocations without giving up per-record outcomes

**This plan covers the Kafka Streams wrapper only.** The Parallel Consumer clients are explicitly
out of scope, and the reason is that they already solved this: KTD10 of
[`2026-08-14-001-feat-language-proxy-plan.md`](2026-08-14-001-feat-language-proxy-plan.md) decided
to coalesce records into one protocol message while keeping `batchSize` at 1, the frozen wire
carries `Dispatch { repeated DispatchRecord }` for exactly that, and `DispatchWaveAssembler`
implements it with a coalescing window. The Streams wrapper has no equivalent in either direction.

---

## Goal Capsule

Cut the per-record cost of crossing the language boundary in the Kafka Streams wrapper by sending
many records per hop instead of one, **without weakening the guarantee that each record gets its own
independent outcome**.

The measured prize is specific: the crossing costs **~120us fixed plus ~6.5us/KB**
([`../inflight/perf-crossing-fixed-versus-per-byte.md`](../inflight/perf-crossing-fixed-versus-per-byte.md)),
and at the owner's stated payload profile - compressed JSON, so roughly 1-3 KB *decompressed* at the
boundary - a bundle of 100 is worth about 16x. Bundling amortises only the fixed term, which is
why the payload profile is load-bearing rather than incidental.

---

## Problem Frame

**The operator contract makes the obvious implementation impossible, and that is the whole problem.**

`ForeignValueMapper` implements `ValueMapperWithKey`, whose `apply(key, value)` must return one
value synchronously. To bundle, a record must be *held* while others accumulate. With a single
stream thread that deadlocks by construction: call 1 blocks waiting for the bundle to fill, and
call 2 never arrives because the thread is still inside call 1. Adding threads does not fix it -
it caps bundle size at the thread count, and threads are capped by partitions, so a bundle of 100
would need 100 partitions.

So bundling is not a tuning change to the existing operator. It requires an operator that can
*accept a record now and emit it later*, which is what the low-level Processor API provides and the
mapper API does not.

### What the guarantee is, and why it cannot be traded away

N records crossing in one frame must still produce N **independent** outcomes. Apache Beam bought
its bundling by giving exactly this up: any element failing discards and retries the whole bundle,
and per-item outcomes were considered and deliberately rejected as not worth the complexity. This
project already declined that trade once, in KTD10, on the grounds that per-item outcome machinery
already existed. The same answer applies here, and
[`../inflight/next-batching-modes-for-clients.md`](../inflight/next-batching-modes-for-clients.md)
states the requirement directly: *"N records in must be able to produce N outcomes out."*

### Vocabulary

Per `CONCEPTS.md`, and the distinction is the reason this plan exists separately from its neighbour:

- **Bundling** - how many records cross the boundary per hop. This plan.
- **Batching** - how many records the user's function receives per call. A different axis, owned by
  [`../inflight/next-batching-modes-for-clients.md`](../inflight/next-batching-modes-for-clients.md).

They are independent. This plan bundles the hop and **keeps handing the host one record at a time**,
so no client-facing signature changes in any language.

---

## Requirements

- **R1.** A bundle carries N records in one `Invocation`-equivalent message and returns N results in
  one response message.
- **R2.** Each of the N results is independently a value or an error. One record's failure must not
  discard, retry, or alter the outcome of any other record in the same bundle.
- **R3.** Per-key ordering is preserved. Records forwarded downstream appear in the order the
  topology would have produced them without bundling.
- **R4.** No record is acknowledged as processed while it is still buffered and unforwarded. A crash
  must not lose a buffered record.
- **R5.** Bundle latency is bounded. A partially-filled bundle is flushed on a timer, so a low-rate
  topic does not stall waiting for a bundle that will never fill.
- **R6.** The host's function signature is unchanged in every language: one record in, one value out.
- **R7.** Bundling is configurable and can be turned off, and off must be behaviourally identical to
  today.
- **R8.** The engine reports what bundling actually achieved - bundle sizes reached and flush causes -
  so the configuration can be tuned against evidence rather than guessed.

---

## Key Technical Decisions

**KTD1. Move the foreign operator from `ValueMapperWithKey` to the Processor API.**
This is forced, not preferred: see Problem Frame. `Processor.process()` may store a record and
return without forwarding, and forward later via `ProcessorContext.forward()`, including from a
punctuator. That is what makes buffering legal. The Processor API classes are already on the
classpath. `TopologyAssembler.mapValues` keeps its handle-table behaviour and its signature to the
host; only what it attaches to the topology changes.

**KTD2. Flush before commit, and treat that as the correctness requirement rather than a tuning
concern.** A record consumed and buffered but not forwarded is a record Kafka Streams may commit an
offset past. The leading mechanism is a `StateStore` registered by the processor whose `flush()`
forces the bundle out, because Kafka Streams flushes state stores as part of preparing a commit -
this gives a pre-commit hook the Processor API does not otherwise expose. **This mechanism must be
verified before it is relied on** (see U1); if it does not hold, the fallback is to bound buffering
strictly inside a punctuator interval shorter than the commit interval, which is weaker and must be
recorded as such.

**KTD3. Keep the host's function record-at-a-time.** The bundle is unwrapped on the host side and
the registered function is called once per record. This keeps R6, keeps every language binding's
signature stable, and keeps this plan strictly on the bundling axis. It also means the host-side
change is small - a loop - which is where the risk should be, given the engine side carries the
semantics.

**KTD4. Results travel as a repeated message with an explicit per-record correlation, not by
position.** Positional correlation is one refactor away from silently pairing the wrong result with
the wrong record, and the failure would be invisible: values are opaque bytes, so a mispairing
produces plausible output. Each result names its record.

**KTD5. The v1alpha1 schema may change freely; the frozen v1 wire is untouched.** `streams.proto`
carries the experimental-and-unfrozen notice, so `Invocation`/`InvocationResult` can be reshaped
rather than extended. Nothing in this plan touches
`parallel-consumer-proxy-protocol/.../proxy.proto`.

**KTD6. Bundling is off by default until measured.** The 16x figure is an upper bound that assumes
bundle assembly is free, and assembly cost is unmeasured. Shipping it on by default would make an
unmeasured claim the default behaviour.

---

## High-Level Technical Design

```mermaid
sequenceDiagram
    participant K as Kafka Streams thread
    participant P as BundlingProcessor
    participant R as InvocationRegistry
    participant H as Host (Python)

    K->>P: process(r1)
    P->>P: buffer r1, return (no forward)
    K->>P: process(r2)
    P->>P: buffer r2, return
    Note over P: buffer full, or punctuator, or pre-commit flush
    P->>R: submit bundle [r1, r2]
    R->>H: InvocationBundle{ [c1,r1], [c2,r2] }
    H->>H: call fn(r1); call fn(r2) - independently
    H->>R: InvocationBundleResult{ c1:value, c2:error }
    R->>P: results, keyed by correlation
    P->>K: forward(r1 -> value)
    Note over P: r2 failed - its own outcome, r1 unaffected
```

The shape that matters: **the buffer sits between `process` and `forward`**, and the flush has three
independent triggers (size, punctuator, pre-commit). Results are matched by correlation, never by
position.

---

## Implementation Units

### U1. Prove the pre-commit flush hook, before anything is built on it

- **Goal:** Establish whether a registered `StateStore`'s `flush()` is invoked before Kafka Streams
  commits offsets, which KTD2 depends on entirely.
- **Requirements:** R4.
- **Dependencies:** none. **This unit gates every other unit.**
- **Files:** `parallel-consumer-proxy-streams/src/test/java/bz/stub/parallelconsumer/streams/PreCommitFlushHookTest.java`
- **Approach:**
  1. Register a trivial `StateStore` from a processor in a `TopologyTestDriver` topology.
  2. Record the ordering of `flush()` against offset commits.
  3. Report the finding either way, including the Kafka Streams version it was established on.
- **Execution note:** This is a characterisation test of a third-party framework's behaviour, not a
  test of our code. Write it to *discover*, then keep it as the regression guard that tells us if a
  Kafka upgrade moves the hook.
- **Test scenarios:**
  - A store registered by a processor has `flush()` called before the task commits.
  - The ordering holds when the commit is triggered by the commit interval rather than by
    `context().commit()`.
  - `Test expectation:` if the hook does not hold, this unit's output is a written finding and the
    plan stops here for a decision, not a workaround.
- **Verification:** the ordering is established by observation and written down, with the version.

### U2. Reshape the invocation messages to carry many records and many results

- **Goal:** The wire can express a bundle and a per-record result set.
- **Requirements:** R1, R2, R4.
- **Dependencies:** U1.
- **Files:** `parallel-consumer-proxy-streams/src/main/proto/parallelconsumer/streams/v1alpha1/streams.proto`,
  `parallel-consumer-proxy-streams/src/test/java/bz/stub/parallelconsumer/streams/StreamsProtocolRoundTripTest.java`
- **Approach:**
  1. Add a bundle message carrying repeated per-record entries, each with its own correlation, key
     and value.
  2. Add a result message carrying repeated per-record results, each naming its correlation and
     carrying either a value or an error. Note the existing `InvocationResult` is NOT a `oneof` - it
     is `correlation` plus independent `optional bytes value` / `optional string error`, so
     "neither set" is representable today. KTD5 permits reshaping v1alpha1, so make the
     per-record result a real `oneof` and remove that hole.
  3. Keep the single-record messages. They remain the off path under KTD6 and the conformance
     baseline.
- **Patterns to follow:** the existing `Invocation`/`InvocationResult` pair, and the frozen wire's
  `Dispatch`/`Report` split, which is the same problem solved once already.
- **Test scenarios:**
  - A bundle of N round-trips with every correlation preserved.
  - A result set where one entry is an error and the others are values decodes with exactly one
    error, and the error is attached to the correct correlation.
  - A result set arriving with entries in a different order from the request still pairs correctly.
  - A result set missing an entry for a correlation is a detectable protocol fault, not a silent
    drop.
- **Verification:** round-trip tests pass; the frozen `proxy.proto` is untouched.

### U3. Replace the foreign operator with a buffering processor

- **Goal:** The topology can hold records and forward them after a bundled crossing.
- **Requirements:** R1, R3, R4, R5.
- **Dependencies:** U1, U2.
- **Files:** `parallel-consumer-proxy-streams/src/main/java/bz/stub/parallelconsumer/streams/BundlingForeignProcessor.java`,
  `parallel-consumer-proxy-streams/src/main/java/bz/stub/parallelconsumer/streams/TopologyAssembler.java`,
  `parallel-consumer-proxy-streams/src/test/java/bz/stub/parallelconsumer/streams/BundlingForeignProcessorTest.java`
- **Approach:**
  1. A processor that buffers on `process`, and forwards on flush in buffer order.
  2. Three flush triggers: buffer reaches the configured size, a punctuator fires, or the pre-commit
     hook from U1 fires.
  3. `ForeignValueMapper` stays for the bundling-off path under KTD6, so the two can be compared.
- **Execution note:** Ordering and the commit hazard are the properties worth proving first; the
  happy path is the easy part.
- **Test scenarios:**
  - Records forwarded in buffer order, so per-key order matches the unbundled topology.
  - A partially-filled buffer flushes on the punctuator within the configured bound.
  - A record failing in the middle of a bundle does not affect its neighbours' forwarded values.
  - Nothing is forwarded before its result arrives.
  - Under `TopologyTestDriver`, the bundled topology produces the same output as the unbundled one
    for the same input - the equivalence oracle.
  - **Beware:** `TopologyTestDriver.close()` deletes the state directory, so any test inspecting
    state must do so while the driver is open. This has produced a green test that asserted nothing
    in this module before.
- **Verification:** bundled and unbundled topologies are output-equivalent on the same input.

### U4. Unwrap bundles on the host side, keeping the function record-at-a-time

- **Goal:** A Python host serves a bundle by calling its registered function once per record.
- **Requirements:** R2, R6.
- **Dependencies:** U2.
- **Files:** `parallel-consumer-proxy-clients/parallel-consumer-proxy-client-python/src/parallel_consumer/streams/_session.py`,
  `parallel-consumer-proxy-clients/parallel-consumer-proxy-client-python/tests/test_streams_session.py`
- **Approach:**
  1. On a bundle, loop the entries and call the registered function per record.
  2. One record raising produces an error for that correlation and values for the rest - the loop
     does not abort.
  3. Assemble one result message and send it once.
- **Patterns to follow:** the existing `_on_invocation`, which already reports a failure rather than
  substituting a value. That behaviour is the per-record guarantee in miniature and must survive.
- **Test scenarios:**
  - A bundle of N produces N results in one message.
  - A function raising on entry 3 of 5 yields four values and one error, with the error on entry 3's
    correlation.
  - A function raising on *every* entry yields N errors, not one aborted bundle.
  - An entry naming an unregistered token is answered with an error rather than dropped, matching
    the single-record behaviour.
  - The fake engine must answer asynchronously, or these tests cannot fail - a synchronous fake has
    already produced two tests in this module that passed against deliberately broken code.
- **Verification:** `make lint test` green; the single-record path still passes unchanged.

### U5. Make it configurable, off by default, and observable

- **Goal:** Bundle size and flush interval are settable, bundling is off unless asked for, and the
  engine reports what it actually did.
- **Requirements:** R5, R7, R8.
- **Dependencies:** U2, U3, U4.
- **Files:** `parallel-consumer-proxy-streams/src/main/java/bz/stub/parallelconsumer/streams/StreamsSessionService.java`,
  `parallel-consumer-proxy-streams/src/main/proto/parallelconsumer/streams/v1alpha1/streams.proto`,
  `parallel-consumer-proxy-streams/src/test/java/bz/stub/parallelconsumer/streams/StreamsSessionServiceTest.java`
- **Approach:**
  1. Carry bundle size and flush interval on the session handshake.
  2. Size 1 means off, and off must take the unbundled path so it is genuinely identical rather than
     a bundle of one.
  3. Report achieved bundle sizes and flush causes, because a bundle configured at 100 that averages
     3 is the failure mode this feature is most likely to have, and it is invisible without this.
- **Test scenarios:**
  - Size 1 produces byte-identical behaviour to bundling absent.
  - A size larger than the in-flight ceiling is rejected at the handshake rather than deadlocking.
  - Reported bundle sizes match what was actually sent.
- **Verification:** a run reports its own bundle-size distribution.

### U6. Measure it, including the assembly cost the 16x figure assumes away

- **Goal:** Replace the upper bound with a number.
- **Requirements:** R8.
- **Dependencies:** U5.
- **Files:** `parallel-consumer-proxy-clients/parallel-consumer-proxy-client-python/demo/streams_demo.py`,
  `docs/inflight/perf-streams-invocation-bundling.md`
- **Approach:**
  1. Sweep bundle size at the payload profile that matters - 1-3 KB, not the demo's short default.
  2. Report per-record cost against bundle size, and the achieved-size distribution beside it.
  3. State the assembly cost as measured, since every prior figure assumed it was zero.
- **Execution note:** The method constraints from the two prior measurements apply and were learned
  expensively: sweep and take a slope rather than measuring one point, hold record count fixed so
  warm-up cancels, interleave rather than running in order, use the broker's log-append clock, and
  run on a quiet machine.
- **Test scenarios:** `Test expectation: none -- a measurement unit. Its output is the note.`
- **Verification:** the note reports a measured per-record cost against bundle size, with run counts
  and spread, and states whether the 16x upper bound survived.

---

## Alternatives Considered

**More stream threads. MEASURED 2026-08-24, and they do not work** -
[`../inflight/perf-crossing-is-cpu-and-serialised.md`](../inflight/perf-crossing-is-cpu-and-serialised.md).
Throughput plateaus at about 1.5x and stops improving after two threads, while CPU per record gets
steadily worse: 8 threads burn 23% more CPU per record than 1 for no additional throughput. The
review was right that this had never been run; running it closed the option rather than opening it.

The same experiment refuted the other objection to this plan. The crossing is **CPU-heavy, not
blocked** - 232us of CPU against 152us of wall time, so one crossing occupies roughly 1.5 cores. The
core-hours case for bundling was understated, not overstated.

**And it found the real mechanism, which changes why this plan is worth building.** Threads plateau
because every crossing is serialised through one lock: `StreamsSessionService` holds one session,
one `StreamObserver`, and one `transmitLock`. Bundling's justification is therefore not primarily
that it amortises CPU, but that it reduces how often that serialised path is traversed.

**One session per stream thread - NOT YET MEASURED, and it should be before this plan proceeds.**
Removing the serialisation directly would need no Processor API migration, no buffer between
process and forward, and no commit hazard. It is a smaller change than this plan and it attacks the
constraint the experiment actually found. Reasoned from the code rather than measured, which is
precisely the status "threads will help" held before it was tested - so it gets the same treatment.

**Bundle at the transport rather than the operator** - coalesce whatever invocations happen to be in
flight, as `DispatchWaveAssembler` does for the PC path. Originally rejected on the grounds that
with one stream thread there is nothing to coalesce. **That reasoning was circular and the review
caught it:** it dismissed transport coalescing using a single-thread assumption while recommending
more threads two paragraphs earlier. With N threads there are N concurrent invocations and the
assumption dissolves. The measurement above now settles it from the other end - threads plateau at
1.5x, so the concurrency needed to make coalescing worthwhile does not exist either. Both are shut,
but for a measured reason rather than a circular one.

**Accept Beam's trade** - bundle-level failure, retry the whole bundle. Rejected: it is the
guarantee this project differentiates on, KTD10 already declined it once, and the per-record
machinery that made declining cheap then still exists.

---

## Scope Boundaries

**Not in this plan**

- The Parallel Consumer clients. Already solved - see the first paragraph.
- The batching axis - what the host's function receives per call. Unchanged here by KTD3.
- Zero-copy and the transport axis. Only available in the embedded FFI mode, and orthogonal.
- The frozen v1 wire.

**Deferred to follow-up work**

- Bundling for stateful operators. This plan bundles a stateless value transform. An aggregation
  holds state per key, and whether a bundle may span keys mid-aggregation is a separate question.
- Adaptive bundle sizing. Fixed size and interval first; tuning against the U5 telemetry later.

---

## Risks and Dependencies

- **The pre-commit flush hook may not hold**, which is why U1 gates everything. If it fails, R4 is
  satisfied only by a weaker time-bound argument, and that should be a decision rather than a
  silent downgrade.
- **Bundles may not fill.** A configured size of 100 that averages 3 delivers almost none of the
  win. U5's telemetry exists because this is the most likely disappointment and is invisible
  otherwise.
- **Latency worsens by design.** A record waits for its bundle. The punctuator bounds it, but this
  trade should be stated to users rather than discovered by them.
- **The equivalence oracle is the safety net.** If the bundled and unbundled topologies ever
  disagree on the same input, that is a correctness bug and not a tuning matter.

---

## Verification Contract

| Gate | Command | Applies to | Done signal |
|---|---|---|---|
| Java suite | `./mvnw -pl :parallel-consumer-proxy-streams -am test` with JDK 17 set per command | U1-U3, U5 | green; read counts from surefire, never scope with `-Dtest=` |
| Python | `make lint test` in the Python client | U4 | green |
| Schema | regenerate stubs and `make proto-check` | U2 | stubs match the schema and are committed |
| Copyright | `COPYRIGHT_CHECK_REQUIRE_FORK_POINT=1 bin/check-copyright-headers.sh`, **after staging** | all | zero violations |
| Equivalence | bundled vs unbundled on the same input | U3 | identical output |
| Measurement | the sweep in U6 | U6 | a note with run counts and spread |

---

## Definition of Done

- A bundle of N crosses in one hop and returns N independently-resolved outcomes.
- One record's failure demonstrably does not affect its neighbours.
- Bundled and unbundled topologies produce identical output for identical input.
- No record is committed while buffered and unforwarded, with the mechanism established by
  observation rather than assumed.
- Bundling is off by default, and off is identical to today.
- A measured per-record cost against bundle size exists, and the 16x upper bound is either confirmed
  or corrected.

---

## Sources

- [`../inflight/perf-crossing-fixed-versus-per-byte.md`](../inflight/perf-crossing-fixed-versus-per-byte.md) - the fixed/per-byte split this plan monetises
- [`../inflight/perf-streams-crossing-attribution.md`](../inflight/perf-streams-crossing-attribution.md) - the crossing cost and that engine-side marginal cost is ~zero
- [`../inflight/next-batching-modes-for-clients.md`](../inflight/next-batching-modes-for-clients.md) - the per-record-outcome requirement, and the batching axis this plan does not touch
- [`../language-bindings.md`](../language-bindings.md) - the five axes; this is axis 3
- [`2026-08-14-001-feat-language-proxy-plan.md`](2026-08-14-001-feat-language-proxy-plan.md) - KTD10, which decided this once for the PC path and declined Beam's trade
- `CONCEPTS.md` - bundling versus batching
