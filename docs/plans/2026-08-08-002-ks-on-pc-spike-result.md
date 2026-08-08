---
artifact_contract: ce-spike-result/v1
type: result
created: 2026-08-08
plan: docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md
origin: docs/plans/2026-08-07-002-investigate-kafka-streams-on-pc-report.md
tracking_issue: astubbs#255
branch: feats/ks-on-pc-spike
title: "Result: can PC's work-shard manager drive a Kafka Streams processor chain?"
---

# Result: can PC's work-shard manager drive a Kafka Streams processor chain?

**Spike:** [`2026-08-08-001-feat-ks-on-pc-spike-plan.md`](2026-08-08-001-feat-ks-on-pc-spike-plan.md) ·
**Origin analysis:** [`2026-08-07-002-investigate-kafka-streams-on-pc-report.md`](2026-08-07-002-investigate-kafka-streams-on-pc-report.md) ·
**Issue:** astubbs#255 · **Branch:** `feats/ks-on-pc-spike`

**Status update, 2026-08-08 (user-directed reversal - plan KTD-S5):** this was written as the deliverable
of a throwaway experiment that would not merge. It now merges: `parallel-consumer-streams-spike` **ships
as a published alpha/experimental artifact** alongside release 0.6.0.0, with its seam off by default.
Maturity is per-module, not global. Nothing in the technical result below changes - the caveats in §7,
§8 and §9 are exactly the reasons it ships as *alpha*. The module's own front door is
[`parallel-consumer-streams-spike/README.md`](../../parallel-consumer-streams-spike/README.md).

Written for someone who was not there. It assumes no knowledge of the branch and none of the
conversation.

---

## 1. The verdict

**Yes, it runs - and the change set is small enough to quote in full.**

Parallel Consumer's `WorkManager` selects records and a worker pool executes the Kafka Streams processor
chain, in place of the serial `PartitionGroup.nextRecord()` -> `StreamTask.process()` loop. Under four
worker threads, with four records demonstrably inside the chain at once, output is **identical** to a
provably-external stock Kafka Streams baseline - for a stateless topology and for a **non-windowed
aggregation over a state store**. Apache Kafka's own `StreamTaskTest`, `ProcessorContextImplTest` and
`RecordCollectorTest` - 188 tests, nothing skipped - pass against the patched classes.

The cost is **530 lines of unified diff, 25 hunks, 4 classes, no new Parallel Consumer API**.

Three qualifications, none of which are hedges:

1. **This is a feasibility result, not a product.** Offsets are committed optimistically, retries are
   off, stream-time punctuation does not fire on the PC path, and the consumer is never paused. Those
   are enumerated in §8 and quantified in §9.
2. **It is pinned to Kafka 3.9.2**, against classes that carry no compatibility guarantee whatsoever.
   §7 prices that.
3. **Caching must be off**, which changes DSL emission semantics for every user of the parallel path.
   Also §7.

The origin report's central prediction - that thread-confining one field pair in
`AbstractProcessorContext` is the single load-bearing change - **held, and is now proven rather than
merely undisturbed.** See §5, which is the most important section in this document.

---

## 2. What "it runs" actually means

The seam:

```
consumer poll
  -> StreamTask.addRecords          --[switched]-->  PcTaskDispatcher.registerRecords
                                                       -> WorkManager.registerWork(EpochAndRecordsMap)
  -> StreamTask.process(wallClock)  --[switched]-->  PcTaskDispatcher.dispatchAvailable
                                                       -> WorkManager.getWorkIfAvailable(n)
                                                       -> worker pool x4
                                                            -> StreamTask.doProcess(record, recordInfo)
                                                            -> the unmodified ProcessorNode chain
                                                       -> WorkManager.handleFutureResult
```

`registerRecords` and `dispatchAvailable` are called only from the StreamThread; workers report
outcomes through a queue that the StreamThread drains, mirroring PC's own controller/mailbox
discipline. `WorkManager` is not thread-safe and is never touched from a worker.

The switch (`PcDispatchSwitch`) defaults **off**, so the stock path remains the one that runs unless a
test turns it on. That default is what makes "with the flag off, zero records reached the pool" a real
assertion rather than a tautology.

---

## 3. What was actually run, and how often it held

All integration tests run against a real broker in Testcontainers, one Kafka `StreamThread`, a worker
pool of 4, and a deliberate 150 ms cost per record so that records genuinely overlap inside the chain.

### 3.1 The stateful arm (this unit, U7)

Topology: `stream -> [probe] -> groupByKey -> count(Materialized.as(...).withCachingDisabled())
-> toStream -> to`, replaying the 30 input records (6 keys x 5) that stock Kafka Streams was fed by
`StockStatefulBaselineFixtureTest` in `parallel-consumer-example-streams`.

| Arm | Runs | Result |
|---|---|---|
| PC dispatch **ON** | 9 repetitions across 3 independent JVM+broker executions | 9/9 green |
| PC dispatch **OFF** (control) | 3 | 3/3 green |

Every PC-ON repetition reported, identically:

```
offered=30 accepted=30 dispatchedToPool=30 succeeded=30 failed=0
peakChainConcurrency=4  probeObservations=30  violations=[]
changelog: probeStore records=30 timestampsBelongingToAnotherRecord=0  countStore records=30
```

What each assertion buys, in order of what it is worth:

- **Per-key aggregate values are correct - no lost updates.** Every key's count climbs `1..5` with no
  value missed and none repeated. The expectation is derived from the fixture's *inputs*, so it is a
  statement about arithmetic, not a second comparison against an output that could be wrong in the same
  way. This is the assertion the stateful arm exists to make.
- **Output equality against the stock fixture**: multiset across the whole run (global order is what
  parallel dispatch is allowed to change), sequence equality *within each key* (which is where order is
  actually claimed, and which survives only because PC's KEY ordering keeps one record per key in
  flight).
- **Changelog records carry the timestamp of the record that produced them.** See §5 - this is the
  confinement property, observed through Kafka's own store stack rather than through a probe this spike
  wrote.
- **The ambient-context probe**: for every record, under concurrent dispatch, `context.topic()`,
  `partition()`, `offset()`, `timestamp()` and `headers()` each match an anchor carried in the record's
  *value* (a method argument), read twice - once on entry and once after a delay long enough for every
  sibling to have been dispatched.

### 3.2 Everything else on the branch, at the point this was written

| Test | What it establishes | Result |
|---|---|---|
| `ShadowedStreamsControlTest` | U3's control arm: the generate-and-patch harness is behaviour-neutral | green |
| `PcDrivenStreamsDispatchTest` | dispatch marker, per-key exclusion, poison-record handling | 3/3 green |
| `PcDrivenStreamsProofTest` | U6 stateless output equality vs the external stock fixture | 3 PC-ON repeats + control, green (24 further PC-ON repetitions recorded across 8 runs during U6) |
| `PcDrivenStatefulProofTest` | this unit | see §3.1 |
| `ShadowedClassLoadingTest`, `ProcessorContextConfinementTest`, `PcTaskDispatcherTest` | unit-level: the patched classes are the ones loaded; confinement mechanics | green |
| Apache Kafka's `StreamTaskTest` + `ProcessorContextImplTest` + `RecordCollectorTest` (the module's normal test run - no profile, no flag) | Kafka's own behaviour oracle, run against the patched classes | 188/188, **0 skipped** |

**Reproduction rate, stated plainly:** the stateful arm reproduced 9/9 under PC dispatch, across three
separate builds and broker containers, on one machine (macOS, arm64, Docker). Nine is not a
soak test. It is enough to say the result is not a scheduling coincidence, and not enough to say
anything about rare interleavings.

### 3.3 Why the baseline had to come from another Maven module

The spike module compiles patched copies of four `processor.internals` classes into its own build
output, which precedes the `kafka-streams` jar on the classpath. **Every `KafkaStreams` instance in that
JVM runs the patch**, including one a test called "the stock arm" - so both arms would share every
defect the patch introduced and the comparison would prove nothing.

The baselines are therefore generated by `parallel-consumer-example-streams`, which does not depend on
the spike module (and, being earlier in the reactor, the spike cannot depend on it either). Externality
is *asserted from both sides*, not argued: the examples side proves the four patched names resolve into
`kafka-streams-*.jar` and that `PcDispatchSwitch` throws `ClassNotFoundException`; the spike side proves
the opposite. The fixture carries the **inputs** as well as the outputs and the spike replays them
verbatim, so the two arms cannot drift in what they were fed.

---

## 4. The change-set size and shape (R8)

`parallel-consumer-streams-spike/src/main/patch/pcspike.patch`:

```
530 lines · 25 hunks · 4 classes
```

| Class | Why it is in the set |
|---|---|
| `AbstractProcessorContext` | thread-confine `recordContext` and `currentNode` |
| `ProcessorContextImpl` | reads/writes those two fields *directly*; the `getfield`/`putfield` pair in `forward()` **is** the save/restore stack, so confining the field without converting these sites is silently a no-op |
| `RecordCollectorImpl` | `offsets` and `producedSensorByTopic` are written from every worker thread through the `to()` sink. **The compiler never demands this class** - it is constructed outside `StreamTask` - so it had to be named up front rather than discovered |
| `StreamTask` | the seam itself, plus per-record `recordInfo`, concurrent `consumedOffsets`/`partitionsToResume`, `volatile commitNeeded`, `LongAdder processTimeMs` |

A second, separate 167-line patch (`pcspike-testfixtures.patch`, 5 hunks, 1 class) covers the test side:
Kafka's own `InternalMockProcessorContext` fixture does `getfield` on the field this spike made private,
and needs the same accessor conversion in order to run at all. It is generated at
`generate-test-sources` in every build, alongside the main patch.

**The class set did not have to grow.** The plan set a stop-threshold of "roughly a dozen classes" and
said sprawl past it would itself be the answer. Four was enough, and a bytecode scan of the whole
`kafka-streams` jar confirmed the blast radius of removing the two `protected` fields is exactly the two
classes already patched.

**No new Parallel Consumer API was required.** Everything the bridge uses -
`PCModule`, `WorkManager.registerWork`/`getWorkIfAvailable`/`handleFutureResult`,
`EpochAndRecordsMap`, `WorkContainer`, `ParallelConsumerOptions` - is already public. The
fork-original code (`PcTaskDispatcher`, `PcDispatchSwitch`, `PcDispatchCounters`) is ordinary tracked
source in this repository; only Kafka's side is expressed as a patch.

**No Apache Kafka source is committed.** `git ls-files | grep 'org/apache/kafka/.*\.java'` returns
nothing. The four classes are unpacked from the published sources jar at `generate-sources`, patched,
and compiled into `target/`. That is also why `wc -l` is a meaningful answer to "how little had to
change": the patch *is* the change set, and it fails loudly the moment `kafka.version` moves.

---

## 5. R9 - is the thread-confinement load-bearing? **Proven.**

This is the section the unit exists for, so it is worth being precise about what was and was not shown.

### 5.1 Why every earlier green result was ambiguous

The origin report §4.4 names one load-bearing change: `AbstractProcessorContext.recordContext` and
`currentNode` are a single mutable slot **per task**, read *ambiently* by the store stack. Every
ambient reader Kafka has is a store class - `ChangeLoggingKeyValueBytesStore`, `CachingKeyValueStore`,
`StoreQueryUtils`, `MeteredKeyValueStore`. A `stream -> mapValues -> to` topology instantiates **none**
of them.

So U6's green result was compatible with two different worlds: "the confinement works", and "the
confinement was never needed here". A green test that cannot go red proves nothing.

### 5.2 A trap worth recording: the obvious assertion is the vacuous one

The natural way to observe the confinement through a store is: run a DSL `count`, read its changelog
topic, and check each changelog record's timestamp.

**That assertion would have proved nothing.** A materialised KTable store is a *timestamped* store, and
`ChangeLoggingTimestampedKeyValueBytesStore` takes the changelog record's timestamp from the timestamp
embedded in the value - which `KStreamAggregate` computed from the `Record` **argument**, not from the
ambient slot. It would have been green with or without the confinement.

Only the plain, non-timestamped `ChangeLoggingKeyValueBytesStore.put` does
`log(key, value, context.timestamp())`, and that `context.timestamp()` is the ambient read. The stateful
arm therefore registers a **second, non-timestamped KV store**, written by the probe node *after* the
processing delay - the interesting failure being a slot that was correct on entry and belongs to a
sibling by the time it is used.

This mirrors a finding from U6: writing the probe against the modern `api.Processor` would also have
been vacuous, because timestamp and headers arrive on the `Record` argument there. The deprecated
`ProcessorContext` is the only surviving ambient reader available to a topology author.

### 5.3 The controlled experiment

Two control arms were run. Each keeps the **entire** patch and changes exactly one term.

**Control A - both slots un-confined** (`ThreadLocal` -> plain field, for `recordContext` *and*
`currentNode`; the `ProcessorContextImpl` accessor conversion and every other hunk left in place):

- PC dispatch **ON**: 3/3 repetitions **RED**. The run does not merely produce wrong output - it dies.
  `ClassCastException while producing data to topic ...-out. The value serializer LongSerializer is not
  compatible to the actual value type: java.lang.String`, raised inside `RecordCollectorImpl.send`,
  reached via `ProcessorContextImpl.forwardInternal`.
  One worker's `forward()` read a `currentNode` that another worker had overwritten, and routed a
  pre-aggregation `String` straight into the post-aggregation `Long` sink. **Topology-graph corruption.**
- PC dispatch **OFF**, same build: green.

Note what made this visible: the stateless topology could not have surfaced it. Every node there
carries `String`, so a leaked `currentNode` would forward to a node that happened to accept the same
types and the corruption would have been silent.

**Control B - only `recordContext` un-confined**, `currentNode` left thread-confined. This is the
same-magnitude-different-position control: it isolates the field the origin report names.

| Arm | Ambient-read violations | Changelog records carrying another record's timestamp | Aggregate counts | Verdict |
|---|---|---|---|---|
| PC **ON**, rep 1-3 | 80+ per run | **25 of 30** | still correct | **RED**, 3/3 |
| PC **OFF**, same build | 0 | **0 of 30** | correct | green |

Read that table again. The **same build**, the **same un-confined field**, differing only in whether
records were dispatched to the worker pool: 25/30 changelog records mis-stamped versus 0/30. That is
the control arm the prediction demanded, and the outcome flipped on the one term that was changed.

Three details worth carrying forward:

- **The counts stayed correct.** Under Control B the aggregate arithmetic was never wrong; only the
  metadata was. That is exactly the failure mode the origin report predicted - *"silent corruption, not
  a crash: changelog records stamped with another record's timestamp"* - and it is why a spike that
  asserted only on aggregate values would have passed while writing a corrupt changelog.
- **It is observable from outside the JVM.** Those mis-stamped timestamps are on a real Kafka changelog
  topic. On restore they would rebuild state with the wrong timestamps, and any downstream consumer of
  the changelog would see them.
- **The instrumentation was verified rather than assumed.** Both control arms were built with `clean`
  and the compiled class checked with `javap` before the result was believed - see §10 for why that is
  not paranoia.

### 5.4 The claim, stated exactly

R9 is **PROVEN**, not merely not-disproven:

- There is a code path in the spike where Kafka's own store stack reads the confined slot ambiently.
- Removing the confinement - and nothing else - makes that path go red, deterministically, 3/3.
- Removing the confinement while *also* removing the parallelism makes it go green again, so the red is
  caused by concurrent dispatch and not by the edit.
- Removing the confinement of `currentNode` as well escalates from silent corruption to a hard failure.

What is **not** claimed: that the confinement is sufficient for correctness in general. It is proven
necessary for this topology; §8's caveats are all still open.

### 5.5 Re-running the control arms

Neither control arm is committed - the tracked patch is the green one. To reproduce either:

```bash
# 1. put the tracked patch into the generated tree
./mvnw -pl parallel-consumer-streams-spike -am process-sources

# 2. edit target/kafka-patched/org/apache/kafka/streams/processor/internals/
#      AbstractProcessorContext.java
#    Control A: turn BOTH ThreadLocal slots back into plain fields.
#    Control B: turn ONLY recordContext back into a plain field.
#    In each case also rewrite that slot's .get()/.set(x) call sites to field reads/writes.
#    RUN NO MAVEN BETWEEN THIS STEP AND THE NEXT - unpack silently reverts your edits.

# 3. re-derive the patch (it will warn that the hunk count dropped; here that is expected)
parallel-consumer-streams-spike/bin/regen-patch.sh

# 4. `clean` is NOT optional - see §10.2
./mvnw -pl parallel-consumer-streams-spike -am clean verify -DskipUTs=true \
  -Dit.test=PcDrivenStatefulProofTest -Dfailsafe.failIfNoSpecifiedTests=false -Dcopyright.skip=true

# 5. confirm the instrumentation actually reached the run
javap -p -classpath parallel-consumer-streams-spike/target/classes \
  org.apache.kafka.streams.processor.internals.AbstractProcessorContext | grep -E 'recordContext|currentNode'

# 6. put it back
git checkout -- parallel-consumer-streams-spike/src/main/patch/pcspike.patch
```

The PC-OFF arm of the same test is the control-of-the-control: it runs the identical un-confined build
serially and must stay green. If it does not, the edit broke something other than the confinement and
the red arm proves nothing.

---

## 6. Which of the origin report's §4 claims held

| Claim | Verdict | Evidence |
|---|---|---|
| §4.1 The `WorkManager` seam is unusually well-matched; `registerWork` / `getWorkIfAvailable` / completion / offsets line up with `PartitionGroup` | **Held** | The whole bridge is 589 lines of ordinary fork-original Java (`PcTaskDispatcher` 406, `PcDispatchCounters` 102, `PcDispatchSwitch` 81), comments included, and needed no new PC API |
| §4.2 PC's KEY ordering already gives one in-flight record per key | **Held** | Per-key sequence equality holds end to end under 4 workers; `keysConcurrentWithThemselves=[]` |
| §4.3 `SynchronizedPartitionGroup` is a free win via `__processing.threads.enabled__` | **Not needed** | The PC path bypasses `partitionGroup` entirely (single path, switched), so the flag is irrelevant to it. It would matter to a design that kept both |
| §4.4 One field pair is load-bearing; failure mode is silent corruption, not a crash | **Held, and now proven** | §5. The predicted symptom - changelog records stamped with another record's timestamp - is exactly what Control B produced, 25/30 |
| §4.4 (implicit) Thread-locals are a sufficient fix | **Held, with a correction** | Not sufficient *alone*: `ProcessorContextImpl` reads the fields directly, and confining them without converting those sites compiles cleanly, passes every single-threaded test, and is a silent no-op. A `javap` assertion is the only cheap way to know it stuck |
| §4.5 Delete the caching layer rather than making it concurrent | **Adopted, not tested** | Caching is disabled; `CachingKeyValueStore` is never instantiated, so this spike says nothing about it either way |
| §4.6 PC's `WorkManager` becomes the offset source of truth | **Not done** | Deliberately deferred. Offsets stay on the stock Streams path and are committed optimistically - §8 |
| §4.7 ~50 lines of mechanical concurrency hygiene | **Held in shape, understated in count** | The mechanical items are indeed mechanical, but the total patch is 530 lines because the seam itself (`pcProcess`/`pcPrepare`/`pcRunChain`, drain-on-suspend, close) is larger than the hygiene |
| §4.7 Concurrent collections fix the mutable maps | **Partially** | They fix *corruption*, not *atomicity*: `commitNeeded` and `partitionsToResume` still have read-modify-write races - §8 |
| §4.8 Punctuators, joins and windowed operators must stay serial | **Consistent, untested** | Out of scope by design; stream-time punctuation demonstrably does not fire on the PC path - §8 |
| §4.9 A build-time parallel-safe reachability pass in `InternalTopologyBuilder` | **Not attempted** | Explicitly out of scope. The spike selects the path with a process-wide flag instead, which a product could not |
| §4.11 "Tier 1 + Tier 2 is the smallest coherent version" | **Consistent** | What was built is Tier 1 minus `StreamsProducer`/`RocksDBStore`, plus the Tier 2 confinement. It runs |
| §6.2 "The diff is bounded" | **Held** | 4 classes, 530 lines, no growth in the class set |
| §6.2 "Route B inherits state stores for free" - the reason it outranks a PC-native DSL | **Held** | The stateful arm ran a real `count` over a real RocksDB-backed store with a real changelog, and matched stock. Nothing in the store subsystem had to be written |

Nothing in §4 was **refuted**. The two corrections above (thread-locals need the accessor conversion;
concurrent collections are not atomicity) are refinements, not reversals.

---

## 7. What a green result commits to

A green spike is not permission to ship. Priced honestly, adopting this route commits you to:

### 7.1 Re-deriving the patch on every Kafka version bump, against classes with no compatibility guarantee

`org.apache.kafka.streams.processor.internals` is package-private, unsupported, and explicitly not an
API. The four classes patched here are free to change shape in any patch release. On Kafka **trunk/4.x**
they already have: `ProcessorContextImpl` is `final` and the record context is mutated in place, so a
green result on 3.9 does **not** transfer unexamined.

The generate-and-patch harness makes this fail *loudly* rather than silently - a patch that no longer
applies breaks the build immediately, where a vendored copy would drift into a runtime
`NoSuchMethodError`. That is a real improvement, but it converts a silent hazard into a recurring
maintenance obligation, not into no obligation. Every Kafka bump becomes a manual re-derivation with a
188-test regression run behind it.

### 7.2 A DSL emission-semantics change, forced on every user of the parallel path

Disabling the record cache is not an implementation detail. With caching **on**, a KTable emits at
flush and downstream sees one record per key per commit interval; with it **off**, every update is
forwarded. The stateful arm's own numbers show it: 30 inputs produce 30 outputs, with counts appearing
as `1,2,3,4,5` rather than a single `5`.

Any topology moved onto the parallel path changes what it emits, how much it emits, and therefore what
its downstream consumers and its output topic retention see. That is a user-visible behaviour change
that no amount of internal correctness makes invisible.

### 7.3 A distribution shape that does not exist yet

**Build-time patching is a spike technique, not a product one.** This spike works because
`target/classes` precedes the `kafka-streams` jar on one module's classpath. A shipped version cannot
rely on classpath ordering in someone else's application. The realistic options are all expensive:

- **Publish a forked `kafka-streams` artifact** under a different coordinate. Legally fine (Apache 2.0,
  with attribution preserved), operationally heavy: a full Kafka Streams release to build, test, sign
  and publish on every upstream release, plus a `dependencyManagement` exclusion users must apply or
  silently get the stock classes.
- **Ship an agent or a classloader trick.** Fragile, hostile to debugging, and a support burden.
- **Upstream the change.** The right answer and the slowest one; it is a KIP, and the confinement has a
  cost for the single-threaded case that upstream would have to accept.

None of these is chosen here. The point of stating them is that "the spike is green" and "we can ship
this" are separated by an unfunded distribution problem, and the spike does not shrink it.

### 7.4 The gap between the PC path and stock Streams, which is measurable

See §9. It is 33 known behavioural divergences, and they are not a surprise - they are the items this
spike deliberately deferred, showing up where you would expect them.

---

## 8. Known caveats, all deliberate

Everything here is a decision recorded rather than a defect discovered.

| Caveat | Detail |
|---|---|
| **No stream-time punctuation on the PC path** | Stream time advances at partition-group *selection*, and the PC path never selects from the partition group. Wall-clock punctuation is unaffected. Irrelevant to the stateless and non-windowed topologies run here; disqualifying for anything windowed |
| **No consumer pausing** | Stock `addRecords` pauses a partition once its buffer exceeds `maxBufferedSize`. The PC path hands everything to `WorkManager`, so PC's own backpressure is the only inflow limit |
| **Failures surface a pump cycle late** | A worker's exception is stored and re-thrown on the StreamThread at the next `process()` call. Stock Streams also surfaces it via the StreamThread, but synchronously; here, records dispatched in between will have run |
| **Optimistic commit** | Offset commit stays on the stock Streams path. `consumedOffsets` is written by workers as they finish, in completion order, so Streams may commit an offset while a lower one from the same partition is still in flight. **The spike is not crash-safe.** §4.6 of the origin report is the fix and was deferred |
| **Retries disabled** | PC's response to a failure is re-dispatch, which would re-run the whole chain including `forward()` calls that already emitted downstream - duplicates stock Streams never produces. Expressed as a retry delay longer than any run. Under KEY ordering a failed record blocks its own key's shard and nothing else |
| **`StreamTask.record` has the same reuse defect as `recordInfo`** | `recordInfo` was made per-record; the `record` field was not, because the PC path passes the record as a parameter and never reads the field. It is left standing, and it is a latent trap for anyone extending the PC path |
| **Two read-modify-write races survive** | `commitNeeded` and `partitionsToResume` were made `volatile`/concurrent, which fixes *corruption*, not *atomicity*. Benign for this spike; not benign for a product |
| **Caching-disabled only** | `CachingKeyValueStore` and `ThreadCache` are never instantiated. The origin report's §4.5 says do not attempt to make them concurrent; this spike neither confirms nor challenges that |
| **Kafka 3.9.2 only** | See §7.1 |
| **At-least-once only** | EOS is out of scope (KTD7), which is what keeps `StreamsProducer` out of the patch entirely |
| **One `StreamThread`, one partition, one task** | Every test runs this shape. Multi-task and rebalance behaviour under PC dispatch is untested |
| **A process-wide static switch** | `PcDispatchSwitch` is global mutable state because `StreamTask` is constructed several layers inside `KafkaStreams` with no seam to inject through. A spike may pay that; a product may not. It is also why every proof test is `@Isolated` |

---

## 9. The 33 failures under PC-ON in Kafka's own `StreamTaskTest` - read this as a worklist

With PC dispatch **off**, Apache Kafka's own tests pass against the patched classes: **188/188, 0
skipped** (`StreamTaskTest` 101, `RecordCollectorTest` 59, `ProcessorContextImplTest` 28). Nothing was
excluded and no assertion was relaxed.

> **Substantiated claim, available for release notes and other promotional use:**
> *"188 of Apache Kafka's own Streams tests pass unmodified against the patched classes, zero skipped."*
>
> **Provenance.** The tests are Apache Kafka's own, taken as **compiled classes** from the `kafka-streams`
> `test` jar published to Maven Central - not re-written, not re-compiled, not excluded, no assertion
> relaxed. They run against the patched `StreamTask`, `AbstractProcessorContext`, `ProcessorContextImpl`
> and `RecordCollectorImpl`, which precede the `kafka-streams` jar on the classpath (proven independently
> by `ShadowedClassLoadingTest`, and cross-checked by the fact that turning the dispatch flag on changes
> the result - the released classes have no such flag). The condition is dispatch **off**: this is a
> *behaviour-preservation* claim about the patch, not a claim about the parallel path.
>
> **Reproduce it** - it runs in the module's normal test run, no profile and no flag:
> `./mvnw -pl parallel-consumer-streams-spike -am test`. Kafka's execution reports separately under
> `parallel-consumer-streams-spike/target/surefire-reports-kafka-upstream/`.
>
> **Do not quote it without the counterpart.** The parallel path's number is the 68/101 below, and the
> honest form of the claim carries both. The count lives in three places - the surefire execution's
> comment in `parallel-consumer-streams-spike/pom.xml`, that module's README, and here. If it changes,
> change all three.

Re-running `StreamTaskTest` with PC dispatch **on** gives **68/101**. That 33-test delta is not a
defect report. It is the best thing this spike produced after the verdict itself: **a quantified,
executable specification of the gap between the PC path and stock Streams**, written by Kafka's own
authors, for free.

| Cluster | Tests | What it corresponds to |
|---|---|---|
| Offset / commit accounting | 11 | §8's optimistic commit, and origin report §4.6 (PC owns committable offsets) |
| Buffering, pause/resume | 5 | §8's "no consumer pausing" - `maxBufferedSize` and `partitionsToResume` are meaningless when nothing fills the partition group |
| Stream-time punctuation | 2 | §8 - stream time advances at selection, which the PC path skips |
| EOS commit gates | 3 | KTD7 - EOS deliberately out of scope, `StreamsProducer` deliberately unpatched |
| Close / suspend | 5 | The drain-before-suspend path added by this spike |
| Error wrapping | 3 | §8's "failures surface a pump cycle late" - the exception type and timing differ |
| Ordering | 1 | Global ordering across a partition, which parallel dispatch necessarily changes |

It doubles as a positive control: the released `StreamTask` has no dispatch flag, so a run whose
behaviour changes when the flag is set is *provably* executing the patched class. Without that, 188/188
could equally have meant the shadowing never reached Kafka's tests at all.

**The next experiment, if there is one, is to work that table top-down.** Offset/commit accounting is
both the largest cluster and the one blocking crash-safety, and origin report §4.6 already describes
the fix.

---

## 10. Two findings worth having regardless of this spike

Both were discovered while building the spike and are independent of whether the route is ever taken.

### 10.1 `junit-platform.properties` leaks out of the core tests jar

`parallel-consumer-core`'s **tests** jar ships a `junit-platform.properties` at its root with
`parallel.enabled=true` and a dynamic factor of 20. Every module that depends on that jar - which is
every module with integration tests - silently inherits 20x JUnit parallelism it never asked for.

That is how Apache Kafka's serial-by-design `StreamTaskTest` ended up failing 159 tests on state-directory
locks before anyone had looked at the patch. Surefire's `configurationParameters` outrank the file and
were used to pin it off for the Kafka execution, but the leak itself is the bug and is still open -
tracked at
[`docs/inflight/bug-core-tests-jar-junit-parallelism-leak.md`](../inflight/bug-core-tests-jar-junit-parallelism-leak.md).

### 10.2 `dependency:unpack` restores original file timestamps, which can fabricate a "confirmed regression"

`maven-dependency-plugin:unpack` preserves the archive's file timestamps. When the patch is removed and
the sources are re-unpacked, the files go **backwards in time** relative to the already-compiled
classes, so `maven-compiler-plugin` decides nothing needs recompiling and **silently keeps the old
class files**.

A control arm run without `clean` therefore tests the *previous* build's classes against what the
developer believes is pristine source. It will happily "confirm" a regression that does not exist, or
miss one that does. Both control arms in §5 were run with `clean` and verified with `javap` before
their results were believed, for exactly this reason.

The general lesson: **verify that instrumentation reached the run.** A configuration change that the
build did not pick up produces a silent false negative that is indistinguishable from a real "no
effect".

---

## 11. If the result had been red - and where a future red result points

The plan required this section regardless of outcome, because a spike that only knows what to do when
it wins is not a spike.

The route taken (KTD0 - cut the seam inside `processor/internals`) was an agent's selection from the
origin report's own taxonomy, not a user decision. A red or ambiguous result would have sent the
question back to:

- **A red at U3 or U5** - the harness is unsound, or records cannot be made to travel the new path ->
  back to **route D, the shipped topic-hop** (origin report §6.4), which already works. The only reason
  to move off it is to remove the hop's latency and operational cost, and a blocked seam means that
  price cannot be paid.
- **A red at U6/U7 on output correctness** -> back to **route C, a PC-native DSL** (§6.3). The report
  ranks route B above route C *solely* because route B inherits state stores for free. A stateful arm
  that could not produce a correct aggregate would delete that entire advantage, and route C becomes the
  better answer for the stateless case it is genuinely cheap at.
- **Sprawl in the patched class set** (the plan's stop-threshold was ~a dozen classes) -> also route C.
  "Bounded diff" is the whole argument for route B; an unbounded one is a maintained fork of Kafka
  Streams, which nobody asked for.
- **Route A - swapping the client via `KafkaClientSupplier`** remains dead in every branch. Origin
  report §3.2: Streams serialises *above* the consumer, so the swap gains nothing. Nothing in this spike
  touched that argument, and the seam being viable one layer lower does not revive it.

**Where a *future* red points.** The live risks are §7.1 (a Kafka bump the patch cannot be re-derived
against) and §7.3 (no distribution shape). Either one turning red does not send the question back to
route C - the technical result stands - it sends it back to **route D**, because a correct fork nobody
can consume is worth less than a topic hop everybody can.

---

## 12. Where the code is, and what happens to it

- **It ships, as alpha** (plan KTD-S5, user-directed - this reverses the original "not for merge, never
  publishes" posture, which is what §1 through §11 above were written under).
  `parallel-consumer-streams-spike` publishes like any other module, alongside release 0.6.0.0, with the
  dispatch seam **off by default** so the artifact is inert unless a user opts in. Its known limitations
  - §7, §8 and §9 - are exactly why it is labelled alpha rather than supported, and they are restated for
  users in [`parallel-consumer-streams-spike/README.md`](../../parallel-consumer-streams-spike/README.md),
  which also asks for field testers.
- `parallel-consumer-streams-spike/src/main/patch/pcspike.patch` is the artefact worth keeping. It is
  the answer to R8 and the starting point for anyone re-deriving against a newer Kafka.
- To work on it: `./mvnw -pl parallel-consumer-streams-spike generate-sources`, edit
  `target/kafka-patched/`, then `bin/regen-patch.sh`. **Any Maven run between those two steps silently
  reverts your edits** - `unpack` runs with `overWriteReleases`. `regen-patch.sh` warns when the hunk
  count drops, which is the tripwire.
- Open follow-ups: the user-facing ones are §8 and the §9 worklist, restated for users in the module
  README. The one defect this spike found in *other* code is tracked at
  [`docs/inflight/bug-core-tests-jar-junit-parallelism-leak.md`](../inflight/bug-core-tests-jar-junit-parallelism-leak.md).
