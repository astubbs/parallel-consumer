# `parallel-consumer-streams` - a Kafka Streams topology on Parallel Consumer's worker pool

> ## ALPHA. EXPERIMENTAL. NOT PUBLISHED. SEAM OFF BY DEFAULT.
> This module is in the reactor so the machinery below can be built and reviewed. It is **not**
> published to Maven Central, deliberately - see
> [The classpath hazard this module does not solve](#the-classpath-hazard-this-module-does-not-solve).
> Tracking issue: [astubbs#255](https://github.com/astubbs/parallel-consumer/issues/255).

Apache Kafka Streams parallelises across *partitions*. Inside one partition,
`PartitionGroup.nextRecord()` hands records over strictly one at a time, so one slow record delays
everything queued behind it whatever key it carries. That serialisation has no semantic justification
when the records are on different keys, and removing it is what this module is for: `StreamTask`'s
record selection is replaced by Parallel Consumer's `WorkManager` and a worker pool, so records on
distinct keys of one partition run concurrently through the *unmodified* processor chain. A topology's
own code does not change.

`processor.internals` is package-private, explicitly not an API, and offers no seam where this needs
one - so the module also carries a repeatable way to change those internals that keeps no Apache
source in this repository, states the size of the change as a reviewable number, and fails at build
time rather than at runtime when the change stops applying.

## Turning it on, and why it is off

```
-Dpc.streams.dispatch.enabled=true
```

**The seam is OFF by default. This is an opt-in preview** - and the reason has now changed twice.
Each time, the measurement that closed one reason named the next, which is worth reading rather than
skipping.

**Reason one, closed: a missing refusal.** Joins, windows, suppression and exactly-once were
unsupported *and not refused*, so a topology using one was dispatched anyway and got wrong behaviour
with nothing in the log to say so. See
[What is refused, and how you find out](#what-is-refused-and-how-you-find-out).

**Reason two, closed: revival.** Run Apache Kafka's own suite against the patched classes with the
seam *on* and `StreamThreadTest` reached `StreamTask.revive()` through Kafka's ordinary
task-corruption recovery - a `TaskCorruptedException` closes the task dirty and revives the same
instance, whose PC dispatcher had been closed on the way down. The loud-failure
`IllegalStateException` that stopped that becoming a silent stall was caught by nothing and left the
run loop uncaught on the StreamThread. A revived task now **rebuilds its dispatcher** over the
partitions it holds at that moment, and the seam-on measurement, taken before and after with nothing
else changed, shows those three cases green, no exception leaving any StreamThread, and no case that
passed before regressing - plus five `StreamTaskTest` close and checkpoint cases going green with
them, because the same work taught `validateClean` to see records that are still running.

**Reason three, closed: a typed control-flow exception raised inside a processor.** A
`TaskCorruptedException` or `TaskMigratedException` is how a topology tells Kafka's `TaskManager` to
recover a task rather than fail. On the PC path a worker's exception was wrapped in a
`StreamsException` *and* delivered one or more pump cycles later, so the type never reached the
machinery that dispatches on it and an application stock Streams would have recovered shut down
instead. Both halves are fixed - see
[Error surfacing: the type, the timing, and the commit fence](#error-surfacing-the-type-the-timing-and-the-commit-fence) -
and the seam-on measurement, taken before and after with nothing else changed, shows
`StreamThreadTest.shouldReinitializeRevivedTasksInAnyState` green on both parameter combinations this
module supports, with no case that passed before regressing.

**So why is it still off?** Because the flip is a decision about the *reconciled* module, not about
any one piece of work in flight against it, and the seam-on numbers move with every rung. There is
also one still-unrefused item left on this path - stream-time punctuation - which belongs to a
different piece of work and is listed under
[What is still unsupported and NOT refused](#what-is-still-unsupported-and-not-refused). What holds
the default now is therefore a *reconciliation step* rather than a named defect, and that is written
down in
[`docs/inflight/streams-dispatch-default-flip-is-reserved-until-the-rungs-reconcile.md`](../docs/inflight/streams-dispatch-default-flip-is-reserved-until-the-rungs-reconcile.md).

Whoever moves the default should re-run the seam-on measurement rather than trusting this section -
three times now it has named the next reason, so treat "no reason left" as something to show rather
than assume.

Three further switches, all process-wide for the reason given in
[`PcDispatchSwitch`](src/main/java/bz/stub/parallelconsumer/streams/PcDispatchSwitch.java) (there is
no seam through `KafkaStreams` to inject a collaborator):

| Property | Default | What it does |
|---|---|---|
| `pc.streams.dispatch.enabled` | `false` | the seam itself |
| `pc.streams.dispatch.poolSize` | `4` | worker threads per task, and PC's `maxConcurrency` |
| `pc.streams.wakeOnWork.enabled` | `true` | the split poll wait; unreachable while the seam is off |
| `pc.streams.backpressure.enabled` | `true` | pausing a partition once PC holds more than `buffered.records.per.partition` for it; unreachable while the seam is off. **Off is the control arm of the memory-bound proof, not a supported mode** - it restores unbounded accumulation under a slow processor |

A value that is neither `true` nor `false` throws rather than being read as the default - a typo in
the property that selects an arm would otherwise produce a run that looks like the arm you asked for
and is not.

**Being process-wide is a real limitation, not a simplification.** Two `KafkaStreams` instances in one
JVM cannot be configured differently. What that would take, and the unfinished implementation of it,
is recorded in
[`docs/inflight/streams-dispatch-switch-is-jvm-wide-not-per-instance.md`](../docs/inflight/streams-dispatch-switch-is-jvm-wide-not-per-instance.md).

---

## The mechanism

Four steps, all with plugins this build already had. The module's [`pom.xml`](pom.xml) carries the
reasoning for each in place; this is the shape.

1. **Unpack** the named classes from the published `kafka-streams` **sources** jar, twice - a
   *pristine* copy that is the regeneration baseline, and a working copy. `maven-dependency-plugin`,
   at `generate-sources`.
2. **Apply** the tracked patch to the working copy, dry-running first so a rejected hunk fails the
   build rather than leaving a half-applied tree that compiles.
   [`bin/apply-patch.sh`](bin/apply-patch.sh), at `process-sources`.
3. **Compile** the working copy into this module's own `target/classes`, by adding it as a source
   root. `build-helper-maven-plugin`.
4. **Let classpath order do the rest.** `target/classes` precedes the `kafka-streams` jar, so the
   patched classes win while every one of their thousand siblings still loads from the jar. Same
   package name and same classloader, so package-private access still works - without a fork.

**No Apache Kafka source is committed to this repository.** Only
[`src/main/patch/pc-streams.patch`](src/main/patch/pc-streams.patch) is tracked; the generated trees
are gitignored. Kafka's own test *fixture* `InternalMockProcessorContext` gets the same treatment
through a second, smaller patch, for the reason given below.

### What the patch changes, and why exactly these classes

| Class | Change | Why it is in the set |
|---|---|---|
| `AbstractProcessorContext` | the current record context and current processor node move from two `protected` fields to thread-confined ones | stock Streams holds one of these per task and gets away with it because exactly one thread is ever inside the task |
| `ProcessorContextImpl` | reads and writes them through the accessors instead of the inherited fields | it is a subclass doing `getfield`/`putfield` on those fields, so leaving it alone defeats the confinement *with no compile error to say so* |
| `RecordCollectorImpl` | two `HashMap`s become `ConcurrentHashMap`s | written from the producer's I/O thread for every in-flight send; the compiler would never have demanded this class, so it is named rather than discovered |
| `StreamTask` | record selection and the commit gates ask the dispatcher instead of the partition group; the processor chain runs on a worker | **the seam itself**, and the only class here that references Parallel Consumer - which is what let the machinery rung land the three above without it |
| `StreamThread` | the poll wait is split: a short poll, then a wait on our own condition that a worker completion ends | it owns the only blocking wait in the run loop, and nothing else can split it - see [wake-on-work](#wake-on-work-why-the-poll-wait-is-split) |
| `KStream`, `KTable`, `KGroupedStream`, `CogroupedKStream` | the refused overloads gain `@DoNotCall`, `@Deprecated` and a javadoc `@deprecated` tag naming this module | a call site resolves to the symbol its receiver's **static type** declares, so the compile-time half of a refusal has to sit on the interface - an annotation on the impl would never be consulted |
| `KStreamImpl`, `KTableImpl`, `KGroupedStreamImpl`, `CogroupedKStreamImpl` | the same operators throw `UnsupportedOperationException` naming the construct, when the seam is on | an interface method has no body to throw from; this is the run-time half, and it is what refuses a topology built reflectively or from a language the annotations do not reach |

That is the whole patch. The set is **named in the pom, not discovered**
(`patched.classes`), and the stop-threshold is stated there: if it has to grow much past a dozen, the
sprawl is itself the answer to "how little had to change", and this is a fork by instalments. It is
now thirteen - past that line rather than at it, which the pom says out loud along with the smaller
alternative that was weighed and its cost.

Kafka's `InternalMockProcessorContext` needs the second patch because it *also* reads those fields
directly. Un-patched, every `RecordCollectorTest` case dies at construction with a
`NoSuchFieldError` - so without the fixture patch the oracle below cannot run at all.

---

## Why we believe the machinery works

The technique's failure mode is **silence**, not error. If the jar's copy of a class wins the
classpath race, every test still passes - it is simply testing unmodified Kafka Streams against
itself, beautifully. So each claim here has a control.

### The generated classes really do win

[`ShadowedClassLoadingTest`](src/test/java/bz/stub/parallelconsumer/streams/ShadowedClassLoadingTest.java)
asserts it directly rather than inferring it from behaviour: each generated class loads from a code
source under `/classes/` and not a `.jar`; `PartitionGroup`, `RecordQueue` and `TaskManager`, which
are deliberately *not* generated, still load from the jar - which is what makes this shadowing rather
than a fork; and every generated class shares both a package name and a **classloader** with them,
because different classloaders split the package and break package-private access while the names
still match.

Those three are chosen for adjacency, not convenience: `PartitionGroup` is the buffer the seam
bypasses, `RecordQueue` is reached into by the seam's own record preparation, and `TaskManager`
constructs `StreamTask`. If generation ever sprawled past the declared set, those are the first three
it would reach.

`StreamTask` and `StreamThread` used to be that list, on the machinery rung, precisely so the
assertion would have to flip *visibly* on the day the generated set grew. It flipped on the execution
seam.

**"Same runtime package" is a per-package property**, and the refusal envelope took the generated set
into two more packages - so the jar-resident list gained `Materialized` and `ConsumedInternal`, one
neighbour for each. Without them the coexistence claim about those packages would have been checked
against nothing while still passing. The test fails loudly on a generated class in a package with no
declared neighbour rather than skipping it, and a further test reads `patched.classes` out of the pom
through surefire and asserts the two lists are the same set - because "keep this in sync" is a comment,
not a check.

### An empty patch is a no-op, and that is checked by the tooling

`apply-patch.sh` treats a patch with no hunks as an explicit, successful no-op - checking emptiness
itself rather than trusting `patch`, whose behaviour on empty input differs between BSD `patch` on
macOS and GNU `patch` on CI. Run the build that way and the generated tree is byte-for-byte the
released sources. Without that baseline, a later behavioural difference could not be attributed
between the technique and the change.

### Apache Kafka's own test suite, run against the patched classes

Kafka publishes its **compiled** tests to Maven Central. This module takes them as a `test`-classifier
dependency and points a dedicated surefire execution at them, so Kafka's own `StreamTaskTest`,
`StreamThreadTest`, `RecordCollectorTest` and `ProcessorContextImplTest` exercise **our** copies of
the patched classes. Nothing is excluded, rewritten, recompiled or relaxed. It runs in the
module's normal `test` phase, on every build, with no profile and no flag.

This is also the reason **no refused signature was deleted**. Those tests are Kafka's own *compiled*
classes, never recompiled here, so a deleted method would link-fail against classes this project does
not own - and the behaviour-preservation evidence would be forfeited by the very change meant to make
the module honest. Refusal is therefore additive: annotations, and a throw guarded on the seam.

That is the behaviour-preservation claim, and it is the strongest one available: **anything the patch
broke that Kafka tested, fails here.**

**The claim is about the patch, and it only means anything with the seam OFF**, so that execution
pins `pc.streams.dispatch.enabled=false` rather than inheriting whatever the default happens to be.
The pin is not redundant with the default being off today - deleting it would make the oracle
silently follow the default the day it moves.

Some of `StreamThreadTest` is skipped, by Kafka's own `assumeTrue`/`assumeFalse` on its
`stateUpdaterEnabled` x `processingThreadsEnabled` parameter matrix. Those assumptions are evaluated
on the first line of each method, from the test's own parameters, before any patched code runs -
so nothing in this patch can influence them. That is a mechanism, not a measurement, which is why it
is stated here without a number.

> **Re-derive the counts; never copy them.** They move with the patch, with `kafka.version`, and with
> the seam. Run the module's whole `test` phase and read the per-class numbers out of
> `target/surefire-reports-kafka-upstream/`.
>
> **Do not scope the run with `-Dtest=`.** It silently overrides that execution's `<includes>`, so
> Kafka's suite does not run at all - and the build still goes green, with the number you were
> checking never computed. It has cost several people a whole run.
>
> **"Zero failures" has one known exception**, and it is Kafka's own flake rather than this patch's:
> `StreamThreadTest.shouldLogAndRecordSkippedRecordsForInvalidTimestamps` asserts on a thread name
> that depends on which thread logged. Diagnosed, with its control arm and what to do about it, in
> [`docs/inflight/test-streamthreadtest-invalid-timestamps-flake.md`](../docs/inflight/test-streamthreadtest-invalid-timestamps-flake.md).
> If that exact case fails, re-run. **Anything else that fails is real.**

### Wake-on-work: why the poll wait is split

`StreamThread` polls the consumer and runs the topology on **one** thread, so blocking in
`Consumer#poll()` for the full `poll.ms` costs stock Kafka Streams nothing - there is by definition no
processing it could be doing instead. Hand records to a worker pool and that inverts: records complete
*during* the poll wait, and neither their completions nor the records they unblock can move until poll
returns. Worse, the inner work loop breaks back out to poll whenever a pass dispatched nothing - which
under an asynchronous dispatcher also means "the pool is full" or "every available key is already in
flight", states that resolve on a worker completion and never on a broker fetch. The loop therefore
chooses to block at precisely the moments a completion is imminent.

So the patched `StreamThread` polls briefly to collect whatever the broker already has, then waits on
[`PcWorkSignal`](src/main/java/bz/stub/parallelconsumer/streams/PcWorkSignal.java) for the rest of its
budget, and a worker completion ends that wait immediately. It is deliberately **not**
`KafkaConsumer#wakeup()`: that word already means *shutdown* to Kafka Streams, and a wake delivered
while the thread is not polling arms the *next* poll instead, so a stray completion could swallow a
shutdown - a failure that shows up once in a thousand shutdowns and never reproduces on demand.

**Wake-on-work was measured as roughly two thirds of the backlog benefit**, by ablating it rather than
by arguing from mechanism - which had predicted the opposite, that a saturated topic would leave the
split wait idle. That measurement, its refuted prediction and its arms are owned by
[`docs/solutions/best-practices/ablate-your-own-change-not-only-the-baseline.md`](../docs/solutions/best-practices/ablate-your-own-change-not-only-the-baseline.md);
**this rung does not re-derive it and no figure is repeated here**, because the benchmark that
produces one is a separate unit and a number copied out of its write-up drifts silently from it. What
this rung ships is the mechanism and the tests that show it is load-bearing.

### Backpressure: what bounds memory on this path

Stock Kafka Streams pauses a partition once its `RecordQueue` holds more than
`buffered.records.per.partition`, and resumes it when processing brings the queue back down. On the PC
path there is no `RecordQueue` to measure: the records went to Parallel Consumer,
`partitionGroup.numBuffered()` answers zero however much PC is holding, the pause never fired, and
**nothing bounded inflow but heap**. A topology with a processor slower than the broker feed simply
accumulated.

The pause is now applied from the same threshold with the occupancy read from PC, and the resume
mirrors it exactly - **only partitions this path paused are ever handed back**, because Kafka pauses
partitions for reasons that have nothing to do with buffering (an offset reset, for one) and a resume
computed from the whole assignment would restart fetches on those. Mirroring is also what makes the
resume provably no more eager than the pause, which is the property the bound rests on.

**"Buffered" means accepted and not yet handed to a worker**, which is what Kafka's own
`RecordQueue.size()` means - a record being processed has left the buffer. The two definitions differ
by the in-flight set, which the pool size bounds, so this is both the faithful analogue and the only
unbounded quantity of the two.

**The count is derived from Parallel Consumer's own incomplete-offset set, not maintained alongside
it.** A count raised by predicting what PC would accept drifts the moment PC applies a rule the
prediction does not model - a re-delivered offset, for instance, which PC's shard drops because it
already holds a live container for it, so the count goes up and never comes back down and the
partition is paused for good. Deriving is immune to it. Why that was not obvious, and the objection it
had to answer, are in
[`docs/solutions/architecture-patterns/predicting-what-a-collaborator-will-accept-drifts-derive-the-count-from-its-own-state.md`](../docs/solutions/architecture-patterns/predicting-what-a-collaborator-will-accept-drifts-derive-the-count-from-its-own-state.md).

**The bound is proven with a control arm rather than asserted.** `BackpressureBoundIntegrationTest`
runs the same topology, data, processing cost, broker and JVM twice, varying only
`pc.streams.backpressure.enabled`, and samples occupancy from a watcher thread throughout - because
the interesting quantity is a *peak*, and a peak is invisible to any assertion made after the run.
Two ways it could pass vacuously are closed: the fixed arm must also **reach** its threshold, or a run
that never filled the buffer would pass with the feature deleted; and every record must come out the
far end, because a bound achieved by dropping records is not a bound.

Over a 600-record backlog with a processor slower than the feed, the arms measured **596 held with the
pause off against 30 with it on** - the fixed arm landing exactly on its derived bound of
`buffered.records.per.partition` plus one poll batch, which is what says the derivation is right rather
than merely generous. Re-derive rather than copy: the figures move with the machine, and the arm prints
them.

### Error surfacing: the type, the timing, and the commit fence

A worker's exception cannot be thrown where it happened, so it is carried back to the StreamThread.
Three things have to be true for that to be equivalent to what stock does.

**The type survives.** `TaskCorruptedException` and `TaskMigratedException` are Kafka's *control-flow*
signals to the `TaskManager` - close this task dirty, revive it, re-initialise its state - and
`StreamThread` dispatches recovery on the type. They are passed through unchanged, exactly as stock's
own catch ladder does. Everything else is wrapped in a `StreamsException` naming the topic, partition
and offset, because what failed is one record out of `poolSize` running concurrently and "which one"
is the first question anyone asks.

**One deliberate divergence from stock, and it is the interesting one.** Stock rethrows a
`TimeoutException` raw, which its `TaskExecutor` reads as *retriable* - keep the task running and come
back to it. That is safe there because the record is still in the queue and will be re-selected. Here
retries are disabled and the failure bar has closed dispatch, so honouring it would mean `process()`
returning false for ever, `task.timeout.ms` never tripping, and the partition staying paused: zero
throughput, no exception, nothing in the log, from an ordinary broker timeout. It is wrapped instead.
**Matching the exception TYPE would have broken the exception CONTRACT.**

**The timing survives.** A pump that dispatched work and then runs out of it waits, bounded, for the
outcome of what is in flight, and re-checks before returning - so the failure of a record reaches the
`TaskManager` from the same `runOnce` that ran it, which is what Kafka's own recovery test asserts.

**That wait is gated on running out of WORK rather than out of pool capacity, and the gate is the
interesting part.** Unconditional, it also fired in the saturated case - pool full, plenty of work,
nowhere to put it - where the StreamThread's next act would have been to poll. Measured with the pause
switched off so nothing else bounded inflow, that cost a **sixteen-fold** intake throttle, and worse:
it silently supplied a second memory bound, which made the memory-bound proof's own control arm look
almost bounded. A contaminated control arm turns a measurement into a reassurance. The residual the
gate leaves - at `poolSize` 1 a single record fills the pool, so the same-`runOnce` guarantee is a
guarantee about a pool with a spare slot - and the latency it still costs in the starved regime are
recorded in
[`docs/inflight/perf-streams-failure-settle-wait-has-no-throughput-arm.md`](../docs/inflight/perf-streams-failure-settle-wait-has-no-throughput-arm.md).

**Nothing commits past a failure.** Kafka's loop runs process, then punctuate, then commit, so a
failure landing in that window would otherwise let a scheduled commit make *another key's* offsets
durable for a task about to be closed dirty and rewound - and for a `TaskCorruptedException` that is
worse than a duplicate, because recovery wipes the state those offsets claim to cover. A pending
failure therefore fences commit-data collection. It does **not** fence the "is there work outstanding"
question, which is the opposite one: that is what `validateClean` turns into a `TaskMigratedException`
so the task closes dirty, and fencing it would make a failed task look clean to close.

A failure also **stops further dispatch**, so what runs after a known failure is bounded to what was
already in flight rather than to a whole poll budget. In-flight records are left to finish rather than
interrupted: a worker cancelled mid-chain leaves a half-forwarded record.

The knowledge behind all of this is owned by
[`docs/solutions/architecture-patterns/an-async-seam-owes-a-control-flow-exception-both-its-type-and-its-timing.md`](../docs/solutions/architecture-patterns/an-async-seam-owes-a-control-flow-exception-both-its-type-and-its-timing.md),
including why fixing the type alone was declined: with the timing untouched, no test can tell the
unwrap from its absence.

### Task lifecycle: what happens when a task changes hands

Kafka Streams moves a task around a great deal - it suspends it, recycles it into a standby, gains and
loses its partitions in a cooperative rebalance, closes it dirty and revives the same object. The
dispatcher was originally built to live exactly as long as one `StreamTask` constructor call, which
held only while none of that happened.

| Event | What the dispatcher does now |
|---|---|
| Partitions change (`updateInputPartitions`) | revoke, then assign, in Parallel Consumer's own order - the revoke is what bumps the epoch that lets a late outcome for a partition somebody else now owns be recognised and dropped |
| Suspend | drain, bounded; a worker still inside the chain would be forwarding into a record collector about to close |
| Recycle to standby (`prepareRecycle`) | close, through the same call the close path makes - this route previously bypassed it and leaked the registry entry, the worker pool, the wake-signal registration and PC's partition state |
| Close | drain, revoke, and mark everything published as covered, because a closed dispatcher owns nothing and will never commit again |
| Revive after a dirty close | **build a new dispatcher** over the partitions the task holds now; the closed one has no route back to a running worker pool |

Two rules hold the rest together. **In-flight work on a revoked partition is abandoned, not awaited** -
the epoch fence makes its outcome unusable, and that is the at-least-once trade Parallel Consumer's
core already makes rather than a policy invented here. And **a question may never mutate**: the
"is there work outstanding" query is reachable from Kafka's state-updater thread, so it reads counters
and touches neither the `WorkManager` nor the completion mailbox, while everything that does touch
them stays owner-thread-only and says so at runtime. That rule and how it was arrived at are owned by
[`docs/solutions/architecture-patterns/a-query-must-never-mutate-derive-thread-safety-from-callers.md`](../docs/solutions/architecture-patterns/a-query-must-never-mutate-derive-thread-safety-from-callers.md).

The distinction that makes a clean close honest is that "is a commit worth attempting" and "is it safe
to walk away" are **the same question on the stock path** - processing is synchronous, so a record is
either finished or not started. Asynchronous dispatch creates a third state, and `validateClean()` is
the caller that has to see it: without that, `closeClean()` over records still inside the processor
chain succeeded silently where Kafka's contract is to throw `TaskMigratedException` and close dirty.

The end-to-end arm is `RebalanceUnderPcDispatchTest`: two `KafkaStreams` instances in one application
id over a multi-partition topic, the second joining mid-run, both dispatching through Parallel
Consumer. It asserts no loss, duplicates bounded by **capacity rather than by a fraction of
throughput**, that the handover actually happened (with a reader `assign`ed and `seek`ed past the
position captured when the second instance started, so it cannot be satisfied by the first
instance's earlier output), and that ownership moved rather than being shared.

---

## What is refused, and how you find out

Everything currently known to be broken on this path is **physically refused**. You do not get a
plausible wrong answer.

That matters more than it sounds. None of these constructs throws in stock Kafka Streams, and none of
them would have thrown here either. They read a stream-time counter that never advances on the PC
path - `PartitionGroup.nextRecord()` is where stock Streams advances it, and the PC path does not go
through it - and several of them read-modify-write a **non-volatile `long`** from every worker, so
under concurrent dispatch the value is corrupted rather than merely stale. Left reachable, the
topology runs to completion and emits the wrong numbers, silently. Refusing is the only honest
behaviour available until the semantics are fixed.

### Three layers, because no one of them covers the surface

| You get | When |
|---|---|
| A **compile error** - `@DoNotCall` under Error Prone, a deprecation warning without it | you write `join`, `leftJoin`, `outerJoin`, `windowedBy` or `suppress` against `KStream`, `KTable`, `KGroupedStream` or `CogroupedKStream` |
| An `UnsupportedOperationException` naming the construct, at topology construction | you build that topology with the seam on |
| An `UnsupportedOperationException` naming the construct, at task construction | your topology reaches a `WindowStore`, `SessionStore`, versioned key-value store or suppression buffer **through the Processor API**, or sets `processing.guarantee` to exactly-once |

The DSL layer only fires for someone who called a refused method. The Processor API reaches the same
machinery without touching `KStream` at all - `topology.addStateStore(Stores.windowStoreBuilder(...))`
connects a window store to a plain `Processor` - and exactly-once is not a topology shape at all, it
is one configuration key. The third layer is what covers both, and it lives in `StreamTask`'s
constructor because that is the one place holding both the topology and the task config, so it costs
no additional patched class.

The store check is by **interface, never by class name**: the stores that reach the task are wrapped
several layers deep, and every wrapper implements the interface it wraps. `instanceof` sees through
the whole stack; a name match sees the outermost wrapper and breaks the first time Kafka adds one.
The versioned key-value store is the trap in that design and is worth knowing about - it extends
`StateStore` directly rather than `WindowStore`, so it looks ordinary, and it is reachable with **no
refused DSL call anywhere** via `Materialized.as(Stores.persistentVersionedKeyValueStore(...))`. The
general rule that came out of that is written up in
[`a-type-gate-is-a-claim-about-a-hierarchy-you-did-not-write.md`](../docs/solutions/architecture-patterns/a-type-gate-is-a-claim-about-a-hierarchy-you-did-not-write.md).

### The two run-time layers are conditional on the seam; the compile-time one cannot be

With `-Dpc.streams.dispatch.enabled=false` the whole of the refusal's run-time half is inert and every
one of these topologies builds and runs exactly as stock Kafka Streams does. That is both the escape
hatch and the reason Kafka's own suite still passes: several of those tests build precisely the
constructs listed here, and an unconditional check would refuse them and void the
behaviour-preservation claim.

**The compile-time layer is not conditional and cannot be** - an annotation in a class file cannot
read a system property. If you have deliberately turned the seam off and want the call anyway,
suppress `DoNotCall` at that call site. This repository does compile under Error Prone, so that is a
hard error here rather than a theoretical one, and the module's own refusal test carries exactly that
suppression: deleting it fails the build with one error per refused call site, which is the cheapest
available falsification of the compile-time layer.

Every refused method also carries a javadoc `@deprecated` tag naming **this module** as the thing
refusing it. Without that, an IDE strikes `stream.join(...)` through with no reason attached, and the
obvious inference - that Apache Kafka deprecated `join` - is false and alarming. The general shape of
that mistake is written up in
[`a-deprecation-without-an-explanation-misattributes-itself-to-the-wrong-party.md`](../docs/solutions/architecture-patterns/a-deprecation-without-an-explanation-misattributes-itself-to-the-wrong-party.md).

### If you hit the task-construction refusal, it will not stop on its own

That throw leaves `StreamTask`'s constructor, is caught by nothing on the way out, and reaches
`KafkaStreams`' uncaught-exception handler. Under `StreamsUncaughtExceptionHandlerResponse.REPLACE_THREAD`
the thread is replaced with no backoff and no attempt limit - and the replacement is refused for the
same reason, so the application rebalances in a loop. The refusal is a property of the topology and
the switch, not a transient failure. **Do not pair `REPLACE_THREAD` with a topology this module can
refuse.** Moving the check ahead of `KafkaStreams.start()` is the structural fix and is not done here.

### Reinstatement is evidence-gated, not judgement-gated

A construct comes off the refused list when Kafka's own suite exercises it with the seam **on** and
passes - not when someone reads the code and concludes it looks fine.

### What is still unsupported and NOT refused

**Stream-time punctuation.** A topology that calls
`context.schedule(interval, PunctuationType.STREAM_TIME, ...)` is a common, non-windowed pattern, and
under PC dispatch stream time never advances, so the punctuator simply never fires: no exception, no
warning, no output. It is the one item of the original unsupported list this envelope does not cover,
because it is a call on the processor context rather than a topology shape or a store type. Wall-clock
punctuation does fire.

**Kafka's own processing-threads mode.** With Kafka's private `__processing.threads.enabled__` config
on, `DefaultTaskExecutor` calls `task.process` from its own thread, which drives the dispatcher off
the thread it was bound to. It is unreachable by default and has been named as out of scope in
[`PcTaskDispatcher`](src/main/java/bz/stub/parallelconsumer/streams/PcTaskDispatcher.java)'s threading
contract since the seam landed; it is listed here so a seam-on run showing that parameter of
`StreamThreadTest` red is recognised rather than re-diagnosed.

A typed control-flow exception raised inside a processor **used to be on this list and is not any
more** - see
[Error surfacing: the type, the timing, and the commit fence](#error-surfacing-the-type-the-timing-and-the-commit-fence).

---

## The classpath hazard this module does not solve

**This module is not published, and that is the reason.**

It compiles a handful of classes into Apache Kafka's *own* package namespace and depends on
`kafka-streams` for the rest. Inside this module's build that is controlled and asserted. As a
published dependency it is not defensible, in three distinct ways:

1. **Classpath order is a convention, not a guarantee.** Maven, Gradle, IDEs, shaded uber-jars and
   Spring Boot's loader may order entries differently. When ours lose, you silently get stock Kafka
   Streams with no error - the worst shape a failure can take.
2. **Class loading is per class, so the result is always a mixture.** That works only while both
   halves are the same version, and nothing checks that they are. A routine `kafka-streams` bump
   would run our patched internals against their newer ones.
3. **It is illegal on the module path.** JPMS forbids split packages outright.

Merging an isolated leaf module is cheap to reverse; publishing an artifact is not - **merge freely,
publish deliberately**. The pom carries the deploy, signing and publishing skips, and says so at the
point of the skip.

`NOTICE` already carries the Apache 2.0 section 4(b) statement naming the modified classes, so the
obligation is discharged for whatever is eventually distributed rather than deferred to the day it is.

---

## Working on the patch

**Never hand-edit `pc-streams.patch`.** Its `@@` headers encode line counts; edit the generated Java
and re-derive.

```bash
# 1. unpack the pristine tree AND produce the patched one. process-sources, NOT generate-sources -
#    generate-sources only unpacks, and regenerating from that tree deletes every hunk.
./mvnw -pl .,parallel-consumer-streams process-sources

# 2. edit parallel-consumer-streams/target/kafka-patched/... like normal Java.
#    RUN NO MAVEN between here and step 3 - unpack silently reverts your edits and says nothing.

# 3. re-derive the tracked patch
parallel-consumer-streams/bin/regen-patch.sh

# 4. commit the patch. The generated trees are gitignored and never committed.
```

The `.` in `-pl .,parallel-consumer-streams` is required: selecting the leaf module alone fails at
`enforcer:enforce`, because the parent is not in the reactor.

[`bin/regen-patch.sh`](bin/regen-patch.sh) warns when the hunk count drops, which is the tripwire for
edits lost to a stray maven run. Its header owns the rest, including why the hunk count is a proxy
rather than an invariant, and how to reconcile two branches that both regenerate the patch (merge the
generated Java, never the patch).

The fixture patch is re-derived the same way, with all three arguments:

```bash
parallel-consumer-streams/bin/regen-patch.sh kafka-test-pristine kafka-test-patched src/main/patch/pc-streams-testfixtures.patch
```

### On a Kafka version bump

The patch is derived against exactly the sources of the reactor's `${kafka.version}`, and
`org.apache.kafka.streams.processor.internals` is package-private, unsupported, and free to change
shape in any patch release. When it stops applying, the build fails **loudly** at `apply-patch.sh`,
which is a real improvement on a vendored copy: that would compile fine and throw `NoSuchMethodError`
in production. It remains a recurring maintenance obligation, with the upstream suite above as the
regression run behind each one.

On Kafka trunk and 4.x these classes have already diverged materially - `ProcessorContextImpl` is
`final` there, and the record context is mutated in place. A green result on 3.9 does **not** transfer
unexamined.

---

## Further reading

- [Patch a dependency's internals at build time instead of vendoring or forking it](../docs/solutions/architecture-patterns/patch-a-dependency-at-build-time-without-vendoring-it.md) -
  the technique in general, its practices, and when not to apply it
- [astubbs#255](https://github.com/astubbs/parallel-consumer/issues/255) - the tracking issue for
  Kafka Streams on Parallel Consumer
- [astubbs#271](https://github.com/astubbs/parallel-consumer/pull/271) - the feasibility study this
  machinery was cut from, where the execution seam and its measurements still live
