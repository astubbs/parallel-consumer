# Lincheck proof of concept: can it rediscover the torn-read family unaided?

**Status:** PoC complete, calibration answered. The harness and the lane are on
`test/lincheck-poc-torn-read-calibration`, not merged.
**Written:** 2026-08-25
**Executes:** `docs/inflight/test-lincheck-jcstress-evaluation.md`, items 1 and 3 (the Lincheck half). The jcstress half is a sibling piece of work and is not covered here.
<!-- file-refs: N/A - the evaluation note and the dossier arrive on master with astubbs#344; until it merges they exist only on fix/encoder-reads-highest-succeeded-after-the-snapshot, so their paths are named deliberately rather than broken. -->

---

## 0. The question, and the answer

The torn-read family (`docs/inflight/bug-torn-read-family.md`, which arrives with astubbs#344) was found by hand, one hunt pass at a time, and it was established empirically that no static analysis this repo runs can see the class.
<!-- file-refs: N/A - as above, the dossier is not on master yet. -->
The open question was whether scheduler-controlled concurrency testing can - and the only honest way
to ask it is to point the tool at a tree that still has the bugs and see whether it finds them
**without being told where they are**.

Every harness here declares operations only. Not one of them names a seam, injects a latch, overrides
a method to widen a window, or orders anything. The operations are what the two real threads do: poll
work in, complete it, sweep a revoked partition, collect commit data. Which pair races, and at which
instruction the switch has to happen, is Lincheck's to find.

**Three of the four, in seconds each, plus one defect that was not on the list. The fourth is
half-found, and not reproducibly.** And - the result that decides how it can be adopted - **every one
of those came from the STRESS strategy. No model-checking arm over the product classes survived into
the lane at all**, for reasons that are specific, diagnosed, and mostly not about this codebase.

### The verdict table

| # | Known bug | Verdict | Strategy | Cost | Reproducibility |
|---|---|---|---|---|---|
| 1 | `WorkManager.handleFutureResult` - staleness check and actions on two separate `partitionStates.get(tp)` lookups (dossier candidate 3, "the worst") | **FOUND** | stress | 2-5s | 3/3 at the bound this document was written against - **corrected in §3.1**, that bound missed about 1 run in 10 |
| 2 | `ShardManager.removeWorkFromShardFor` - `containsKey` then `get`, dereferenced unconditionally (dossier candidate 2) | **FOUND** | stress | ~1.6s | 3/3 |
| 4 | `PartitionState.createOffsetAndMetadata` - the confluentinc#894 two-read of the offset-to-commit | **FOUND** | stress | ~4.7s | 3/3 |
| 3 | `OffsetMapCodecManager.encodeOffsetsCompressed` - incompletes snapshot filtered on one read of `offsetHighestSucceeded`, a second read as the encoder's range top | **HALF-FOUND, NOT REPRODUCIBLE** - see §1.5 | model checking | 640s, once | 1 of 8 model-checking attempts across five configurations |
| - | **`PCMetrics.registeredMeters` is a plain `ArrayList` written from both threads** - NOT on the calibration list | **FOUND, unprompted** | stress | seconds | seen on two independent paths |

Row 3 is stated at its weakest defensible strength on purpose. What was demonstrated, once, is the
*snapshot* leg - the incompletes set going stale relative to the rest of the commit. The specific
divergence the dossier names, where the two reads of `offsetHighestSucceeded` return **different**
values, was never exhibited, and is **not expressible** in the harness as committed (§1.5).

---

## 1. The findings, with the traces

### 1.1 `WorkManager.handleFutureResult` (dossier candidate 3)

Harness: `WorkManagerLincheckTest`. Two operations - a completed unit of work returning from the
worker pool, and a rebalance that revokes and reassigns the partition.

```
= Invalid execution results =
| ------------------------------------------------------------- |
|         Thread 1          |             Thread 2              |
| ------------------------------------------------------------- |
<!-- issue-refs: exempt-begin - verbatim Lincheck trace output; #1 is a thread/object label, not an issue -->
| revokeAndReassign(): void | completeWork(): AssertionError #1 |
<!-- issue-refs: exempt-end -->
| ------------------------------------------------------------- |

Exception stack traces:
<!-- issue-refs: exempt-begin - verbatim Lincheck trace output; #1 is a thread/object label, not an issue -->
#1: java.lang.AssertionError: null
<!-- issue-refs: exempt-end -->
	at bz.stub.parallelconsumer.state.PartitionState.onSuccess
	at bz.stub.parallelconsumer.state.PartitionStateManager.onSuccess
	at bz.stub.parallelconsumer.state.WorkManager.onSuccessResult
	at bz.stub.parallelconsumer.state.WorkManager.handleFutureResult
```

Precisely the harm the dossier records for the success path: the result passes checkpoint 3 against
the pre-revoke state, then acts on the post-reassignment one, which has never seen the offset.
Sequentially neither order throws - a revoke landing *before* the check bumps the epoch and the result
is dropped - so this is a genuine linearizability violation rather than an artefact of the harness.

**What Lincheck could NOT see here**, and the calibration is worth less if this is not said: the
dossier's *other* harm on the same defect - a stale container re-added to `retryQueue` where nothing
removes it, the confluentinc#857-family stall signature - is a leak. It changes no return value and
throws nothing, so a linearizability verifier is blind to it. Catching it needs an `@Validate`
invariant over the retry queue, which is a hint about where to look, i.e. exactly what the unaided
calibration forbids. **Lincheck found this defect through its noisiest symptom, not its worst one.**

### 1.2 `ShardManager.removeWorkFromShardFor` (dossier candidate 2)

Harness: `ShardManagerLincheckTest`, under KEY ordering. Four operations - add polled work, success,
failure, revoke sweep.

```
= Invalid execution results =
| -------------------------------------------------------------- |
|       Thread 1       |                Thread 2                 |
| -------------------------------------------------------------- |
<!-- issue-refs: exempt-begin - verbatim Lincheck trace output; #1 is a thread/object label, not an issue -->
| revokeSweep(0): void | revokeSweep(0): NullPointerException #1 |
<!-- issue-refs: exempt-end -->
| -------------------------------------------------------------- |

<!-- issue-refs: exempt-begin - verbatim Lincheck trace output; #1 is a thread/object label, not an issue -->
#1: java.lang.NullPointerException: Cannot invoke
<!-- issue-refs: exempt-end -->
    "bz.stub.parallelconsumer.state.ProcessingShard.remove(long)" because "shard" is null
	at bz.stub.parallelconsumer.state.ShardManager.removeWorkFromShardFor
	at bz.stub.parallelconsumer.state.ShardManager.removeAnyShardEntriesReferencedFrom
```

Note what Lincheck chose: **the sweep racing itself**, not the sweep racing `onSuccess`. The
hand-built reproduction on `test/torn-read-candidates-reproduction` uses the `onSuccess` →
`removeShardIfEmpty` route, because that is the production pairing a human traced. Lincheck found a
shorter one - two sweeps, where the first empties the shard and garbage-collects it between the
second's `containsKey` and its `get`. Same defect, same fix, a route nobody had written down. That is
the whole argument for owning this tool.

### 1.3 The confluentinc#894 commit tear

Harness: `PartitionStateLincheckTest`. **The oracle is stated as the reader's view, not the
writer's.** The `commit()` operation does not return the `OffsetAndMetadata` it produced. It returns
what a consumer would *reconstruct* on the next bootstrap: the committed offset, plus the incomplete
offsets obtained by decoding the payload against that same committed offset - which is the only number
the broker hands back. Comparing raw base64 would flag any encoding difference as a bug; decoding
first states the actual correctness property, so a violation reads as "this commit tells the broker to
replay the wrong records".

```
= Invalid execution results =
| ---------------------------------------------------------- |
|                Thread 1                 |     Thread 2     |
| ---------------------------------------------------------- |
| commit(): resumeFrom=1 replay=[1, 2, 3] | succeed(0): void |
| ---------------------------------------------------------- |
```

Neither sequential order can produce that. Commit-then-succeed gives `resumeFrom=0 replay=[0, 1, 2]`;
succeed-then-commit gives `resumeFrom=1 replay=[1, 2]`. The observed value takes the committed offset
from the *late* read and the payload from the *early* one, so every decoded offset is shifted up by
one: offset 3, which had succeeded, is scheduled for replay, and offset 0's completion is asserted
although it was the very record that moved the base. That is confluentinc#894, reconstructed from
nothing but "a record succeeded" and "collect commit data".

### 1.4 A defect that was not on the list

Not a torn read, and not something anyone pointed the tool at: two concurrent `commit()` calls throw

```
java.lang.ArrayIndexOutOfBoundsException: Index 11 out of bounds for length 10
	at java.util.ArrayList.add
	at bz.stub.parallelconsumer.metrics.PCMetrics.getCounterFromMetricDef
	at bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.getCounterMeterForEncoding
	at bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.encodeOffsetsCompressed
```

`PCMetrics` accumulates every meter it registers into a plain `ArrayList`, and registers one per
encode - on a path that runs on the control thread or the broker-poll thread depending on commit
mode. Written up in `docs/inflight/bug-pcmetrics-registered-meters-is-a-plain-arraylist.md`, with a
second sighting from the rebalance path.

This is the answer to "can it find what we have not already found", which the calibration proper
cannot address by construction. One proof of concept, one new defect, on a hot path, unprompted.

### 1.5 The encoder tear: what was actually shown, and what was not

The model checker did once put a thread switch exactly between the two reads the dossier names:

```
| commit(): resumeFrom=0 replay=[0, 1, 2]                                           |            |
|   state.createOffsetAndMetadata(): OffsetAndMetadata#1                            |            |
|     tryToEncodeOffsets(): Optional#9                                              |            |
|       getOffsetToCommit(): 0                                                      |            |
|       getOffsetHighestSucceeded(): 3                                              |            |
|       om.makeOffsetMetadataPayload(0, PartitionState#1): "bAAECAA="               |            |
|           encodeOffsetsCompressed(0, PartitionState#1): ByteArray#7               |            |
|             partitionState.getIncompleteOffsetsBelowHighestSucceeded(): TreeSet#1 |            |
|             switch                                                                |            |
|                                                                                   | succeed(1) |
|                                                                                   | succeed(2) |
|             partitionState.getOffsetHighestSucceeded(): 3                         |            |
|             OffsetSimultaneousEncoder.<init>(0, 3, TreeSet#1)                     |            |
```

Nothing in the harness asked for that placement, and that is the capability the seam tests cannot
have: a deterministic double-injection test proves a *known* seam still tears; this located the seam.

**Three things stop that being a FOUND, and saying so is worth more than the row would have been.**

- **Both reads returned 3.** What this trace shows is the incompletes SNAPSHOT going stale, not the
  range top moving between the two reads. Same family, different leg.
- **It happened once in eight model-checking attempts** across five configurations (§3.2), and the
  one that fired took 640s and reported through Lincheck's non-determinism abort path (§2.5).
- **The range-top leg is NOT EXPRESSIBLE in the harness as committed.** It needs an offset ABOVE the
  current highest-succeeded to complete inside the window, so `offsetHighestSucceeded` has somewhere
  to move. Tracking one makes it expressible - and takes the stress arm from finding a violation
  every run to one run in three, because every extra value the offset generator can produce dilutes
  the chance a random scenario contains the pair that tears. That trade is only worth taking for a
  model-checking arm, which places the switch rather than hoping to land in it, and no such arm
  survived. `HIGHEST_POLLED_OFFSET` in `PartitionStateLincheckTest` carries this note; widen it and
  the generator together if a model-checking arm ever returns.

**A harness that finds a bug is not evidence that it can find the bug you think it is aimed at.**
The first version of this harness produced the trace above and looked like it covered the encoder,
while the mechanism the dossier names was structurally unreachable in it.

---

## 2. What it cost to get a first result at all

Six obstacles, none of them in Lincheck's documentation, and the worst of them **silent**: the build
stayed green while the tool verified nothing.

### 2.1 A red control is not optional here - the first run was a false PASS

`LincheckToolchainProbeTest` is a nine-line `containsKey`-then-`get` on a `ConcurrentHashMap` with the
result dereferenced unconditionally: the dossier's own textbook probe, the one SpotBugs at
`effort=Max` reported nothing on. It has a known answer, so a clean run on it means the toolchain is
broken rather than the code being correct.

It earned its place immediately. The very first run reported **no violations, in three seconds**,
against code that cannot survive two threads. The cause was an ASM version conflict: `wiremock-jre8`
declares `org.ow2.asm:asm` 9.4 as a direct dependency, so Maven's nearest-wins put 9.4 on the test
classpath beside the 9.9.1 `asm-commons` Lincheck brings. Lincheck's class-file transformer then died
inside *every* transform with `NoSuchMethodError: org.objectweb.asm.Type.getArgumentCount(String)` - a
method that only exists from ASM 9.6 - **caught it per class, logged it, and carried on with the class
uninstrumented**. The model checker observed no shared memory at all and reported success.

The pin is in `parallel-consumer-core/pom.xml`, the same shape as the `byte-buddy` pin already there
for the same stale wiremock transitives. Without the control probe this was undetectable, and every
result in §1 would have read "not found".

### 2.2 JDK 17 module access - model checking only

The model checker walks and restores the object graph of the class under test between invocations, and
dies on the first field it cannot read: `InaccessibleObjectException: module java.base does not "opens
java.util"`. The `lincheck` Maven profile supplies the `add-opens`/`add-exports` set; the stress
strategy needs none of it.

They live in a profile rather than the default build deliberately: applied to a module they weaken
encapsulation for *every* test in the fork, and a permanent blanket grant is how a real encapsulation
failure elsewhere stops being visible.

### 2.3 Jabel is a non-issue - the real blocker is Lombok

The evaluation note flagged the Java 17-source/Java 8-bytecode cross-compilation as the thing most
likely to fight bytecode instrumentation. **It does not.** Lincheck instruments this project's Java 8
class files without complaint, at every strategy, on JDK 17. Nothing here needed a Jabel change, a
`release.target` change, or a second JDK.

The blocker that does exist is a Lombok interaction, and it is unconditional:

> Lincheck 3.7's `ConstantHashCodeTransformer` rewrites **every** `hashCode()I` call site into
> `Injections.hashCodeDeterministic(receiver)`, which dispatches **virtually**. It does not distinguish
> `INVOKESPECIAL` from `INVOKEVIRTUAL`. So `super.hashCode()` inside an overriding `hashCode` becomes a
> call to itself and recurses to `StackOverflowError`. Lombok's `@EqualsAndHashCode(callSuper = true)`
> generates exactly that shape.

`ShardKey.KeyOrderedKey` - the shard map's key under KEY ordering - is such a type, so
`ShardManagerLincheckTest` cannot be model-checked at all, and neither can `WorkManagerLincheckTest`,
which reaches the shard manager to register work. And the shard bug is KEY-only (`removeShardIfEmpty`
returns early under PARTITION and UNORDERED), so there is no ordering mode that both reaches the
defect and avoids the blocker.

Settled with a control arm rather than asserted: `LincheckSuperHashCodeProbeTest` holds two classes
identical but for the `super.hashCode()` call. The one that delegates upwards crashes the model checker
deterministically; the one that does not runs clean. A `ManagedStrategyGuarantee` does **not** help -
transformation happens before analysis sections are consulted, so neither `treatAsAtomic` nor `ignore`
prevents the rewrite. That probe is the tripwire: it asserts the defect is still present, so a future
Lincheck that fixes it turns the test red and names the follow-up.

### 2.4 Why no model-checking arm over the product classes survived

Beyond §2.3, the commit path defeats the model checker for a second, independent reason: **replaying a
failing interleaving requires the code under test to be deterministic, and this path is not.** A single
run logs dozens of `Cannot reproduce the interleaving` and `trying to switch the execution to thread 0,
but only the following threads are eligible to switch: []`, abandoning those branches. Two sources were
identified, and both are properties of the product code rather than of Lincheck:

- **micrometer.** Every encode updates a timer, two distribution summaries and a counter. Excluding the
  metrics stack from analysis cut a model-checking run from 287s to 29s - a tenfold tax paid on every
  invocation, for state no harness here is asking about.
- **`parallelStream()` in two `PartitionState` accessors**
  (`getIncompleteOffsetsBelowHighestSucceeded`, `getAllIncompleteOffsets`). A parallel stream runs on
  the common ForkJoinPool, and the model checker can only schedule threads it started. That two
  commit-path accessors run a parallel stream over a handful of longs is worth a second look on its own
  account, independently of Lincheck.

Neither exclusion made the tear findable, so both guarantee helpers were deleted rather than left as
unexercised code; `LincheckHarness`'s javadoc says so and points here.

### 2.5 One reporting path that looks like a tool failure and is not

Model checking sometimes delivers a verdict as `IllegalStateException: Non-determinism found` rather
than `LincheckAssertionError` - when it replays the failing interleaving and gets a *different* wrong
answer the second time. The message still carries both `= Invalid execution results =` reports and the
full trace, so the finding is real and fully evidenced; Lincheck simply declines to minimise a scenario
it cannot replay. Two different wrong answers from one interleaving is, if anything, a stronger
statement about a torn read.

`LincheckHarness.runExpectingViolation` distinguishes the two paths and logs which fired, because the
naive `assertThrows(LincheckAssertionError.class, ..)` would also pass on Lincheck's **internal crash**
(§2.3), which is thrown as the same type. A harness that cannot tell a verdict from a crash
manufactures exactly the false confidence this exercise exists to avoid.

### 2.6 Three smaller ones, each of which cost a run

- **No first-time classloading inside an operation.** `UniLists.of(..)` resolves its factory through
  `ServiceLoader` on first use. The model checker cannot tell one-time class initialisation from a spin
  loop and reported "All unfinished threads are in livelock due to non-terminating loops" instead of the
  real bug. Build operation arguments in the constructor.
- **JaCoCo probes are shared state.** With coverage on, every trace line is padded with `$jacocoInit`
  and `RuntimeData.getProbes` frames, burying the three lines that matter. The lane sets
  `-Djacoco.skip=true`.
- **Two Lincheck classes must not run concurrently in one JVM.** Lincheck installs a JVM-wide agent and
  drives the fork's scheduler; core's default JUnit thread parallelism runs test classes concurrently.
  The lane sets `-Dparallel-tests=false`. Nothing warns about this - results are simply meaningless.

**A harness must also model what production can actually do.** `WorkManagerLincheckTest` first reported
two concurrent rebalances tearing the per-partition counter maps - a real defect, but in a state the
Kafka consumer contract cannot reach, and Lincheck stops at the first violation, so the checkpoint-3
tear was never reached. `@Operation(nonParallelGroup = "rebalance")` restores fidelity; the defect that
scenario exposed is recorded rather than deleted along with it.

---

## 3. Cost accounting

Measured on JDK 17.0.18-tem, `surefire.forkCount=1`, an Apple-silicon laptop. CI (`forkCount=1C`,
smaller cores) will be slower, and the lane is serial by construction (§2.6).

### 3.1 The lane as committed

`bin/lincheck-test.sh`, whole lane, three consecutive runs: **26s, 27s, 29s wall clock including the
build**, all green, no flakes.

| Class | Strategy | Bound (iterations x invocations) | Per-run |
|---|---|---|---|
| `LincheckToolchainProbeTest` | model checking + stress | 30 x 1,000 / 30 x 10,000 | 4.1-4.3s |
| `LincheckSuperHashCodeProbeTest` | model checking, both arms | 1 x 10 | ~0.2s |
| `ShardManagerLincheckTest` | stress | 50 x 5,000 | 1.6-1.7s |
| `PartitionStateLincheckTest` | stress | 300 x 5,000 | 4.6-4.8s |
| `WorkManagerLincheckTest` | stress | 200 x 5,000 (**raised to 1,000 x 5,000** - see the correction below) | 2.1-5.0s |

**Reproducibility is a tuned property, not a given.** `WorkManagerLincheckTest` found the tear in 2 of
3 runs at 50 iterations and 3 of 3 at 200 - a rebalance is expensive next to a mailbox handoff, so the
window is a small fraction of each invocation. Anyone adding a harness here must measure its hit rate
across several runs before believing it: the failure mode of an under-budgeted stress arm is a flake,
and a flake fails this build with no retry, by design.

**Correction (2026-08-25, during the astubbs#347 review). Three runs was not enough runs, and the
200-iteration bound was a latent flake.** A later pass measured 2 misses in 8 single-class runs on
one machine and 0 in 8 on another, which is the signature of a bound sitting on the edge rather than
of a difference between machines. The row above and the `WorkManagerLincheckTest` row in the table
before it both describe that edge, so read both through this correction.

Three runs cannot separate a 10% miss rate from a 0% one, so the follow-up measured the underlying
per-iteration probability instead of the bound's outcome. **Deliberately under-budgeting is the
cheap way to do that**: at `iterations(25)` the harness hit 2 times in 8, so the per-iteration
survival is `0.75^(1/25) = 0.9886` and each iteration finds the tear with probability 1.14%. That
one number prices every bound, and it took eight two-second runs to obtain:

| `iterations` | Predicted miss rate | Measured |
|---|--:|---|
| 25 | 75% | 6 misses in 8 |
| 200 (as first committed) | 10% | 0 misses in 8 here, 2 in 8 elsewhere |
| 400 | 1% | - |
| 1,000 (committed now) | 0.001% | 0 misses in 8 |

**The other two harnesses were probed the same way and are not marginal.** Starved to a tenth of
their committed bounds - `ShardManagerLincheckTest` at 5 iterations instead of 50,
`PartitionStateLincheckTest` at 30 instead of 300 - both hit 8 times in 8. Zero misses in eight
starved runs puts the miss probability at that starved bound below 31% with 95% confidence, which
compounds to below 1e-5 at the bounds they actually carry. Their per-run times are also tightly
clustered (3.1-4.8s and 6.6-12.8s) where `WorkManagerLincheckTest` ranged 4.7-32.2s across whole-lane
runs, and that spread is itself the signature of a search finishing near the edge of its budget. So
the flake was specific to the one harness, and the other two bounds are left alone.

**Raising `iterations` is free on the path that matters**, which is why the bound moved to 1,000
rather than to the smallest sufficient number. Lincheck stops at the first violation, so a run that
finds the tear never reaches the extra iterations: measured 6.7-19.1s at 1,000 against 5.3-23.8s at
200, the same distribution. The only path that gets longer is the one where the harness is going to
fail anyway - either a real flake, which this change is removing, or the designed inversion when the
fix PR lands, which happens once and wants the extra certainty. The whole-lane wall clock in the
table above is unchanged for the same reason.

The default unit suite is unaffected: `bin/ci-unit-test.sh` runs green across every module with the
ASM pin and the `argLine` change in place, and selects no Lincheck class.

**Adding a lane touches five places, and only one of them was checked.** The pom's default
`excluded.groups` is the obvious one - but `bin/ci-unit-test.sh`, `bin/ci-integration-test.sh` and
`bin/ci-build.sh` deliberately do NOT inherit it (a pom edit must not be able to change what gates),
so a tag excluded by the pom and not by them runs **in the gating suite**. That is where this lane
was heading, silently, and nothing would have said so. `QuarantinedAnnotationContractTest` now carries
a check that every group the pom default excludes is also excluded by all three wrappers, so the next
lane cannot make the same mistake. Two of its existing assertions pinned the whole
`excluded.groups` literal and had to become membership checks first - a whole-list assertion fails
on any unrelated addition, with a message about quarantined tests gating, and the obvious repair is
to paste the new literal in and be pinning a list nobody reasoned about. The mutation lane is the fifth
place, excluded by name rather than by tag because pitest's handling of `excludedGroups` is
explicitly unverified in that script's own header, and being wrong there costs a re-run of a
scheduler search per mutant.

### 3.2 Model checking, and why none of it is in the lane

Every model-checking attempt against `PartitionStateLincheckTest`, in order:

| Configuration | Wall clock | Found? |
|---|---|---|
| 30 x 500, 2 actors/thread, narrow generator, no guarantees | 640s | **yes** - via the non-determinism abort path |
| 10 x 100, 2 actors, wide generator (three runs) | ~85s each | no |
| 30 x 200, 2 actors, wide generator | 410s | no |
| 50 x 200, 1 actor, wide generator | 287s | no |
| 30 x 200, 1 actor, narrow generator | 176s | no |
| as above, plus metrics excluded from analysis | 29s | no |
| as above, plus the `parallelStream` accessors atomic | 21s | no |
| 50 x 300, 2 actors, both guarantees | 86s | no |

One hit in eight. The guarantees bought a tenfold speedup and no additional finding. A stress arm that
finds the same defect in seconds, every run, is worth more in a lane than a model-checking arm that
finds a different one once in eight attempts - so the lane ships the former, and this table is the
record of the latter.

### 3.3 What would make CI adoption painful

- **Non-gating is mandatory, not a preference.** Every harness here asserts that a bug EXISTS, so the
  four fix PRs in flight (astubbs#337, astubbs#344, astubbs#345, astubbs#346) turn this lane red on
  merge, by design. A gating lane would block them.
- **Serial, single-fork.** The lane cannot use the `forkCount=1C` parallelism the unit suite relies on
  (§2.6), so its wall clock does not shrink with CPU count.
- **The `-Plincheck` JVM args weaken module encapsulation for the whole fork.** Acceptable in an opt-in
  lane, not in the default build.
- **MPL-2.0.** Test scope only; it must never reach a compile or runtime dependency.
- **The output is enormous.** A lane run writes tens of thousands of lines, most of it
  `[WARN] Failed to get object field offset`. Fine for a job summary that greps; painful in a console.

---

## 4. Recommendation

**Adopt, for these classes only, stress strategy only, as a non-gating opt-in lane.** Concretely:

1. **Keep the four harnesses and the two probes.** They cost half a minute, they found three of the
   four named defects and one nobody had named, and - this is the actual prize - **once the four fix
   PRs land and the assertions are inverted, they stop being calibrations and become regression
   detectors over the whole operation set of these classes**, not just the seams somebody thought of.
   That inversion is a task with an owner rather than a hope: each test's javadoc names the PR that
   triggers it.
2. **Do not adopt the model checker yet, and do not quietly drop it either.** It is the strategy with
   the unique capability - it placed a switch between two named reads - and it is currently unusable
   here for two reasons, both tracked by tests rather than prose. `LincheckSuperHashCodeProbeTest`
   fires when JetBrains fixes the `super.hashCode()` rewrite, and `LincheckToolchainProbeTest` keeps a
   model-checking arm exercised meanwhile so the capability cannot rot unnoticed. The second reason -
   replay non-determinism from micrometer and `parallelStream()` on the commit path - is ours, not
   JetBrains', and is a reason to look at that code on its own account.
3. **Wire it to CI only as a manual/dispatch lane at first**, following the chaos suite's precedent.
   `bin/lincheck-test.sh` already refuses to report success when it selects zero tests, which is the
   failure mode that matters most for an opt-in lane.
4. **Do not extend it with `@Validate` invariants until (1) is done.** They would catch the
   retry-queue leak in §1.1 and they are the obvious next step - but an invariant naming the retry
   queue is a hint, and the value proven here is what the tool finds unaided.

The one-line version: **it works, it is cheap, and it found something we did not know - but only its
weaker strategy runs on this codebase today, and the reason is one Lombok idiom plus two habits in the
commit path.**

---

## 5. What this PoC deliberately did not do

- **No jcstress.** The evaluation note's item 2 - the `offsetHighestSucceeded` plain-`long` visibility
  question - is memory-model territory and a separate exercise. Nothing here bears on it.
- **No fixes.** All four defects are still unfixed on this branch, on purpose: it is the calibration
  tree.
- **No CI workflow.** `bin/lincheck-test.sh` exists and the tag is excluded from the default suite the
  way `chaos` is, but no workflow calls it. That is a deliberate stopping point, pending §4.
