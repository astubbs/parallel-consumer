# jcstress PoC: do the plain non-volatile longs in `PartitionState` misbehave on real hardware?

Date: 2026-08-25
Branch: `test/jcstress-poc-plain-long-visibility`
Module: `jcstress-poc/` (standalone - deliberately not in the root pom's `<modules>`)

## The verdict, first

**Yes, on the machine these runs were taken on - Apple M2 Pro (arm64, 8P+4E, 12 logical),
Temurin OpenJDK 17.0.18+8, jcstress 0.16 - and the cheap fix is demonstrated to close it.**
Every rate below is that machine's; `Outcome tables` states the environment in full, and
`One machine` under `What this settles, and what it does not` says why another box may differ.

- The commit path's publication pair - `PartitionState.onSuccess` writing `offsetHighestSucceeded`
  then `dirty`, `getCommitDataIfDirty` reading `dirty` then `offsetHighestSucceeded` - **does** admit
  the message-passing anomaly. Observed 194,305 times in 1.03e10 samples (1.9e-5) in the reduced
  probe, and **still 298 times in 2.12e9 samples (1.4e-7) with every real surrounding access in
  place**, so it is not an artefact of the reduction.
- The documented invariant `offsetHighestSucceeded <= offsetHighestSeen` **is** violable: 2,677,426
  in 8.07e9 (3.3e-4).
- **Word tearing is absent**: 0 in 1.02e10 samples. The half-written-offset fear is dead on 64-bit
  HotSpot, as expected.
- **Making only the `dirty` flag volatile is sufficient** - the plain `long` payload rides the
  release/acquire edge. 0 anomalies in 4.29e9 samples, with the outcome declared FORBIDDEN so the run
  would have failed had it appeared. The hot `long` does not need to become volatile.

The direction of every anomaly found is **safe** (replay, never skip), so this is a
duplicate-delivery and lost-commit-cycle defect, not a data-loss one. That is what sets the priority
below.

## Prior art - what each check returned

Per AGENTS.md, before forming any hypothesis. Reported including the nothings.

| Check | Result |
|---|---|
| `ls docs/plans/`, then grep | **Nothing.** No plan mentions jcstress, memory model, or `volatile` on these fields. |
| `grep -rl <mechanism> docs/solutions/` | **Nothing** for jcstress / word tearing / happens-before. |
| `ls docs/inflight/`, grep | **Nothing on this branch's base** - `docs/inflight/test-lincheck-jcstress-evaluation.md` exists only on `fix/encoder-reads-highest-succeeded-after-the-snapshot`, alongside `bug-torn-read-family.md`. Both were read from that ref via `git show`. This work executes item 2 of that evaluation's scope early. |
| `grep -rn jcstress` over the whole tree | **Nothing** - no pom, script, workflow or doc mentions it. Greenfield. |
| Cross-thread readers of the two fields | Established by grep rather than assumed - see the correspondence section. |
<!-- file-refs: N/A - the evaluation note named in that row never reached master: it was deleted on
     astubbs#344 once both its arms had executed, so no commit on master holds it and there is no history
     pointer to give. Its scope survives in the successor notes it handed its open items to -
     test-lincheck-lane-open-items.md and test-jcstress-probe-module-open-items.md - and the note's own text
     is on astubbs/parallel-consumer#344. -->

The evaluation note lives on another branch, so this branch cannot tick its item 2. Whoever merges
second should update it there.

## The question, and the code it comes from

The standing residual, from the confluentinc#894 review: `PartitionState.offsetHighestSucceeded` and
`offsetHighestSeen` are plain non-volatile `long`s, written on the control thread and read on the
broker-poll thread with no happens-before edge. The class comment names confluentinc#200 ("Consider a
shared nothing architecture") as the structural fix. Every fix landed this month narrowed *logical*
races; none added synchronisation.

All in `parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/state/PartitionState.java`
unless stated.

**Writers (control thread).**

| Site | Writes, in order |
|---|---|
| `maybeRaiseHighestSeenOffset` (via `addNewIncompleteRecord`) | `offsetHighestSeen = offset` |
| `onSuccess` | `incompleteOffsets.remove(offset)`; then `updateHighestSucceededOffsetSoFar` → `this.offsetHighestSucceeded = thisOffset`; then `setDirty()` → `stateChangedSinceCommitStart = true`, `dirty = true` |
| `initStateFromOffsetData` (via `maybeTruncateBelowOrAbove`, reset-below arm) | `offsetHighestSeen = ...`; `incompleteOffsets = new ConcurrentSkipListMap<>()`; `offsetHighestSucceeded = this.offsetHighestSeen` |

**Readers (broker-poll thread).** `AbstractOffsetCommitter.retrieveOffsetsAndCommit` →
`WorkManager.collectCommitDataForDirtyPartitions` → `PartitionStateManager.collectDirtyCommitData` →
`getCommitDataIfDirty`, which reads `isDirty()`, writes `stateChangedSinceCommitStart = false`, then
`createOffsetAndMetadata` → `tryToEncodeOffsets` (`incompleteOffsets.isEmpty()`) →
`getOffsetHighestSequentialSucceeded`, whose first statement is
`long currentOffsetHighestSeen = offsetHighestSucceeded;`.

**One finding worth recording before any fix is scoped.** `offsetHighestSeen` has, in main code, **no
cross-thread reader other than the Micrometer gauge** registered in `initMetrics`.
`PartitionStateManager.getHighestSeenOffset` exists but its only caller in the whole tree is
`OffsetEncodingBackPressureTest`. So the invariant violation this PoC confirms is, today, observable
only as a momentarily inconsistent pair of gauges. `offsetHighestSucceeded` is the field that feeds a
broker commit.

## Method

Five probes in `jcstress-poc/src/main/java/bz/stub/parallelconsumer/jcstress/`. Each carries a
"correspondence to production code" javadoc block naming the methods, field declarations and quoted
comments it models - **a jcstress probe tests the pattern, not the class**, and nothing enforces that
the pattern still matches, so the correspondence is written down to be checkable by hand.

**Calibration is two-armed, deliberately.** A clean negative control proves nothing on its own: a
harness that observes nothing also comes back clean. So the canonical word-tearing probe (must stay
clean) is paired with the store-load / Dekker probe (must fire). Only read together do they show the
harness discriminates.

**Every claim about a fix has a control arm.** The volatile variants declare the anomaly `FORBIDDEN`,
so jcstress fails the run if it appears. That is what makes "volatile closes it" a measurement rather
than an argument.

**The commit-path group has a faithful arm.** The reduced probe drops accesses the reduction argues
cannot matter, and that argument can be wrong in either direction - a `ConcurrentSkipListMap` CAS is a
full fence on most hardware, so the surrounding code might have been closing the hole by accident, in
which case a synchronisation fix buys nothing real. The faithful arm keeps every real access,
including the reader's own plain write to `stateChangedSinceCommitStart`. It settles that by
measurement.

## Outcome tables

Environment: Apple M2 Pro (arm64, 8P+4E, 12 logical), macOS 26.5.2, Temurin OpenJDK 17.0.18+8,
jcstress 0.16. jcstress preset `default` (5 iterations per fork, 1000 ms per iteration, forked across
four JVM configurations: `±UseBiasedLocking` crossed with the C2 randomisers
`StressLCM/StressGCM/StressIGVN/StressCCP`, split C1/C2 per-actor compilation).

### Calibration

| Probe | Outcome | Grade | Samples | Rate |
|---|---|---|---|---|
| `CalibrationProbes.PlainLongWordTearing` | `-1` | ACCEPTABLE | 4,690,474,443 | |
| | `0` | ACCEPTABLE | 5,481,281,345 | |
| | *(any torn value)* | **FORBIDDEN** | **0** | **0 / 1.02e10** |
| `CalibrationProbes.PlainFieldStoreLoadReordering` | `0, 0` | **ACCEPTABLE_INTERESTING** | **3,555,520,156** | **54.5%** |
| | `0, 1` | ACCEPTABLE | 1,593,464,849 | |
| | `1, 0` | ACCEPTABLE | 1,373,039,392 | |
| | `1, 1` | ACCEPTABLE | 133,951 | |

Negative control clean at 1.02e10 samples; positive control fires at 54.5%. **The harness
discriminates**, so the zeros below are worth reading.

### Question 1a - the invariant `offsetHighestSucceeded <= offsetHighestSeen`

`r1` = observed `offsetHighestSucceeded`, `r2` = observed `offsetHighestSeen`.

| Probe | Outcome | Grade | Samples | Rate |
|---|---|---|---|---|
| `SeenSucceededOrderingProbes.PlainFields` (as shipped) | `0, 0` | ACCEPTABLE | 4,774,701,904 | |
| | `0, 1` | ACCEPTABLE | 17,000,231 | |
| | **`1, 0` succeeded advanced, seen stale** | **ACCEPTABLE_INTERESTING** | **2,677,426** | **3.3e-4** |
| | `1, 1` | ACCEPTABLE | 3,275,646,947 | |
| `SeenSucceededOrderingProbes.VolatileFields` (control arm) | `1, 0` | **FORBIDDEN** | **0** | **0 / 4.31e9** |

### Question 1b - the pair the commit path actually performs

`r1` = observed `dirty` (1 = set), `r2` = observed `offsetHighestSucceeded`. `1, 0` is the anomaly:
the commit fires on the strength of an offset having succeeded, commits an offset that excludes it,
and `onOffsetCommitSuccess` → `setClean()` then clears the flag.

| Probe | Outcome | Grade | Samples | Rate |
|---|---|---|---|---|
| `CommitPathVisibilityProbes.PlainDirtyPublishesSucceeded` (reduced, as shipped) | `0, 0` | ACCEPTABLE | 4,959,323,069 | |
| | `0, 1` | ACCEPTABLE | 5,223,339 | |
| | **`1, 0` ANOMALY** | **ACCEPTABLE_INTERESTING** | **194,305** | **1.9e-5** |
| | `1, 1` | ACCEPTABLE | 5,291,925,155 | |
| `CommitPathVisibilityProbes.FaithfulOnSuccessVersusCommitCollection` (all real accesses) | `0, 0` | ACCEPTABLE | 1,759,454,260 | |
| | `0, 1` | ACCEPTABLE | 9,912,675 | |
| | **`1, 0` ANOMALY** | **ACCEPTABLE_INTERESTING** | **298** | **1.4e-7** |
| | `1, 1` | ACCEPTABLE | 351,229,515 | |
| `CommitPathVisibilityProbes.VolatileDirtyPublishesPlainSucceeded` (control arm - **only the flag volatile**) | `1, 0` | **FORBIDDEN** | **0** | **0 / 4.29e9** |

The faithful arm's rate is ~130x lower than the reduced arm's. The real `ConcurrentSkipListMap`
accesses and the extra store therefore **suppress the window substantially but do not close it**.
That is the result that matters: reasoning from "the surrounding concurrent-map operations probably
fence it" would have been wrong, and only the faithful arm could show that.

### Question 3 - the `initStateFromOffsetData` triple write (dossier candidate 1)

Pre-reset state: `offsetHighestSucceeded = 100`, incompletes `{51}`; the broker has reset below, so
the correct commit is 0. `r1` = observed `offsetHighestSucceeded`, `r2` = the offset
`getOffsetToCommit()` would send.

| Outcome | Meaning | Grade | Samples | Rate |
|---|---|---|---|---|
| `-1, 0` | wholly post-reset - commits 0, the mandated replay | ACCEPTABLE | 905,607,841 | 59.1% |
| `100, 51` | wholly pre-reset - commits 51, correct for that state | ACCEPTABLE | 625,194,565 | 40.8% |
| **`100, 101`** | **new empty map + old succeeded - commits 101, re-asserting a pre-reset offset and cancelling the replay** | **ACCEPTABLE_INTERESTING** | **922,360** | **6.0e-4** |
| **`-1, 51`** | third write visible without the second - commits 51; safe direction, but reordering-only | **ACCEPTABLE_INTERESTING** | **215,342** | **1.4e-4** |

Both intermediates are observable, and they are **not the same finding**. `100, 101` needs no
memory-model exotica at all - it is ordinary program-order staleness, the reader landing between
writes 2 and 3 - so its rate is a window measurement, not evidence of reordering. `-1, 51` is the one
that requires the JMM: the *last* write visible without the *middle* one. Reporting these as one
"tear count" would have conflated a window with a reordering.

This measures the window, not a live production defect: `bug-torn-read-family.md` records candidate 1
as **fenced** by the dirty gate, because the commit path collects only dirty states and a
bootstrap-phase state can only have been dirtied through candidate 3.

## What this settles, and what it does not

**Settles, for this hardware and JVM:**

1. Plain `long` word tearing does not occur (0 / 1.02e10). Any future argument for `volatile` on these
   fields must rest on visibility, not on atomicity.
2. The commit path's publication of `offsetHighestSucceeded` through the plain `dirty` flag is
   genuinely broken under the JMM, and remains broken with the real surrounding code in place.
3. `volatile` on the flag alone is sufficient, and leaving the `long` plain is not a compromise.

**Does not settle:**

- **Absence is at N, not proof.** The zeros above are 0 in 4.29e9 and 0 in 4.31e9 samples
  respectively, on four JVM configurations. That is strong, not conclusive.
- **One machine.** arm64 Apple M2 Pro. arm64 permits store-store reordering that x86-64's TSO
  forbids, so a **lower or zero rate on CI's ubuntu x86 would not close the question** - it would only
  show that the hardware half of the effect is absent there while the compiler half (C2 reordering,
  stale register-cached reads) remains. The interesting cross-check would be x86 *with* the C2
  randomisers, which these probes already enable; running the same jar on the self-hosted highcpu
  runner would answer it in under ten minutes.
- **The probes model the pattern, not the class.** If `PartitionState`'s write order changes, nothing
  goes red. The correspondence blocks are the mitigation, and they are prose.
- Only the read pairs named above. The probes say nothing about `ShardManager`, `WorkManager`
  checkpoint 3, or any logical race - that is Lincheck's half of the evaluation.

## Cost accounting

| Step | Wall clock |
|---|---|
| `./mvnw -f jcstress-poc/pom.xml clean package` (uberjar, cold-ish) | 3.2 s |
| Full suite, `-m quick` (8 probes) | 64 s |
| Full suite, `-m default` (8 probes, run in three batches) | ~8 min 40 s |

Both run at 770-925% CPU on a 12-core machine - jcstress saturates every core by design.

**CI viability: recommend NOT adding a lane, and the reason is not the runtime.** An on-demand
workflow like the chaos suite would fit the cost easily (`-m quick` is a 64-second smoke). The problem
is what it would gate. The only `FORBIDDEN` outcomes live in the *volatile control arms*, which are
copies documenting a fix - they cannot go red because `PartitionState` changed, because they are not
`PartitionState`. A green run would assert only that the JVM still honours `volatile`. The module's
value is one-shot evidence for a decision, so it should stay an on-demand experiment: build it and run
it when the question returns, and re-read the correspondence blocks against the source when you do.

The module cannot affect the main build: it has no `<parent>`, is not in the root `<modules>`, and is
reached only by an explicit `-f jcstress-poc/pom.xml`.

## Recommendation

**Make the publication edge explicit; do not make the `long`s volatile, and do not gate it on a
benchmark.**

1. **`PartitionState.dirty` becomes `volatile`** (or the write becomes a release / the read an acquire
   via a `VarHandle`, if the `@Setter(PRIVATE)` Lombok accessor is kept). This is the measured fix: it
   publishes everything `onSuccess` wrote before it - the map removal, `offsetHighestSucceeded`, and
   `stateChangedSinceCommitStart` - to every reader that passes through `getCommitDataIfDirty`'s
   `isDirty()` gate, which is the whole commit path. Control arm: 0 / 4.29e9.
2. **Leave `offsetHighestSeen` and `offsetHighestSucceeded` plain.** Making them volatile costs a
   barrier per record on the control thread's hot path and buys nothing the flag does not already buy
   for the commit path. The only reader it would additionally fix is the Micrometer gauge pair, whose
   worst case is a momentarily inconsistent scrape.
3. **No benchmark gate.** The added cost is one release store per *successful record* on the control
   thread and one acquire load per *commit cycle* on the poll thread. The same `onSuccess` already
   performs a `ConcurrentSkipListMap.remove`, which is strictly more expensive. Confirm on the
   existing perf lane after the change rather than blocking it beforehand - and note the observed
   anomaly rate is not the argument for the fix; the harm is.
4. **Priority: after the release, not before it.** Every anomaly found is safe-direction. The realistic
   production harm is a *burnt commit cycle*: an offset that succeeded is not committed, and the
   partition is then marked clean, so the advance waits for the next success on that partition. On a
   partition that then goes idle, it waits until rebalance or restart - i.e. redelivery of already
   processed records. That is within an at-least-once contract, but it is exactly the shape users
   report as "reprocessed after restart", so it is worth fixing, at a rate (1.4e-7 in the faithful
   probe, per raced pair) that does not justify jumping the release queue.
5. **The bootstrap triple write stays as recorded** - candidate 1 is fenced by the dirty gate, and
   point 1 hardens its publication as a side effect. Harden the write ordering itself with the planned
   racing-double unification, as `bug-torn-read-family.md` already says.

## Reproducing

```bash
JAVA_HOME=~/.sdkman/candidates/java/17.0.18-tem ./mvnw -f jcstress-poc/pom.xml clean package
~/.sdkman/candidates/java/17.0.18-tem/bin/java -jar jcstress-poc/target/jcstress.jar -m quick -v
```

`-m default` for the numbers above; `-t <regexp>` to select probes; `-r <dir>` for the HTML report.
The raw `jcstress-results-*.bin.gz` lands in the working directory, not in `-r` - it is git-ignored.
