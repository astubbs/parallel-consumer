---
title: "Assert the commit frontier, not the path of ticks that reached it"
date: 2026-08-13
category: test-flakiness
module: parallel-consumer-core
problem_type: test_failure
component: testing_framework
severity: medium
symptoms:
  - "`processInKeyOrder(CommitMode)[1]` red on CI, green on 3 consecutive local `-Pci` runs of the same commit"
  - "ConditionTimeoutException on `assertCommitLists`: actual `{...-0=[1, 3], ...-1=[4]}` vs expected `[...-0=[3], ...-1=[4]]`"
  - "The extra offset is a legitimate intermediate commit, not a wrong one - both readings are correct"
  - "Only one of three `CommitMode` parameters fails, and it is the slowest one"
  - "The await burns its full 30s: once the tick has landed, the condition is permanently false"
root_cause: async_timing
resolution_type: test_fix
status: "Fix committed as `e8c9bb12` on `test/inactive-test-remediation` (astubbs#264), UNMERGED as of 2026-08-13. Nothing enforces the rule; it is convention plus the helper's javadoc."
applies_when:
  - Writing or reviewing any assertion over a PC commit history in a core unit test
  - Re-enabling a long-dark test and rewriting its commit expectations from scratch
  - A test is green across repeated local runs but red on a loaded CI runner for only some parameters
related_components:
  - testing
  - development_workflow
tags:
  - flaky-tests
  - commit-offsets
  - assertion-design
  - key-ordering
  - awaitility
  - ci
  - compounding-gap
related_prs:
  - "astubbs#264 - re-enables `processInKeyOrder` (`dbbfd62c`) and carries the fix (`e8c9bb12`); OPEN"
  - "astubbs#260 - merged as `c42cd322`; the same defect class days earlier, written down only in a commit message and two javadocs"
  - "astubbs#263 - landed the audit that predicted this failure class"
  - "astubbs#101 - fixed this same assertion family once before, for the opposite symptom"
related:
  - "docs/solutions/test-flakiness/vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md - sibling rule, opposite direction: an await that tests nothing"
  - "docs/solutions/test-flakiness/unforceable-trigger-commit-lock-timeout-2026-08-07.md - sibling rule: a test awaiting a trigger it cannot force"
  - "docs/test-hardening/inactive-tests-audit-2026-08-08.md - section 1.2 predicted exactly this class of assertion failure"
  - "docs/inflight/test-untracked-ci-flakes.md - held the parent rule this one specialises, until its entry closed and the rule was removed with it. Read it as recorded: `git show b42733abef45e792df6fca1b3fb8d49d7dfc7946:docs/inflight/test-untracked-ci-flakes.md`, grep `do not compare two moving values`"
---

# Assert the commit frontier, not the path of ticks that reached it

A commit assertion may only name **where a partition ended up**. The sequence or set of offsets it
passed through on the way there is a function of where the periodic commit tick happened to fall, and
a test that pins it is asserting the speed of the machine it runs on.

This is the second instance of that rule in a week. The first, astubbs#260 (merged as `c42cd322`),
stated the principle correctly but only in the javadoc of the two helpers it patched, scoped to
*repeat* commits. The rule did not travel, and the next test hit the same class of defect in a
different shape. That failure to generalise is why this record exists - see **The compounding
failure** below.

## Problem

`processInKeyOrder` had been `@Disabled` since 2020 (upstream `c1fefbc64`). It was re-enabled and
rewritten on astubbs#264 in `dbbfd62c`, which replaced its flattened `assertCommits` expectations with
per-partition `assertCommitLists` calls pinning the exact set of offsets committed for each partition.

It passed three consecutive local runs under `-Pci` and went red on the first CI run of that commit.
The library was right; the assertion was wrong.

## Symptoms

CI run 31654978866, job 94307427273 ("Unit Tests" leg):

```
[ERROR] bz.stub.parallelconsumer.ParallelEoSStreamProcessorTest.processInKeyOrder(CommitMode)[1] -- Time elapsed: 40.35 s <<< ERROR!
org.awaitility.core.ConditionTimeoutException:
Assertion condition ... [Which offsets are committed and in the expected order]
Expecting actual:
  {input-0.31588349109546965-0=[1, 3], input-0.31588349109546965-1=[4]}
to contain exactly (and in same order):
  [input-0.31588349109546965-0=[3], input-0.31588349109546965-1=[4]]
```

One red test out of 345 - and note Surefire's own summary is `Failures: 0, Errors: 1`, because an
Awaitility timeout arrives as an uncaught exception rather than an assertion failure. Grepping a job log
for "Failures: 0" is not evidence that the leg was green.

Param `[1]` is the first constant of `ParallelConsumerOptions.CommitMode`
(JUnit's invocation index is 1-based), which is `PERIODIC_TRANSACTIONAL_PRODUCER` - by its own javadoc
"the slowest of the options" (grep `slowest of the options` in `ParallelConsumerOptions`).

That is a speed correlation, not a routing one. Unlike `assertCommits`, **`assertCommitLists` is
equally strict in both branches**: `AbstractParallelEoSStreamProcessorTestBase#assertCommitLists` sends
the transactional and consumer branches into the same `KafkaTestUtils#assertCommitLists`, which ends in
`containsExactlyEntriesOf`. The mode did not select a stricter code path; it just left a wider window
open.

The 30s bound is `defaultTimeoutSeconds` in the test base (grep `defaultTimeoutSeconds = 30`), so the
40.35s elapsed is the await burning its full budget on a condition that had already become permanently
false - not a slow system, and not something a larger timeout would rescue.

## Both readings were correct

The relevant records, all created through `KafkaTestUtils#makeRecord` (grep `offset++` - one **global**
offset counter, not one per partition):

| Offset | Partition | Key | Role |
|---|---|---|---|
| 0 | 0 | `key-0` | `primeFirstRecord`, locked |
| 1 | 0 | `key-0` | `sendSecondRecord`, unlocked early but blocked behind 0 by key order |
| 2 | 0 | `key-1` | independent key, completes early |
| 3 | 0 | `key-1` | locked |
| 4-8 | 1 | `key-2`, `key-3` x3, `key-4` | the partition-1 scenario |

At the failing checkpoint, record 0 has just been unlocked. Completing 0 frees 1 (same key), and 2 was
already done, so the run 0-2 becomes contiguous and partition 0's frontier moves to 3.

Whether `1` also appears in the history depends on nothing but the tick:

- Commit tick falls **between** 0 completing and 1-2 completing: PC commits `1` ("record 0 done, resume
  at 1"), then later `3`. History `[1, 3]`.
- The whole run 0-2 completes **inside** one tick: only `3` is ever committed. History `[3]`.

Both are correct. A fast, idle box closes that window; a loaded CI runner opens it. The assertion was a
statement about wall-clock tick placement wearing a correctness assertion's clothes.

(Aside for anyone grepping the test: the surviving comment reads "0, 1 and 2 are all key-1 on partition
0". The mechanism it describes is right; the key attribution is loose - 0 and 1 are `key-0` from
`primeFirstRecord`/`sendSecondRecord`, and 2 is the first `key-1` record.)

### The offsets are exclusive, which is why it is 3 and not 2

`PartitionState#getOffsetToCommit` returns `getOffsetHighestSequentialSucceeded() + 1`, described as
"Next offset expected to be polled, upon freshly connecting to a broker". Finishing records 0, 1 and 2
therefore commits **3**, not 2. A committed offset says where to resume, not which record was last
done. Half of what `dbbfd62c` had to fix in this test's original expectations was that same confusion.

### Why partition 1 sits at 4 before doing any work

Partition 1's first record is offset 4 because record creation uses that one global counter. Its
bootstrap commit of `4` is a **starting point, not progress** - and the flattened
`AbstractParallelEoSStreamProcessorTestBase#assertCommits` cannot tell the difference, because it only
trims a genesis of `0` (grep `trimGenesis` and `KafkaTestUtils#trimAllGenesisOffset`). That is why this
test asserts per-partition at all.

## What Didn't Work

**Three green consecutive local runs under `-Pci`.** This was real verification - it is the standard
this repo holds, and it was met before the commit was pushed. It still could not have found this
defect, and the honest lesson is not "run it more times".

The trigger is *machine speed*, and the local box is fast and idle. Every local run closed the tick
window the same way, so repetition on one machine sampled the same point in the space over and over. A
defect whose discriminator is the environment is structurally invisible to repetition within one
environment.

That is a genuine boundary case for local verification rather than a contradiction of it. Local runs
are still the gate; what they cannot do is *probe a timing window*. When an assertion's truth depends on
where a periodic tick falls, the only instruments that help are (a) reasoning about whether an
intermediate state is reachable at all, and (b) deliberately perturbing the timing - loading the box, or
latching the race open the way astubbs#260's regression test does. Reaching for a fourth run is reaching
for a machine that will keep agreeing with itself.

**Prior art that would not have helped either.** astubbs#260's `collapseRepeatedCommits` was already in
the tree and merged when this failed. It collapses a *repeat* of the immediately preceding commit; `[1,
3]` is two distinct offsets, so it passes straight through (session history). Fixing the class narrowly
had already been tried; that is the point of the next section.

## Solution

Commit `e8c9bb12` (on `test/inactive-test-remediation`, **unmerged** as of writing - astubbs#264 is
open). It adds one private helper to `ParallelEoSStreamProcessorTest` and routes every post-completion
checkpoint through it:

```java
private void awaitFrontier(int partition, long expected) {
    var tp = new TopicPartition(INPUT_TOPIC, partition);
    await().timeout(defaultTimeout)
            .untilAsserted(() -> assertThat(highestCommitFor(tp))
                    .as("committed offset frontier for partition %s", partition)
                    .hasValue(expected));
}

private Optional<Long> highestCommitFor(TopicPartition tp) {
    return getCommitHistory().stream()
            .map(groupHistory -> groupHistory.get(CONSUMER_GROUP_ID))
            .filter(Objects::nonNull)
            .map(partitionCommits -> partitionCommits.get(tp))
            .filter(Objects::nonNull)
            .map(OffsetAndMetadata::offset)
            .max(Comparator.naturalOrder());
}
```

The checkpoints become:

```java
-await().timeout(defaultTimeout).untilAsserted(() -> assertCommitLists(of(of(3), of(4))));
+awaitFrontier(0, 3);
+awaitFrontier(1, 4);
```

and likewise for the three later ones, ending at `awaitFrontier(1, 9)` / `awaitFrontier(0, 4)`.

`getCommitHistory` in the test base already dispatches on commit mode (producer
`consumerGroupOffsetsHistory()` versus `consumerSpy.getCommitHistoryWithGroupId()`), so the helper is
mode-agnostic by construction.

**The first checkpoint deliberately keeps `assertCommitLists`:**

```java
await().timeout(defaultTimeout).untilAsserted(() -> assertCommitLists(of(of(), of(4))));
```

Nothing has completed on partition 0 there, so no intermediate value is reachable and the exact-set
form is stable. The rule is not "never assert exactly".

## Why This Works

`max([1, 3]) == 3`. The frontier is invariant under which intermediate offsets the ticks happened to
capture, so this specific failure cannot recur *by construction* rather than by timing luck.

**It does not weaken the test.** The frontier only moves when work completes contiguously
(`getOffsetHighestSequentialSucceeded`, javadoc: "the offset one below the lowest incomplete offset"),
so a partition that advanced past in-flight work still fails. The invariant `processInKeyOrder` exists
for survives intact: partition 1 will not move past its base offset 4 while `key-2`'s record 4 is in
flight even though 5, 6 and 8 have completed, and when 4 finishes it jumps to **7, not 9**, because 8 is
complete but not contiguous.

Mutation-checked rather than trusted for being green: per `e8c9bb12`'s message, asserting that partition
1 reaches 9 instead of 7 at that checkpoint - the exact claim that it advanced past in-flight work -
reds all three commit modes.

Each checkpoint also re-asserts the *stationary* partition (`awaitFrontier(0, 4)` while partition 1
moves), so an over-advance is caught at the moment it would happen, not only at the end.

**What is given up, stated plainly.** The audit that scoped this re-enable calls this test's
end-to-end, multi-partition, per-`CommitMode` commit assertion the one real coverage gap it found
(`docs/test-hardening/inactive-tests-audit-2026-08-08.md`, section 1.2). The frontier form keeps all of
that - per-partition resume points, per commit mode, across partitions, under key-order blocking. What
it gives up is the intermediate commit *sequence*, which was never a property of the system under test.
This is a deliberate narrowing, not an accidental weakening.

**One honest limitation.** `highestCommitFor` takes a max, so - like the consumer-side branch of
`assertCommits`, which carries the same caveat in its comment - it cannot see a committed offset going
*backwards* after the frontier has been reached. Detecting that stays with
`KafkaTestUtils#assertCommits`, where the history is a single ordered stream.

## The compounding failure: astubbs#260 fixed this class and the rule stayed in a javadoc

Days earlier, astubbs#260 (merged as `c42cd322`) diagnosed the same class of defect in
`queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown`, flaking as `[1, 1, 2]` where
`[1, 2]` was expected (at a rate of 3/45 CI runs, per that commit's message). PC re-commits the same base offset when a completing record cannot advance
the frontier, so the fix made the comparison tolerant: `KafkaTestUtils#collapseRepeatedCommits` was
introduced, keyed on the partition and applied before the genesis trim, and
`awaitForCommitExact(int partition, int offset)` moved to `atLeastOnce()`.

That work **did** state the general rule, twice. From `collapseRepeatedCommits`'s javadoc:

> how many such commits occur depends on where the wall-clock commit ticks fall, so it is not something
> a test can assert on

and from `awaitForCommitExact(int, int)`'s:

> Requiring exactly one turns a wait into an assertion about where the wall-clock commit ticks happened
> to fall.

Both are correct and both are narrow. Each is bound to *repeat commits of the same offset*, and each
lives in the javadoc of the helper that handles repeats - a place you only read if you are already
holding that helper. `git grep` across `docs/solutions/` finds no learning doc for astubbs#260 at all:
the reasoning existed in a commit message and two code comments, and nowhere a future author would meet
it before writing a new assertion.

So the tolerance built was for repeats, and the trap that fired next was **an extra distinct
intermediate offset on the way to the same frontier**. Every mechanism astubbs#260 built passes it
straight through:

- `collapseRepeatedCommits` collapses only a repeat of the *immediately preceding* commit for that
  partition, so `1` then `3` is untouched.
- `assertCommitLists` already ignored repeats independently, by collecting each partition's offsets into
  a `HashSet` (grep `partitionToCommittedOffsets`). A distinct offset is simply a new set element.

The narrowness is structural, not an oversight in one helper. And this test had already been fixed once
before for a *third* shape of the same family: astubbs#101 addressed the mirror-image symptom, a commit
that never happened (session history). Three point-fixes to one assertion family is the signal that the
family, not the instance, is what needed writing down.

The generalisation was available at the time and cost one sentence: **never assert the sequence or set
of commits, only where they ended up.** Written somewhere a reviewer would meet it, it would have caught
this test at review, because the rewritten `assertCommitLists` expectations violate it on their face.

This is also a specialisation of a rule the repo had already stated elsewhere and equally locally - "do
not compare two moving values; await a quiescent state, then read both", recorded inside one row of
`docs/inflight/test-untracked-ci-flakes.md`. A commit history is a moving value; the frontier is the
quiescent reading of it.

*(Citation repair: that row is gone. Its entry closed once astubbs#265 fixed and re-enabled the test,
and the rule text went with it - which is this write-up's own complaint about local recording,
arriving. Read the row as this document read it:*

```
git show b42733abef45e792df6fca1b3fb8d49d7dfc7946:docs/inflight/test-untracked-ci-flakes.md
grep 'do not compare two moving values'
```

*`b42733abe` is astubbs#288, the commit that RECORDED the rule, and it is on `master` - deliberately
not the commit that removed it, which lived only on the removing branch and would not survive that
branch's squash-merge or deletion. A repair whose own pointer expires is not a repair. The claim
above is left as written: the rule WAS recorded there, and that it no longer is does not change what
was true on 2026-08-13.)*

## Prevention

**The rule.** Assert the frontier - the highest committed offset for a partition, i.e. where consumption
would resume. Assert an exact set or sequence only at a checkpoint where **no intermediate state is
reachable**, which in practice means no record has completed since the last checkpoint.

**Reach for these:**

```java
// Best: the frontier per partition, invariant under tick placement.
awaitFrontier(0, 3);

// A wait for one partition reaching one offset; atLeastOnce, deliberately.
// See its javadoc for why exactly-one would be an assertion about tick timing.
awaitForCommitExact(partition, offset);

// Set-wise across the flattened history; tolerates repeat commits via collapseRepeatedCommits.
assertCommits(of(3, 4));

// Exact per-partition sets are FINE where nothing has completed yet:
await().timeout(defaultTimeout).untilAsserted(() -> assertCommitLists(of(of(), of(4))));
```

**Avoid this:**

```java
// An exact per-partition set at a checkpoint where completion has already occurred.
// [1, 3] and [3] are both correct; which one you get is the tick.
await().timeout(defaultTimeout).untilAsserted(() -> assertCommitLists(of(of(3), of(4))));
```

**Review checklist for any new commit assertion:**

1. Has any record completed since the previous checkpoint? If yes, an intermediate commit is reachable
   and the exact-set form is a timing assertion. Use the frontier.
2. Is the expected value the *exclusive* offset (`highestSequentialSucceeded + 1`)? Finishing 0-2
   commits `3`.
3. Is any expected value actually a partition's **base offset** rather than progress? Records are
   numbered by a global counter, and the flattened helper only trims a genesis of `0`.
4. Green locally is not evidence here. Ask instead: *is there a tick placement that makes this assertion
   false?* If you can name one, the assertion is already wrong.

**The remainder, swept.** The strict call sites left standing after astubbs#260 and astubbs#264 were
checked against the rule rather than left as a prediction. **None is exposed**, and the reason gives
the criterion in its sharpest form:

> The exact form is only unsafe where a partition can pass through a **non-genesis intermediate
> frontier** that the expected value omits. That needs three or more records on one partition
> completing such that the frontier advances in *two* steps between checkpoints.

- `assertCommitLists(of(` - the four sites outside `processInKeyOrder` are all two-records-per-partition
  scenarios. Partition 0 goes base-`0` (trimmed as genesis) straight to its next frontier, and partition
  1's expected values `{2}`, `{2,3}`, `{2,3,4}` *are* the successive frontiers, so no omitted
  intermediate exists to be caught out by.
- `awaitForCommitExact(int)` - the single-argument overload, documented as staying strict "because it
  asserts the whole commit list". Its two-argument sibling was relaxed by astubbs#260; this one was not,
  and does not need to be: every call site commits a single record (`VertxTest` twice, and
  `ParallelEoSStreamProcessorTest` at offsets 0, 2 and 1), and the only value between base and frontier
  is the genesis `0` that `assertCommits` trims.

`processInKeyOrder` was the only site with the three-record shape - partition 0 holding offsets 0, 1
and 2, with 2 completing before 0 - which is exactly why it was the only one that failed. Relaxing the
others would have cost assertion strength for no defect.

**And the meta-rule that produced this document:** when a fix is justified by a general principle, write
the principle where the next person will meet it, not only in the javadoc of the helper that implements
the narrow case. astubbs#260's reasoning was excellent and reached exactly one call site's worth of
future readers.

## Related

- Fix: `e8c9bb12`, on astubbs#264 (OPEN, branch `test/inactive-test-remediation`) - unmerged as of this
  writing. The re-enable that introduced the defect is `dbbfd62c`, same PR. Both SHAs are branch-local
  and will not survive a squash merge; astubbs#264 is the durable reference, and the branch is the place
  to grep for `awaitFrontier` if they no longer resolve.
- The earlier instance of this class: astubbs#260, merged as `c42cd322`. The earlier-still instance on
  this same test family: astubbs#101.
- Failing run: 31654978866, job 94307427273 ("Unit Tests").
- The test was dark from upstream `c1fefbc64` (2020) until `dbbfd62c`.
- `docs/test-hardening/inactive-tests-audit-2026-08-08.md` section 1.2 predicted this: "the test asserts
  exact commit sets, which is precisely the class of assertion that commit rewrote everywhere else in
  the file". Read it as the prediction that came true, not as a doc needing repair.
- Same family, different mechanism:
  [`unforceable-trigger-commit-lock-timeout-2026-08-07.md`](unforceable-trigger-commit-lock-timeout-2026-08-07.md)
  and
  [`vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md`](vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md)
  - an await that tests nothing, versus an await that tests the machine.
- Not a load-tightness flake, and worth ruling out explicitly: there is no margin to widen. Once the tick
  has landed the condition is permanently false, so the await always burns its full budget. Compare
  [`parallel-integration-tests-flaky-under-concurrency-2026-07-28.md`](parallel-integration-tests-flaky-under-concurrency-2026-07-28.md).
