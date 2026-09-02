---
title: "A test asserting the MDC is null was armed to fail by whichever earlier class in the same fork had put and removed a key"
date: 2026-09-02
category: test-flakiness
module: parallel-consumer-core
problem_type: test_design_bug
component: testing
severity: medium
root_cause: precondition_asserted_a_logging_framework_detail_null_vs_empty_map_that_an_earlier_class_on_the_same_thread_decides
resolution_type: test_fix_establishes_its_precondition_and_accepts_null_or_empty
status: "SOLVED - test-side fix, reached independently by two sessions with two complementary shapes; both are applied. The control experiment below is the evidence. Nothing enforces the rule beyond this write-up."
applies_when:
  - "A test asserts what a thread-local looks like BEFORE the code under test has touched it"
  - "A unit suite runs classes across reused forks (surefire forkCount with reuseForks), so the class that ran before yours on the same thread is decided per run"
  - "Two values are distinguishable to the assertion library but identical to the product (null vs an empty map, empty string vs absent)"
  - "A flake's rate jumps on a branch whose diff cannot explain it"
symptoms:
  - "expected: null but was : {} on the first assertion of the method, before the product is involved"
  - "Passes alone and in most full runs; fails at a low, branch-independent rate on CI, occasionally several times in a row"
  - "Two consecutive heads of astubbs#416 (a branch with no Java) red on CI at the same :194, then green on the next head - jobs 100271906271 and 100274796081"
  - "Reproduces deterministically by class order alone, with no code change"
  - "Two trees at the same test count disagree: 2 of 2 full runs red on one, 0 of 3 on another, identical MDC code"
related_components:
  - MdcContextPropagationTest
  - MdcPropagation
  - AbstractParallelEoSStreamProcessor
  - ProducerManagerTest
  - TransactionalBulkCommitTest
tags: [mdc, logback, surefire, junit, fork-reuse, fork-distribution, order-dependent, precondition, flake]
---

# A test asserting the MDC is null was armed by whichever earlier class had put and removed a key

## The symptom

`MdcContextPropagationTest.anEmptyCallerContextIsHandledAndNothingLeaks` opens with a precondition -
"the caller has no diagnostic context at all" - written as `assertThat(MDC.getCopyOfContextMap()).isNull()`.
It failed with `expected: null but was : {}` on the JUnit thread, before PC was constructed. Sightings:
locally on 2026-09-01 on the branch that became astubbs#262; on CI on 2026-09-02 on two consecutive heads
of astubbs#416, a branch with no Java in its diff; and then on master's own push build for the astubbs#415
merge, which turned master red. The Codecov test history over the surrounding runs showed it as the only
failure across many branches, at a low rate.

## The hypothesis that was wrong, and the probe that killed it

The first write-up guessed that logback's `MDC.clear()` leaves an empty map behind, so the class's own
`@AfterEach` clear would arm the next method. A one-file probe against the exact jars the build uses
(logback 1.6.1, slf4j 2.0.18) said otherwise:

```
fresh thread, never touched:      null
same thread, after put + clear:   null
same thread, after put + remove:  {}
another fresh thread:             null
```

`clear()` nulls the thread-local. `remove(key)` does not: it leaves the emptied map in place, and
`getCopyOfContextMap()` then returns `{}` forever on that thread until something clears it. So the arming
action is a put-then-remove with no clear after it, on the thread that later runs this test.
`MDC.setContextMap(emptyMap)` leaves `{}` behind the same way.

Two sessions ran this independently, one with a throwaway class against the jars and one in `jshell`,
and got the same table. Ruled out on the way, each by a paired run on one fork and one thread with the
MDC class last:

- **The class's own siblings** through its `@AfterEach MDC.clear()` - the probe above kills it, and the
  class alone on one thread passes 3 of 3.
- **`MdcPropagationTest`**, the obvious suspect with the same teardown - paired with the failing class,
  alphabetical: passes.
- **`ManagedPCInstance`**, which does `MDC.remove(MDC_INSTANCE_ID)` on its caller's thread - lives in
  `src/test-integration`, and no surefire test uses it. Out of the fork.
- A bisect over every test class that calls into the control loop from the runner thread: four of six
  clean, and the two named below each reproduced the failure exactly once per run.

## The mechanism

The only `MDC.remove` in product code sits in the control loop's mailbox processing: it puts the
work-descriptor key around handling a completed work container and removes it after. On a live PC that
runs on the controller thread, which nothing else ever reuses, so the leftover `{}` is invisible. Two unit
test classes drive `controlLoop(...)` directly on the test thread - `ProducerManagerTest` and
`TransactionalBulkCommitTest` - and neither clears the MDC afterwards. From then on that JVM's main thread
carries `{}`.

Whether that matters to this test is decided by fork placement. The ci profile runs surefire at
`forkCount=1C` with `reuseForks`, and JUnit's intra-JVM parallelism is off there, so each fork runs its
classes one after another on one thread, and which classes share a fork is settled per run. Land in a fork
after one of those two classes and the precondition reads `{}`; land anywhere else and it reads `null`.
That is a per-run coin, which is exactly the shape the sighting ledger recorded and could not explain -
and it is why the same tree failed every time at one test count and depended on the run at another:
adding eight tests elsewhere moved the fork split.

The inflight note, extended on astubbs#416 while the bisect was running, reached the same shape
independently from the CI record: no `MethodOrderer`, no JUnit parallelism under `-Pci`, so a sibling
alone would make it red every time or never - what varies per run is which fork the class lands in and
which earlier class touched the MDC there. That was its hypothesis; the bisect and the class-order arm
are the confirmation, with the two classes named.

## The control experiment

One term changed, the class order; everything else identical (one fork, parallelism off, the two classes
named explicitly):

| Arm | `surefire.runOrder` | Order | Outcome |
|---|---|---|---|
| A | `reversealphabetical` | `ProducerManagerTest` then the MDC test | MDC test fails at the precondition, `expected: null but was : {}` |
| B | `alphabetical` | MDC test then `ProducerManagerTest` | both classes pass |

Then arm A again against the fixed test: both classes pass. Command shape, from the core module:

```
./mvnw -pl parallel-consumer-core -am test -Dtest='ProducerManagerTest,MdcContextPropagationTest' \
  -Dsurefire.runOrder=reversealphabetical -Dsurefire.forkCount=1 -Dparallel-tests=false
```

## The fix, and why it is in the test - twice over

Two sessions diagnosed this on the same day without seeing each other's work, agreed on the mechanism,
and fixed the test in two different places. Both fixes are applied, because they answer two different
rules and each covers a case the other does not:

1. **The test establishes the state it asserts.** `clearCallersContext` is `@BeforeEach` as well as
   `@AfterEach` - the shape its sibling `MdcPropagationTest` already had. A JUnit runner thread is shared
   by every class in the fork, and which class ran before yours is decided by fork distribution, so a
   precondition on anything thread-local - MDC, a `ThreadLocal`, interrupt status - is a claim about
   every one of them unless the test makes it true first.
2. **The assertion says what the product means, not how the binding spells it.** PC treats `null` and
   `{}` identically - both mean "no diagnostic context", and `MdcPropagation` is written that way. The
   precondition was asserting an implementation detail of the logging binding and calling it PC's
   behaviour. It now reads the map and, if non-null, asserts it is empty. With the `@BeforeEach` in place
   this reads `null` on logback, so the relaxation looks redundant there - it is not, on a binding that
   returns an empty map for an empty context - a hazard `MdcPropagation`'s class javadoc already names
   from the other direction ("The zero is a property of the binding, not of this class"), where the
   retired inflight note that first recorded it migrated its content. The comment above the assertion points at the class
   javadoc that names the arming classes, so the next reader does not rediscover them.

Also corrected: `MdcPropagation.capture()`'s comment claimed it returns `null`, never an empty map, for
an empty context. It returns `{}` on a thread that has put-and-removed. Both readers (`enter`, `adopt`)
already treat null and empty alike, so no behaviour changed; the comment did.

The product-side leftover is deliberately not touched: `{}` on PC's own controller thread is harmless,
and changing control-loop code to satisfy a test precondition would be the wrong direction. Making the
two arming tests clean up after themselves was considered and rejected by both sessions - the courtesy
would have to be repeated by every future test that borrows the runner thread, and it would hold only
until the next one forgot. The one place the rule can be enforced is the test that depends on it.

## What to take from it

- **A test may assert a precondition only on state it established.** A precondition on shared thread
  state is a claim about every test that ran before yours on that thread. With reused forks, that set is
  decided per run, so the assertion has a per-run outcome even when nothing about the test changes -
  and it reads as a flake in exactly the runs where it matters least.
- **Assert the meaning, not the representation.** Where the product treats two values as one, a test
  that distinguishes them will go red on binding upgrades, on fork placement, on anything that alters
  nothing a user can observe.
- **Kill the hypothesis with a probe before designing the experiment.** The `clear()` story would have
  sent the experiment after method order inside the class, which is fixed and would have shown nothing.
  Fifteen lines against the real jar redirected it to class order, which reproduced on the first try.
- **Run the prior-art search when you start the FIX, not only when you start the diagnosis.** Two
  sessions diagnosed this on the same day, both correctly, and landed sound fixes on different branches
  editing the same lines - the second started after the first had already committed. One
  `bin/inflight.mjs prior-art MdcContextPropagationTest` at that moment would have shown the first
  branch, because that is exactly how the third session found both. A diagnosis that took hours is not
  wasted by that check; a fix that conflicts with a sibling's is.
