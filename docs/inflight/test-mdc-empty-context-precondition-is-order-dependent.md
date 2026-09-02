# `anEmptyCallerContextIsHandledAndNothingLeaks` asserts `null` where the MDC gives `{}`

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

<!-- post-merge: checked-begin - a dated sighting, attributed to the PR it was seen on; both stay true -->
`MdcContextPropagationTest.anEmptyCallerContextIsHandledAndNothingLeaks` failed the core unit suite on
2026-09-01, run locally on astubbs#262:
<!-- post-merge: checked-end -->

```
MdcContextPropagationTest.anEmptyCallerContextIsHandledAndNothingLeaks:194
  expected: null
   but was : {}
```

**It is the test's own PRECONDITION that failed, not its subject.** Line 194 is the first assertion in
the method, before any PC involvement:

```java
// deliberately NO MDC.put here - the caller has no diagnostic context at all
assertThat(MDC.getCopyOfContextMap()).isNull();
```

So this says nothing yet about MDC propagation. It says the JUnit thread arrived at this test carrying
an empty-but-non-null context map.

## Master-state, not PR-state

<!-- post-merge: checked-begin - the control that made this master-state, stated as evidence gathered -->
astubbs#262 changes nothing MDC-related - `git diff origin/master...HEAD` touches no `Mdc*` file, no
`ParallelConsumerOptions.propagateMdc`, and no worker-pool lifecycle. MDC propagation arrived on this
branch from master with astubbs#205.

The stronger evidence is a control that was already to hand: the immediately preceding full run of the
same suite, on the same tree apart from a test-helper extraction (`ProduceLockHandover`, which touches
only three transactional tests), was **567 tests, 0 failures**. Same code, same machine, minutes apart,
different outcome - which is order or scheduling, not the diff.
<!-- post-merge: checked-end -->

## Second sighting, on a different branch - and it narrows the hypothesis

<!-- post-merge: checked-begin - a dated sighting attributed to the PR it was seen on, in the past
     tense; both stay true once that PR lands -->
Seen again 2026-09-02 on astubbs/parallel-consumer#207, in the core unit suite immediately after that
branch merged master. Identical failure, identical line: `expected: null but was: {}` at the
precondition. That branch changes offset decoding and documentation and touches nothing MDC-related,
so **the two sightings are on two branches with no code in common** - which is the branch-independence
this note's own "master-state" argument previously had to make from a neighbouring run instead.

**The class ALONE passed 5 of 5 on the same tree, minutes later.** That is the control the sighting is
worth recording for, because it bears on the hypothesis below rather than just repeating it: if the
arming `MDC.put` were a sibling *in this class*, running the class by itself should still lose whenever
JUnit ordered a putting method first. It did not. One green isolated run does not prove within-class
ordering can never arm it, but it points the search at a **different class sharing the fork's JUnit
thread**, not at this one's siblings - which is a cheaper thing to falsify than the original wording
suggests.
<!-- post-merge: checked-end -->

## Third sighting - on MASTER, and it is holding up somebody else's fix

<!-- post-merge: checked-begin - a dated observation about master's own build; independent of any
     branch, so it stays true whatever lands -->
2026-09-02, master's own `full build (master)` job at `ebd8bcb19`: **581 tests, 1 failure**, this
method, this assertion. That ends the master-state argument - no branch attribution is needed for a
failure on master itself, and the two branch sightings above are corroboration rather than the case.

**It is not only a red build, it is blocking a fix that is already merged.** The `build` job is the
only one that uploads coverage on master. It fails here, so nothing uploads; astubbs/parallel-consumer#414
landed the corrected coverage globs and its own stated proof - master's `unit` and `integration` flags
no longer reporting one identical figure - **has not happened**, because the job that would produce it
has not completed since. `node bin/inflight.mjs codecov` still shows both flags at the same number.
Every PR therefore keeps comparing against a base assembled the old way and keeps seeing red per-flag
gates, which reads as each PR's own fault.

So the cost of this test is no longer "one confusing red in a suite". It is: master red, no coverage
upload, a merged fix that cannot take effect, and a misleading gate on every open PR.
<!-- post-merge: checked-end -->

## The hypothesis, and it is NOT yet established

`@AfterEach` already calls `MDC.clear()`, so this is not a missing teardown. The candidate mechanism is
that on logback 1.6.1 (`<logback.version>1.6.1</logback.version>` in the root pom) `MDC.clear()` leaves
an empty map rather than nulling the thread-local, so `getCopyOfContextMap()` returns `{}` and not
`null` for any thread that has previously touched the MDC. Every other test in the class does
`MDC.put(...)`, so whichever of them runs first on a given JUnit thread would arm this.

That would make the failure **order-dependent and deterministic**, not random: green when this method
runs before its siblings on its thread, red when after.

**`branch-mdc-context-propagation.md` names this exact hazard from the other side**, which is why the
hypothesis is worth testing rather than inventing: "the zero is a property of logback's `MDCAdapter`,
which returns `null` for an empty context. Some other SLF4J bindings return an empty map instead."
The test encodes the logback-returns-null behaviour as a *precondition*, and an empty map is the same
state by any meaning that matters to the product.

**Falsification.** Run the class with a forced method order, or call `MDC.clear()` on a thread that has
had a key and read `getCopyOfContextMap()` directly. If it returns `{}`, the hypothesis holds and the
fix is in the assertion (accept null-or-empty, e.g. assert the map is null or empty rather than
strictly null) plus a note on the precondition saying why both are legal. If it returns `null`, the
hypothesis is dead and something else is leaving a map behind - look for a sibling test or a shared
base that calls `MDC.setContextMap(...)`.

## Deliberately not quarantined

Rule 1 of [`docs/quarantined-tests.md`](../quarantined-tests.md) wants a diagnosis or a sighting
ledger, and this has three sightings - one of them master's own build - and an untested hypothesis. Quarantining now would hide a test that
may be catching a real product-side asymmetry, and the registry is currently empty - putting the first
entry back on a guess is the wrong trade. Diagnose it first.

## Why it matters beyond one red run

The product treats `null` and `{}` as the same thing - both mean "no diagnostic context" - and
`MdcPropagation` is written to. If the test insists on one of them, it will keep going red on binding
or logback changes that alter nothing a user can observe. That is a test asserting an implementation
detail of the logging framework while claiming to assert PC's behaviour, which is the direction that
wastes the next person's time.
