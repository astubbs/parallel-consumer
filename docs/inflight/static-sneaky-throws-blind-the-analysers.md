# `@SneakyThrows` defeats SpotBugs dataflow, at a couple of dozen sites in main code

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-vetted: 2026-09-08 - applied: the accepted count correction, done by replacing the numbers (title included) with the greps that yield them, per this directory's rule against writing down a count; checked: the blind spot is still open and untracked elsewhere, the `docs/solutions/best-practices/` write-up it rests on is still here, and both greps run clean in the tree - main code has drifted upward from the 21 recorded in 2026-08-26 and the test figure matches neither number previously written down, depending on whether `src/test-integration/` is counted -->

**The question this note exists to answer was asked in review and had no home:** is removing sneaky
throws tracked anywhere, given they cost us analysis coverage? It was not. It was one row inside
[`static-spotbugs-rule-registry.md`](static-spotbugs-rule-registry.md)'s ranked next-five, which is
the right place for a *rule* and the wrong place for a *code change spanning every `@SneakyThrows`
in main code*.

## What is established

[`docs/solutions/best-practices/sneaky-thrown-checked-exceptions-defeat-spotbugs-dataflow.md`](../solutions/best-practices/sneaky-thrown-checked-exceptions-defeat-spotbugs-dataflow.md)
records the mechanism: a checked exception thrown without being declared is invisible to SpotBugs'
dataflow, so any analysis that reasons about what a method can throw reasons about the wrong method.
That write-up is the evidence; this note is the open work it implies.

How many, and where, is a grep rather than a number written down here - the figure recorded when this
note was opened was already wrong by the time it was re-read:

```bash
grep -rn "@SneakyThrows" --include=*.java parallel-consumer-*/src/main/    # main code - the scope of this note
grep -rn "@SneakyThrows" --include=*.java parallel-consumer-*/src/test*/   # test code - out of scope, see below
```

Main code is the smaller set by a wide margin, and it is where the analysis coverage is bought back.

`EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS` fires 12 times and sits at rank 4 in the registry's
next-five, marked *investigate first* precisely because nobody has established whether those twelve
are style or the visible edge of this blind spot.

## Why this is not simply "remove them"

Lombok's `@SneakyThrows` is used here to keep checked exceptions out of lambda bodies and functional
interfaces, which is a real constraint rather than laziness - the alternative at most sites is a
wrapper exception, and the codebase already has `PCInternalRuntimeException` for that. So the work is
a judgement per site, not a sweep:

- where the sneaky throw crosses a **public API boundary**, it is a contract question, not a tidy-up;
- where it is inside a lambda passed to the user's function, removing it changes what the user sees;
- where it merely avoids declaring `throws` on a private method, it is free to remove.

**Do not start with a global find-and-replace.** The point is analysis coverage, and coverage is
bought back site by site.

## What would settle it

1. Read the 12 `EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS` sites and record which are the free case
   above. That is the cheapest evidence about whether the remaining main-code uses matter.
2. If the free case dominates, remove those and re-measure whether any SpotBugs finding appears that
   was previously invisible - a finding that only shows up after the removal is the whole argument.
3. If it does not, close this note saying so. "We looked and the blindness costs us nothing
   measurable" is a complete answer and better than leaving it open forever.

Test code is explicitly out of scope until main is settled - it is several times the main-code set
(the second grep above), and the analysis value there is lower.
