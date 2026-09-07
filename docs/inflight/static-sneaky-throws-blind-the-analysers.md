# `@SneakyThrows` defeats SpotBugs dataflow, and there are 21 of them in main code

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->

**The question this note exists to answer was asked in review and had no home:** is removing sneaky
throws tracked anywhere, given they cost us analysis coverage? It was not. It was one row inside
[`static-spotbugs-rule-registry.md`](static-spotbugs-rule-registry.md)'s ranked next-five, which is
the right place for a *rule* and the wrong place for a *code change spanning 21 sites*.

## What is established

[`docs/solutions/best-practices/sneaky-thrown-checked-exceptions-defeat-spotbugs-dataflow.md`](../solutions/best-practices/sneaky-thrown-checked-exceptions-defeat-spotbugs-dataflow.md)
records the mechanism: a checked exception thrown without being declared is invisible to SpotBugs'
dataflow, so any analysis that reasons about what a method can throw reasons about the wrong method.
That write-up is the evidence; this note is the open work it implies.

Counted 2026-08-26: **21 `@SneakyThrows` in main code**, 122 in test code.

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
   above. That is the cheapest evidence about whether the other nine main-code uses matter.
2. If the free case dominates, remove those and re-measure whether any SpotBugs finding appears that
   was previously invisible - a finding that only shows up after the removal is the whole argument.
3. If it does not, close this note saying so. "We looked and the blindness costs us nothing
   measurable" is a complete answer and better than leaving it open forever.

Test code is explicitly out of scope until main is settled: 122 sites, and the analysis value there
is lower.
