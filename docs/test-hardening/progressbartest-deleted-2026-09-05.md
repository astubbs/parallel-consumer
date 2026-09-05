# `ProgressBarTest.width` deleted - the last `@Disabled` test on master

**Date:** 2026-09-05
**Scope:** the one test the [2026-08-08 inactive-tests audit](inactive-tests-audit-2026-08-08.md)
left standing under "nothing to cover - leave it alone" (its §1.5). This entry does not repeat that
audit's other findings; read it for the full inventory this one narrows.

## What changed and why

`ProgressBarTest.width()`
(`parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/sanity/ProgressBarTest.java`)
carried `@Disabled("For reference sanity only")` since it was created, `af1fa5de41` on 2020-06-17. It
rendered a progress bar with `Thread.sleep(100)` per step for a human to look at, and asserted
nothing. The 2026-08-08 audit confirmed on three axes - the explicit message, zero assertions, and
placement in the `...integrationTests.sanity` package - that it was a deliberate manual/visual check
with no product behaviour behind it, and recommended leaving it alone.

That recommendation stood until `docs/inflight/release-0.6.0.0.md`'s release gate ("0.6.0.0 does not
ship while any test is disabled") made it the deciding factor: every other test that section named as
disabled had since been fixed, deleted or quarantined by other work (`VertxTest.handleHttpResponseCodes`
deleted, both `ParallelEoSStreamProcessorTest` methods running unconditionally,
`MultiInstanceRebalanceTest.largeNumberOfInstances` moved to `@Quarantined`), leaving
`ProgressBarTest.width` as the sole remaining `@Disabled` test blocking the gate.

**Deleted rather than fixed or quarantined**, because the earlier audit's own diagnosis already rules
out both alternatives: there is no assertion to make pass (fixing implies restoring a behaviour claim,
and none was ever made), and `@Quarantined` exists for a test that is expected to sometimes fail, not
one that asserts nothing by design. A manual visual aid has no home in an automated suite gate.

## Evidence the deletion is complete

```bash
grep -rn "@Disabled" --include="*.java" .
```

Returns no live `@Disabled` annotation anywhere in the tree - only prose in javadoc and a
`@Quarantined` reason string that mention the annotation historically, none of them applied to a test.

## Copyright header check

`docs/copyright.md` gives no header treatment for a plain deletion - nothing to register in
`RENAMED_FROM_UPSTREAM`, `EXTRACTED_FROM_UPSTREAM` or `RECOVERED_FROM_UPSTREAM_BRANCH`, since the file
is removed rather than moved, extracted or recovered.
