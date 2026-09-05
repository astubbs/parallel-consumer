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
<!-- file-refs: N/A - this commit deletes the path named above; naming it is the point of the paragraph -->

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

## The alternative this deletion overrides

`docs/inflight/test-progressbar-width-needs-a-machine-assertion.md` argued for a third option, and is
retired by this change - so its case is recorded here rather than lost with it. It proposed
**splitting** the test: a machine assertion (render to a string or an in-memory sink at a known
terminal width, then assert that the total width is respected, that bar plus label fits, that nothing
wraps, that truncation happens where expected, and what happens at narrow widths and at a width
smaller than the label), plus the visual demo kept deliberately runnable - tagged out of the gating
lanes the way the chaos and performance lanes already are. Its argument was that deleting loses the
ability to *see* the rendering, and that the assertion answers *is the rendering correct* while the
demo answers *does it look right*.
<!-- file-refs: N/A - this commit retires the inflight note named above; naming it is the point of the paragraph, per docs/inflight/AGENTS.md's rule that an overridden argument is migrated before its note is removed -->

Deletion won on three grounds, none of which closes the door on that split:

- **The test asserted nothing**, so nothing a suite relied on was removed. The proposed assertion
  describes a test that would have to be written from scratch, not one being preserved - the deletion
  neither blocks it nor makes it harder.
- **The utility is not going unexercised.** `ProgressBarUtils.getNewMessagesBar` keeps its other
  in-tree callers - every large-volume, load and multi-instance test across core, vert.x, reactor and
  mutiny builds one. `grep -rn "ProgressBarUtils" --include="*.java" .` is the list.
- **The demo's value is a maintainer's call, not a gate's.** Reinstating a tagged, deliberately
  runnable visual check - with or without the machine assertion beside it - stays available at any
  time. What the release gate could not accept was a `@Disabled` test sitting inside the suite while
  running as no part of it, which is the one thing all three options agree on.

## Evidence the deletion is complete

```bash
grep -rn "@Disabled" --include="*.java" .
```

**No live bare `@Disabled` remains on any test class or method.** The command is not silent, though,
and quoting it without saying what it returns invites the next reader to think the gate has slipped.
It matches three kinds of thing, none of them a muted test:

- **javadoc and comment prose** that mentions the annotation historically - in `VertxTest`,
  `Quarantined`, `TransactionalClaimCoverageTest` and `MultiInstanceRebalanceTest`.
- **two `@Disabled` string literals** inside `TransactionalClaimCoverageTest`'s assertion messages
  (`" - is @Disabled"` and `" - its class " + simpleName + " is @Disabled"`). That test *reports*
  unreachable coverage claims, so it names the annotation in the text it prints.
- **one live `@DisabledOnOs(OS.WINDOWS)`** on `AbstractQuarantineScriptTest` - a class-level platform
  guard on tests that shell out to bash scripts, whose own comment says "CI and dev machines are
  POSIX". It shares a substring with the grep, but it is a platform predicate rather than a mute: on
  every machine this project builds on, the class runs.

So the gate holds on its own terms - no test is switched off. Re-run the command rather than trusting
a count written here; the prose matches will keep changing while the conclusion does not.

## Copyright header check

`docs/copyright.md` gives no header treatment for a plain deletion - nothing to register in
`RENAMED_FROM_UPSTREAM`, `EXTRACTED_FROM_UPSTREAM` or `RECOVERED_FROM_UPSTREAM_BRANCH`, since the file
is removed rather than moved, extracted or recovered.
