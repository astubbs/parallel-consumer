<!-- post-merge: checked -->
# Wiring the Lincheck lane into CI: one arm does not fire on the machine that ran it

<!-- inflight-type: task -->
<!-- inflight-labels: concurrency -->
<!-- inflight-impact: test-debt -->

<!-- post-merge: checked-begin -->
**HANDOFF. The PR that carries this note wires the Lincheck lane into CI as a gating leg, and the
lane is RED on the machine it was verified on.** That is deliberate: the branch is being handed over
with the failure measured and diagnosed as far as the evidence goes, rather than made green first.
Whoever picks this up decides the fix; this note is the evidence they would otherwise have to
re-derive, including the hypothesis that looked obviously right and is refuted.

[`test-lincheck-lane-open-items.md`](test-lincheck-lane-open-items.md) **owns the lane itself** - its
coverage gaps, the stress-arm calibration method, and the 3.4x machine-dependence finding. This note
owns only the gating attempt and the one arm blocking it. Where they disagree, that one wins.

## What the PR does

Six Lincheck test classes and `bin/lincheck-test.sh` landed with astubbs#347 and were never wired
into CI: no workflow referenced them, and every CI script passed `-Dexcluded.groups=...,lincheck`, so
the tag was excluded everywhere and included nowhere. That is the case the root `AGENTS.md` names
directly - a test that never runs is not a passing test, and nothing goes red to tell you - and it
held for the lane's whole life. The PR adds a `Lincheck` leg to the `test` matrix in
`.github/workflows/maven.yml` and corrects the runner script's header, which described the lane as
"non-gating and opt-in" when the truth was that nobody had connected it.

## The failure

`ShardManagerLincheckTest.stressMustNotRediscoverTheShardTear`, and only that one - the other five
classes pass and the lane costs about 2m50s.

```
Lincheck completed WITHOUT finding a violation. On this tree that means either the bug is gone
(invert this test) or the harness is not exercising it.
```

The arm is **inverted**: it calls `LincheckHarness.runExpectingViolation`, so it needs Lincheck to
find *something*, and then asserts that what was found contains no `NullPointerException` - a
regression detector for astubbs#345's fix. When nothing is found at all, the `assertThrows` fails
before the real assertion is ever reached.

**It is consistent, not flaky.** Eight consecutive runs at the committed bound
(`iterations(50)`, `invocationsPerIteration(5_000)`) all missed. A flake would not do that.

**The runner propagates the failure correctly** - `bin/lincheck-test.sh` exits 1 - so there is no
silent-green problem to chase. Only the arm is at issue.

## The hypothesis that looked right and is REFUTED - do not re-run it

The obvious reading, and the one this note exists to stop the next agent spending an hour on:

> The violation this arm relies on is the `ProcessingShard#addWorkContainer` check-then-act that
> `test-lincheck-lane-open-items.md` describes as its counterexample. astubbs#336 rewrote that
> method - the code now reads "ADMIT FIRST, then let the map itself say what happened - never the
> read above" - so the bug is gone and the arm should be inverted again.

The correlation is strong: astubbs#336 rewrote 182 lines of `ProcessingShard` **and edited that note
in the same commit**, which is why the note reads as current while describing a defect that commit
addressed.

**The control refutes it.** Restoring the genuine pre-astubbs#336 shape - `workMap.get(key)`, branch
on the read, `workMap.put` inside each branch, so the check-then-act window is back - produced **no
violation either**, over three runs. If the fix were what silenced the arm, reintroducing the defect
would restore the counterexample. It does not.

So the harness is **not exercising the seam on this machine at this bound**. That is the second
branch of its own message, not the first.

**One earlier control was invalid and its result should be ignored** if it turns up in the
transcript: a first attempt kept the `put` atomic and only moved the population accounting, so it
never reintroduced a check-then-act at all. Reading the real pre-astubbs#336 method before writing a control
is the lesson; `git show 3e668a448^:<path>` is how.

**Caveat on the valid control, stated so it can be tightened rather than trusted.** It restored the
get/branch/put structure but kept `population.onAdmitted()` in the arriving branch and dropped
`onRetired`, because `RecordPopulation` post-dates the code being reverted. The structural feature
the counterexample turns on is back; the accounting around it is not byte-for-byte the old method. A
stricter control checks out the whole pre-astubbs#336 file and fixes up compilation.

## What is open, in the order worth doing

1. **Does a larger bound make it fire?** This is the experiment that separates "under-powered here"
   from "unreachable here", and it was running when the branch was handed over - `iterations(500)`,
   ten times the committed bound, on the clean tree. Re-run it and record the answer, because
   everything below branches on it. If it fires, the bound was priced on another machine, which is
   exactly the 3.4x finding the lane's own note warns transfers to no other machine.
2. **Decide what the lane gates on.** The commit's argument for gating is that *a Lincheck violation
   is a real finding rather than a timing wobble, because the model checker explores interleavings
   deterministically instead of sampling them*. That is true of the model-checking arms and **false
   of the stress arms**, which sample - so the argument never covered the arm that failed. Gating the
   model-checking arms while running the stress arms advisory is the split the reasoning actually
   supports, and it is the recommendation this note hands over unless (1) changes the picture.
3. **Do not reprice the bound to go green.** The lane's own note establishes that no single-machine
   calibration of a stress arm transfers, so a bound tuned until this box passes is a bound priced on
   this box - the same mistake one layer along. And the root `AGENTS.md` rule binds regardless: a
   test failing under stress may be exposing a real bug, and loosening it is not a diagnosis.
4. **Do not add a retry.** This repo removed surefire reruns because they retried failures into green
   and hid three flakes. Demote to advisory before masking anything.

## Reproducing

```bash
LINCHECK_TEST=ShardManagerLincheckTest bin/lincheck-test.sh   # the failing arm alone
bin/lincheck-test.sh                                          # the whole lane
```

Both need `JAVA_HOME` on Temurin 17. The lane is not cheap: `WorkManagerLincheckTest`'s inverted arm
cannot stop early and always pays its full bound, which is most of the 2m50s.
<!-- post-merge: checked-end -->
