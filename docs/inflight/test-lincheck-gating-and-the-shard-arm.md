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

**Read [*What CI added*](#what-ci-added)
first.** The lane has since run in CI, and the result rules out two of the three remaining causes -
so the open question is narrower than the rest of this note was written to leave it.

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

1. **The bound is NOT the answer - that experiment is done, and it came back negative.**
   `iterations(500)`, ten times the committed bound, on the clean tree: no violation, twice. Taken
   with the control above, neither *reintroducing the defect* nor *spending ten times the search
   budget* produces a violation here. So this is not an under-priced bound and not the 3.4x
   machine-dependence the lane's note describes - those would both yield hits at some cost. The
   harness is not reaching the seam on this machine at all, and the next question is why.

   **What has not been ruled out**, roughly in order of cheapness: the JDK or Lincheck version
   differing from the one that priced the arm; the operation set no longer producing the interleaving
   after astubbs#335 and astubbs#373 also rewrote `ProcessingShard` (both post-date the harness);
   or a host property - core count, scheduler - that changes which interleavings stress mode reaches.
   Confirming the arm still fires *anywhere* is the cheap first move: run it on the machine that
   wrote it, or in CI, before spending anything on the harness.

   **DONE, and it narrows the list to one.** See *What CI added* below: two of those three are now
   ruled out, and the surviving one is the operation set.
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

## What CI added

**The arm misses on a second, structurally different host - but stress mode works there.**

The cheap first move above has been run. The lane executed on this PR's head for the first time -
`ubuntu-latest`, four cores, the opposite end of the range from the 32-core box - and the arm
**missed again**, with the identical message. Nine tests, one failure, and it is the same one.

**The decisive part is not the miss - it is what passed alongside it.** On that same runner, in that
same JVM, two *other* stress arms found their violations:

| Arm | Mode | Outcome on `ubuntu-latest` |
|---|---|---|
| `LincheckToolchainProbeTest` control probe | stress | violation found |
| `LincheckToolchainProbeTest` control probe | model checking | violation found |
| `PartitionStateLincheckTest` | stress | violation found |
| `ShardManagerLincheckTest.stressMustNotRediscoverTheShardTear` | stress | **no violation** |

The toolchain probe exists to answer exactly this question, and it answers it: Lincheck's stress
strategy works on that host, the JDK is adequate, the `-Plincheck` flags are landing, and an inverted
stress arm can and does fire. So the three candidates listed above collapse to one:

- **A JDK or Lincheck version difference - ruled out.** The probe's stress arm fires on both hosts.
- **A host property (core count, scheduler) - ruled out.** 32 cores and 4 cores agree, and a second
  production stress arm fires on the 4-core host.
- **The operation set no longer producing the interleaving - the only one left standing.**
  `astubbs#335`, `astubbs#336` and `astubbs#373` all rewrote `ProcessingShard` after this harness was
  written. The next question is not about bounds or hosts at all: it is whether this arm's declared
  operations can still reach the seam they were written against.

**This also re-reads the earlier control.** Reintroducing the check-then-act produced no violation
either - which under the old three-candidate list was ambiguous, and under this one is corroborating:
if the operations no longer reach the seam, restoring the defect *at* the seam would change nothing,
which is what was observed. The caveat on that control still stands, and a stricter one (whole
pre-astubbs#336 file, compilation fixed up) is now worth more than it was.

**Cost correction.** The lane is ~2m50s locally but **7m42s** on a hosted runner, `WorkManagerLincheckTest`
alone accounting for 364s of it. The matrix entry's `timeout: 20` still covers it; the `~2m30s`
estimate in that entry's comment does not, and should be corrected whenever the entry is next touched.

Evidence: run `33511221453`, job `99867204560`, head `58c8c7cd1`.

## Reproducing

```bash
LINCHECK_TEST=ShardManagerLincheckTest bin/lincheck-test.sh   # the failing arm alone
bin/lincheck-test.sh                                          # the whole lane
```

Both need `JAVA_HOME` on Temurin 17. The lane is not cheap: `WorkManagerLincheckTest`'s inverted arm
cannot stop early and always pays its full bound, which is most of the 2m50s.
<!-- post-merge: checked-end -->
