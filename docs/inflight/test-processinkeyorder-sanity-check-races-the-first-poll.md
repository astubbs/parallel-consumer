# `processInKeyOrder` fails its own sanity check under load - a second, undocumented flake in that test

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

`ParallelEoSStreamProcessorTest.processInKeyOrder` has a documented flake and a landed fix -
[`docs/solutions/test-flakiness/assert-the-commit-frontier-not-the-tick-path.md`](../solutions/test-flakiness/assert-the-commit-frontier-not-the-tick-path.md),
fixed by astubbs#264, which is merged. **This is not that one.** The symptom there was a commit-list
mismatch from an extra intermediate commit; this one fires earlier, in the test's own preamble:

```
java.lang.AssertionError:
[sanity check input data]
Actual and expected should have same size but actual size is: 0 while expected size is: 9
Actual was: []
```

The assertion is `assertThat(polled).as("sanity check input data").hasSameSizeAs(locks)`, placed
immediately after `awaitForOneLoopCycle()`. It asserts that the mock consumer has already handed
over every seeded record after one control-loop cycle - which is a claim about how much the machine
got done in that window, and the same defect class the solutions write-up above names: *"a test that
pins the speed of the machine it runs on."* The fix that landed hardened the commit assertions
further down and left this line alone.

**Observed rate, and the conditions, because a verdict without them is not evidence.** Local,
macOS, JDK 17, several agent sessions sharing the box:

| Arm | Result |
|---|---|
| Full default build (`bin/build.sh`), branch under heavy concurrent load | red, 2 of 2 runs - `[2]` and `[3]` in one, `[3]` in the other |
| Full default build, same branch, quieter box | green |
| Full default build, unmodified `master`, same box, same window | green |
| The method alone (`-Dtest=ParallelEoSStreamProcessorTest#processInKeyOrder`) | green, 3 of 3 |

**The "unmodified `master` ... green" row above has since been refuted - read the last sighting
before relying on this table.** A later control arm ran the same suite on unmodified `master`, on a
box with nothing else on it, and got *three* failures where the branch under concurrent load got
one. Load still moves it, but load is not what makes it possible.

**It is not PR-state.** The work that observed it - the polyglot build scaffolding under
`parallel-consumer-proxy-clients/` - adds a module tree and touches no file under
`parallel-consumer-core`, and the failing parameterisations varied between the two red runs. Load is
the variable that moves it, which is why it has been seen locally and not on a CI runner - and
equally why a loaded runner will eventually see it.

Not quarantined: quarantine wants a diagnosis or a sighting ledger proving master-state, and one
machine under self-inflicted load is neither. This note IS the start of that ledger - add sightings
rather than rewriting it, and if CI produces one, that is the evidence the quarantine registry
wants.

## Sightings

<!-- post-merge: checked-begin -->
<!-- The row names the PR rather than its branch, because a PR outlives the branch it merges from,
     and it describes a run that had already happened when it was written - so nothing in it depends
     on that PR still being open. -->

| When | Where | What was seen |
|---|---|---|
| 2026-09-01 | astubbs/parallel-consumer#390 (the ten foreign dispatch clients), full reactor `test` with `-Dpc.foreignClients`, macOS | `processInKeyOrder(CommitMode)[1]` red on the **sanity check**, `actual size is 0 while expected size is 9` - the same assertion and the same shape as the original sighting, a different parameterisation again. Same load condition: a second agent was running `parallel-consumer-core` tests in another worktree on the same box at the time. That PR's diff touches no file under `parallel-consumer-core`. |
| 2026-09-01 | the same, second run, on a box with nothing else running | red again, and wider: `processInKeyOrder` failed on **all three** parameterisations plus `inFlightMessagesCommittedIfProcessedDuringShutdown[3]`, same assertion each time. **A control arm was run and it does not support "load".** The identical command on the base branch, same box, same window: green. The method alone on the branch: green 3 of 3. `parallel-consumer-core` is byte-identical between the two branches, so nothing in the diff can explain the split - and CI's `tests` job, which runs the same suite on a Linux runner, was green on that PR. Reported as an unexplained rate rather than a verdict: red 2 of 2 locally on one branch, green 1 of 1 on its base, green on CI. |
| 2026-09-01 | astubbs/parallel-consumer#293 (the language-proxy engine), while merging the Wagon A stack into it; macOS, JDK 17 | **The control arm here reddens `master` itself, which no previous sighting had done, and it is the first evidence that this is master-state rather than branch-state.** On the merged branch, full default reactor `test` with another agent's `parallel-consumer-streams` suite running in a second worktree: `processInKeyOrder(CommitMode)[1]` red on the sanity check, 1 failure in 543. The control - unmodified `master` at `b2e6c190d`, `-pl parallel-consumer-core -am test`, same box, **nothing else running** - was **redder**: `processInKeyOrder` failed on **all three** parameterisations, 3 failures in 533, same assertion at the same line. The method alone on the branch: green 3 of 3. So the branch arm is not the worse arm, and the quieter box is not the greener one - which retires "load is the variable that moves it" as the whole story, and specifically refutes the row above's "the identical command on the base branch was green". Whatever this is, it is in `master` and it does not need a proxy branch to appear. |
| 2026-09-01 | astubbs/parallel-consumer#331 (the ten per-language demos), while forward-merging astubbs/parallel-consumer#328 into it; macOS, JDK 17 | `processInKeyOrder(CommitMode)[3]` red on the sanity check, 1 failure in 543, same assertion and same line. **The strongest same-day control arm so far, and it points the same way as the row above.** `parallel-consumer-core` is byte-identical between that merge's result and the one a rung below it - `git diff --cached feats/java-sidecar-demo -- parallel-consumer-core` is empty - and the rung below ran the identical full-reactor command **five times on the same box within the same few hours: 543 tests, 0 failures, every time**. So one tree red once and a byte-identical tree green five times, with no code difference to appeal to. That is a rate, not a cause: it rules out both branches' diffs and leaves the test itself. |
| 2026-09-01 | astubbs/parallel-consumer#340 (the FFI rung), while forward-merging astubbs/parallel-consumer#331 into it; macOS, JDK 17 | **The widest local sighting yet, and the first with three byte-identical trees to compare.** 5 failures in 543 - `processInKeyOrder[1]`, plus `inFlightMessagesCommittedIfProcessedDuringShutdown[1]` and `[3]` and `executorThreadsInterruptedOnShutdownTimeout[1]` and `[3]` - all on the same `[sanity check input data]` assertion, `actual size is 0`. That cluster is the "red again, and wider" shape two rows above, so the three tests move together rather than being three flakes. **The whole day's tally on one box, over three trees whose `parallel-consumer-core` is byte-identical (`git diff --cached <lower rung> -- parallel-consumer-core` empty at each link):** the demo rung green 5 runs of 5; the per-language rung red 1 of 543 on one run, green on the next; this rung green on its first run and red 5 of 543 on its second. Same code, same box, same afternoon, and the outcome flips run to run - so the remaining variable is the run, not the tree. Nothing in any of the three diffs touches `parallel-consumer-core`. |
<!-- post-merge: checked-end -->

The fix, when somebody takes it, is the same rule the solutions write-up already states: await the
condition the test actually depends on (every seeded record polled) rather than a fixed number of
loop cycles, so the sanity check cannot be reached before it can be true.
