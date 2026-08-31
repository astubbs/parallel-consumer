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
<!-- post-merge: checked-end -->

The fix, when somebody takes it, is the same rule the solutions write-up already states: await the
condition the test actually depends on (every seeded record polled) rather than a fixed number of
loop cycles, so the sanity check cannot be reached before it can be true.
