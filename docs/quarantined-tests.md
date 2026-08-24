# Quarantined tests - live registry

The task list of tests currently in the quarantine lane: known-failing-on-master tests carrying
`@Quarantined`, excluded from the gating CI suites (so green checks mean mergeable) but still run on EVERY PR push
and after every merge to master (workflow_dispatch on demand) by the "Quarantine Lane / tests"
job - whose test step is `continue-on-error`, so a red quarantined test cannot block a merge
(see [`docs/testing.md`](testing.md)). The lane posts a sticky per-test report comment on the PR (🔴 failing-as-expected / 🟡🎲
flapper passed / 🚨 PASSED) and, when a DETERMINISTIC quarantined test passes (its fix landed), opens
a MERGE-BLOCKING review thread demanding the annotation + entry be deleted (the repo requires
conversation resolution). Tests that pass unreliably are marked `flapping = true` on the annotation -
their passes are report-only.
The seconds-fast "Quarantine Audit" job enforces the rules below on every PR. Each entry is an open
task: get the test fixed and back into the gating lane.

**This file is enforced, not advisory**, by two checks (run locally by `bin/quarantined-test.sh`, on
every PR by the Quarantine Audit job, and again before every lane run):

- `bin/check-quarantine-registry.sh` fails on any drift between the `@Quarantined` annotations in the
  code and the entries below - a quarantined test missing here, or a stale entry for a test no longer
  quarantined, is an error.
- `bin/check-quarantine-owners.sh` verifies each entry's owner claim against reality: the owning PR
  must exist and be open (closed-unmerged = orphaned entry = error; merged with the test still
  quarantined = re-enable overdue = error), and once the quarantine reaches that PR's base branch, its
  merge preview is checked for actually removing the annotation (advisory until it does).

**Machine-parsed format** (the checks depend on it): each entry is an unchecked checkbox line, `- [ ]`,
immediately followed by the backticked test reference (`Class.method`), and its text must contain
`Owner: PR astubbs#NN` (omit only for diagnosed-but-unowned entries, which the checks flag as
advisory). A bare `Owner: PR #NN` still parses, so older entries keep working, but prefer the
qualified form: `bin/check-issue-refs.sh` rejects a bare `#NN` on an added line because the
fork's numbers sit inside confluentinc's range, and before the qualifier was accepted this file
could not satisfy both gates at once. `Owner: PR astubbs/parallel-consumer#NN` parses too.
Unreliable tests carry `flapping = true` on the annotation itself (compile-checked, read by the lane
reporter).

Rules (full discipline in [`docs/testing.md`](testing.md), AGENTS.md, and the `@Quarantined` javadoc):

1. **No quarantine without evidence** - and *evidence* is not the same as a root cause. Either will
   do: a diagnosed mechanism, **or** a recorded sighting ledger (dates, runs, the failure signature,
   and what shows it is master-state rather than PR-state). What stays banned is quarantining on a
   hunch - "it's red sometimes" is not evidence, and a single failure is a sighting, not a ledger.

   The rule used to demand a diagnosis outright. That was wrong in a way worth recording: it
   conflated *"we don't know the mechanism"* with *"we don't know whether it's ours"*, and only the
   second justifies blocking. A test with a sighting ledger **is** a finding - it is known
   master-state flaky - it simply has no root cause yet. Demanding one before quarantine leaves an
   undiagnosed red blocking every unrelated PR, which trains everyone to read red as normal; this
   repo already deleted surefire retries for hiding flakes, and a permanently-red gate destroys the
   same signal more thoroughly. The tell that the old default was miscalibrated: its escape hatch was
   an owner-granted exception, and the exception had become the routine path.

   The bar it does NOT lower: quarantine still defers rather than forgives. The lane keeps running
   the test, the registry keeps it loud, and rule 5 still blocks a release while the list is
   non-empty. And it is never a licence to label something "just a flaky test" - that is the label
   the drain-zombie carried right up until it turned out to be a real product bug.
2. **Quarantine is master-state, not PR-state** - see AGENTS.md, Testing.
3. **Re-enable = the owning fix PR deletes the annotation AND this entry in the same commit**, after
   merging master - atomically restoring the test to the gating lane.
4. An owning fix PR is the goal, not a precondition. An entry without one is unowned and legal - the
   audit flags it as an advisory, not an error, and the lane report on every PR plus the release
   guard keep it loud until someone owns it. What the checks *hard-fail* is an owner claim that is
   wrong: a closed-unmerged owner (orphaned entry), or a merged owner with the test still
   quarantined (re-enable overdue).
5. **A release is blocked while this list is non-empty** (`release.yml` guard; dry runs warn instead) -
   a release must not ship while tests are held out of the gates. Snapshots still publish (dev
   artifacts, master is always `-SNAPSHOT`).

## Working with the registry

- Fast, no-Docker consistency check while editing: `bin/check-quarantine-registry.sh` (seconds).
- Owner-claim check needs an authenticated `gh` (`gh auth status`): `bin/check-quarantine-owners.sh`.
- Run the whole lane manually: `gh workflow run quarantine-lane.yml -R astubbs/parallel-consumer`.

## Currently quarantined

Every entry below is a timing flake rather than a deterministic failure, so all carry
`flapping = true`: a pass proves nothing and the lane reports it without demanding action. All
were hidden by the surefire retry until astubbs#224 removed it.

- [ ] `PCMetricsTest.metricsRegisterBinding` - asserts `PARTITION_LAST_COMMITTED_OFFSET` equals a
  **completion counter** while the suite runs `UNORDERED`. Commits are contiguous and bounded by the
  lowest incomplete offset; completions are not ordered, and workers call `latch.await()` *before*
  `counter.incrementAndGet()`, so a latched worker's offset never completes and the gap is
  **permanent**. The 120s `atMost` cannot close it - it only makes the failure cost 140s of every CI
  run. Quarantined on a **diagnosed mechanism**, which is the stronger half of rule 1, not on a
  sighting ledger. The fix is one comparand -
  `PARTITION_HIGHEST_SEQUENTIAL_SUCCEEDED_OFFSET` is the contiguous high-water mark the commit metric
  actually tracks - but the sibling assertions on `PARTITION_HIGHEST_COMPLETED_OFFSET` and
  `PARTITION_INCOMPLETE_OFFSETS` derive from the same counters and want reading as a set first, so it
  is not a one-line change. Diagnosis in
  `docs/inflight/bug-pcmetrics-committed-offset-vs-completion-count.md`. No owner yet.

  **History, and why this entry says "re-quarantined".** astubbs#265 released it on a causal fix that
  addressed the *opposite* direction: that diagnosis was that the metric could be *more* current than
  the expectation testing it (`expected 203.0 but was 207.0`), so the `Thread.sleep(1000)` became an
  `await().untilAsserted(...)` on the trailing meters. What fails now is the metric *behind* and never
  converging - `PARTITION_LAST_COMMITTED_OFFSET` for partition 1 stays short of
  `counterP1 + p1StartingOffset` for the whole 120s budget - which is the mechanism above, seen from
  the outside. Sighted twice in a row on one head (astubbs#116, 2026-08-14) as
  `expected 1213.0 but was 1209.0` then `expected 1207.0 but was 1195.0`: a shortfall that varies, so
  no wait closes it. That is the shape
  [`assert-the-commit-frontier-not-the-tick-path.md`](solutions/test-flakiness/assert-the-commit-frontier-not-the-tick-path.md)
  warns against, and it rhymes with the `OffsetEncodingBackPressureTest` entry below, whose committed
  high-water mark also never reaches its expectation with a different actual each run - **worth ruling
  in or out as one phenomenon rather than two.** Whether the un-committed tail is a wrong test
  assumption or real commit behaviour is undecided and is the open task; further diagnosis in
  [`docs/inflight/test-untracked-ci-flakes.md`](inflight/test-untracked-ci-flakes.md).

  *This was two separate entries until 2026-08-25.* `origin/master` carried the diagnosis; the
  re-quarantine arrived independently by cherry-pick from astubbs#116 (`f4a16a625`, originally
  `eb602ae39`) and did not know the first existed. Two entries for one `@Quarantined` annotation
  fails `bin/check-quarantine-registry.sh`, and they were describing the same failure from two
  directions - the mechanism and its sightings - so they are folded here with nothing dropped.

- [ ] `ProducerManagerTest.producedRecordsCantBeInTransactionWithoutItsOffsetDirect` - fails inside
  the shared `BlockedThreadAsserter#assertUnblocksAfter` helper rather than in the test's own
  assertions, so the same signature can surface from any test that uses it. The unblocker is
  scheduled *before* the elapsed clock starts, so the measured window begins later than the delay it
  is compared against and is systematically short by however long arming the scheduler takes;
  `isAtLeast(unblocksAfter)` then fails by a millisecond or two under load. Seen as `getElapsed()
  expected to be at least PT20S but was PT19.998S` - 2ms short on a 20s bound - on a PR whose diff
  contained no Java at all, which is what rules out PR-state under rule 2. Diagnosis in
  [`docs/inflight/test-untracked-ci-flakes.md`](inflight/test-untracked-ci-flakes.md).
  Owner: PR astubbs#262, which anchors the measurement to a nanos stamp taken just before
  `schedule()`, leaving the residual error sub-millisecond and in the safe direction.

- [ ] `OffsetEncodingBackPressureTest.backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing` -
  **UNDIAGNOSED, quarantined on its sighting ledger (rule 1)**: at 4/45 it is the
  most frequent tracked flake and blocked every PR. Fails as `ConditionTimeout` at the
  `getHighestSeenOffset()` assertion - the committed high-water mark never reaches
  `expectedHighestSeen` (139), with a different actual each run (136 and 132 seen). An earlier
  quarantine attributed it to the retry-delay sleep and was reverted: that code runs *after* the
  failing assertion, so it cannot be the cause. No owner - diagnosing it is the open task; the
  unverified hypothesis and its falsification path are in
  [`docs/inflight/test-untracked-ci-flakes.md`](inflight/test-untracked-ci-flakes.md).

