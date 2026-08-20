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

1. **No quarantine without diagnosis** - undiagnosed red stays red and blocks, on purpose. The
   repository owner can grant an explicit exception when the blocking cost outweighs the pressure;
   the entry must say so ("rule-1 exception"), keep the failure signature as its reason, and carry
   the diagnosis as its open task.
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

- [ ] `OffsetEncodingBackPressureTest.backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing` -
  **UNDIAGNOSED, quarantined as an explicit rule-1 exception by owner decision**: at 4/45 it is the
  most frequent tracked flake and blocked every PR. Fails as `ConditionTimeout` at the
  `getHighestSeenOffset()` assertion - the committed high-water mark never reaches
  `expectedHighestSeen` (139), with a different actual each run (136 and 132 seen). An earlier
  quarantine attributed it to the retry-delay sleep and was reverted: that code runs *after* the
  failing assertion, so it cannot be the cause. No owner - diagnosing it is the open task; the
  unverified hypothesis and its falsification path are in
  [`docs/inflight/test-untracked-ci-flakes.md`](inflight/test-untracked-ci-flakes.md).
