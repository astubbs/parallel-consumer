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
`Owner: PR #NN` (omit only for diagnosed-but-unowned entries, which the checks flag as advisory).
Unreliable tests carry `flapping = true` on the annotation itself (compile-checked, read by the lane
reporter).

Rules (full discipline in [`docs/testing.md`](testing.md), and the `@Quarantined` javadoc):

1. **No quarantine without diagnosis** - undiagnosed red stays red and blocks, on purpose.
2. **Quarantine is master-state, not PR-state** - stated in AGENTS.md, Testing, because no script
   checks it.
3. **Re-enable = the owning fix PR deletes the annotation AND this entry in the same commit**, after
   merging master - atomically restoring the test to the gating lane.
4. Every entry needs an owning fix PR. An entry without one is diagnosed-but-unowned: flag it, find it
   an owner.
5. **A release is blocked while this list is non-empty** (`release.yml` guard; dry runs warn instead) -
   a release must not ship while tests are held out of the gates. Snapshots still publish (dev
   artifacts, master is always `-SNAPSHOT`).

## Working with the registry

- Fast, no-Docker consistency check while editing: `bin/check-quarantine-registry.sh` (seconds).
- Owner-claim check needs an authenticated `gh` (`gh auth status`): `bin/check-quarantine-owners.sh`.
- Run the whole lane manually: `gh workflow run quarantine-lane.yml -R astubbs/parallel-consumer`.

## Currently quarantined

(none - both astubbs#80-family quarantines were re-enabled when PR astubbs#80 landed its drain-zombie and
nudge-race fixes)
