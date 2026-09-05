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

   **A ledger does not have to be assembled by hand from logs that expire.**
   `node bin/inflight.mjs codecov test <name>` prints that test's recorded outcome per commit, and
   `codecov flaky` lists every test ever recorded with more than one outcome - Codecov keeps this far
   longer than a CI log is retained. It reports **candidates and never a verdict**: two outcomes
   across two commits fits a regression that landed between them just as well as a flake, which is
   the distinction this rule exists to make, so it informs the ledger rather than writing it.
   [`docs/inflight-tool.md`](inflight-tool.md) owns those commands.

   The rule used to demand a diagnosis outright. That was wrong in a way worth recording: it
   conflated *"we don't know the mechanism"* with *"we don't know whether it's ours"*, and only the
   second justifies blocking. A test with a sighting ledger **is** a finding - it is known
   master-state flaky - it simply has no root cause yet. Demanding one before quarantine leaves an
   undiagnosed red blocking every unrelated PR, which trains everyone to read red as normal; this
   repo already deleted surefire retries for hiding flakes, and a permanently-red gate destroys the
   same signal more thoroughly. The tell that the old default was miscalibrated: its only escape
   hatch was an owner-granted exception, so every undiagnosed red had to be escalated to the owner
   or left blocking - the rule had no path a contributor could take on the evidence they had.

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

The one entry below is an unreliable failure rather than a deterministic one, so it carries
`flapping = true`: a pass proves nothing and the lane reports it without demanding action. It was never
hidden by the surefire retry astubbs#224 removed, because the test did not run in a gating lane until
the PR that quarantines it.

**The other entry that stood here has gone, and not by a lapse.**
`ProducerManagerTest.producedRecordsCantBeInTransactionWithoutItsOffsetDirect` is astubbs#262's rule-3
re-enable: astubbs#265 deleted the wall-clock assertion that flaked, and astubbs#262, its owner,
deletes the annotation and its entry together.
(`OffsetEncodingBackPressureTest.backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing` went
earlier, diagnosed and fixed on master by astubbs#351 - it asserted an offset it had itself frozen.)

- [ ] `MultiInstanceRebalanceTest.largeNumberOfInstances` - a rebalance stall whose mechanism is now
  **measured**: the Kafka consumer group protocol under this profile's churn rate, not a PC defect. The
  chaos monkey restarts members faster than a join phase completes, a LeaveGroup sent mid-join is
  answered only when the phase completes, and one phase was observed open for 17s during which
  `consumer.poll()` returns nothing to any member - the `FLAT` count. In every failing run no
  coordinator request was slow. Every PC-side candidate was refuted by measurement. 4 in 60 on the Linux
  runner, 0 in 22 on an M2 desktop. Full chain, instruments and the refuted hypotheses:
  [`docs/inflight/test-largenumberofinstances-residual-failures-measured-not-explained.md`](inflight/test-largenumberofinstances-residual-failures-measured-not-explained.md).
  Stays quarantined because a test whose failures are the protocol's cannot gate merges; where it
  should live instead is `docs/inflight/test-largenumberofinstances-cannot-gate-a-merge.md`.

  **Rule 2 is satisfied prospectively rather than retrospectively, and that is worth stating plainly
  rather than letting a later reader find it.** The ledger was measured while this test was
  PR-state: on master it is `@Disabled`, so it cannot fail there and no master-state ledger for it
  can exist. The PR carrying this entry enables it into the required `Performance Tests` lane, which
  is exactly the act that makes its failures master-state - master would otherwise inherit a gating
  check that fails about one run in ten. The quarantine lands in the same change as the enablement,
  so the test never spends a day blocking merges on an unexplained stall. If the enablement were
  ever reverted, this entry should go with it.
