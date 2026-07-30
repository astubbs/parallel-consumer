# Quarantined tests - live registry

The task list of tests currently in the quarantine lane: known-failing-on-master tests carrying
`@Quarantined`, excluded from the gating CI suites (so green checks mean mergeable) but still run on
every PR by the non-gating "Quarantined Tests" job. Each entry is an open task: get the test fixed and
back into the gating lane.

**This file is enforced, not advisory**: `bin/check-quarantine-registry.sh` (run locally by
`bin/quarantined-test.sh` and in CI by the Quarantined Tests job) fails on any drift between the
`@Quarantined` annotations in the code and the entries below - a quarantined test missing here, or a
stale entry for a test no longer quarantined, is an error.

Rules (full discipline in `AGENTS.md` → Testing, and the `@Quarantined` javadoc):

1. **No quarantine without diagnosis** - undiagnosed red stays red and blocks, on purpose.
2. **Quarantine is master-state, not PR-state** - a test red on only one PR is that PR's problem.
3. **Re-enable = the owning fix PR deletes the annotation AND this entry in the same commit**, after
   merging master - atomically restoring the test to the gating lane.
4. Every entry needs an owning fix PR. An entry without one is diagnosed-but-unowned: flag it, find it
   an owner.

## Currently quarantined

- [ ] `PartitionStateCommittedOffsetIT.committedOffsetRemoved` - the `[latest]` nudge race: the single
  pre-await tail-nudge record can be produced before the consumer's `auto.offset.reset=latest` reset
  resolves on a slow/loaded broker, so the reset leapfrogs it and the await is unwinnable at any
  timeout; only the `[latest]` parameter ever fails. **Owner: PR #80** (`awaitWithTopicNudge`:
  nudge-inside-await + timeout self-diagnosis; 20/20 clean acceptance). Tracking:
  `docs/solutions/test-flakiness/latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md` (on the
  fix branch).
