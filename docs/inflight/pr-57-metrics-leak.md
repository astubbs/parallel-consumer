# astubbs#57 - PCMetrics leak (confluentinc#859) + cherry-picks

<!-- inflight-type: bug -->
<!-- inflight-impact: crash -->


Fixes duplicate Micrometer meter re-registration on assignment/revocation, and bundles the
`confluentinc#893` (offset accuracy on assignment) and `confluentinc#905` (max-queued-records-per-shard
metric) cherry-picks into one PR instead of a 3-deep stack, superseding the old closed stack
(astubbs#42 → astubbs#43 → astubbs#45).

Owns `PCMetrics.java`, `PCMetricsDef.java`, `PartitionState.java`, `PartitionStateManager.java`,
`ShardManager.java` - which is why astubbs#51 and anything touching partition state sequences after it.

## Handoff - 2026-08-20

**State:** base `master`, all gates green, hook self-tests green.
Three review rounds landed clean on the library code; the last two rounds' findings were all in the
merge resolution and in `.claude/hooks/check-upstream-map-merged.sh`, and are fixed. astubbs#325 has
merged, so the dependency is discharged.

### Two decisions are the operator's, and nothing proceeds without them

- **Merge strategy: recut, not squash - and it has not been done.** Rebase-merge as-is is unsafe:
  three of the branch's merge commits carry hand-made conflict resolutions that a replay would
  drop. Squash would collapse three separable upstream fixes (astubbs#120, astubbs#121,
  confluentinc#905) that the changelog generator wants as distinct entries. Proposed six atomic
  units - the three fixes, the un-quarantine, the `workManager` setter removal, and the docs.
  Reset to the **merge-base**, never to `origin/master`, and verify `git diff <old-tip> HEAD` is
  empty.
- **No human LGTM.** The `review: human LGTM` gate reports green, but every review on the PR is
  `COMMENTED`, never `APPROVED`. Do not read the gate as approval.

### Traps this branch has already sprung - do not re-learn them

- **The repo went shallow mid-session.** `check-issue-refs.sh` failed with "cannot find a merge base";
  `rev-list HEAD..origin/master` claimed 269 behind an hour after a clean merge. `git fetch
  --unshallow` fixed it. A gate failing for a content-shaped reason may be a depth problem.
- **A merge that reports clean has not proved it preserved intent.** Merging master resurrected
  `ManagedPCInstanceLifecycleTest`, which astubbs#325 had deleted, with no conflict raised - a
  deletion leaves no textual evidence. Sweep deletions explicitly after any large merge.
- **The GitHub reviewer edits its comment in place.** A review that starts as a checklist becomes
  findings in the *same* comment, and a comment already marked `dispatched` is never reactivated by
  an edit. Re-read `updated_at` rather than trusting the watcher.
- **`bug-857-family.md` has been got wrong three times** - renumbered headings without their body
  cross-references, then both parents' drafts of one supersession note kept, then a missing blank
  line. Ordinals there are positions in a list two branches append to; cite the seed, not the number.
- **`bin/test-check-agent-hooks.sh` sections are order-sensitive.** `HOOK_UNDER_TEST` is file-global
  and set per section; a block inserted mid-section silently repoints its neighbours. Placed wrong
  twice here.

### Open, not blocking

- `core-pcmetrics-lock-held-across-registry-calls.md` and `core-pcmodule-injection-seam.md` - both
  deferred tasks this PR raised and deliberately did not fix.
- Chaos Pain Suite reds are confluentinc#857, advisory, not in the required checks - not this PR's.
