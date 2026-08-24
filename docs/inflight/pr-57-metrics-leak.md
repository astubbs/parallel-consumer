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

## Update - 2026-08-24

**Two claims in the 2026-08-20 handoff above are no longer safe to read as written.**

- **"all gates green"** - `Chaos Pain Suite` is red, on every head. It is advisory by design (its
  workflow header says "NEVER PR-gating"), but a failing check still keeps `mergeStateStatus` out of
  `CLEAN`, so **this PR cannot reach a green merge state while confluentinc#857 is open**. Two
  consecutive heads have now demonstrated it. That is a decision - exclude the check from this PR's
  merge state, or merge with it knowingly red - and it wants settling *before* the recut, which is
  the moment a clean signal is most useful.
- **"hook self-tests green"** - true on Linux, false on macOS, where 10 cases fail `expected DENY,
  got ALLOW`. The cause is master state, not this branch:
  [`ci-merge-guard-fails-open-on-bsd-stat.md`](ci-merge-guard-fails-open-on-bsd-stat.md). Read the
  original claim as platform-specific rather than as a property of the branch.

**`Check PR Dependencies` has never run.** Three pushes have each reported a bypassed rule violation
saying that required status check "is expected". It is required and absent, which is a different
state from red, and it will block the merge.

**The "No human LGTM" bullet above is wrong on its stated reason, and its warning is worth keeping
for a different one.** `bin/check-human-lgtm.sh` passes on an owner review whose body contains
"lgtm" in any case; it never inspects `APPROVED`, so "every review is COMMENTED" was never the
criterion. The owner has in fact said `lgtm` twice - 2026-08-19, and again 2026-08-23 conditional on
this file being synced. **But the gate is documented as not head-sensitive and permanent**, and this
PR contains the proof: the 2026-08-19 `lgtm` latched it green, and the owner's 2026-08-20 review
body - `nearly` - could not turn it red again. So the gate answers "has the owner ever said lgtm",
never "does the owner approve of this head", and a retraction is invisible to it. Read the reviews,
not the tick.

### confluentinc#893 / astubbs#121 - what the carried fix is actually evidenced by

Reviewed 2026-08-24 because the upstream issue body is three bullets and explains nothing. The
evidence is real but it is **not in the issue**, and the upstream PR's own body is misleading:

- **The PR body describes an abandoned fix.** `sangreal` first proposed changing
  `offsetHighestSucceeded` seeding; `rkolesnev` rejected that reading, and the author replaced it
  with the dirty-read fix on 2025-10-29. The body was never updated, so it argues a root cause the
  diff does not implement. The diff touches `PartitionState.java` only and never touches
  `offsetHighestSucceeded`.
- **The real walkthrough is a PR comment** (2025-11-05), with concrete offsets, and it matches the
  shipped code: the payload is encoded against one base, a completion lands between the two
  `getOffsetToCommit()` reads, the higher offset is committed, and the decode shift compounds across
  rebalances until a poll goes out of range.
- **Field evidence is one datapoint**: the reporter ran it privately for "more than a week" against
  a fault that recurred "once every several days".
- **The approving reviewer approved with a live suspicion** - "i still think there might be another
  edge case here but i havent fleshed it out yet" - and separately believed there is a distinct
  offset-advance-by-one bug. Neither was ever resolved.
- **No behavioural reproduction exists anywhere.** `rkolesnev` asked for one; none was produced.
  Upstream ships no test at all. The test this fork adds asserts `getOffsetToCommit()` is called
  once - it pins the fix's shape and fails against the old code, but it is single-threaded and
  cannot fail for the reason the bug occurs.

So: high confidence the change is a faithful carry that closes a real window in the safe direction;
**low confidence that it is the whole of confluentinc#894**. Two consequences worth honouring at
merge - word the changelog as closing the dirty-read window rather than as fixing offset-reset, and
do not let the merge auto-close astubbs#121.

Unread: `Parallel Consumer Offset reset Issue flow.pdf`, attached to confluentinc#893 on 2025-10-31,
which may hold the reproduction nobody wrote down.
