# Splitting astubbs#322 - ten PRs, and the order they have to go in

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

astubbs#322 is 66 commits over 206 files carrying **ten unrelated workstreams**. Its title is the
confluentinc#909 load reproduction, which is 13 commits and 20 files of it. Nothing is wrong with the
commits - they are atomic and well-titled - the branch is what is wrong. Delete this note when the
last PR below is open.

## "Merge master in first?" - already done, and it does not fix replay

`git rev-list --count HEAD..origin/master` is **0**: the branch merged master at `b29f3de04` and
master has not moved since. There is nothing to merge.

It would not have helped anyway, and the reason decides the whole method. `git cherry-pick` replays a
commit's **original diff**, which is immutable and predates master's `da049f703`; merging master into
the branch cannot change what an old commit's diff says. Worked example: `92c9a73c2` conflicts on
`bin/test-check-agent-hooks.sh` because master gained that file from astubbs#299 *after* these commits
were written, and the branch's one merge already resolved that overlap - a resolution cherry-pick
throws away.

**But the branch being current is exactly what makes the alternative sound.** Because HEAD is fully
reconciled with master, `git diff master...HEAD -- <files>` describes the *resolved* state, so it
applies to master **by construction**. Verified on the worst group: all 35 of its exclusive files
applied clean (`git apply --check --3way`).

## Two extraction methods

- **Replay** (`git cherry-pick -x`) - keeps every commit body, and commit bodies feed the release
  notes. Use where measured clean.
- **Diff extraction** - `git diff master...HEAD` restricted to the group's files, committed as one
  well-written commit per PR authored from the group's commit bodies. Use where replay conflicts.
  Nothing that matters is lost provided the message carries the reasoning - `docs/merge-checklist.md`
  owns that standard.

## Build it as a STACK, not ten independent branches

**61 of 205 files are touched by more than one group**, and the overlap is dominated by one thing:
PR 2 adds `inflight-type`/`inflight-impact` headers to ~70 existing inflight notes, while other
groups edit those same notes' bodies. Extracted independently from master, every one of those is a
conflict. Extracted **in order, each branch cut from the previous one**, the tag headers are already
present and the body edits apply on top.

So: each PR branches from its predecessor and carries
`depends on astubbs/parallel-consumer#<parent>` in its description, per AGENTS.md. The PR-dependency
gate then holds each child until its parent merges.

## The ten, in order

| # | Workstream | Commits | Files | Replay measured | Method |
|---|---|---|---|---|---|
| 1 | CI gates (citations, issue-refs, dups, roadmap, review) | 8 | 51 | **8/8 clean** | replay |
| 2 | Agent harness + inflight tracker | 16 | 87 | 1/16 to first blocker | diff |
| 3 | Ideation / roadmap / release docs | 13 | 61 | 12/13 clean | replay + 1 fixup |
| 4 | confluentinc#909 core fix + reproduction | 13 | 20 | 12/13 alone, **13/13 stacked** | replay |
| 5 | astubbs#177 commit-response timeout | 4 | 14 | 2/4 | diff |
| 6 | astubbs#209 close-vs-distribution race | 4 | 14 | **4/4 clean** | replay |
| 7 | confluentinc#857 ledger + sightings | 5 | 14 | **5/5 clean** | replay |
| 8 | Test logging harness | 1 | 10 | **1/1 clean** | replay |
| 9 | Quarantine rule + PCMetrics diagnosis | 2 | 10 | 1/2 | replay + 1 fixup |
| 10 | Conflict-marker gate (new work) | - | ~3 | n/a | new |

Measured 2026-08-19 by cherry-picking each group onto master in a throwaway worktree, chronological
order; "clean" is `git cherry-pick` exiting 0.

**PR 1 first - it is the only group measured fully independent.** Later groups' docs would trip the
citation and issue-ref gates it installs, so it has to exist before they land.

**PR 2 second, and it is the big one.** Nothing later can add a tagged inflight note until
`bin/check-inflight-tags.sh` knows the vocabulary. Worth stacking as 2a (hooks + merge guard) and
2b (tracker schema, gate, session index); they interleave on
`.claude/hooks/inject-recorded-knowledge.sh`, so 2b cannot go first.

**PR 4 keeps the astubbs#322 number** - re-cut the existing branch down to the confluentinc#909 commits rather
than opening a fresh PR, so the review history and any LGTM survive. That is a force-push to a pushed
branch: **owner's call, do not do it unasked.**

**PRs 5-9 in any order** once 1, 2 and 4 are in. 6, 7 and 8 are measured fully clean.

**A grouping error worth recording:** "agent hooks" and "inflight tracker" were first counted as two
workstreams. They are one - the merge guard and the tracker's session index are the same hook system.

## PR 10 - the conflict-marker gate

Found while porting a sighting to astubbs#29: commit `9f7710217` on
`bugs/857-paused-consumption-multi-consumers-bug` committed a merge with the conflict **unresolved**
and pushed it. `docs/inflight/bug-857-family.md` carries `<<<<<<< HEAD` at line 87 and
`>>>>>>> origin/master` at 392 - **305 lines of a 392-line file inside the conflict**, holding the
whole confluentinc#857 ledger. Nothing went red; `ls bin/ | grep -i conflict` finds nothing.

`bin/check-conflict-markers.sh` + `bin/test-check-conflict-markers.sh`, wired into Repo Hygiene. The
gate must exempt its own self-test, which carries the markers as fixture data - the same exemption
`bin/check-shell-sigpipe.sh` documents for itself.
<!-- file-refs: N/A - the two check-conflict-markers scripts are what this entry PROPOSES to write; they do not exist yet, which is the point of it -->

**Repairing astubbs#29's file is separate from the gate** and belongs to whoever owns that branch: the
resolution is an editorial call about where `## Deleting these files` sits relative to 300 lines of
sightings.

## What must not be lost in the diff-extracted commits

- The confluentinc#909 **third precondition** (no take-scan between the stale inserts and the fresh arrival)
  and the reachability proof - they are why the fix is what it is.
- The test-logging measurements: 5,520 -> 3,687 lines, 196 banner dumps, and that a whole suite at
  `-Dpc.log.level=debug` emits 469,202 lines and fails three tests on a latch.
- Why quarantine rule 1 moved from *diagnosis* to *evidence*, including that the owner had been
  granting the exception routinely.
- The retry-queue orphan's re-classification to `stall`, and the clamp that makes it one.
