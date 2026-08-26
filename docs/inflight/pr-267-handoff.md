# astubbs#267 handoff

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
<!-- post-merge: exempt-file - this note IS the PR's handoff, and is deleted when it merges -->

Branch `fix/concurrent-listener-registration`; `git worktree list` says which worktree holds it, and
`gh pr view 267 -R astubbs/parallel-consumer` its head, checks and unresolved threads. Merge master
before working on it - the branch is long-lived and the base moves under it.

## What it is

Started as a `ConcurrentModificationException` from concurrent listener registration; became a
defect class - **PC runs user code where its own bookkeeping cannot survive a throw**. Six
workstreams: the original collections fix, `ThrowableUtils` (`describeWithRootCause`,
`logWithoutEscaping`), retriable classification (`isPresentIn` / `isTransparentWrapper`), the
`retryDelayProvider` stall, per-partition counter maps, and `ExternalEngine.onAsyncFailure`.

## Do not re-derive these

**Extracted, because this file is deleted when the PR merges and these outlive it.** Two documents now
own what used to be listed here, and they are the ones to read - not this summary:

- [`../solutions/best-practices/a-guard-outlives-the-claim-that-motivated-it.md`](../solutions/best-practices/a-guard-outlives-the-claim-that-motivated-it.md)
  owns the refuted premise and the instruction not to restore a stronger claim, what *is* measured as
  against what is not, and why the defect-class sweep must enumerate by shape rather than by module.
- [`core-unmailboxed-container-recovery.md`](core-unmailboxed-container-recovery.md) owns the open
  question - what recovers an un-mailboxed container - and the three options the review thread on that
  line is waiting on.

What stays here, because it is only true while this PR is open:

- **PCMetrics is astubbs#57's**, deliberately not fixed here.
- **No review round has come back clean**, and more than one found the same defect class at a sibling
  site the round before it missed. Assume the sweep is incomplete.

## Outstanding

1. **Antony's LGTM** - `reviewDecision` is empty. `CLEAN` is not approval. Do not merge without it.
2. **Squash message** - not written; the PR title is current and correct, so let GitHub use it
   (never pass `--subject`, it drops the `(#N)`).
3. **The head moves faster than the review gate.** `bin/check-pr-analysis-surfaces.sh 267` and
   `gh pr view 267 -R astubbs/parallel-consumer --json statusCheckRollup` say which commit the last
   review actually ran on; do not assume the newest one. Codex is rate-limited, so a fresh round
   would be Claude-only, and that is Antony's call rather than automatic.

**Master moves fast under this branch, and several merges on 2026-08-26 needed real work rather
than a conflict resolution. Read those merge commits before assuming a merge here is routine.** The
ones that changed something rather than resolving it: `ManagedPCInstance` was the last caller of the
removed `getSuccessfulWorkListeners()`; `bin/check-branch-self-reference.sh` now requires every
mention of this PR in `docs/inflight/` to read correctly after it lands; astubbs#335 replaced the
field this branch guarded in `onUserFunctionFailure` with a state transition, and that guard was kept
on top of it rather than dropped - the reasoning is at the call site and in the merge commit, and it
is the one place a merge here chose between two deliberate designs.

## Parked, deliberately

`docs/inflight/core-blanket-safe-logging.md` (declined, with numbers),
`static-archunit-main-code-rules.md` (the `getMessage()` rule, costed at one site),
`ci-duplication-report-can-fail-to-post.md` (a required check went red with its finding posted
nowhere - the reason `.claude/hooks/after-push-check-ci.sh` exists).

**Delete this file when astubbs#267 merges.**
