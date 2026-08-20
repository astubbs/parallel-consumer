# astubbs#267 handoff

Worktree `.claude/worktrees/pr267`, branch `fix/concurrent-listener-registration`, head `cf7ef75c6`.
All checks green, 32/32 threads resolved, **31 behind master** (merge it first).

## What it is

Started as a `ConcurrentModificationException` from concurrent listener registration; became a
defect class - **PC runs user code where its own bookkeeping cannot survive a throw**. Six
workstreams: the original collections fix, `ThrowableUtils` (`describeWithRootCause`,
`logWithoutEscaping`), retriable classification (`isPresentIn` / `isTransparentWrapper`), the
`retryDelayProvider` stall, per-partition counter maps, and `ExternalEngine.onAsyncFailure`.

## Do not re-derive these

- **The "records stay in flight forever" premise is UNPROVEN.** Ablated three ways on two engines;
  records come back regardless of the per-container guards. What *is* proven: the `runUserFunction`
  reorder (21s vs 2.3s) and the `retryDelayProvider` return-value validation. The loop guards are
  defence-in-depth - both tests' javadoc says so. Do not "restore" a stronger claim.
- **Open question nobody has answered:** what recovers an un-mailboxed container? If it is a
  timeout sweep, these guards are a latency fix, not a stall fix.
- **PCMetrics is astubbs#57's**, deliberately not fixed here.
- **Six review rounds, none clean**, and three found the same class at a sibling site the previous
  round missed. Assume the sweep is incomplete.

## Outstanding

1. **Merge master** (31 behind).
2. **Antony's LGTM** - `reviewDecision` is empty. `CLEAN` is not approval. Do not merge without it.
3. **Squash message** - not written; the PR title is current and correct, so let GitHub use it
   (never pass `--subject`, it drops the `(#N)`).
4. **Current head is unreviewed** - the gate last ran on `e3119abe`. Codex is rate-limited, so a
   round would be Claude-only. Antony's call, not automatic.

## Parked, deliberately

`docs/inflight/parked-blanket-safe-logging.md` (declined, with numbers),
`next-archunit-main-code-rules.md` (the `getMessage()` rule, costed at one site),
`ci-duplication-report-can-fail-to-post.md` (a required check went red with its finding posted
nowhere - the reason `.claude/hooks/after-push-check-ci.sh` exists).

**Delete this file when astubbs#267 merges.**
