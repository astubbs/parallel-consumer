# Agent polling loops leak, because nothing bounds them

<!-- inflight-priority: medium -->

Found 2026-08-18 while clearing a merged worktree: it could not be removed because a shell from a
session **five days earlier** still held it as its working directory.

That process was an `until` loop waiting for a CI check to report, waking every 15 seconds:

```bash
until gh api repos/.../commits/<sha>/check-runs -q '...' | grep -qE 'success|failure|neutral'; do
  sleep 15
done
```

**The check it waited for did not exist on that commit.** No run had produced it, so the `grep` could
never match and the loop had no other exit. It had been running for 5 days 22 hours - on the order of
34,000 `gh api` calls - and would have run until the machine rebooted.

## Why this is a shape, not an incident

Measured the same day, once the first one was noticed: **four more** shells older than an hour, the
oldest **6.3 days**, about **20 process-days** of accumulated idle polling between them. Re-measure
rather than trusting that figure - the point is the shape, and `ps -eo pid,etimes,cmd` with a filter
on age and on `sleep`/`until` finds them in one command.

Three properties make it silent:

- **The failure mode is waiting, not erroring.** A loop that will never exit looks exactly like a
  loop that has not exited yet. Nothing goes red, nothing is logged, no budget is exceeded.
- **The condition can be unsatisfiable from the first iteration.** Waiting on a check that is never
  created, a job on a re-run attempt that no longer exists, or a workflow that was skipped are all
  the same to the loop: the predicate is simply always false.
- **It outlives its session.** The agent that started it moves on or is compacted away; the process
  keeps its worktree pinned, which is how this one was found rather than by anyone looking.

## The rule worth adopting

**A polling loop gets a deadline, and says so when it hits it.** Not a retry count - a wall-clock
bound, because the failure is duration:

```bash
deadline=$(( $(date +%s) + 900 ))
until <condition>; do
  [ "$(date +%s)" -lt "$deadline" ] || { echo "gave up waiting for <thing> after 15m" >&2; break; }
  sleep 15
done
```

The `echo` matters as much as the bound. A loop that exits silently at its deadline is
indistinguishable from one whose condition came true, which reproduces the original defect one level
up - the same false-clean shape as
[a check that reports success without having run](../solutions/workflow-issues/a-check-that-reports-success-without-having-run.md).

**Prefer not writing the loop at all.** Most of these waits exist to watch CI, and the harness
already has a change-detector for that; where one is available, it wakes on the event instead of
asking every 15 seconds. Reach for a loop only when nothing else can observe the thing.

## What this needs to become

Unowned. Two candidate homes, neither written yet: a line in
[`bin/AGENTS.md`](../../bin/AGENTS.md) if the rule should bind anything committed under `bin/`, and
a line wherever agent shell conventions live if it should bind ad-hoc commands too - which is where
all five of these came from, none of them committed anywhere.

Sweeping the current leaks is a separate, cheap job: they belong to sessions that may still be live,
so check before killing rather than matching on age alone.
