---
title: "A read-only gate truncated the shared clone - `git fetch --depth` writes state every worktree reads"
date: 2026-08-26
category: workflow-issues
module: tooling
problem_type: workflow_issue
component: development_workflow
severity: high
status: "Fixed in bin/check-quarantine-owners.sh, and the class is now gated by the shared-git-state row in bin/check-shell-hazards.sh."
applies_when:
  - Writing a check or hook that needs a ref it does not have
  - Reaching for `git fetch --depth=1` because the check only needs one commit
  - Diagnosing a repository that appears to have lost history, or a merge-base that returns nothing
  - Running several agent sessions against worktrees of one clone
---

# A read-only gate truncated the shared clone

`bin/check-quarantine-owners.sh` verifies that an owning PR's merge preview really removes the
quarantine it claims to fix. To see the preview it needed two remote refs, and it fetched them the
cheap way:

```bash
git fetch --quiet --depth=1 origin "$base"
git fetch --quiet --depth=1 origin "pull/$pr/merge"
```

**A depth-limited fetch writes the `shallow` file, and that file lives in the `--git-common-dir`.**
It is not per-worktree. One run of this gate truncated history for every worktree of the clone at
once - including worktrees whose sessions had unshallowed a minute earlier, and one of which failed
its repair with *"shallow file has changed since we read it"* because a sibling was fetching
concurrently.

## Why it cost so much more than a 4-line script bug should

- **It fired from the sweep everyone is told to run.** `bin/check-all.sh` globs `bin/check-*.sh`, so
  the mandated pre-push sweep was also an instruction to corrupt the clone.
- **The damage landed on other commands, and none of them errored.** `git merge-base` returned
  empty, ahead/behind counts read 295 and 835 against true values in the tens, and commits that had
  demonstrably landed reported "NOT an ancestor of master". Read naively, that says master was
  rewritten and a day's work is gone. Two sessions nearly acted on it.
- **The gates that depend on history degraded to `CANNOT RUN`**, so the first half of the sweep
  weakened the second half.

## The fix, and why it is not a conditional depth

The obvious repair is to pass `--depth=1` only when the clone is already shallow. It is not enough:
it still *samples* shared state, and a sibling worktree can change that state between the sample and
the fetch. The gate now fetches into a throwaway git dir (`git --git-dir=<mktemp -d> fetch ...`) and
reads `FETCH_HEAD` there, so:

- the working clone is never a fetch target, under any interleaving - nothing to race on;
- a clone that arrived shallow on purpose (CI checks out at depth 1) is not deepened either, because
  its depth is neither read nor written;
- an interrupted run leaves the clone exactly as it was, because it was never touched.

Measured cost of the isolation on this repository: **~1.4s and ~2.3MB** for the first fetch, with the
dir reused for the rest of the run.

Two things surfaced while proving it, both worth keeping:

- **`exit` from inside a signal handler is documented to run the `EXIT` trap and measurably does not
  always.** Instrumented, the `TERM` handler ran in the main shell and the `EXIT` trap did not
  follow, on roughly one run in five. Signal handlers that must clean up should call the cleanup
  themselves.
- **A second signal during teardown re-enters the handler and abandons a half-finished `rm -rf`.**
  Disarm `INT`/`TERM` as the first act of cleanup.

## The generalisation

The class is **a check that writes state its callers read**, and `bin/check-shell-hazards.sh` now
carries it as the `shared-git-state` row - the second category in a table built for GNU-vs-BSD
divergence ([`gnu-only-constructs-fail-silently-on-bsd-2026-08-25.md`](gnu-only-constructs-fail-silently-on-bsd-2026-08-25.md)),
because both have the same signature: no error, a wrong answer somewhere else, unreachable by a
linter. `git clone --depth` is not the hazard - a clone owns its own depth. `git fetch --depth` is.

## Three vectors, and all three are now closed

Fixing the script only removes the instance. The class has three entrances, and each needed its own
guard, because none of them can see the others:

| Vector | Guard |
|---|---|
| A script in `bin/` or `.claude/hooks/` fetches with `--depth` | `bin/check-shell-hazards.sh`, `shared-git-state` row - fails the build |
| An agent or human types it into a shell | `.claude/hooks/check-shallow-history.sh` - denies the command before it runs, and names the throwaway-git-dir alternative |
| The clone is already truncated and a query answers from the graft | `.claude/hooks/check-shallow-history.sh` - denies `merge-base`, `rev-list` and range queries while shallow |

The last of those already existed and could only ever deny the *symptom*. The first two are the
cause, and the hook's two directions are deliberately gated in opposite senses: a depth-dependent
query is wrong only in a clone that is **already** shallow, while a shallowing fetch is only worth
stopping in one that is **not** - denying it in an intentionally shallow CI clone would be noise, and
noise is what gets a hook switched off.

## What was checked and ruled out

- **`.github/workflows/pr-checklist.yml`** runs `git fetch --no-tags --depth=1 origin <base>` from a
  JS step. It is a CI runner's own workspace, checked out shallow on purpose, with no worktrees
  sharing it. Not an instance - and out of the hazard gate's corpus, which is `bin/` plus
  `.claude/hooks/`.
- **`actions/checkout` with `fetch-depth: 1`** in several workflows: a fresh per-job clone, not a
  fetch into a shared one.
- **`bin/ci-mutation-test.sh`** fetches its base ref with **no** `--depth`, so it writes no `shallow`
  file. A plain fetch is additive.
- **`git clone --depth=1`** in `bin/test-check-shallow-history.sh` and in this fix's own self-test:
  building a shallow *fixture*, in a scratch directory that owns its depth.
- **Every other `git` mutation in `bin/`** - `update-ref`, `reset`, `checkout` in the self-tests -
  targets a `mktemp -d` fixture repository, not the working clone. Checked individually.
