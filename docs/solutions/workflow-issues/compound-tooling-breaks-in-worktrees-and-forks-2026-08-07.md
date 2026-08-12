---
title: "Knowledge-capture tooling silently misreads a worktree and a fork - three failures that all look like clean results"
date: 2026-08-07
category: workflow-issues
module: tooling
problem_type: workflow_issue
component: development_workflow
severity: medium
status: "Known, unfixed upstream. Workarounds for 1 and 2 are per-invocation; a plugin update discards any local patch. 3 has a per-clone fix (`gh repo set-default`), but it is local config, so the habit still has to hold."
applies_when:
  - Running any Compound Engineering skill from a git worktree rather than the main checkout
  - Running any tool that derives a repo identity from the working directory's basename
  - Running any `gh`-based verification in a fork whose upstream still exists
  - Reading a tool's "clean" or "nothing found" result and deciding it is evidence
tags:
  - tooling
  - worktrees
  - fork
  - gh-cli
  - false-negative
  - knowledge-capture
---

# Knowledge-capture tooling silently misreads a worktree and a fork

## Context

This repo mandates worktrees for all work (`AGENTS.md`, *Worktree ownership*) and is a hard fork whose
upstream still exists. Both facts break tooling that infers identity from its surroundings, and all
three failures found so far fail **quietly** - they report a clean or empty result that is
indistinguishable from a genuine one.

That is what makes them worth a doc. A tool that crashes gets fixed; a tool that returns "nothing
found" gets believed.

## Guidance

### 1. A worktree's basename is not the repo name

Anything computing `REPO_NAME=$(basename "$(git rev-parse --show-toplevel)")` gets the **worktree**
directory name. Here that is the branch-ish slug, not `parallel-consumer`, so a session-history search
keyed on it matched **zero** sessions where the correct name matched **37**. The workflow then recorded
"no relevant prior sessions" and carried on.

Derive the repo name from the **common** git dir, not the working root:

```bash
# worktree-safe: same answer from the main checkout and from any worktree
d=$(git rev-parse --path-format=absolute --git-common-dir); d=${d%/.git}; basename "$d"
```

`--git-common-dir` is the point: unlike `--show-toplevel`, it resolves to the *shared* git directory,
so every worktree of a repo agrees on it. Take the **dirname** - the common dir is `…/<repo>/.git`, so
a bare `basename` of it returns `.git`, not the repo name. (Verified from both a worktree and the main
checkout; the obvious `basename … | sed 's/\.git$//'` returns an empty string, which is exactly the
kind of silent-empty result this doc is about.)

### 2. A worktree does not have the repo-root config

A health check that looks for `.compound-engineering/` relative to `git rev-parse --show-toplevel`
reports it missing when run from a worktree, because the directory lives in the main checkout. Run
repo-config health checks **from the main checkout**, and treat a config complaint raised in a worktree
as unproven until re-checked there.

That path is deliberately unresolvable from here, in two ways at once: it is absent from this worktree,
and it is gitignored so it is absent from the tree entirely. A path checker run against this doc will
flag it - correctly, and beside the point. It is cited as a thing that is *not* where the tool looked,
which is the whole subject of this section.

### 3. Bare `gh` in a fork resolves upstream

`gh pr view` with no `-R` resolved to the upstream repository and returned *"no pull requests found for
branch …"* for a branch that has an open PR on the fork. Always pass the repo explicitly:

```bash
gh pr view -R <owner>/<repo> <n>        # not: gh pr view <n>
gh pr list -R <owner>/<repo> --state merged
```

This one is worse than a missing result, because a verification step that treats "not found" as
"unverified" will **downgrade a correct claim**: an open PR citation gets softened to "pending", and a
doc that was right becomes wrong.

The cause is `gh`'s own preference: with both an `origin` and an `upstream` remote and nothing saying
otherwise, it picks `upstream`, so `gh pr list`, `gh pr view`, `gh run view` and `gh pr create` all
address `confluentinc` out of the box. There is a one-time `gh repo set-default` fix and a command
that verifies it; `AGENTS.md` carries both, because they belong before an agent's first command. It
writes local, uncommitted config, though: CI runners, other machines and fresh agent sandboxes all
start without it, so qualifying `gh` in anything written down remains the durable half.

**The loud case is the harmless one.** `gh pr view 259` errors with "Could not resolve to a
PullRequest" purely because upstream happens to have no PR 259, and you go looking. The damaging case
succeeds: the merged-PR prior-art search returns **upstream's** history, finds nothing relevant, and
reads as "no prior art" - the exact false confidence *Before you investigate anything* exists to
prevent. That command sat unqualified in that very table in `AGENTS.md` until astubbs#278, so every
agent who ran it searched `confluentinc` and had no way to tell.

## Why This Matters

Each failure produces a *plausible* output, so nothing prompts a second look:

| What the tool reports | What is actually true |
|---|---|
| "no relevant prior sessions" | the search never matched the repo |
| "example config missing" | it exists, in the main checkout |
| "no pull requests found" | the PR exists, on the fork |

All three were caught only because the output contradicted something already known - an empty result
where prior sessions were certain to exist, a config file seen minutes earlier, a PR opened in the same
session. Absent that, each would have passed as a finding. The general rule: **when a tool reports
absence, confirm it is looking where you think it is** before treating the absence as evidence.

## When to Apply

- Before believing any "nothing found" from a tool run inside a worktree.
- Before believing any `gh` result in this repo that does not name `-R`.
- When a tool's result would *weaken* a claim you have independent evidence for - the tool is the thing
  to check first, not the claim.

## Examples

Diagnosing the session-history miss took one command:

```
$ git rev-parse --show-toplevel
/Users/.../parallel-consumer/.claude/worktrees/commit-timeout
$ basename ...            -> commit-timeout      # what the probe searched for
$ discover-sessions.sh commit-timeout    7  ->  0 sessions
$ discover-sessions.sh parallel-consumer 7  -> 37 sessions
```

The same shape works for the fork trap - run the command with and without `-R` and compare.

## Related

- `unforceable-trigger-commit-lock-timeout-2026-08-07.md` - the learning being captured when all three
  surfaced. Its own lesson (verify instrumentation actually reached the run) is the same principle
  applied to a build rather than to tooling.
- `AGENTS.md` *Worktree ownership* - why every run here is in a worktree, which is what makes these
  defects routine rather than rare.
- `AGENTS.md` *Before you investigate anything* - the merged-PR search there depends on `-R` being
  passed; without it the search silently returns nothing on this fork.
- `AGENTS.md`, the `gh` default-repo section - the rule and the one-time fix, citing this write-up
  for the incident behind them.
