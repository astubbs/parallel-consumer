---
title: Prove the instrument could have said yes before trusting a negative result
date: 2026-08-11
category: workflow-issues
module: tooling
problem_type: workflow_issue
component: development_workflow
severity: high
root_cause: inadequate_documentation
resolution_type: workflow_improvement
applies_when:
  - A search, scan, or query returns "nothing found" and you are about to act on that
  - Running the prior-art checks before forming a hypothesis
  - Running a negative control in an experiment, where the expected result is failure
  - Reviewing a diff to judge a branch's scope
  - A cache, a default, or an implicit argument sits between your command and the thing it queries
symptoms:
  - A reachability experiment succeeds against a deliberately bogus endpoint
  - A branch diff shows files deleted and rewritten that the branch never touched
  - A prior-art search returns nothing for work that demonstrably exists
tags:
  - false-negative
  - investigation
  - git-diff
  - caching
  - prior-art
  - negative-control
  - tooling
---

# Prove the instrument could have said yes before trusting a negative result

## Context

A positive result gets scrutinised. Somebody asks where the number came from, whether the sample was
right, whether the fix explains the symptom. A **negative** result gets acted on immediately, because
it is a stopping condition: no findings means move on, no prior art means start fresh, no diff means
nothing to review.

That asymmetry is the whole problem. Three negatives here, which looked unrelated, were all artefacts
of the instrument rather than facts about the world:

| The negative | What was actually true |
|---|---|
| "The scanner is reachable and the token is valid" | A warm on-disk cache was answering; the endpoint was bogus and the token expired |
| "This branch rewrote `README.adoc` and deleted `STRATEGY.md`" | The branch touched neither - the diff was rendering *master's* newer commits as the branch's deletions |
| "No prior art" | The search ran against `confluentinc`, not the fork |

Each is cheap to catch and expensive to miss, and none of them announces itself.

## Guidance

**Before trusting a negative, produce a positive.** Name what a hit would look like, and confirm the
instrument can still produce one. If you cannot make it say yes, its "no" carries no information.

This is the same discipline [`docs/investigating.md`](../../investigating.md) already applies to
experiments - *verify your instrumentation actually reached the run*, because a config change the
build never picked up reads as a real "no effect". The generalisation is that **every** negative
needs that treatment, not just instrumented ones.

### 1. A cache converts a negative control into a no-op

The OSS Index client keeps an on-disk report cache, and a warm cache returns full, correct-looking
findings even against a bogus base URL and an expired token. The experiment was "prove this
configuration cannot reach the service"; the run came back with a complete report; and the obvious
reading - "so it *can* reach it" - was wrong twice before anyone noticed. The paths, the `rm`, and why
CI must never cache that directory are in the sibling doc and in
`docs/inflight/ci-ossindex-audit-dead.md`.

The transferable part is the shape. **Before running any experiment whose expected outcome is
failure, ask what is stored between your command and the thing you are testing.** A cache does not
have to be a bug for it to invalidate your control arm - it is doing exactly its job, on a question
you did not mean to ask.

### 2. Two dots and three dots answer different questions

On a branch that is behind master by even one commit, `git diff origin/master..HEAD` renders master's
newer commits as the *branch's* deletions. Demonstrated on this repo against a ref three commits
behind:

```
$ git diff --stat origin/master..<behind-ref> | tail -1
 56 files changed, 767 insertions(+), 1237 deletions(-)

$ git diff --stat origin/master...<behind-ref>
 (nothing)
```

The branch changed nothing. The two-dot form said it had rewritten 56 files, including documents it
had never opened. It fooled more than one agent in the same session, and one of them came close to
reporting a PR as wildly out of scope on the strength of it.

- **`A..B` compares two endpoints.** Anything in `A` but not `B` shows as a deletion, whether `B`
  deleted it or simply has not caught up.
- **`A...B` compares `B` against the merge base.** That is "what did this branch change", which is
  almost always the question being asked of a branch or a PR.

**Neither is the correct default - the question decides.** Two dots are right when the endpoints
*are* the subject: `git log --oneline <old-base>..<new-base>` is exactly how
[`read-the-commits-you-inherit-2026-08-10.md`](read-the-commits-you-inherit-2026-08-10.md) says to
list what a rebase just handed you, and three dots there would answer the wrong question. The trap is
not the syntax; it is reaching for a diff without deciding which of the two questions you are asking.

The positive control is cheap: if a branch diff shows changes to files you know the branch never
touched, the instrument is wrong before the branch is.

### 3. A tool with a default target may be pointed elsewhere

`gh` in this fork resolves to `confluentinc` unless told otherwise, so the merged-PR prior-art search
returns *upstream's* history and reads as "no prior art" - the exact false confidence that check
exists to prevent. The loud failure (`Could not resolve to a PullRequest`) is harmless; the silent
success is the damaging one.

This one is already written up in full, mechanism and fix, so it is cited rather than restated:
[`compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md`](compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md),
with the one-time `gh repo set-default` fix and the habit that outlives it in `AGENTS.md`.

### A positive control for each of the prior-art checks

`AGENTS.md` requires six checks before forming a hypothesis, and requires you to report what each
returned including "nothing". A "nothing" is only worth reading if the check could have said
otherwise. Cheap ways to show it could:

| Check returning nothing | Show it could have said yes |
|---|---|
| `grep -rl <mechanism> docs/solutions/` | Grep a term you *know* is in there; if that also returns nothing, your path or flags are wrong |
| `gh pr list --state merged ...` | `gh repo view --json nameWithOwner` prints `astubbs/parallel-consumer`, and the unfiltered list is non-empty |
| `gh issue list --state all` | The result set contains `upstream-mirror` issues - if it does not, you are on the wrong repo |
| A branch or PR diff | Diff a file you know the branch touched and confirm it appears |
| A scanner or audit | The run reports how many components it examined, not just how many were bad |

The last row is the general form: **prefer instruments that report their denominator.** The audit job's
real output reads *"5 vulnerable component(s) … Scanned 214 resolved components across 9 module(s)"* -
and the denominator is what makes the numerator readable. Had it found nothing, "0 of 214 across 9
modules" would still be a result, where a bare "no vulnerabilities found" is a sentence a broken
scanner produces just as readily.

## Why This Matters

A false negative does not feel like an error. It feels like efficiency - the search was fast, the
answer was clean, and there is nothing to follow up. Nothing about the output invites a second look,
so the only thing that catches it is contradicting knowledge you happened to already have. Every one
of the three instances here was caught that way, by luck rather than method: someone knew the token
had expired, knew the branch had not touched `STRATEGY.md`, knew the PR existed.

The `gh` case is the sharpest, because the check it broke was itself a safeguard against false
confidence. A prior-art search that silently answers about a different repository does not merely fail
to help - it converts "I did not look" into "I looked and there was nothing", which is a stronger and
more wrong claim than the one you started with.

## When to Apply

- **Any "nothing found"** you are about to write into a PR description, a handoff, or a decision.
- **Any negative control** - an experiment whose expected outcome is failure is exactly the experiment
  a cache, a stale artefact, or a skipped step can fake.
- **Any diff used to judge scope**, before saying a branch is bigger or smaller than claimed.
- **Any tool with an implicit target** - a default repo, a default branch, a default profile, a
  default working directory. Ask what it resolved to, do not assume.

## Examples

The positive control is usually one command, and it is the one nobody runs because the answer feels
obvious. Before believing a prior-art search that came back empty:

```bash
$ gh repo view --json nameWithOwner -q .nameWithOwner
astubbs/parallel-consumer      # not confluentinc - the search was pointed at the right repo
```

Deliberately unqualified: it tests what the bare default resolves to, so adding `-R` would defeat the
check. That is the pattern in general - **the control has to exercise the same defaults the real
command did**, or it proves something about a different command.

## Related

- [`a-check-that-reports-success-without-having-run.md`](a-check-that-reports-success-without-having-run.md)
  - the sibling. That doc is about *checks* that pass without running; this one is about
  *investigations* that return a negative because the tool was pointed at nothing. The OSS Index cache
  appears in both: there as the thing that nearly faked out the guard's own proof, here as the general
  trap.
- [`compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md`](compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md)
  - three worked instances of the same class in knowledge-capture tooling, including the `gh` default
  repo in full.
- [`read-the-commits-you-inherit-2026-08-10.md`](read-the-commits-you-inherit-2026-08-10.md) - the
  other half of the moved-base problem: which diff form to use is this doc; what to read once you have
  it is that one.
- [`../../investigating.md`](../../investigating.md) - *verify your instrumentation actually reached
  the run*, and *a fix that works is not evidence of the cause*.
- [`../build-errors/maven-multi-module-plugin-and-resolution-traps.md`](../build-errors/maven-multi-module-plugin-and-resolution-traps.md)
  - includes a tool whose *probe* was the broken thing while it reported this project as the problem.
- astubbs/parallel-consumer#278 (merged) - the `gh` default-repo write-up in `AGENTS.md`: the one-time
  `set-default`, and the habits that survive it being local and uncommitted.
