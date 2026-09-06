---
title: A harness that cannot tell "never ran" from "ran and agreed" scores the silence as a pass
date: 2026-09-02
category: workflow-issues
module: build-system
problem_type: workflow_issue
component: development_workflow
severity: high
root_cause: logic_error
resolution_type: code_fix
applies_when:
  - Writing a negative control, mutation check, or any test whose verdict is "the thing under test disagreed"
  - A check drives the system under test as a subprocess and reads its exit code or output
  - A test fixture prepares state with a command whose failure is not checked
  - A gate decides WHERE to run from a value it did not verify
  - A suite is green on macOS and CI, or on one and not the other, and nobody knows why
symptoms:
  - A mutation check passes although the mutant was never executed
  - One check in a suite fails while its siblings, which exercise the same path, stay green
  - A CLI prints zero bytes and exits 0, and a control keyed on a non-zero exit reads that as success
  - A fixture step exits non-zero, nothing reads the status, and the assertion downstream is satisfied by the unprepared state
  - A pre-commit gate reports defects in files the commit never touched
  - A hook says the command "did not say where it runs" when the command carried a `git -C "$VAR"`
tags:
  - test-infrastructure
  - mutation-testing
  - vacuous-pass
  - false-negative
  - symlink
  - realpath
  - bsd-vs-gnu
  - pre-commit-hook
  - macos
---

# A harness that cannot tell "never ran" from "ran and agreed" scores the silence as a pass

[`a-check-that-reports-success-without-having-run.md`](a-check-that-reports-success-without-having-run.md)
is the case where a CHECK goes quiet. This is the case one level up: the HARNESS around the check is
asked "did the mutation get caught?", the thing under test never executed at all, and the harness -
seeing an answer that differs from the expected one - reports **caught**. Nothing was measured, and the
report says it was. Three instances turned up in one day, in one file and its neighbour, and each was
invisible in CI; a fourth turned up the next day, inside the fix for the third.

## The shape

A negative control expects the system under test to **disagree** - exit non-zero, print a specific
complaint, fail an assertion. When the system does not run, it produces exit 0 and empty output.
That also differs from the expected answer. So the control scores it as "went red", and the harness
prints `ok`.

The control is not wrong about what it saw. It is wrong about what it did not verify: **that the
system under test ran at all.** A control keyed only on "the output differed" is satisfied equally by
a caught mutation and by an absent process.

## Four instances

**1. A guard that never ran the CLI.** `bin/inflight.mjs` decided whether it was invoked directly by
comparing `import.meta.url` to `pathToFileURL(process.argv[1])`. Node resolves the first through
symlinks and not the second, so under any symlinked path - macOS `os.tmpdir()`, where the self-test
builds every mutant - the guard was false and the CLI body did not execute. `inflight help` printed
zero bytes and exited 0 *whatever the mutation had done*. Only one check in the suite asserted
`code === 0`, so only one went red; every other invoke()-driven mutant scored "caught". CI never saw
it because Linux `/tmp` is not a link. Fixed by realpathing both sides of the guard, and by giving the
guard its own control that asserts exit 0 **and non-empty output** - the second half is what does the
work, since exit 0 alone is exactly what the broken guard produced.

**2. A fixture step that failed silently.** A freshness check aged a `FETCH_HEAD` with
`touch -d 2020-01-01`, a GNU spelling. BSD `touch` exits 1 with "illegal time specification"; nothing
read the status; the file kept its real mtime; the mutant, which reads that mtime, correctly saw no
staleness and stayed green. The control on that one flipped on the single term: `touch` gives a green
mutant, `utimesSync` gives a red one. Fixed by using `utimesSync`, which is cross-platform and throws
rather than exiting into a status nobody checks.

**3. A gate that checked the wrong tree.** `.claude/hooks/pre-commit-gate.sh` resolves which
worktree to gate from the hook payload's `cwd`, on the assumption that it names the subagent's
directory. It names the SESSION's launch directory. Three subagents committing in three worktrees
were each gated against the main checkout - clean, and behind master - and told about dangling
citations in files none of them had touched. The gate ran, read a real tree, and reported honestly on
it; it was simply not the tree being committed to. Fixed by refusing, with the remedy named, when the
resolved tree has nothing to commit: git would reject that commit anyway, so the only thing a gate
can do there is read the wrong files.

**4. The fix for instance 3 had the same shape inside it.** The remedy it names is `git -C
<worktree>`, and an agent that writes that as `git -C "$W" commit` hands the hook the literal text
`$W` - the hook reads the command before the shell expands it. Joined onto the cwd, that is a path
that does not exist, so `git status` there *errors* instead of answering "clean", and the clean-tree
check reads the error as "cannot tell" and falls through. Bash then finds no such directory and gates
`$CLAUDE_PROJECT_DIR` under a label saying the command did not say where it runs - when it did, in
text nobody could read. A probe that never ran was scored the same as a probe that ran and found a
dirty tree. Seen three times the day after the third instance shipped. Fixed by refusing, naming the
value, whenever any commit's `-C` holds a `$`, a backtick or a leading `~` - before the resolver is
asked to make sense of it.

## What separates these from ordinary flakes

In every case the failing arm is the one that would **not** ship a defect - a macOS-only harness path,
a BSD-only fixture failure, a wrong-tree gate that blocks rather than passes. So the signal arrives as
a nuisance on a developer machine and never as a red on the merge gate. The instinct is to fix the
nuisance locally (realpath the temp root; add `|| true`; commit with `git -C`). Each of those turns the
one visible red green while leaving the vacuous controls vacuous.

The discriminating question for a control is therefore not "did the answer differ" but **"could this
answer have been produced by nothing running?"** If yes, the control needs a second assertion that
only a running system can satisfy - output that must be non-empty, a side effect that must exist, a
status that must be read.

## What was refuted, so nobody chases it

The obvious next worry was that once the guard actually ran the CLI, mutants that had been passing
vacuously would be exercised for the first time and at least one would turn out weak. The full suite
passed with the guard fixed. The vacuous scoring was real; it was not masking a second defect.

## Where the fixes live

- `bin/inflight.mjs` - `invokedDirectly()`, realpath on both sides, fails closed on an unresolvable `argv[1]`.
- `bin/test-inflight.mjs` - `the-front-door-runs-through-a-symlinked-path`, and `utimesSync` in the freshness fixture.
- `.claude/hooks/pre-commit-gate.sh` - the clean-tree refusal, exit code 3 from the resolver; the
  unexpanded-`-C` refusal, exit code 4.
- `bin/test-check-agent-hooks.sh` - the refusal, its dirty-tree control, the `--allow-empty` and
  `--amend` exceptions, and the control that a flag named only inside the commit message exempts
  nothing; the `$W`, backtick and tilde refusals with their literal-path and leading-`cd` controls.

## Related

- [`a-hook-processes-own-directory-describes-the-session-not-the-command-2026-08-31.md`](a-hook-processes-own-directory-describes-the-session-not-the-command-2026-08-31.md) -
  the previous incident on the same hook. It records derivation rule 3, the payload `cwd`, as the
  directory a subagent arrives in; the third instance above is that claim failing. The payload `cwd`
  is the session's launch directory, and a subagent that changed directory in an earlier tool call
  has nothing in the payload that names its tree.
- [`silent-cwd-reset-runs-git-in-the-wrong-checkout.md`](silent-cwd-reset-runs-git-in-the-wrong-checkout.md) -
  why a `cd` in one tool call does not carry into the next, which is how the third instance arises.
- [`gnu-only-constructs-fail-silently-on-bsd-2026-08-25.md`](gnu-only-constructs-fail-silently-on-bsd-2026-08-25.md) -
  the class the second instance belongs to; `touch -d` is a site that sweep did not cover.
- [`a-check-that-reports-success-without-having-run.md`](a-check-that-reports-success-without-having-run.md) -
  the check-level case this document generalises.
