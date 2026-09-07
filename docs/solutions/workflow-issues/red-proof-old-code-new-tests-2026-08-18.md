---
title: "Red-proof a checker with old code + new tests, and never trust a git command whose success output looks like the action"
date: 2026-08-18
category: workflow-issues
module: tooling
problem_type: workflow_issue
component: development_workflow
severity: high
root_cause: wrong_api
resolution_type: workflow_improvement
applies_when:
  - "Red-proofing a new regression test - verifying it fails against the old, pre-fix code before trusting it as a regression guard (the bin/AGENTS.md requirement)"
  - "Using git stash, git checkout <ref> -- <paths>, or commit --amend around uncommitted work"
  - "Verifying that a commit or amend actually captured the working-tree changes it was meant to"
symptoms:
  - "git stash round-trip made the red-proof vacuously green - old tests ran against old code, 0 FAILs"
  - "git commit --amend printed healthy-looking stats but captured only the message, because stash pop had unstaged everything"
  - "git checkout HEAD -- <paths> silently overwrote the only copy of uncommitted work, twice in one session"
tags:
  - "git"
  - "git-stash"
  - "git-checkout"
  - "git-amend"
  - "red-proof"
  - "self-test"
  - "regression-testing"
  - "silent-data-loss"
  - "worktree"
related_components:
  - "git-workflow"
  - "testing_framework"
---

# Red-proof a checker with old code + new tests, and never trust a git command whose success output looks like the action

## Context

`bin/AGENTS.md` requires that a new checker self-test be *verified red against the old code* -
"a regression test that has never failed proves nothing" (see
[`bin/AGENTS.md`](../../../bin/AGENTS.md), "Scripts that guard other scripts" - the phrase
line-wraps there, so search the section, not the literal string). While adding
opt-out features to the issue-ref gate (`.github/scripts/issue-ref-gate.js`,
`bin/check-issue-refs.sh`, plus the new `bin/test-check-issue-refs.sh`), a session attempted that
red-proof three ways. The first was methodologically void, and two of the attempts silently
destroyed uncommitted work - the same uncommitted work, twice - with every command printing output
indistinguishable from success.

The three failures, in order:

1. **Void method.** `git add -A; git stash -q` → run self-test → observe "0 FAILs" →
   `git stash pop -q`. The stash reverted the *new tests together with the new code*, so old tests
   ran against old code. Matched pairs always agree; the "0 FAILs" proved nothing while looking
   like a completed red-proof step.
2. **Silent loss via stash pop + amend.** `git stash pop` (without `--index`) restored the files
   but left everything **unstaged**. The next `git commit --amend -F <msg>` therefore amended only
   the message - and printed "6 files changed, 200 insertions(+)", the same healthy stats as the
   original commit, so the author believed the round-two work was in HEAD. It was not.
3. **Silent loss via checkout-pathspec "restore".** The next red-proof used the correct shape -
   `git checkout origin/master -- .github/scripts/issue-ref-gate.js bin/check-issue-refs.sh`
   (old code only, new tests kept in place) and correctly observed failures - but then "restored"
   with `git checkout HEAD -- <same files>`. Because of loss 2, HEAD did not contain the round-two
   implementation, so the restore overwrote the only surviving copy of the uncommitted work.
   `git checkout <ref> -- <paths>` gives no warning when clobbering uncommitted changes.

Recovery required re-applying the edits from the conversation record (the only surviving copy),
then proving capture: `git add -A`, amend, `git status --short` printing **nothing**, and
`git show HEAD:.github/scripts/issue-ref-gate.js | grep -c "LINE_OPT_OUT\|BLOCK_BEGIN\|FILE_OPT_OUT"`
returning a symbol count only the new work could produce. The final honest red-proof: with the
new tests in the tree and only the two code files checked out from `origin/master`, 7 of 8 harness
cases FAILED and the unit-test run aborted; code was then restored by re-applying, never by
checkout-to-HEAD until HEAD provably contained the work.

## Guidance

**1. A red-proof is OLD CODE + NEW TESTS. Never stash.**

The point of the exercise is a *mismatched* pair: yesterday's implementation, today's assertions.
Any mechanism that reverts code and tests together - `git stash`, `git checkout <old-ref>` of the
whole tree, switching branches - produces a matched pair and a vacuous pass. Two safe shapes:

- Good: `git checkout <old-ref> -- <code files only>` while the new test files stay put. You must
  then restore correctly (see rule 4).
- Best: a throwaway worktree at the old ref with the new test files copied in - zero mutation of
  the live tree, and worktrees are already this repo's house pattern (`AGENTS.md`, "Worktree
  ownership"):

  ```bash
  git worktree add /tmp/redproof origin/master
  cp bin/test-check-issue-refs.sh /tmp/redproof/bin/
  (cd /tmp/redproof && bash bin/test-check-issue-refs.sh)   # MUST fail
  git worktree remove --force /tmp/redproof
  ```

A red-proof that does not fail is a red flag about the *method*, not a green light: first suspect
that you reverted both sides of the pair.

**2. After any commit or amend meant to capture work, PROVE it captured the content.**

An amend that captured nothing prints stats that look exactly like success (`--amend` re-reports
the whole commit's stats, not what the amend added). Two checks, both cheap:

```bash
git status --short          # must print NOTHING
git show HEAD:<file> | grep -c "<symbol only the new work has>"   # must be > 0
```

Pick a symbol unique to the new work (a new constant, a new flag name). `git show --stat HEAD`
is corroboration, not proof - it was exactly the output that lied in this incident.

**3. Treat `git stash pop` as an index-destroying operation.**

Whatever was staged before the stash is not staged after `pop` (only `pop --index` tries to
restore it, and even that can fail on conflicts). Never sandwich a stash between `git add` and a
commit/amend; re-run `git add` and re-verify `git status --short` after every pop.

**4. Treat `git checkout <ref> -- <paths>` as `rm` for uncommitted work.**

It overwrites the working copy with the ref's version, silently, with no reflog entry for what it
destroyed. Before using it to "restore", prove the ref actually contains the version you want
back:

```bash
git show HEAD:<path> | grep -c "<new symbol>"   # must be > 0 FIRST
git checkout HEAD -- <path>                     # only then
```

If the grep returns 0, HEAD does not have your work and the checkout would delete the only copy.
The worktree form of the red-proof (rule 1) avoids this entire trap: the live tree is never
mutated, so there is nothing to restore.

## Why This Matters

The defect class is **git commands whose success output is indistinguishable from the intended
action**. Each command here exited 0 and printed plausible output while doing something other than
what the operator believed: the stash round-trip "verified" nothing, the amend "captured" nothing,
the checkout "restored" a stale version over live work. No step errored; the loss was discovered
only because a later red-proof behaved impossibly.

This repo has already paid for siblings of this class, and documents them:

- A whole catalogue of checks that report success without having run, and the guard-design rules
  that fix them:
  [`a-check-that-reports-success-without-having-run.md`](a-check-that-reports-success-without-having-run.md)
  (this doc is the git-plumbing instance of that genus - its "prove it by making it fail" rule is
  the same move as grepping the new symbol in `git show`).
- Piping a git command through `tail`/`head` swallows the exit code, so a failed
  `git checkout <branch>` in the wrong worktree let an `&&`-chained rebase run against the wrong
  branch - `AGENTS.md`, "Worktree ownership" ("never pipe a git command whose failure must stop an
  `&&` chain"). Same command family, different mechanism: there the checkout *failed* silently;
  here it *succeeded* at the wrong thing.
- Negative results need an instrument that could have said yes:
  [`negative-results-need-an-instrument-that-could-have-said-yes.md`](negative-results-need-an-instrument-that-could-have-said-yes.md)
  - the "0 FAILs" from the void stash method is exactly a negative result from an instrument that
  could not have said yes.

The common counter-move is the same in every case: do not read the tool's success output as
evidence the intended state was reached - **probe the state directly** (grep the symbol in
`git show`, check `git status --short` is empty, count the FAILs that must exist).

The stakes are concrete: here, two rounds of implementation work survived only in the
conversation record. Without that record, the work would have been gone with no reflog entry, no
stash entry, and no error message to say when it died.

## When to Apply

- Every time `bin/AGENTS.md`'s red-proof requirement fires: a new `bin/test-check-*.sh`, or a new
  case added to one after fixing a checker bug. The proof must pit old code against new tests.
- Any workflow that temporarily reverts files to an older ref and then restores them - red-proofs,
  bisects by hand, "let me just check the old behaviour".
- Any `git commit --amend` intended to *add content* (not just reword): verify capture before the
  next destructive command, especially before any `git checkout <ref> -- <paths>`.
- Any use of `git stash` while an index is staged for a specific commit.
- Not needed when the tree is clean and committed: `git checkout <ref> -- <paths>` on a clean tree
  is recoverable via `git checkout HEAD -- <paths>`, because HEAD then provably has everything.

## Examples

**Red-proof - wrong vs right:**

```bash
# WRONG - vacuous: stash reverts new tests AND new code together; matched pairs always agree
git add -A && git stash -q
bash bin/test-check-issue-refs.sh     # "0 FAILs" - proves nothing
git stash pop -q                      # ...and now nothing is staged (see next example)

# RIGHT (in-tree) - old code, new tests; failures are the proof
git checkout origin/master -- .github/scripts/issue-ref-gate.js bin/check-issue-refs.sh
bash bin/test-check-issue-refs.sh     # MUST fail (here: 7 of 8 cases FAILED)
# restore by re-applying or from a ref PROVEN to contain the work - never blind checkout-to-HEAD

# RIGHT (safest) - throwaway worktree, live tree untouched
git worktree add /tmp/redproof origin/master
cp bin/test-check-issue-refs.sh /tmp/redproof/bin/
(cd /tmp/redproof && bash bin/test-check-issue-refs.sh)   # MUST fail
git worktree remove --force /tmp/redproof
```

**Amend capture - trusting stats vs proving content:**

```bash
# WRONG - stash pop unstaged everything; this amends ONLY the message,
# yet prints "6 files changed, 200 insertions(+)" - identical to real success
git stash pop -q
git commit --amend -F /tmp/msg

# RIGHT - stage, amend, then prove capture with state, not stats
git add -A
git commit --amend -F /tmp/msg
git status --short                                       # must print NOTHING
git show HEAD:.github/scripts/issue-ref-gate.js \
  | grep -c "LINE_OPT_OUT\|BLOCK_BEGIN\|FILE_OPT_OUT"    # must match the new symbols
```

**Restoring after a red-proof - blind vs proven:**

```bash
# WRONG - if HEAD lacks the round-two work, this deletes the only copy, silently
git checkout HEAD -- .github/scripts/issue-ref-gate.js bin/check-issue-refs.sh

# RIGHT - prove the ref holds the version you want back BEFORE overwriting the tree
git show HEAD:.github/scripts/issue-ref-gate.js | grep -c "LINE_OPT_OUT"  # 0 → STOP
```
