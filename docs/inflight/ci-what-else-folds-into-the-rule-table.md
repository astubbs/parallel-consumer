# What else folds into the rule table - surveyed, and it is a short list

<!-- inflight-type: task -->
<!-- inflight-impact: refactor -->

`bin/lib/source-patterns.mjs` exists so a rule is a row rather than a script. The obvious next question
is which of the remaining `bin/check-*.sh` are really just pattern matchers. Surveyed 2026-09-01, and
the answer is **one strong candidate and no queue behind it** - recorded so nobody re-derives it, and
so nobody assumes the table should swallow gates it does not fit.

## The one real candidate: `check-shell-hazards.sh`

**It is already a rule table, written in shell.** A `HAZARDS` heredoc of patterns with categories and a
`why` per entry, a file-level opt-out (`# hazard-ok-file:`), and a comment guard whose own comment says
*"A COMMENT ABOUT A HAZARD IS NOT A USE OF IT"*.

That is the same design, and it arrived there independently - which is the best available evidence that
the design is right, and the reason this fold is worth doing. It also means the fold is not a one-liner:
the runner would have to match its opt-out semantics and its comment handling exactly, and getting
either subtly different is how a migration quietly covers less than the thing it replaced. That already
happened once here - the first sigpipe port flagged thirteen files the shell gate passes, because it
lacked the comment guard.

**Do it as its own change, with its arms ported case for case**, the way sigpipe was.

## What does NOT fold, and why - so nobody tries

- **`check-action-versions.sh`** - aggregates across files (collect every `action@version`, find the
  ones appearing twice). A per-file forbidden pattern cannot express "these two files disagree".
- **`check-quarantine-registry.sh`** - cross-file consistency between the registry and the annotations
  it claims to describe. Same reason.
- **`check-inflight-tags.sh`** - validates a value against a closed vocabulary. That is a REQUIRED
  pattern, not a forbidden one, and adding a "must match one of" mode to fit it would be expanding the
  abstraction to swallow a gate rather than because the abstraction wanted it.
- **`check-shell-lint.sh`** - wraps ShellCheck. It is not a candidate for folding, it is the thing the
  table should defer TO.
- **Everything that talks to GitHub** (`check-human-lgtm`, `check-review-posted`, `check-pr-ready`,
  `check-quarantine-owners`, `check-branch-self-reference`, `check-issue-refs`,
  `check-pr-analysis-surfaces`) - these are programs that fetch and reason, not greps.

## The rule that governs additions

**Check what we already run before adding a row.** ShellCheck, SpotBugs with
fb-contrib/findsecbugs/findbugs-slf4j, Infer, forbiddenapis, ArchUnit and CodeQL are all in this build.
A rule one of them covers must not become a row - a second implementation of somebody else's check will
disagree with theirs eventually, and then both are suspect. Two rules were written for this table and
deleted before it shipped for exactly that reason; the header of
`bin/lib/source-patterns.mjs` records which and why.
