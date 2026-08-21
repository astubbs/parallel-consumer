# In-flight & parked work - how this directory works

Shared, cross-branch working notes kept on `master`, so any branch or session can see what is open
right now. **Not** an issue tracker and **not** a backlog. This doc owns how these notes are
written; the root AGENTS.md routes here and keeps only what binds every session.

This was one file until 2026-08-04. It became a directory because *every* PR edited it - it appeared
in 26 of the last 30 master commits - so unrelated PRs conflicted with each other constantly, purely
because their notes were adjacent. One file per item means two PRs never touch the same file.

## Writing a note

**One item per file**, named `<category>-<slug>.md`. The prefix is the point: an agent listing this
directory sees the shape of what is open without reading anything.

| Prefix | For |
|---|---|
| `bug-` | A known defect in the product code |
| `test-` | Test-infrastructure problems: flakes, missing coverage, suite behaviour |
| `ci-` | CI, gates, runners, review automation |
| `deps-` | Dependency upgrades held back, and what unblocks them |
| `pr-` | Context about an open PR that `gh` cannot tell you |
| `branch-` | Work sitting on a branch with no PR |
| `release-` | Release status and blockers |
| `parked-` | Deliberately deferred ideas, with the reasoning that will be needed to restart them |
| `next-` | Candidate work, ranked |

New prefixes are fine when something genuinely does not fit. Do not add subdirectories - the prefix
is the grouping. One deliberate exception exists: `clients/<lang>.md` holds the language-proxy
fan-out's per-language notes (astubbs#242) - a family of parallel same-shape items where a prefix
would just re-spell the directory; do not add a second subdirectory without the same argument.

## Labels, and the trailer that outlives the note

Two tags already exist on most notes and are read by tooling and by agents scanning the directory:

```
<!-- inflight-type: bug | test | ci | deps | pr | branch | release | parked | next | feature | task -->
<!-- inflight-impact: performance | correctness | coordination | process | architecture -->
```

**A third is now available**, for cross-cutting marks that are not the note's type or its impact:

```
<!-- inflight-labels: release-note, security, breaking-change, needs-measurement -->
```

Comma-separated, lowercase, hyphenated. Add a label when something needs to be **found later by a
scan that does not know which note to look in** - the release-note sweep being the obvious one.
`grep -rl 'inflight-labels:.*release-note' docs/inflight/` is the whole mechanism.

**But a label on a note cannot survive the note.** This directory's first rule is that a closed item
gets `git rm`'d, so anything that must still be findable after the work lands does **not** belong only
here. Put it in the **commit message**, as a trailer:

```
Release-Note: Adaptive concurrency now backs off when the downstream system saturates.
```

Git history is the durable index. `git log --grep='^Release-Note:'` finds every one of them, across
every note that has since been deleted, and it works at release time when the inflight file that
prompted it is long gone. The repo already uses `Upstream-Issue:` the same way.

**Use both when both apply**: the label so the open item is findable while it is open, the trailer so
the *outcome* is findable once it is not. A label alone is a note to ourselves; a trailer is the
record.

**What earns a `Release-Note:` trailer**: a user- or operator-visible change. Not refactors, not test
work, not internal measurement. The bar is the same as `CHANGELOG.adoc`'s - see the root AGENTS.md on
changelog discipline - and the trailer exists to make assembling that changelog a scan rather than an
archaeology exercise.

## Rules

- **Track only what is currently OPEN**, plus cross-branch context a future branch should inherit.
  When something closes, **`git rm` its file**. Do not rewrite it into a "FIXED/DONE" narrative:
  making a stale entry *accurate* is the wrong move. If it leaves open follow-ups, shrink the file to
  those and rename it.
- **Work your current PR resolves is tracked by that PR - delete its file in that PR.** Never leave a
  "delete this when #NN merges" marker on `master`. The merge is exactly when nobody is looking here,
  so the marker outlives the work and the next reader inherits a stale note that reads as live.
- **Known problems with the code on this branch belong here**, even when a GitHub issue exists - link
  the issue and keep it short. An agent picking up work scans this directory; it will not read every
  issue on the tracker. An unrecorded defect is one the next session rediscovers, or ships on top of.
- **Never write down what a command can answer.** Open PRs are `gh pr list`; branch divergence is
  `git rev-list --left-right --count`; worktrees are `bin/worktree-status.sh`. Copying those here
  creates a second tracker that is wrong within a day and that a reader cannot tell is wrong. Record
  what no command knows: why something is parked, what blocks it, which decision is pending, what
  collides.
- **No committed index.** An index file would be edited by every PR, which is the problem this
  directory exists to solve. `ls docs/inflight/` and `grep -r` are the index. (`docs/todo-index.md` is
  the cautionary case: committed, generated, and stale until a reviewer caught it on astubbs#110.)
- **If you are given new guidance about how these notes are written, update this file too**, so other
  sessions inherit the rule instead of rediscovering it.

## Reference convention

Below `#1000`, **name the repo**: `astubbs#NNN` for this fork, `confluentinc#NNN` for the original.
The fork's numbering sits entirely inside upstream's range, so a bare number is a coin flip - and one
that resolves to the wrong issue looks fine. See
[`docs/issue-references.md`](../issue-references.md) for the full rule;
`.github/scripts/issue-ref-gate.js` enforces it on added lines, so a note written the old way fails CI.
Fork branch names encode the *upstream* number (`bugs/857-...`, `fix/909-...`, `upstream-pr-905`), so
a number in a branch name is `confluentinc#NNN`, never a fork issue.

## Where other things live

`CHANGELOG.adoc` (what shipped) · PR bodies and commit messages (history) ·
[`docs/solutions/`](../solutions/) (lessons from solved problems) ·
[`docs/refactoring.md`](../refactoring.md) (deferred internal work - deliberately still one file, it
is touched by 2 commits in 30) · [`docs/quarantined-tests.md`](../quarantined-tests.md) (quarantine
registry) · [`docs/todo-index.md`](../todo-index.md) (code markers) ·
[`src/docs/development/upstream-map.yaml`](../../src/docs/development/upstream-map.yaml) (the source
of truth for fork↔upstream mapping - record mappings there, not here).
