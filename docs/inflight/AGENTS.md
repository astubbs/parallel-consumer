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
is the grouping.

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

## Marking a note high priority

Most notes here are looked up when you go looking. A few describe things you will **collide with
without knowing they exist** - and for those, being findable is not enough. Such a note carries, on
the line after its heading:

```markdown
<!-- inflight-priority: high - one line saying why, in the imperative -->
```

`.claude/hooks/inject-recorded-knowledge.sh` lifts every marked note into a **Read these first**
block at session start, with that reason, above the flat list of everything else. The reason is the
payload: "live flakes; read this before diagnosing any red test" tells an agent *when* the note
applies, which a filename cannot.

**The test is collision, not importance.** Every note here matters or it would be deleted. What
earns the marker is that an agent working on something unrelated will otherwise waste the work, or
repeat it, or draw a wrong conclusion. The first one is
[`test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md), which qualifies exactly that way: a
red test on a docs-only branch has been diagnosed from scratch twice, on astubbs/parallel-consumer#308
and astubbs/parallel-consumer#320, while the ledger already held the sighting.

**If everything is high, nothing is.** The block has to stay short enough that it is read rather
than skimmed past - so a handful of notes, not a category. When you add one, look at the others and
ask whether one of them has stopped earning it; a marker is not permanent, and the work landing is
not the only reason to remove it.

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
