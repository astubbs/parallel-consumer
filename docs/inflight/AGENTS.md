# In-flight & parked work - how this directory works

**A structured wiki about the code's current situation, kept in the repository.** One file per
item, tagged with a type, an impact and a state, and delivered into an agent's context at session
start by `.claude/hooks/inject-recorded-knowledge.sh`. This doc owns how a note is written; the root
`AGENTS.md` routes here and keeps only what binds every session.

**It sits one layer DOWN from GitHub Issues, and is not a mirror of them.** A few things have a
reflection in both - an issue for the public record, a note for what working on it actually
involves - but most notes have no issue and never will, because they are too small, too transient,
or too specific to a branch to be worth one. Issues are the public, linkable, coarse layer. This is
the fine-grained layer underneath: what is true about the code *right now*.

**It is where tribal knowledge goes when there is no tribe.** Every codebase carries a body of
understanding that is not in the code: what was tried and abandoned, which test lies, why that
option exists, which branch you must not merge before doing something else first. A team absorbs it
by osmosis - standing next to each other for a year. **An agent gets none of that.** It arrives with
the code and nothing else, every session, forever, and the most expensive failures come from acting
confidently on a codebase whose unwritten rules it cannot see.

So the knowledge has to stop being tribal and start being an artefact - not documentation of how the
system *works*, which the code and the topic docs already carry, but of what is *true about it right
now*. That is a different thing and it has never had a home. This is that home.

**The point is friction.** Tracking reality only happens if recording it is cheaper than not
recording it. Opening an issue, choosing labels, and writing for an audience is enough friction that
the small true things never get written down - and those are exactly the things that cost the next
person a day. A file in the tree you are already editing is fast enough to actually do.

Being in the repo is what makes it track reality rather than describe a past one:

- **It shifts as the code shifts.** A note travels on the branch with the work that produced it, so
  context that is only true on one branch lives there - and stops being true when that branch does.
- **It arrives with the code.** Delivered at session start, so an agent inherits state it did not
  create instead of needing to know a search term. The failure this prevents is not misreading a
  note; it is never learning the note exists.
- **Nobody needs an account.** Readable by anything that can read the tree.

**Never write down what a command can tell you** - open PRs are `gh pr list`, branch divergence is
`git rev-list --left-right --count`, worktrees are `bin/worktree-status.sh`. A second copy of an
answerable fact is wrong within a day, and a reader cannot tell which one is stale.

The trade, stated rather than glossed: distribution is git's, so two branches can disagree about
what is open, and a note read on a stale branch can describe a world that no longer exists.

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

## Tagging a note

**This section is the source of truth for the tag vocabulary.** `bin/check-inflight-tags.sh`
enforces it and names this file when it fails, so a value here and a value there must never
disagree - change both in the same commit.

Three fields, as HTML comments after the heading. Only `inflight-type` is always required:

```markdown
<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->
<!-- inflight-state: closed - will not do -->
```

- **`inflight-type`** - what KIND of item it is. One of **`bug`**, **`feature`**, **`task`**. This is
  a tracker, so it uses a tracker's vocabulary.
- **`inflight-impact`** - what it COSTS you to not know. **Required on `bug` and `task`; forbidden on
  `feature`** - proposed work has an opportunity, not a consequence.
- **`inflight-state`** - disposition. **Absent means open**, which is the common case, so most notes
  carry two fields. When present it must give a reason: `<state> - <why>`.

**Classify by CONSEQUENCE, not by what kind of file it is.** The `<category>-` filename prefix
already says the latter. The impact answers the question a reader actually has, and it spans
prefixes - `misdirection` covers notes filed under `ci-`, `test-`, `branch-`, `deps-` and `static-`.

The impacts, in the order `.claude/hooks/inject-recorded-knowledge.sh` presents them at session
start. **The order is not severity - signal integrity comes first**, because you cannot judge the
state of the code through instruments that lie, and acting on a false green is worse than acting on
nothing:

| Impact | Valid on | The consequence |
|---|---|---|
| `misdirection` | bug | the signal is actively WRONG - a green that asserted nothing, a hidden flake, a contaminated control arm, a scanner returning 401 while appearing to scan |
| `blind-spot` | bug | there is no signal - untested behaviour, unscanned code, an obligation nobody tracks |
| `data-loss` | bug | a record is dropped or mis-committed |
| `stall` | bug | something stops making progress and stays stopped |
| `security` | bug | a grant or permission wider than it should be |
| `config-lie` | bug | an option does not do what it says |
| `throughput` | bug | backpressure or fetch behaviour is wrong, with no data risk |
| `release-gate` | task | blocks publishing |
| `coordination` | task | two pieces of work will collide, or one is blocked waiting on another |
| `stranded-work` | task | work or knowledge that will be lost if nobody acts |
| `deps-debt` | task | upgrades deliberately held back |

**An impact names a CONSEQUENCE, never a state.** "in progress", "standard work", "medium" and the
like are status labels wearing an impact's clothes - they answer "where is this?" when the question
is "what does it cost me to not know?". An open PR is not its own impact: not knowing about it costs
you a collision, so it is `coordination`. This rule exists because the first draft added an
`active-work` value and had to remove it. `candidate` and `decided-no` were removed for the same
reason - the first is `type: feature`, the second is a `state`.

**Add a value when the corpus needs one, do not force a note into a poor fit** - the set was derived
by reading the notes, not chosen in advance. Add it to this table AND to `bin/check-inflight-tags.sh`
in the same commit, and say why. The gate checks SHAPE, never judgement: it cannot tell you that a
valid impact is the wrong one for that note, and six such corrections were needed when this scheme
landed.

**A note that no group claims is listed at session start under its own heading**, so a missing or
misspelt tag is visible rather than silent - and a note carrying a state is excluded from the index
with a count, never silently.

**If everything is high, nothing is.** The block has to stay short enough that it is read rather
than skimmed past - so a handful of notes, not a category. When you add one, look at the others and
ask whether one of them has stopped earning it; a marker is not permanent, and the work landing is
not the only reason to remove it.
