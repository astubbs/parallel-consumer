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

**One item per file**, named `<area>-<slug>.md`. The prefix says the **area** a note is about, so an
agent listing this directory sees the shape of what is open without reading anything.

**A filename must never carry state.** It is the note's identifier, and state changes while a name
does not: rename it and every citation breaks, leave it and the name lies. `parked-` and `next-` were
removed for exactly that - `parked-docs-site` was open, `parked-stale-arrival-guard-...` was tagged
`blocked`, and **all 23 `next-` files were `open`**, so the prefix ranked nothing while implying a
ranking. Where a note sits is `inflight-state`'s job; what it is ranked against is
`process-candidate-ranking.md`'s.

| Prefix | For |
|---|---|
| `core-` | Product behaviour: the engine, its contracts, proposed changes to them |
| `bug-` | A known defect in the product code |
| `test-` | Test-infrastructure problems: flakes, missing coverage, suite behaviour |
| `static-` | Static analysis: ArchUnit, SpotBugs |
| `perf-` | Performance work and measurement |
| `ci-` | CI, gates, runners, review automation, the agent harness |
| `deps-` | Dependency upgrades held back, and what unblocks them |
| `pr-` | Context about an open PR that `gh` cannot tell you |
| `branch-` | Work sitting on a branch with no PR |
| `release-` | Release status and blockers |
| `upstream-` | Upstream tracking: mirrors, sweeps, coverage obligations |
| `process-` | How the work itself is organised, ranked or recorded |
| `docs-` | Documentation deliverables |
| `web-` | The web GUI / demo |

New prefixes are fine when something genuinely does not fit **and names an area, not a status**. Do
not add subdirectories - the prefix is the grouping.

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

## What belongs here, and what belongs in `docs/refactoring.md`

**The axis is weight, not timing.** [`docs/refactoring.md`](../refactoring.md) is a lightweight list
of refactors *not complex enough to deserve their own note* - a line or two, no owner, no tags, no
state. Nothing about being there says when the work happens.

A note here is anything needing context, evidence, tracking or a decision - **including work decided
to happen later**, which carries `inflight-state: deferred - <what it waits on>` and stays here.

**When a `refactoring.md` line outgrows a line or two - it acquires a decision, a blocker, or evidence
worth keeping - promote it to a note here and delete the line in the same commit.** Neither file may
state it twice; that is how the two drift apart.

## Tagging a note

**This section is the source of truth for what each tag MEANS.** The machine-readable sets live in
`bin/lib/inflight-tags.sh`, sourced by both the gate (`bin/check-inflight-tags.sh`, which names this
file when it fails) and the session index - so a value here and a value there must never disagree:
change this table and that lib in the same commit.
<!-- file-refs: N/A - the tag gate and its shared vocabulary lib ship in astubbs#324; this doc is their owner and lands first, so the notes it describes are never explained by a retired scheme -->

Three fields, as HTML comments after the heading. Only `inflight-type` is always required:

```markdown
<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->
<!-- inflight-state: closed - will not do -->
```

- **`inflight-type`** - what KIND of item it is. One of **`bug`**, **`feature`**, **`task`**,
  **`register`**. This is a tracker, so it uses a tracker's vocabulary - with one addition a tracker
  usually lacks. A **register** is consulted, never completed: a ranked backlog, a collision list. It
  has no done state, so filing one as a `task` implies a discrete action someone could finish, and
  sorts it among the things waiting to be done when it is the thing you READ to decide what to do
  next. Registers appear in their own section at the TOP of the session index, above open work.
- **`inflight-impact`** - what it COSTS you to not know. **Required on `bug` and `task`; optional on
  `feature`, and expected whenever the feature addresses a consequence.** The tag exists so work falls
  out in priority order, not so it is filed under the correct part of speech: a commit-failure seam
  whose motivation is PC shutting down is `feature` + `crash`, and tagging it impact-less buries it
  among cosmetic features. A genuinely new capability with no problem behind it carries none.
- **`inflight-state`** - disposition. **Absent means open**, which is the common case, so most notes
  carry two fields. When present it must give a reason: `<state> - <why>`.

  **A state containing `deferred` OR `parked` means decided-but-not-now, and is treated differently
  from closed.** `<!-- inflight-state: deferred - after v6 -->`. Deferred notes leave open work and get
  their own section at the BOTTOM of the session index, ordered by the same impact scale, each showing
  its reason - so `grep 'deferred - after v6'` finds a release's worth of them.

  **`parked` and `deferred` are the same disposition** - Antony's ruling, and the two words never
  named different things here. The word can sit anywhere in the state and in either order: `parked`,
  `deferred - parked`, `parked - deferred, gated on X` all read as deferred. Requiring one word, in
  one position, is what stranded notes: a state the index recognised as neither open nor deferred
  fell in with `closed` and `blocked` under "not shown", which is where two notes vanished on
  astubbs#323. `closed` and `blocked` still mean what they say and are still excluded.

  **The rule that makes this a schedule rather than a label: all non-deferred work happens before any
  deferred work.** Running out of open work is the trigger to re-read the deferred section. That is
  also why nothing needs re-tagging when a version ships - a note is touched only when the decision
  about it actually changes, never because the calendar moved. Write the reason as what you are
  waiting for (`after v6`, `needs a product decision`), not as a priority.

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
| `security` | bug, task, feature | a grant or permission wider than it should be, or a known exposure carried |
| `config-lie` | bug | an option does not do what it says |
| `throughput` | bug | backpressure or fetch behaviour is wrong, with no data risk |
| `release-gate` | task | blocks publishing |
| `coordination` | task | two pieces of work will collide, or one is blocked waiting on another |
| `stranded-work` | task | work or knowledge that will be lost if nobody acts |
| `deps-debt` | task | upgrades deliberately held back |
| `crash` | bug, feature | the process dies, or exhausts a resource until it does - a leak ends here |
| `reliability` | bug, task, feature | it survives less than it should, and no single defect is named yet |
| `ci` | task, feature | the build, gates, review automation or agent harness are wrong or missing |
| `test-debt` | task, feature | tests that should exist and do not |
| `refactor` | task, feature | the code is harder to change than it needs to be |
| `process` | task, feature | how the work itself is ranked, recorded or organised |

**An impact names a CONSEQUENCE, never a state.** "in progress", "standard work", "medium" and the
like are status labels wearing an impact's clothes - they answer "where is this?" when the question
is "what does it cost me to not know?". An open PR is not its own impact: not knowing about it costs
you a collision, so it is `coordination`. This rule exists because the first draft added an
`active-work` value and had to remove it. `candidate` and `decided-no` were removed for the same
reason - the first is `type: feature`, the second is a `state`.

**Add a value when the corpus needs one, do not force a note into a poor fit** - the set was derived
by reading the notes, not chosen in advance. Add it to this table AND to `bin/lib/inflight-tags.sh`
in the same commit, and say why. The gate checks SHAPE, never judgement: it cannot tell you that a
valid impact is the wrong one for that note, and six such corrections were needed when this scheme
landed.
<!-- file-refs: N/A - the tag gate and its shared vocabulary lib ship in astubbs#324; this doc is their owner and lands first, so the notes it describes are never explained by a retired scheme -->

**A note that no group claims is listed at session start under its own heading**, so a missing or
misspelt tag is visible rather than silent - and a note carrying a state is excluded from the index
with a count, never silently.

**The index is only useful while it is short enough to be read.** Every open note now appears at
session start, so the anti-inflation duty moved from a "high" marker (the old scheme) to the ledger
itself: when you add a note, look at the others and ask whether one has stopped earning its place -
delete it or give it a state; the work landing is not the only reason to remove one.
