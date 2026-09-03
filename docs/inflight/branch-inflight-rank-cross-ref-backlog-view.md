# `inflight rank` - the cross-ref backlog view, and what it refuses to say

<!-- inflight-type: task -->
<!-- inflight-impact: process -->

A read-only subcommand on `bin/inflight.mjs` that reads open in-flight notes from every ref, groups
them the way the session index groups them, and reports where that picture disagrees with
`process-candidate-ranking.md`. The design is in
`docs/plans/2026-09-03-002-feat-inflight-rank-backlog-view-plan.md`; what is here is the part a
later reader would otherwise take for an oversight.

## The gather step was already solved, and the command says so

`bin/inflight.mjs docs list inflight <impact>` already reads every ref, keeps open notes, groups them
by impact, scopes to one bucket, and marks an off-baseline note with the branch it was read from.
`rank` does not rebuild any of that - it reuses `corpusIndex`, `classifyNote` and `inflightGroupOf`,
and the group rule in particular must stay identical or the two surfaces would rank the same note
differently. What `rank` adds is the carrying branch's pull request, the live-versus-archival split,
the filename number, the register delta, and an accounting of the open notes no impact bucket
claimed.

## It never says a branch fixes a note, and that is the point

A note travels on the branch that produced it. That makes "which branch carries this note" cheap and
"which branch fixes what this note describes" unavailable - and conflating them is the expensive
wrong answer. `core-revoke-commit-skips-the-work-mailbox-drain.md` is the worked case: it exists on
one branch only, and the note's own text says the bug predates that branch's pull request. A row
reading "owned by" that pull request would be confidently wrong.
<!-- file-refs: N/A - the cited note exists only on a branch; being absent from the baseline is the property that makes it the worked example -->

So the command reports two relations it can prove - the number in the filename position, and which
refs carry the note - and prints neither as ownership. `fixes` is never printed.

## The `candidate` relation was designed, measured, and dropped

An earlier cut carried a third relation: a branch whose name encodes a number matching the note's.
It was dropped rather than shipped behind a caveat. Only a small minority of notes carry a positional
number at all, the matches that fire are dominated by a single issue family, and some are
cross-namespace by construction - `docs/inflight/AGENTS.md` records that a note filename carries a
fork number while branch names here encode upstream ones, and that the `pr-` prefix carries a pull
request rather than an issue. Reproduce the population with
`ls docs/inflight/ | grep -cE '^[a-z]+-[0-9]+-'`.

A relation whose own caveat tells the reader it may be meaningless reintroduces exactly the
confidently-wrong hint the ownership refusal exists to prevent, and acting on it means redoing by
hand the check the command was built to remove. It is recorded as deferred in the plan, not as a
rejected idea: a stronger signal - a pull request body naming the note path - could earn it later.

## The filename number is printed without a repository

`docs/inflight/AGENTS.md` states the `<area>-<NNN>-<slug>` convention and its exceptions in the same
breath: pre-convention names carry confluentinc numbers, and `pr-` carries a fork pull request. The
root `AGENTS.md` rule that settles the design is that a wrong reference which resolves is worse than
a broken one - so the row prints the number and a repo-qualified command, and never asserts which
repository owns it.

## The open filter follows the code, not a list of words

`classifyNote` sets `open` from the *presence* of any `inflight-state:` marker, not from the words in
it. A note declaring `inflight-state: open - <reason>` is therefore not open by that rule, and one
such note is on the baseline today. Stating the filter as a list of state words would have forked the
requirement from the code; it is stated in the code's terms instead.

## Open notes outside the impact buckets are counted, not dropped

`inflightGroupOf` sends a register to `registers`, an impact-less feature to `feature`, and an
unknown or misspelt impact to `unmatched`. Keeping only the impact buckets would drop the largest
group in the corpus silently - and the register names at least one note in each of the first two. So
`rank` emits `feature` and `unmatched` after the impact buckets, and its closing line accounts for
every open note the impact buckets did not claim. `bin/inflight.mjs docs list inflight` prints the
per-group figures.

## The filename number is attributed by the NOTE, not by the convention

The first cut printed `gh issue view <n> -R astubbs/parallel-consumer` for every non-`pr-` number,
on the reasoning that the convention says the number is this fork's. `AGENTS.md` names the exceptions
in the same breath, and one of them is on the baseline: `bug-857-family.md`, whose own title reads
"The confluentinc#857 family". That command resolves to the wrong issue the moment this fork's
counter passes 857 - the failure `AGENTS.md` rates worse than a broken reference.

So the number is attributed by the note's own text: a qualified mention of its own number decides
it, and a note that names neither gets both lookups and is labelled unattributable. The filename
alone cannot carry this, which is why the first cut was wrong and its docstring said the opposite of
what its code did.

## Two crashes that only a real corpus produced

Both were found by running the command, not by reading it, and both are now pinned by a check whose
mutant restores the defect.

- **A note closed on every live ref but still open on a preserved tag.** The chosen version's refs
  and the path's live refs are then disjoint sets, so the read ref resolved to nothing and the row
  threw on `.replace()`. The read ref now comes from the chosen version's own refs.
- **A number's text read from outside its scope**, which threw on any note carrying one - most of
  them - and exited 1, neither of the tool's two documented codes.

The second is the more useful lesson: the command has a `{ok, reason}` contract and an exit-code
contract, and neither protects against a `ReferenceError`. Nothing in this repository asserts that
the front door cannot throw, so the guard is the self-test running the real command end to end.

## A note the ref listing named and `cat-file` did not return

`blobContents` can answer ok overall while one blob comes back `missing` - a partial clone, a gc
race. That note was being dropped with no accounting, which is the failure-rendering-as-an-empty-
result shape this whole file is organised against. Unreadable paths are now named and the run says
the answer is incomplete.

## Citing the interface rule is not following it

The command's own help text says every level of this front door prints the next level's commands -
and its rows did not. `docs list inflight <impact>` prints `docs show <path>` beside each row; `rank`
printed the path and left the reader to know that command exists. A scoped group with no rows printed
nothing at all, and the exclusion counts were whole-corpus while sitting among scope-limited lines.

All three were invisible to the suite for the same reason: every check drove the `rank()` data
function and nothing asserted on rendered text, so the view had no coverage at all. Worth knowing for
the next command added here - a view that no check renders is a view whose contract is unenforced,
however carefully the query underneath it is tested.

## Pull requests come from the bulk snapshot only

`branchView` falls through to `prForBranch` when the bulk map misses, because it answers about one
branch. `rank` answers about many, `bin/lib/cache.mjs` deliberately does not cache an absence for
that kind, and `prForBranch` passes no timeout - so the same fall-through here would fire a fresh,
untimed `gh` subprocess per pull-request-less branch on every run. `rank` uses the snapshot and its
age, and prints `bin/inflight.mjs branch <ref>` as the command that answers exactly for one branch.
