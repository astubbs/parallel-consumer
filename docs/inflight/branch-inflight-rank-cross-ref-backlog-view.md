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

## Pull requests come from the bulk snapshot only

`branchView` falls through to `prForBranch` when the bulk map misses, because it answers about one
branch. `rank` answers about many, `bin/lib/cache.mjs` deliberately does not cache an absence for
that kind, and `prForBranch` passes no timeout - so the same fall-through here would fire a fresh,
untimed `gh` subprocess per pull-request-less branch on every run. `rank` uses the snapshot and its
age, and prints `bin/inflight.mjs branch <ref>` as the command that answers exactly for one branch.
