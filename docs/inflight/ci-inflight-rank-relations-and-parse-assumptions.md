# `inflight rank` - the relation it will not print, and the assumptions nothing enforces

<!-- inflight-type: task -->
<!-- inflight-impact: process -->
<!-- inflight-vetted: 2026-09-07 - re-read the code behind every item: `bin/lib/docs-shape.mjs` still picks `index.baseline` unconditionally and `[...cluster.liveRefs].sort()[0]` otherwise, and still groups with the imported `inflightGroupOf`; `unreadableRefs` is read only by `formatRank` in `bin/lib/views.mjs` (and `rank.mjs`), not by `formatFind` or `formatStranded`; and `docs show` still resolves its path with `args.filter(...)[0]` in `bin/lib/docs-commands.mjs`. The `candidate` relation is still absent and `parseRegister`'s indentation assumption still unenforced -->

What is still open about `bin/inflight.mjs rank` once it has landed. The design and the alternatives
are in `docs/plans/2026-09-03-002-feat-inflight-rank-backlog-view-plan.md`; the reasoning behind each
decision is in `bin/lib/rank.mjs`'s own header, beside the code it explains; the worked example is in
`docs/inflight-tool.md`. Nothing here restates any of those.

The general lessons this command paid for twice have durable owners and are no longer here:
`docs/solutions/best-practices/a-source-that-can-disagree-with-itself-needs-a-reader-that-says-so.md`
(version selection, and parsing a document written for people) and
`docs/solutions/test-issues/a-view-no-check-renders-has-an-unenforced-contract.md` (the renderer that
no check drove, and the control whose mutant died on a field nothing renders).

## The `candidate` relation is deferred, not rejected

An earlier cut carried a third relation beside carriage and the filename number: a branch whose name
encodes a number matching the note's. It was **dropped rather than shipped behind a caveat.** Only a
minority of notes carry a positional number at all, the matches that fire are dominated by a single
issue family, and some are cross-namespace by construction - `docs/inflight/AGENTS.md` records that a
note filename carries a fork number while branch names here encode upstream ones, and that `pr-`
carries a pull request rather than an issue. Reproduce the population with
`ls docs/inflight/ | grep -cE '^[a-z]+-[0-9]+-'`.

A relation whose own caveat tells the reader it may be meaningless reintroduces the confidently-wrong
hint the ownership refusal exists to prevent, and acting on it means redoing by hand the check the
command was built to remove.

**What would earn it:** a signal that binds the branch to the note rather than to a number - a pull
request body naming the note path is the obvious one. That is a different query, not a stronger
heuristic over the same filename.

## A continuation line is assumed to be an indented line

`parseRegister` treats a list item as owning every following **indented** line, and any non-indented
line as ending the item. That is what keeps the register's own "What is NOT on this list" paragraph
out of the last entry, and it fits how `process-candidate-ranking.md` is written today.

**Nothing enforces it.** A register that wraps a continuation flush-left - which markdown permits and
a human writing prose would not think twice about - silently loses that half of the entry, and the
delta then reports the entry as citing a note that does not exist. The failure is a wrong answer, not
an error.

Left as an assumption deliberately: the alternatives are a real markdown parser, which is a large
dependency for one document, or a rule the register's authors must remember, which is the kind of
unenforced convention this repository keeps finding rotted. The honest middle - **have the parse say
when an entry resolved to nothing so the reader can look** - is what the coverage line already does.

## `carryingRefs` counts refs carrying *any* version, `readRef` names one

A row reads `carried by N refs, read from X`. Those two numbers answer different questions: `N` is
every ref carrying **any** version of that path, while `X` is a ref of the **chosen** version. So a
note carried by many refs, most of which have closed their copy, reads as more widely carried than it
is - the sentence overstates carriage of the version actually being reported.

It is not wrong, and both halves are labelled, which is why it was left. The fix is to count the
chosen version's own refs and say so; the reason to wait is that `N` over the whole path is also the
useful number for a branch-only note, so the row may want both rather than a replacement.

## The same defect classes, found elsewhere and deliberately not fixed here

Merge-prep sweep for other instances of what review found in `rank`. Three live, each left alone
because fixing it changes a command outside this work's scope - not because it is not real.

**`docsShape` still makes both version choices `rank` had to abandon.** `bin/lib/docs-shape.mjs`
picks `index.baseline` unconditionally when the path is on the baseline, and
`[...cluster.liveRefs].sort()[0]` otherwise - the two defects `rank` fixed in that order. It then
groups the note it read with the same imported `inflightGroupOf`, so a note deferred on the baseline
and open on a branch is grouped as deferred in the session-start index every agent is handed.

This is the sharpest of the three and the most expensive to change: that index is injected into every
session, so altering which version it reads changes what every agent sees. It is also the case `rank`
exists to *report* - the two surfaces disagreeing is a finding the tool can now surface about itself.
Worth taking deliberately, with a measurement of how many documents move, rather than as a rider.

**`note find` and `stranded` ignore `unreadableRefs`.** Both call `corpusIndex` and neither reads the
field, so a ref whose listing failed leaves them answering from a corpus they know is incomplete, at
exit 0. `stranded` is the one that matters: a ref that could not be listed can make a cluster look
preserved when a live ref carries it. `rank` now fails the run for this; the other two do not, and
`runFailure` in `bin/lib/rank.mjs` is the shape to reuse rather than re-derive.

**`docs show` takes the first positional and drops the rest.** `showDocument` resolves the path with
`args.filter(...)[0]`, so `docs show a.md b.md` silently answers about `a.md` - the same
answering-a-different-question shape `rank stall` had. `docs for-branch` refuses extra arguments and
is the model to copy.

**Checked and ruled out.** `bin/lib/terms.mjs` already recognises all three issue-reference spellings
and its docstring says so, so the short-form-only regex was one instance, not a pattern. Among the
self-test's controls, only the ownership refusal asserted on a data field whose wording is written
out independently; the other `JSON.stringify` assertions are about data the renderer prints directly.

## Where this note goes next

Nothing here is blocked; every item is a decision someone could take up. When one is taken up, its
paragraph moves with the work rather than being marked done here.
