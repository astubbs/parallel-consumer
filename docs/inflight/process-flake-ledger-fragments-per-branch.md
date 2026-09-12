# The flake ledger forks with every branch, so no reader ever sees all of it

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

About [`test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md) as an **instrument**, not about any
flake in it. The ledger is the evidence route the quarantine discipline depends on, and it is
distributed the way every note here is - which for a register that accretes sightings is a different
proposition than it is for a note that describes one item.

## What the tooling reports

`node bin/inflight.mjs docs header docs/inflight/test-untracked-ci-flakes.md` says the copy on
`origin/master` is the baseline, and then that divergent versions of it, spread across live refs,
**carry content `origin/master` has never held** - out of a ref corpus that includes archival refs as
well as live ones. It prints the largest of those versions by what each one added, and what they add
are not stray lines: they are whole dated sighting sections with their own headings. Run the command
for the shape of it; `node bin/inflight.mjs note drift docs/inflight/test-untracked-ci-flakes.md` and
`node bin/inflight.mjs docs show docs/inflight/test-untracked-ci-flakes.md --ref <ref>` read the
individual versions.

Every number above is deliberately left to the command. It is the fact here most certain to be wrong
tomorrow, because each open branch appends to its own copy.

## Why that matters for this file specifically

- **A reader on one branch sees a fraction of the sightings.** The ledger's own rows argue from
  rate - "one sighting is not a rate", "quarantine needs a rate, not a third data point" - and a rate
  read off one branch's copy is computed over a subset of what the corpus recorded, with no marker
  saying so. That is the signal being wrong rather than absent, which is why this is tagged
  `misdirection`.
- **A sighting recorded on a feature branch is lost when that branch stops moving**, and nothing
  goes red. The branch does not have to die: it only has to have its PR merged, after which nobody
  opens another from it.
- **The quarantine discipline rests on it.** Rule 1 of [`docs/quarantined-tests.md`](../quarantined-tests.md)
  admits two kinds of evidence, *"a diagnosed mechanism, **or** a recorded sighting ledger (dates,
  runs, the failure signature, and what shows it is master-state rather than PR-state)"*. So a
  ledger that fragments per branch cannot serve as the second route without the reader first
  establishing which version they are holding - and the check that route exists to make is precisely
  **master-state versus PR-state**, which a branch-local copy is the worst possible instrument for.

## What was checked before asserting any of that

- **The quarantine dependency is real, quoted above from the registry itself**, not inferred. The
  registry also names `RegistrationRaceStaleResidentIT` as its worked example of the *flake diagnosed
  and fixed* exit, and it says that test *"was quarantined on a sighting ledger with no mechanism"* -
  so the ledger route has been used in earnest, not just described.
- **No divergent version has ever reached master.** By construction of what the command reports -
  content `origin/master` has never held - but the interesting case was checked directly rather than
  assumed. One ref carrying a divergent version is labelled with a **merged** PR, which reads at a
  glance as "so it did come back". It did not: `git merge-base --is-ancestor <ref> origin/master`
  says the ref is still not an ancestor of master, and the sections its copy carries are dated
  **after** that PR's merge date. The branch went on collecting sightings once its PR was merged, and
  there is no longer any PR that would carry them anywhere.
- **The format fragmented too, not only the data.** The ledger has a convention for recording a
  regression of a test it had already declared fixed - a `### Seen again after being called fixed:
  ...` section, stating that a recurrence of something declared fixed is the one case where a single
  sighting is worth writing down. That convention exists only on branch-side versions;
  `origin/master`'s copy has never held it. It was recovered for the sighting recorded in
  [`test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md) by reading another ref's version,
  which is not a step anyone would think to take.
- **Prior art: none.** `node bin/inflight.mjs prior-art 'branch-side ledger' 'fragments per branch'
  'sightings never reach master'` returns nothing under `docs/plans/`, nothing under
  `docs/solutions/`, nothing elsewhere under `docs/`, no open or merged PR and no issue, across the
  whole ref corpus it searches. Its single hit is the ledger itself, for the phrase *branch-side
  sighting ledger*, which astubbs#490 introduced there while retiring a row - the phenomenon has been
  noticed in passing and never written down as a problem.

## Options, for the owner to decide between

Named, not recommended, and none of them costed beyond the obvious. They are not exclusive.

- **A sighting always lands on a master-based branch.** Keeps the ledger where it is and keeps one
  copy authoritative. The cost is friction, and friction is the thing
  [`AGENTS.md`](AGENTS.md) in this directory names as the reason small true things go unrecorded: it
  asks for a second branch and a second PR at the moment someone is in the middle of other work, and
  it separates the sighting from the branch that saw it.
- **The ledger moves somewhere a branch cannot fork** - an issue, or any store outside the tree. The
  cost is everything the in-repo design buys, which that same document states as a deliberate trade:
  it arrives with the code at session start, it shifts as the code shifts, and nobody needs an
  account to read it.
- **The tooling reconciles on read.** `bin/inflight.mjs` already reads every ref, so a unioned view
  is reachable. The costs are that independently written prose does not union cleanly, that a reader
  who opens or greps the file still gets only their branch's copy so the tool becomes the only honest
  entrance, and that nothing would prune sightings whose branches are finished.
- **The sighting evidence stops being prose.** `node bin/inflight.mjs codecov test <name>` answers
  recorded outcome per commit from a central store that does not fork with the tree, and rule 1 of
  the quarantine registry already points at it for exactly this purpose. The cost is that it carries
  outcomes and durations only - not the failure signature read from source, not the mechanism-clear
  that says a branch could not have caused it, and not the master-state argument, which is the half
  the prose ledger exists for. It also warns, on both pages read while writing this note, that it has
  hit a page bound and is not the whole history.

## What is not established here

Whether any sighting has actually been *lost* to this - that would need reading each divergent
version against master's and saying what is only on one side, which was not done. The claim made
here is narrower: master's copy is not the whole ledger, nothing reconciles the versions, and the
discipline that consumes the ledger asks a question a branch-local copy cannot answer.
