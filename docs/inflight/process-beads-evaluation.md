# Run Beads against this repo's actual corpus, and record what it replaces

<!-- inflight-type: task -->
<!-- inflight-impact: process -->

[`process-adopt-external-harness.md`](process-adopt-external-harness.md) **owns the adopt-or-build
decision** and defers it until after v6. This note owns the half that does not have to wait: the
evidence. Deferring a decision is not a reason to defer the measurement it will be made on, and the
measurement is the expensive part - by the time v6 ships, whoever picks this up will otherwise be
starting from the same README everyone starts from.

**Run it; do not read about it.** [`docs/agent-harness.md`](../agent-harness.md)'s standing rule is
that claims about harness behaviour are tested rather than read off the documentation, and it exists
because that file's own first version asserted four things that turned out false, each with a design
already built on top. A tracker's feature list is exactly the kind of claim that reads as settled and
is not. The deliverable here is a transcript of it operating on this repository's notes, not a
comparison table assembled from two websites.

## The question that probably decides it, and it is not a feature question

**Where does the state live, and what happens when two branches both change it?**
`docs/inflight/` was one file until 2026-08-04 and became a directory because every PR edited it - it
appeared in 26 of the last 30 master commits, so unrelated PRs conflicted purely because their notes
were adjacent. One file per item is what fixed that, and it is also what makes a note travel on the
branch that produced it and stop being true when that branch does.

A tracker keeping its state in one JSONL file, one database, or anywhere outside the tree gives that
property back up. If adoption reintroduces the conflict that split this directory, no amount of
dependency-graph query power pays for it - so establish this **before** evaluating anything else, on
a real two-branch test rather than from the storage format alone.

## What to measure, in this order

1. **Storage and merge behaviour**, as above. Two branches, each adding and editing an item, merged.
2. **Does it round-trip our corpus without losing the axes?** Every note carries a type, an impact and
   optionally labels, and those are three deliberately independent axes - the filename prefix says the
   AREA, the impact says the CONSEQUENCE, the labels say the MECHANISM.
   [`AGENTS.md`](AGENTS.md) owns why none of the three can be collapsed into the others. A tracker
   offering one severity field and one tag bag stores our data lossily, and the loss is silent.
3. **Does it cache a GitHub link graph?** This is the specific thing that would replace work we are
   otherwise about to build - what closes what, what references what, resolved closing references.
   Beads advertises dependency graphs and context injection, which is close enough that building
   before checking would be careless; whether those edges are *its own* items or *GitHub's* is the
   whole question.
   The note stating that gap, `ci-issue-index-has-no-edges.md`, is on `docs/inflight-gh-link-graph`
   and has not merged - named rather than linked, because a link from master would dangle until it
   does.
4. **Does its context injection beat ours?** Ours orders by impact with signal integrity first -
   `misdirection` above everything, because acting on a false green is worse than acting on nothing.
   Generic severity ordering does not encode that, and if it cannot be configured to, the injection is
   a downgrade wearing the same name.
5. **What does it do that we have no answer to at all?** Memory decay is the advertised one, and we
   have nothing like it - the anti-inflation duty currently sits on a human noticing the index has got
   long. That is the case *for* adoption and should be recorded as carefully as the cases against.

## Then answer the three questions, in these words

- **What it gives us** - capabilities, demonstrated, with the command that showed each.
- **What of ours it replaces** - name the SURFACE, not the file: the note format, the tag gate, the
  session index, the issue index, the push and merge reminders. Partial replacement is the likely
  answer and the dangerous one, because a half-adopted tracker means two places to look.
- **What we have beyond it.** The parent note already claims two - that the conventions are not in
  anybody's tooling, and that per-gate self-test density is unmatched. **Those are claims to test
  here, not to repeat.** The third is the impact vocabulary itself: it was derived by reading this
  corpus, not chosen in advance, and it is the part least likely to survive a migration precisely
  because it looks like it could be swapped for any other tracker's priority field.

## Delete when

The verdict is recorded in [`process-adopt-external-harness.md`](process-adopt-external-harness.md),
which owns it. This note holds the evidence-gathering only, and must not grow a second copy of the
decision.
