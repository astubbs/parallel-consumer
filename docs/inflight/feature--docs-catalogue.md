# A machine-readable catalogue of the product's features

Named after three failed attempts, each of which misled the ideation that read it. It was
`next-per-pr-docs-and-feature-index.md`, which put the PR first when a PR is merely when an entry gets
written. Then `next-feature-docs-index.md`, which reads as an index *of feature docs*. Then
`next-feature-index.md`, which still said "index" when there is no index file at all - there is a
directory, and a single listing file is the thing `docs/inflight/AGENTS.md` forbids because every PR
would edit it. It is a catalogue: a collection, not a lookup.

**Starting now.**

## What it is, and who it is for

A permanent, machine-readable catalogue of what the product does. One file per feature. It carries enough
prose per entry to be useful to a human and to seed the documentation site, but the structure is the
point and the prose rides along.

**Its primary consumer is an agent working on one narrow thing.** That is the reason it exists in this
shape. A general documentation site, or a single large README, is too unstructured for an agent to
write into safely: it has to read a lot to find where its change belongs, it can only get there by
editing something shared, and a wrong guess damages material it was never working on. Give the same
agent one file that it owns, in a declared format, addressed by the feature it is working on, and it
can write a complete, correct entry without reading or risking anything else.

The conflict-free property follows from that rather than being the goal. Two agents on two features
write two files and cannot collide, but the deeper win is that neither had to understand the whole
corpus to contribute to it correctly.

## It is not like `docs/inflight/`, and the difference matters

Both are directories of single-item files, and the mechanism is deliberately borrowed. The lifecycle is
the opposite, and confusing the two would be a serious error:

| | `docs/inflight/` | this catalogue |
|---|---|---|
| Tracks | work that is currently open | what the product does |
| A file's life | ends when the work closes; `git rm` it | lasts as long as the feature does |
| Reader | whoever picks up the work next | anyone using the product, and the site that imports it |

An entry here is retired when the capability is removed, not when someone finishes writing about it.
**The catalogue lives as long as the product does.**

## The three properties that make it worth building

**Backfilled.** The existing feature surface has no entries, and a catalogue covering only what lands next
is a directory of recent news rather than a catalogue. Backfilling is the work, not a follow-up. The
cheapest honest instrument is a committed baseline of capabilities that have no entry yet, gated so it
may shrink but never grow: a new capability cannot be added to a frozen list, so it arrives documented,
while the existing debt stays visible and monotonically decreasing. Delete the baseline when it
empties. Same shape as the quarantine registry, which is a known-bad set that cannot silently grow.

**Machine readable.** Front matter carries what a tool needs to group, order, cross-reference, and bind
an entry to the code it describes. Anything a tool would have to infer from prose is a field that
should have been declared. One rule governs every future field and it protects the whole design: **no
field's correct value may depend on another file's value.** Numeric ordering prefixes fail it - two
concurrent authors pick the same number and the collision returns in a new costume. Ordering comes from
content; grouping from the filename prefix.

**A seed for the documentation site.** The site (astubbs#208) is parked, and this is the corpus it
imports when it lands. Hence entries must stand alone as markdown read on GitHub today, and must not
depend on `README_TEMPLATE.adoc` embedding anything - the coupling being removed is exactly the one a
site cannot inherit.

## Settled: both corpora are maintained until the cutover

This does **not** replace the README's feature documentation yet. Until the site lands and the project
cuts over, a change to a documented capability updates both the README template and the feature entry.

Deliberate duplication with a known end date, and a known cost: two corpora that can disagree, with a
reader arriving at the wrong one from a search. The rule that keeps it survivable is that the feature
entry is authoritative on the capability and the README section stays a shorter treatment pointing at
it, so drift surfaces as the README being thin rather than as the two contradicting each other.
Extracting a README section and leaving a pointer is what happens **at** the cutover, not before.

## Open

- **The front matter schema**, field by field, and how a feature's identity is expressed so an entry can
  be bound to the code it describes. Binding both directions is what catches an entry that has rotted after
  a rename, rather than only catching a capability with no entry.
- **What counts as a feature**, precisely, since that is what any gate turns on. The most promising
  answer is derivable rather than judged: the public configuration and API surface moved.
  `changelog-ref-gate.js` chose that shape for its own scoping problem and wrote the reasoning into its
  source - a gate that fires on every PR gets N/A'd reflexively.
- **The entry shape.** Fixed headings that are the user's questions, checked for presence and content,
  which solves the empty-entry problem and the voice problem at once. Nobody can write a release note
  under "when to reach for it"; the question refuses the answer.
- **Whether a committed listing file is safe.** `docs/inflight/AGENTS.md` forbids one because every PR
  would edit it; `docs/TODO_INDEX.md` is committed, generated and CI-checked for drift, which is how it
  went from cautionary case to working pattern. Safe exactly when no human edits it and CI fails on
  drift; otherwise `ls` is the listing.
- **The gate, and how a contributor clears it.** The PR checklist already requires a reason for any
  N/A, so it is a real gate rather than a formality, and nothing added may become a way to turn a red
  gate green. The failure message is the only documentation most contributors will read about this
  convention, so it should name the command that scaffolds a correct entry.

## Delete when

This note goes when the catalogue exists, the backfill baseline is empty, the schema is settled and
checked, and the README duplication has ended at cutover. The catalogue itself is never deleted.
