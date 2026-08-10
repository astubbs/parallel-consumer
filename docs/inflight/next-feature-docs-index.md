# A feature documentation index: one page per feature, machine readable

Renamed from `next-per-pr-docs-and-feature-index.md`. The old name led with the wrong noun and the
ideation inherited the error: this is **per feature, not per PR**. A PR is merely when a page gets
written or changed. The corpus is keyed by capability, and it has to cover the capabilities that
already exist, not only the ones that arrive next.

**Starting now.**

## What it is

Each user-visible feature gets its own markdown file in an index directory (`docs/features/` or
similar). Two features are two files, so two concurrent PRs cannot conflict - the same design, and the
same reason, as `docs/inflight/`, which became a directory because it was appearing in most master
commits and unrelated PRs kept colliding purely because their notes were adjacent.

**What these files are: user documentation for a capability.** What it does, when to reach for it, how
to configure it, what to watch out for. That is a different artefact from a changelog entry, and the
distinction governs how they are written. A changelog records that something changed, is read once
around a release, and is organised by version. Feature documentation explains how something works, is
read whenever someone needs the capability, and is organised by topic. A file that reads like a
release note has been written wrongly.

The conflict-free directory is a borrowed *mechanism*, not a borrowed purpose: towncrier and changesets
use one-file-per-change directories precisely because per-PR edits to a shared file conflict
constantly. Their file contents are changelog fragments, which is not what these are.

## The three things that make it worth doing

**Backfilled.** The existing feature surface has no pages, and an index that only covers what lands
next is a directory of recent news rather than documentation. Backfilling is the work, not a
follow-up. The cheapest honest instrument is a committed baseline of capabilities that have no page
yet, gated so it may shrink but never grow: new capabilities cannot be added to a frozen list, so they
arrive documented, while the existing debt is visible and monotonically decreasing. Delete the file
when it empties. This is the same shape as the quarantine registry, which is a known-bad set that
cannot silently grow.

**Machine readable.** Front matter carries what a site needs to group, order and cross-reference:
title, category, module, and the capability's identity. Anything a tool has to guess at from prose is
a field that should have been declared. One rule governs every future field, and it is the rule that
protects the whole design: **no field's correct value may depend on another file's value.** Numeric
ordering prefixes fail this - two concurrent PRs both pick the same number and the conflict returns in
a new costume. Ordering comes from content, grouping from the filename prefix.

**A seed for the documentation site.** The site (astubbs#208) is parked, and this is the corpus it will
import when it lands. That is the reason the pages must stand alone as markdown read on GitHub today
and must not depend on `README_TEMPLATE.adoc` embedding anything - the coupling being removed is
exactly the coupling a site cannot inherit.

## Settled: both corpora are maintained until the cutover

This does **not** replace the README's feature documentation yet. Until the docs site lands and the
project cuts over to it, a change to a documented capability updates both the README template and the
feature page.

That is deliberate duplication with a known end date, and it carries a known cost: two corpora that
disagree, with a reader landing on the wrong one from a search. Do not pretend otherwise. The rule
that keeps it survivable is that the feature page is authoritative on the capability and the README
section stays a shorter treatment that points at it, so drift shows up as the README being thin rather
than as the two contradicting each other. The extraction-and-replace approach - moving a README
section out and leaving a pointer behind - is what happens **at** the cutover, not before it.

## Open

- **The front matter schema**, field by field, and how a capability's identity is expressed so a page
  can be bound to the code it documents. Binding both directions is what catches a page that has
  rotted after a rename, as opposed to merely catching a capability with no page.
- **What "user-visible" means precisely.** The most promising answer is that it is derivable rather
  than judged: the public configuration and API surface moved. `changelog-ref-gate.js` already chose
  that shape for its own scoping problem, and wrote the reasoning into its source - a gate that fires
  on every PR gets N/A'd reflexively.
- **The page shape.** Fixed headings that are the user's questions, checked for presence and content,
  which solves the empty-file problem and the voice problem at once. Nobody can write a release note
  under "when to reach for it"; the question refuses the answer.
- **Whether a committed index is safe.** `docs/inflight/AGENTS.md` forbids one because every PR would
  edit it. `docs/TODO_INDEX.md` is committed, generated, and CI-checked for drift, which is how it
  went from cautionary case to working pattern. An index is safe exactly when no human edits it and CI
  fails on drift; otherwise `ls` is the index.
- **The gate, and how a contributor clears it.** The existing PR checklist already requires a reason
  for any N/A, so it is a real gate rather than a formality. Whatever is added must not become a way
  to turn a red gate green. The failure message is the only documentation most contributors will read
  about this convention, so it should name the command that scaffolds a correct page.

## Delete when

The index directory exists with the backfill baseline empty, the front matter schema is settled and
checked, and the project has cut over to the docs site so the README duplication has ended.
