# Every PR carries its own documentation, via a conflict-free feature index

**Status:** the catalogue exists as `docs/features/`, one record per capability, backfilled over the existing surface and structurally validated on every pull request. There is no gate requiring a record, and the docs site that consumes them is parked.
**Migrated:** 2026-08-10, from `docs/inflight/`. The reasoning below is why the artefact has the shape it does.

Two linked problems. Features land without user-facing documentation, so the docs drift behind the
code. And the obvious fix - "update the shared docs file in your PR" - makes every concurrent PR
conflict on the same file, which is why that rule is always quietly abandoned.

## The mechanism: one file per feature, not edits to a shared file

Each PR that adds or changes a user-visible feature drops **a new file in an index directory**
(`docs/features/` or similar) rather than editing a shared page. Two PRs adding two features add two
files and cannot conflict. The docs site then imports the directory and renders each file as a page.

**What these files are: user documentation for a feature.** Each one explains a capability at the
level someone needs in order to *use* it - what it does, when to reach for it, how to configure it,
what to watch out for. That is a different artefact from a changelog entry, and the distinction
matters for how they are written. A changelog records that something changed, is read once around a
release, and is organised by version. Feature documentation explains how something works, is read
whenever someone needs the capability, and is organised by topic. A file that reads like a release
note has been written wrongly.

The conflict-free directory is a borrowed *mechanism*, not a borrowed purpose: Python's **towncrier**
and JavaScript's **changesets** use one-file-per-change directories precisely because per-PR edits to
a shared file conflict constantly. Their file *contents* are changelog fragments, which is not what
these are - take the collision-avoidance design and leave the rest.

Fits the parked docs site (astubbs#208, MkDocs + Material, versioned with `mike`) - a directory of
markdown files is exactly what it wants as input. Note the standing constraint in
`next-docs-site.md`: do not build anything new that depends on `README_TEMPLATE.adoc` embedding
other documents, because that coupling is being removed.

Design questions to settle when this is picked up:

- Front matter for title, category, since-version, and module, so the site can group and order pages.
- What "user-visible" means precisely, since that is what the gate below turns on.

## A useful side effect at release time

Release notes are already generated when a release is cut, from `git log <last-tag>..HEAD`, and a PR
never adds a changelog entry (AGENTS.md → Changelog). Nothing here changes that, and the feature
index is not a changelog input in the sense towncrier means.

It is, however, useful material for whoever writes the release notes. The commit log establishes
*what* changed; it cannot explain why a change mattered or how a fix was shown to hold. Several
existing bodies of work can be drawn on at that point:

| Source | What it offers the notes |
|---|---|
| `docs/features/` | Capabilities described in user-facing language, already written |
| `docs/solutions/` | How a defect was diagnosed and proven fixed, rather than merely that it was |
| The roadmap | Which themes this release advanced (`docs/plans/2026-08-10-003-docs-roadmap-plan.md`) |
| Maturity table | Any module whose status changed (`docs/plans/2026-08-10-002-docs-module-maturity-plan.md`) |

Drawn on, not concatenated - a feature page and a release note serve different readers, and pasting
one into the other produces a poor version of both.

## Documentation includes the agent-facing manual

"Docs" here means any of three audiences, and a PR should satisfy whichever apply:

- **End users** - the feature page in the index above.
- **Agents and contributors** - AGENTS.md and the docs it points to. This is a real audience now, not
  a metaphor: agents read those files as the operating manual, and a feature they cannot discover
  effectively does not exist.
- **Promotional material** - where a change is worth telling people about rather than merely
  recording. Feeds the release material.

## Enforcement

The existing PR checklist already has a "Docs updated - or N/A" box, and the "PR Checklist" check
fails a PR that leaves a box unresolved. That is the hook. Worth considering whether a
user-visible-feature PR should be required to add a `docs/features/` file specifically, rather than
being able to satisfy the box with any documentation edit.

Keep the escape hatch honest: N/A must carry a reason, and is never a way to make a red gate go
green. A refactor genuinely needs no feature page; a new option does.

## Remaining

The gate, and whatever scaffolds a correct record for a contributor - the failure message is the only documentation most will read about this convention. Note this is per feature, not per PR: a PR is merely when a record gets written.
