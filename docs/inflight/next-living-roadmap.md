# A living roadmap, in the docs, doing four jobs at once

Wanted: an actual roadmap tracked as documentation - used for feature planning and implementation,
for prioritisation and scheduling, and imported directly into the docs site as promotional material
explaining what is planned and why, ahead of deciding on 1.0.

## Altitude: themes, not work items

The roadmap tracks features at a deliberately high level - entries of the order of *refactor the main
codebase*, *Streams proof of concept*, *Connect proof of concept*, *known bugs fixed*, *micro
batching*, *dead letter queue*.

That altitude is what keeps it maintainable. A roadmap pitched at the level of individual work items
would duplicate the issue tracker and drift from it within weeks; one pitched at themes does not,
because a theme changes state a handful of times in its life rather than continuously. Several
issues sit beneath a single entry, and their comings and goings do not disturb it.

It also keeps the division of labour clean:

| | Owns |
|---|---|
| **Issue tracker** | The unit of work and its live status |
| **Roadmap** | The themes, their order, and the reasoning behind it |

The README currently directs readers to the issue tracker as the place to find what is planned. Once
the roadmap exists, that wording should name both, with the roadmap answering *where this is going*
and the tracker answering *what is happening right now*.

## Publish rationale and order, not dates

The dual audience is the second trap. The same document is being asked to serve internal scheduling
and external promotion, and those want different things - internally a date is a plan, externally a
date is a promise, and readers remember the promise long after the plan changed.

Publish **order, rationale, and what unblocks what**. That is genuinely useful to a reader deciding
whether to adopt, and it is the part that does not go stale the moment something slips. Keep any
internal scheduling detail out of the imported view, or accept that it is a commitment.

The rationale is the valuable half anyway: *why* something is next says more about the project's
judgement than a list of features does.

## Anchor it on a stated definition of 1.0

The roadmap's most useful single section is **what 1.0 means** - the exit criteria, not a date.
This connects to `next-module-maturity-table.md`: pre-1.0 reserves the **API surface**, not
reliability. So the 1.0 criteria are largely about settling that surface plus the remaining
refactoring, and saying so converts a vague "not 1.0 yet" into something a reader can check progress
against.

It also lets the roadmap answer the adoption question directly: *what would change for me at 1.0?*
If the honest answer is "the API stops moving, reliability is already there", that is a far stronger
statement than a version number.

## Living, which means it needs a trigger

"Living document" fails by default - it is written once and rots, and a stale roadmap is worse than
none because it is confidently wrong in public. Decide what forces an update: release cuts are the
obvious trigger, and the per-PR feature index (`next-per-pr-docs-and-feature-index.md`) is the
natural feed for "this shipped, move it".

Markdown in the repo, so the docs site (astubbs#208, MkDocs + Material, versioned with `mike`) can
import it and PRs can amend it under review like anything else. Respect the constraint in
`parked-docs-site.md`: do not build anything that depends on `README_TEMPLATE.adoc` embedding other
documents.

## Delete when

The roadmap exists, the docs site imports it, the README's "single place to look" wording is
reconciled, and 1.0 has stated exit criteria.
