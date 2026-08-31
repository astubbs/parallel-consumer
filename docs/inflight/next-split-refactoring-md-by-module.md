# Split `docs/refactoring.md` into module-scoped lists, keeping a cross-cutting one

Raised 2026-08-17 after a proxy-specific list was filed into the global backlog and had to be moved
out again the same hour. Not started.

## The problem

`docs/refactoring.md` is **711 lines across 9 sections**, and it mixes two different scopes:

- **Genuinely global**: breaking changes queued for the next major (release-gated), the
  `TODO`/`FIXME`/`XXX` marker triage, cross-cutting and architectural items, the idea banks.
- **Module-scoped**: `By file / module (parallel-consumer-core)` is the largest section in the file
  and is about one module, read by people working in that module, and of no use to anyone else.

Two costs, and the second is the one that bites:

- **A reader has to skim the whole file to find their module's items**, so they do not, so the items
  are not read.
- **Every PR that adds an item touches the same file.** That is the exact problem that turned
  `docs/inflight/` from one file into a directory in 2026-08 - it appeared in 26 of 30 consecutive
  master commits and unrelated PRs conflicted purely because their notes were adjacent.

The trigger: a list of *declined* simplifications was added here and then moved to
`parallel-consumer-proxy/docs/simplifications-declined.md`, because it was both the wrong kind
(decisions not to act, where this file is deferred work) and the wrong scope. The scope half of that
mistake is structural and will keep happening.

## The shape to aim for

- **Module-scoped items move next to their module**, following the precedent already set by
  `parallel-consumer-proxy/docs/`, which holds the client authoring guide, the protocol specification
  and now the declined-simplifications list.
- **`docs/refactoring.md` keeps only what is genuinely cross-cutting**: the release-gated breaking
  changes, the marker triage, architectural items spanning modules, and the idea banks.
- It keeps its role as the entry point, listing where each module's list lives - the same
  router-plus-owners shape `AGENTS.md` uses.

## Before doing it

- **`AGENTS.md` names this file and its contents**, as does `docs/inflight/AGENTS.md` and probably
  more. `grep -rn 'refactoring.md' . --exclude-dir=.git --exclude-dir=target` before moving anything -
  the repo's own rule is that bold labels and headings get cited from poms, scripts and workflow YAML
  as often as from markdown, and nothing checks those citations.
- **Do it on a branch that owns little else.** This file is high-conflict by nature, which is the
  argument for splitting it and also the reason a split collides with everything in flight.
- Decide whether a module with no items yet gets an empty list or nothing at all. Nothing at all is
  probably right - an empty file is a thing to maintain.

## Not urgent

Nothing is broken. The items in it are still findable, and the marker triage still works. This is
worth doing when the branch traffic is low, not in the middle of a large feature - and the module
precedent is already set, so a partial split (core first, since it is the largest section) is a legal
and useful stopping point.
