# The release-documentation records are flat files, and the per-module fragment mechanism is not here

<!-- inflight-type: decision -->
<!-- inflight-impact: process -->
<!-- inflight-state: open -->

`docs/data/module-maturity.yaml` and `docs/data/testing-evidence.yaml` are single hand-maintained
files with one entry per module. Every module in the reactor now has a row in both, so nothing is
missing today - what is missing is anything that would **fail** if a future module arrived without
one.

This note is what is left of
`docs-proxy-client-modules-carry-no-maturity-or-evidence-rows.md`, whose subject - eleven client
modules with no row - was resolved by the rung that gave those modules real content and rows in the
same change. What did not resolve with it is the mechanism question below, so it is split out rather
than deleted with the rest.

## The mechanism that would make a missing row loud, and why it is not here

astubbs/parallel-consumer#293 carries a per-module fragment scheme: `docs/data/module-maturity.d/<artifact>.yaml`
merged at check time, with a `deferred: {reason, lifted_by}` block, a cross-check that every module
in any aggregator's `<modules>` has either a row or a recorded deferral, and a companion gate
(`bin/check-deferred-modules.sh`) that fails a module with real source still marked deferred. Read
them with `git show origin/feats/proxy-requirements:bin/check-deferred-modules.sh` and
`git show origin/feats/proxy-requirements:docs/data/module-maturity.d/`.
<!-- file-refs: N/A - the fragment directory and its gate ship on feats/proxy-requirements; the paths above are deliberately not paths in this tree -->

**The hygiene sweep (astubbs#378) recommended the polyglot scaffolding rung carry it, and that rung
declined for reasons that were correct then and are now spent.** Its argument was that the modules
had nothing to put in a row, so the only fragment they could carry was a deferral - and the gate
that makes a deferral honest fails a module with source beyond a skeleton allowlist, which every one
of those modules already had. Both halves of that are gone: the modules have content and they have
rows.

**What is left is the merge path itself**, and it is a real trade rather than an oversight:

- **For**: one file per module means one PR per module, so two module waves never conflict on the
  same file - which is the same reasoning that turned `docs/inflight.md` into a directory. It also
  makes a module with no row a build failure rather than a thing nobody notices.
- **Against**: it is a schema extension, a merge step in `bin/check-docs-data.sh`, and a second gate,
  for a repository whose module count changes a few times a year. The flat files are readable in one
  scroll, which is what a release reader actually does with them.

Nobody has to decide this now. It becomes worth deciding when two module waves collide on
`module-maturity.yaml`, or when a module reaches `master` with no row and nothing says so.
