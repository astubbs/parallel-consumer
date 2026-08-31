# A documentation site - planning draft, direction not chosen

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - after v6, planning draft only -->


The docs are heading for a proper generated site rather than one enormous `README.adoc`.

**Priority rationale raised 2026-08-29/30** (owner, during the strategy follow-up): as the
product surface grows - engine, controller, facades, adapters, GUI, polyglot - the structured
manual is what keeps the project explainable *without the owner in the critical path*. The README
conversion stops being cosmetic the moment attention arrives.

**Tracked in [#208](https://github.com/astubbs/parallel-consumer/issues/208)** - options, the
recommendation (MkDocs + Material, versioned with `mike`), the KIP-932 share-groups chapter, and the
domain question all live there. This file stays only for the constraint below, which binds work
happening *now*; everything else about the site belongs in the issue.

**Consequence to know about now:** `README_TEMPLATE.adoc` used to `include::` the whole of
`CHANGELOG.adoc`, publishing every release note as a README chapter and making each changelog edit a
two-file change. That embed is being removed (astubbs#113); the site work removes the rest of the coupling.
Don't build anything new that depends on the README embedding other documents.
