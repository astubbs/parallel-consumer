# Parked: documentation site

<!-- inflight-type: feature -->


The docs are heading for a proper generated site rather than one enormous `README.adoc`.

**Tracked in [#208](https://github.com/astubbs/parallel-consumer/issues/208)** - options, the
recommendation (MkDocs + Material, versioned with `mike`), the KIP-932 share-groups chapter, and the
domain question all live there. This file stays only for the constraint below, which binds work
happening *now*; everything else about the site belongs in the issue.

**Consequence to know about now:** `README_TEMPLATE.adoc` used to `include::` the whole of
`CHANGELOG.adoc`, publishing every release note as a README chapter and making each changelog edit a
two-file change. That embed is being removed (astubbs#113); the site work removes the rest of the coupling.
Don't build anything new that depends on the README embedding other documents.
