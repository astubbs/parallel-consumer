# Parked: documentation site

The docs are heading for a proper generated site rather than one enormous `README.adoc`.

**Consequence to know about now:** `README_TEMPLATE.adoc` used to `include::` the whole of
`CHANGELOG.adoc`, publishing every release note as a README chapter and making each changelog edit a
two-file change. That embed is being removed (#113); the site work removes the rest of the coupling.
Don't build anything new that depends on the README embedding other documents.
