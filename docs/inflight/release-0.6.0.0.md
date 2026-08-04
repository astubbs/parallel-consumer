# Release 0.6.0.0

Not yet released: the pom is `0.6.0.0-SNAPSHOT`, there is no `v0.6.0.0` tag, and the changelog section
is written. Release = strip `-SNAPSHOT` and merge to `master`; `publish.yml` runs after CI succeeds,
deploys via the `maven-central` profile, tags `v<version>` and cuts a GitHub release (AGENTS.md →
*Releasing*).

**No longer blocked by the quarantine guard** - #80 emptied the registry when it merged, so
`release.yml`'s "no release while tests are quarantined" gate now passes.
