# Release 0.6.0.0 - correctness of the artefacts we are about to publish

Scope: are the things 0.6.0.0 *publishes* (`CHANGELOG.adoc`, `README.adoc`) true on the day we cut it?
Release mechanics live in [`release-0.6.0.0.md`](release-0.6.0.0.md); the tracking issue is astubbs#197.

## Still open

- **The package rename `io.confluent.parallelconsumer.*` → `bz.stub.parallelconsumer.*` must land
  before 0.6.0.0 ships.** Decided: it goes ahead in v6. Nothing is published under the fork's groupId
  yet, so today it costs downstream users nothing; the moment v6 is on Central it costs every adopter
  a second migration, and there is no third moment. The evidence, the Apache 2.0 analysis and the
  task inventory are in
  [`docs/plans/2026-08-11-001-refactor-package-rename-plan.md`](../plans/2026-08-11-001-refactor-package-rename-plan.md);
  the cross-branch note is [`branch-package-rename.md`](branch-package-rename.md).
  **The README already describes the new namespace**, so it is now ahead of the code: if the rename
  slips out of v6 the `== Upgrading` section and the drop-in paragraph have to be reverted in the
  same breath, or we publish an artefact that documents imports nobody can use. Two passages in the
  `== 0.6.0.0` changelog section still assert the packages are unchanged - the opening "only required
  change is the Maven groupId" paragraph, and the `=== Breaking` bullet's "the library API is
  otherwise unchanged from upstream" - and must be corrected at release-note generation, which reads
  commits and will not notice a stale claim. And the real work is the copyright-provenance model, not
  the rename itself.
- **The rest of astubbs#197's triage list.** Four non-blocking defects were found while checking the
  two blocking ones; three are still open (the fourth, an `OffsetEncoding` magic-byte hazard, was
  fixed in astubbs#217): `PCModule` builds `DynamicLoadFactor(static, static)` when
  `messageBufferSize` is set, so "Max loading factor steps reached" WARNs on every control-loop pass
  for anyone following the README's own tuning advice (astubbs#155); MDC context is not captured at
  submit time, so a caller's `trace_id` is lost into the worker pool and the vert.x event loop;
  and `release.yml` publishes an empty GitHub Release body, so the
  curated changelog never reaches the release page.
- **After it ships:** ~11 mirrored issues describe 0.6.0.0 in the future tense and need the real
  coordinate; astubbs#186, astubbs#188 and astubbs#195 close with a pointer to the release.
- **Three `3.9.1` references - one was wrong, two are not.** The genuine defect was the CI
  description telling contributors PRs run "split suites on default Kafka 3.9.1" when CI's default
  is whatever `pom.xml` says - now `3.9.2`. **Fixed in astubbs#272**, which moved that text into
  `docs/ci.md` and dropped the version entirely, so it names no number that can go stale again.
  The other two are `bin/ci-build.sh 3.9.1` command examples - one in `AGENTS.md` under *How to
  Build*, and `src/docs/README_TEMPLATE.adoc:1133`, which does reach the published `README.adoc`. Being
  inside a published artefact does not make that one an error: it demonstrates that the script *takes*
  a version argument and asserts nothing about which version CI defaults to, so it stays correct
  whatever the pom says. Do not "fix" either of them.

## Context worth inheriting

- **`README.adoc` is generated - never hand-edit it.** Edit `src/docs/README_TEMPLATE.adoc` and
  regenerate with `./mvnw -N asciidoc-template:build` (or `mvn process-sources`). Every README change
  is therefore a two-file diff, and a PR that touches only the template has silently not changed the
  published README.
- **`CHANGELOG.adoc` is not a per-PR chore, and the `== 0.6.0.0` section is not final** (AGENTS.md →
  *Changelog*). It is regenerated from the commit log when the release is cut, so the text there now
  is working text - do not quote it as the release notes. A PR editing the file is either fixing a
  factual error in existing text or is wrong. When several agents work in parallel, exactly one may
  hold that file - it is the highest-collision file in the repo and conflicts in it are pure noise.
- **Anything asserting a dependency version in prose can drift silently.** The `3.9.1`/`3.9.2` mismatch
  arose because a Dependabot group bump moved `kafka.version` after the release note was written, and
  nothing cross-checks prose against `pom.xml`. Re-read the `=== Dependencies` section against the pom
  immediately before cutting, not weeks earlier.
