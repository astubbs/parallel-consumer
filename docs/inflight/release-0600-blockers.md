# Release 0.6.0.0 - correctness of the artefacts we are about to publish

Scope: are the things 0.6.0.0 *publishes* (`CHANGELOG.adoc`, `README.adoc`) true on the day we cut it?
Release mechanics live in [`release-0.6.0.0.md`](release-0.6.0.0.md); the tracking issue is astubbs#197.

## Still open

- **Go/no-go on the package rename `io.confluent.parallelconsumer.*` → `bz.stub.parallelconsumer.*`,
  and the decision expires when 0.6.0.0 ships.** Nothing is published under the fork's groupId yet,
  so today the rename costs downstream users nothing; the moment v6 is on Central it costs every
  adopter a second migration, and there is no third moment. The evidence, the Apache 2.0 analysis and
  the task inventory are in
  [`docs/plans/2026-08-11-001-refactor-package-rename-plan.md`](../plans/2026-08-11-001-refactor-package-rename-plan.md);
  the cross-branch note is [`branch-package-rename.md`](branch-package-rename.md). Two things a
  release-time reader needs: the README's drop-in claim and the `=== Improvements` changelog entry
  both assert the packages are unchanged, so a "go" makes them false in published artefacts and they
  must be corrected in the same pass - and the real work is the copyright-provenance model, not the
  rename itself.
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
- **Three `3.9.1` references survive this PR - one is wrong, two are not.** The genuine defect is
  `AGENTS.md:183`, which tells contributors PRs run "split suites on default Kafka 3.9.1" when CI's
  default is whatever `pom.xml` says - now `3.9.2`. It is a one-word fix, left alone only to keep this
  PR scoped to what 0.6.0.0 publishes: `AGENTS.md` is contributor documentation, not a release
  artefact. The other two are `bin/ci-build.sh 3.9.1` command examples - `AGENTS.md:58`, and
  `src/docs/README_TEMPLATE.adoc:1133`, which does reach the published `README.adoc:1382`. Being
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
