# Release 0.6.0.0 - correctness of the artefacts we are about to publish

Scope: are the things 0.6.0.0 *publishes* (`CHANGELOG.adoc`, `README.adoc`) true on the day we cut it?
Release mechanics live in [`release-0.6.0.0.md`](release-0.6.0.0.md); the tracking issue is #197.

## Still open

- **The rest of #197's triage list.** Four non-blocking defects were found while checking the two
  blocking ones and none are fixed: `PCModule` builds `DynamicLoadFactor(static, static)` when
  `messageBufferSize` is set, so "Max loading factor steps reached" WARNs on every control-loop pass
  for anyone following the README's own tuning advice (#155); MDC context is not captured at submit
  time, so a caller's `trace_id` is lost into the worker pool and the vert.x event loop;
  `OffsetEncoding` throws on an unknown magic byte *before* `invalidOffsetMetadataPolicy` is consulted,
  so a future encoding kills an older reader regardless of policy - a forward-compatibility hazard in a
  wire format this fork now owns; and `release.yml` publishes an empty GitHub Release body, so the
  curated changelog never reaches the release page.
- **After it ships:** ~11 mirrored issues describe 0.6.0.0 in the future tense and need the real
  coordinate; #186, #188 and #195 close with a pointer to the release.
- **Two stale `3.9.1` references survive outside the published artefacts**, left alone here to keep
  this PR scoped to what 0.6.0.0 publishes. `AGENTS.md:183` says PRs run "split suites on default
  Kafka 3.9.1" - genuinely wrong, CI's default comes from `pom.xml`, which is `3.9.2`; it is a
  one-word fix in a file this PR does not otherwise touch. `AGENTS.md:58` and the
  `bin/ci-build.sh 3.9.1` line in `src/docs/README_TEMPLATE.adoc` are illustrative command examples
  where the argument is the point, not the version, so they are arguably fine either way.

## Context worth inheriting

- **`README.adoc` is generated - never hand-edit it.** Edit `src/docs/README_TEMPLATE.adoc` and
  regenerate with `./mvnw -N asciidoc-template:build` (or `mvn process-sources`). Every README change
  is therefore a two-file diff, and a PR that touches only the template has silently not changed the
  published README.
- **`CHANGELOG.adoc` is frozen up to `== 0.6.0.0`.** It is not a per-PR chore (AGENTS.md → *Changelog*),
  so a PR editing it is either fixing a factual error in a frozen section or is wrong. When several
  agents work in parallel, exactly one may hold that file - it is the highest-collision file in the
  repo and conflicts in it are pure noise.
- **Anything asserting a dependency version in prose can drift silently.** The `3.9.1`/`3.9.2` mismatch
  arose because a Dependabot group bump moved `kafka.version` after the release note was written, and
  nothing cross-checks prose against `pom.xml`. Re-read the `=== Dependencies` section against the pom
  immediately before cutting, not weeks earlier.
