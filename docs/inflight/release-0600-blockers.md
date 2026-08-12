# Release 0.6.0.0 - correctness of the artefacts we are about to publish

Scope: are the things 0.6.0.0 *publishes* (`CHANGELOG.adoc`, `README.adoc`) true on the day we cut it?
Release mechanics live in [`release-0.6.0.0.md`](release-0.6.0.0.md); the tracking issue is astubbs#197.

## Still open

- **The package rename landed, so what is left is keeping the release notes honest about it.**
  `io.confluent.parallelconsumer.*` → `bz.stub.parallelconsumer.*` went in with astubbs#294, and the
  README and the `== 0.6.0.0` changelog section now both describe a namespace that exists - the docs
  are no longer ahead of the code. The release-day risk is regeneration: that changelog section is
  rebuilt from the commit log when the tag is cut, and generation reads commits, so it will not notice
  that it has dropped a claim the current text makes. After regenerating, confirm the opening
  paragraph and the `=== Breaking` bullet still name **both** changes - the Maven `groupId` and the
  Java packages every import names - rather than reverting to "the only required change is the Maven
  groupId". Reasoning, Apache 2.0 analysis and task inventory:
  [`docs/plans/2026-08-11-001-refactor-package-rename-plan.md`](../plans/2026-08-11-001-refactor-package-rename-plan.md);
  the project entry is [`branch-package-rename.md`](branch-package-rename.md).
- **Recheck the documentation data before the tag, and again after the critical fixes land.** The
  published claim is now "every known **critical** defect resolved and evidenced", not "all known
  defects" - the earlier wording was a promise the project cannot keep. Nothing verifies that claim
  automatically; `bin/check-docs-data.sh` checks structure only, on purpose. So at release:
  - Confirm no known critical defect is open in scope. The `confluentinc#857` deadlock is the live
    one, and a module cannot honestly be described as fit for production use while a known defect can
    lock up a consumer. If it is still open, amend the claim rather than the standard.
  - Move the staged content up as its modules land: the Streams and Connect rows in
    `docs/data/staging/module-maturity-rows.yaml`, and the record in `docs/features/staging/`. Each
    move belongs to the PR that lands the thing, not to a later sweep - astubbs#271 for Streams,
    astubbs#269 for Connect, both open and neither touching the data yet.
  - **A third module has no row at all.** astubbs#268 adds `parallel-consumer-dashboard`, which is
    also the 1.0 gate the roadmap calls a running-instance view. It needs a staged row and a feature
    record before it lands, or it ships undocumented.
  - Re-read the maturity wording itself. `stable` was withdrawn because it was untrue; the
    replacement, `production-use`, is only as good as the critical-defect gate holding.
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
  Build*, and the `bin/ci-build.sh 3.9.1` line in `src/docs/README_TEMPLATE.adoc`, which does reach the published `README.adoc`. Being
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
