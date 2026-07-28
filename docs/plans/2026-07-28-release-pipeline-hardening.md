---
title: "Release pipeline hardening — tag-as-truth, no history scanning"
type: fix
status: accepted
date: 2026-07-28
---

# Release pipeline hardening

## Decision (accepted)

**Option 1 — tag-as-truth, dispatch-triggered `maven-release-plugin`**, implemented in
`.github/workflows/release.yml`. `release:prepare` tags the release commit and pushes it; the job then
checks out that **exact tag** and deploys it. **Nothing scans git history.**

**Ruleset finding:** `master`'s ruleset requires a PR (`pull_request` rule) with an **empty bypass list**,
so *nothing* — including Actions — can push to `master` directly. Option 1 therefore needs a **bypass**:
a fine-grained **`RELEASE_PAT`** (repo admin, Contents: write) + the **"Repository admin" role added to
the ruleset bypass list**. The PAT authenticates the `release:prepare` push, which then bypasses the PR
rule. (Rejected: release-please — a Google-only pattern in Maven-land, Conventional-Commits, drops
maven-release-plugin. Rejected: PR-based prepare — merge-strategy-fragile.)

`publish.yml` is reduced to **snapshot-only** (the history-scanning release logic is deleted). The
half-built `prepare-release.yml` is removed.

## The incident (what we're fixing)

The `publish.yml` shipped in #58 detects the release commit by **scanning `git log -30` for the
`[maven-release-plugin] prepare release <X>` commit subject**, then deploys that SHA. On its maiden run
it reached the **original upstream `io.confluent...:0.5.3.3` commit** (subject text matched; that commit
still lives in our history) and tried to **re-release it**. It only failed safely because we don't own
the `io.confluent` namespace, so Central rejected the deploy (403).

**Two compounding bugs:**
1. **Unbounded search** — `git log -30` reaches ancient history, not just this release.
2. **Unreliable guard** — the "already released?" check was `git rev-parse refs/tags/<X>`, but the old
   `0.5.3.x` tags were **never pushed to origin** (`git ls-remote --tags origin` is empty), so on the CI
   runner they "don't exist" → the commit looked unreleased.

**Current state:** the buggy `publish.yml` is **live on master**. It is *safe but broken* — every master
push now mis-detects `0.5.3.3` and fails at deploy (auth), so it fails loudly and publishes nothing bad,
but snapshots are also **not** publishing. No emergency, but it must be fixed before any real release.

## Prior art (why my approach was wrong)

Researched the canonical Maven/CI release patterns. The unanimous finding:

- **`release:prepare`** bumps poms → commits release version → **tags that exact commit** → commits
  next `-SNAPSHOT`. It also writes local `release.properties` recording `scm.tag`.
- **`release:perform`** reads `release.properties`, **checks out that tag**, and deploys it. **It never
  inspects `git log` or commit messages.** If `release.properties` is absent it errors and demands an
  explicit `-DconnectionUrl` — it never falls back to history-scanning.
- **The tag is the single source of truth**, everywhere, in every tool. Scanning `[maven-release-plugin]
  prepare release` *subjects* matches a naming convention, not content — exactly why forked/upstream
  history bit us. **No canonical setup scans history.**
- **Tag-triggered publish** (`on: push: tags: 'v*'` → checkout that ref → deploy) is the dominant CI
  pattern: "the trigger *is* the commit," so there is no find-the-commit step at all.
- Sources: Apache maven-release-plugin prepare/perform docs; Fränkel & sgitario GH-Actions-Maven guides;
  danielflower/multi-module-maven-release-plugin; googleapis/release-please; JReleaser 2025 guide;
  allegro/axion-release ("tag is truth").

**Takeaway: stop searching. Make the tag the trigger and the source of truth.**

## Options

### Option 1 — Tag-triggered, release performed in one CI job (no PR for the release commits) — RECOMMENDED for robustness
`workflow_dispatch` "Release" job runs `release:prepare` **and** `release:perform` in the *same* working
copy. Prepare tags `vX`; perform reads `release.properties`, checks out the tag, deploys it. Snapshots
publish separately on master push.
- ✅ Purest prior-art; **impossible** to release the wrong commit (perform only knows the tag it just made).
- ✅ Simplest — no cross-job/tag-timing/merge-strategy handling.
- ❌ The two version commits land on master **without a PR** (release automation commits directly; human
  gate is the dispatch button). Conflicts with "release via PR."

### Option 2 — PR-based prepare + tag-triggered perform (what we were building, done right)
Dispatch prepares on a branch → `release: vX` PR → merge → tag pushed → `on: push: tags` deploys the tag.
- ✅ Release goes through a PR (review).
- ❌ Most moving parts / largest failure surface: the tag must end up on the *post-merge* release commit
  and trigger at the right time. Merge-commit preserves the SHA (tag valid); **rebase rewrites SHAs**
  (tag orphaned → must re-tag); squash destroys it (guard). This merge-strategy sensitivity is the
  fragility that caused the incident's cousin. Doable, but the most to get wrong.

### Option 3 — release-please (PR-native, replaces maven-release-plugin)
Standing "Release PR" bumps poms + changelog; on merge it tags + fires a release event; publish triggers
off that event. No history scanning by design; the merge commit is unambiguous.
- ✅ Cleanest PR-native release; robust by construction.
- ❌ Uses Conventional Commits; **drops maven-release-plugin** (new tooling + mental model). Bigger pivot.

## Recommendation

**Option 1.** Now that "never mis-release again" is the priority, the simplest mechanism with the
smallest failure surface wins, and it's the textbook prior-art pattern. The release commits skipping PR
review is a small price — they are machine-generated version bumps, and the dispatch button is the
deliberate human gate. If PR review of the release itself is a hard requirement, **Option 3
(release-please)** is the robust PR-native choice; **Option 2 is not recommended** — it reintroduces the
merge-strategy fragility we're trying to eliminate.

## Detailed design (Option 1)

**`.github/workflows/release.yml`** — `workflow_dispatch` (inputs: `releaseVersion`, `developmentVersion`):
1. `actions/checkout` `ref: master`, `fetch-depth: 0`, **`fetch-tags: true`** (tags are real).
2. Bot git identity; JDK 17; GPG + Central creds (as today).
3. Guard: refuse if `git rev-parse refs/tags/v${releaseVersion}` already exists (belt-and-suspenders;
   `release:prepare` also fails on an existing tag).
4. `./mvnw -B release:prepare release:perform` in one invocation:
   - `preparationGoals=validate` (poms only, no build in prepare — the real build/deploy is `perform`).
   - `perform` reads `release.properties`, checks out **tag `v${releaseVersion}`**, forks Maven, runs the
     `maven-central` deploy of exactly that tag. **No SHA discovery anywhere.**
5. `gh release create v${releaseVersion} --generate-notes` (or JReleaser later).

**`.github/workflows/publish.yml`** → snapshots only: `on: push: [master]` (or keep the `workflow_run`
gate) → deploy a snapshot from the tip. **Delete the release/detect/tag logic entirely** — that whole
mechanism moves into `release.yml` and is tag-driven.

**`pom.xml`** — keep the `maven-release-plugin` config; ensure `scm` is correct (it is) and
`tagNameFormat=v@{project.version}`. Drop `pushChanges=false` (Option 1 wants prepare to push).

**Why it cannot mis-release:** `perform` deploys only the tag `prepare` just created in this same run.
There is no `git log`, no subject matching, no cross-run state. An old upstream commit can never be
selected because nothing selects commits at all — the tag is created and consumed atomically.

## Verification (before trusting it)

1. **Dry run**: dispatch with a throwaway version (e.g. `0.5.99.0` / `0.5.99.1-SNAPSHOT`) and
   `--dry-run`/`-DdryRun=true` on `release:prepare` first — prepare in dry-run makes no commits/tags, so
   we can confirm the workflow wiring without side effects.
2. **Assert snapshot path**: confirm a normal master push publishes a *snapshot* and never a release.
3. Only then do the real `0.6.0.0`.

## Rollout

1. Build Option 1 on `ci/tag-triggered-release-fix` (this worktree), **ask before opening the PR**.
2. Land it on master to replace the broken `publish.yml` (removes the history-scan entirely).
3. Retire the half-built `prepare-release.yml` if superseded.
4. Then proceed to `0.6.0.0` via the verified flow.

## Open questions for the user
- **Option 1 (no-PR release, simplest/robust) vs Option 3 (release-please, PR-native) vs Option 2
  (PR-based, fragile — not recommended)?**
- If Option 1: OK that the two version commits push directly to master (dispatch is the gate)?
