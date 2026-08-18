# Releasing, and generating the release notes

How a release is cut, and how `CHANGELOG.adoc`'s section for it is written. AGENTS.md carries the
one rule every agent needs - **a PR never adds a changelog entry** - because that one binds work
that has nothing else to do with releasing. Everything else is here.

## Cutting a release

**Tag-as-truth, dispatch-triggered.** `master` is **always** a `-SNAPSHOT`. A dispatch runs
`maven-release-plugin`'s `release:prepare` (which tags the release commit) and then deploys **that
exact tag** - nothing scans git history. An earlier history-scanning version re-released an ancient
upstream commit; see
[`docs/plans/2026-07-28-release-pipeline-hardening.md`](plans/2026-07-28-release-pipeline-hardening.md).

1. Run the **Release** workflow (Actions → *Release* → *Run workflow*) with the release version
   (e.g. `0.6.0.0`) and the next dev version (e.g. `0.6.0.1-SNAPSHOT`). Tick **Dry run** first to
   rehearse with no commits, tags or deploy.
2. It runs `release:prepare` (rewrites poms, makes the two release commits, tags `v<version>`,
   **pushes to `master`** via `RELEASE_PAT`), refuses if master's latest *CI* workflow run is not
   green, then checks out that tag, deploys it to Maven Central, and cuts a GitHub release.
   `master` ends on the next `-SNAPSHOT`.

Snapshots publish automatically on every push to `master` (`publish.yml`). Releases are blocked
while the quarantine lane is non-empty (`release.yml` guard; snapshots still publish) - see
[`docs/testing.md`](testing.md).

Workflows: `release.yml` (release), `publish.yml` (snapshot-only).

**Required GitHub repo secrets:**

- `RELEASE_PAT` - fine-grained PAT (repo **Contents: write**) owned by a repo admin, so
  `release:prepare` can push to `master`; the **"Repository admin" role must be in the master
  ruleset's bypass list**.
- `MAVEN_CENTRAL_USERNAME` - Sonatype Central Portal token username
- `MAVEN_CENTRAL_PASSWORD` - Sonatype Central Portal token password
- `MAVEN_GPG_PRIVATE_KEY` - armored GPG private key for signing artifacts
- `MAVEN_GPG_PASSPHRASE` - passphrase for the GPG key

## What state a changelog section is in

`CHANGELOG.adoc` holds the release notes. **Nothing about it is a per-PR chore.**
**Release-time generation is in effect now, and it covers `0.6.0.0` itself.** "Frozen" below is a
statement about *text already written in the file* - leave it alone - and never a claim that some
release's published notes are settled. What state a section is in follows from whether its release
has **shipped**:

| Section | State |
|---|---|
| `== 0.5.x` and below | Hand-written legacy from before the fork, and shipped. **Frozen.** |
| `== 0.6.0.0` - the release being cut | **Not shipped, so not settled.** Whatever sits under this heading now is working text. It will be **regenerated at release time from `git log <last-tag>..HEAD`**, replacing what is there, and frozen only once 0.6.0.0 ships. |
| Every release after it | Same treatment: generated when that release is cut, frozen once it ships. |

Two readings this rules out. **`0.6.0.0` is not on the hand-written side of the line** - generation
does not start at some later release. And **the current contents of `== 0.6.0.0` are not what v6
will publish** - do not cite them as the release notes, and do not treat the section as appendable
just because the release has not gone out. It is not yours to add to *or* to trust.

The policy removes the file that every PR used to touch - it appeared in 30 of the last 30 master
commits, dragging the generated `README.adoc` with it - and removes the ordering problem where an
entry had to cite a PR number that did not exist when the entry was written.

## What this asks of a commit

Nothing extra. The commit log is the raw material, so write it as you already should: a subject that
says what changed and, where it matters to a user, what it changed *for them*; the diagnosis, the
experiment and the rejected alternatives in the body. A good commit message is now doing double
duty, which is a reason to keep writing them properly rather than a new process.

## Label the issue with the release it ships in

**When work lands, put the release's version label on its issue** - `0.6.0.0` today. Nothing enforces
this, and the failure is silent: the work ships, the changelog entry gets written from the commit log,
and only the label is missing, so nothing looks wrong until someone asks the tracker what went into
the release. astubbs#209 shipped in 0.6.0.0 unlabelled and was caught by hand afterwards.

**The label is the exact version string, not a `v`-prefixed short form.** There is no `v6` label, so
`gh issue list --label v6` returns nothing - which reads as "no issues in this release" rather than as
a bad query. The right search is:

```bash
gh issue list -R astubbs/parallel-consumer --state all --label 0.6.0.0 --limit 200
gh pr list    -R astubbs/parallel-consumer --state all --label 0.6.0.0 --limit 200
```

**Two labelling schemes coexist, and they answer different questions.** The version label
(`0.6.0.0`) says *this shipped in that release* and is carried by both issues and PRs. The relative
labels (`next-feature-release`, `next-breaking-release`, `next-patch-release`) say *this is queued for
whichever release comes next* and are used on issues only. Queue with the relative label while the
work is pending; add the version label when it lands. At release time the version label is the one to
search - the relative ones move as releases cut, so they cannot tell you what a *past* release
contained.

This is a convenience, not the record. `CHANGELOG.adoc` and the commit log remain the source of
truth, and a missing label never makes a release wrong - it makes it harder to audit.

## At release time

An agent reads `git log <last-tag>..HEAD` - full messages, not just subjects - and drafts the
release section. The judgement it applies, and that a human should re-apply before freezing:

- **The entry test.** Can a *user or operator* observe this without reading our repo - API,
  behaviour, performance, logs and metrics, or the published artifact? If not, it gets no entry.
  Most CI, tooling, refactor and docs commits produce nothing, and that is correct: the changelog
  answers one question, "should I upgrade, and will anything change for me?"
- **One sentence, about 25 words, then the link.** Name what a reader would have *seen*, and who it
  hits when that is not everyone - not how the bug worked. An entry that runs to a paragraph is
  written for its author; an entry too short to tell you whether you are affected (`fix: Paused
  consumption across multiple consumers`) is no better.
- **Assemble as a set, not one commit at a time.** Merge related commits into a single entry, drop
  what turned out not to matter, and rewrite for someone who was not there. This is the part a
  per-PR entry could never do.
- **One `=== Build & CI` entry for the whole release** - a short bullet list of the big hitters
  (quarantine lane, chaos suite, mutation testing) that tells a reader how carefully the library is
  tested, with the detail left to the log.
- **Sections:** `=== Breaking`, `=== Improvements`, `=== Fixes`, `=== Dependencies`, `=== Examples`,
  `=== Build & CI`.
- **Reference convention inside `CHANGELOG.adoc` only:** a bare `#NN` is this fork, `upstream #NN`
  is confluentinc; make issue links explicit (`.../issues/NN[#NN]`), since GitHub numbers issues and
  PRs from one sequence. The file is in the issue-reference gate's `EXEMPT_PATHS` for this reason -
  everywhere else, [`docs/issue-references.md`](issue-references.md) applies.

## The `PR Checklist` changelog gate is a different, narrower check

`.github/scripts/changelog-ref-gate.js` fails a human PR that adds a `CHANGELOG.adoc` bullet under
`Breaking`, `Improvements`, `Fixes` or `Examples` without an explicit `/issues/NN` link. **Do not
read a green gate as compliance with the no-entries rule** - it is neither a subset nor a superset
of it, and it is not dormant:

- It **passes** entries the policy forbids. The gate only cares about the *citation*, so an added
  entry that links an issue sails through. astubbs#57's entries all cite issues.
- It **fires** on the one edit the policy allows. The gate cannot tell an edit from an addition -
  its own header explains that matching removed bullets against added ones was tried and abandoned
  as the subtlest code in the file - so a correction like astubbs#198's looks like a new entry. That
  is what `changelog-ref: N/A - <reason>` on its own line in the PR body is for; the workflow names
  this case explicitly.
- PRs **do** still touch the file. astubbs#51, astubbs#57, astubbs#105 and astubbs#106 were all open
  before this policy landed and all modify `CHANGELOG.adoc`; every PR predating the policy is in the
  same position.

So the gate enforces the *citation convention* on entries, and a human author and reviewer enforce
"no entries in a PR". Tightening it to reject *every* addition was considered and rejected: "adds an
entry" and "corrects an existing one" are both `+*` lines and are not mechanically distinguishable.
A blanket rule's only escape hatch would be a self-declared `changelog-ref: N/A - <reason>` in the
PR body, which legitimises a violating addition exactly as easily as a legitimate correction: no
enforcement the written rule does not already have, at the cost of an opt-out on every correction.
