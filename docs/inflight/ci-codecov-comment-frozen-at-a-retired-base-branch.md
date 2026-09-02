# Codecov's PR comment is frozen, and nobody knows whether the integration still works

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

The `codecov-commenter` comment on astubbs/parallel-consumer#29 is from April 2026 and has not moved
since, on a PR that has been pushed to as recently as this week. It reports coverage on files that
left that PR months ago, cites pre-rename `io/confluent/parallelconsumer/...` paths, and asks for a
base report from `master-confluent`.

**`master-confluent` is not a typo and not garbage.** It is a real, ruleset-protected mirror branch
pinned at the pre-rebrand tip, and astubbs/parallel-consumer#29 was cut against it -
`docs/inflight/branch-stale-and-diagnostic.md` says do not delete it, and
`docs/inflight/pr-blockers-and-collisions.md` records why astubbs#29 and astubbs#31 had to be retargeted off it.
That retarget **is done**: the PR's base is `master` today, and
`docs/solutions/workflow-issues/keeping-both-sides-of-a-merge-conflict-resurrects-a-deleted-abstraction.md`
records it (anchor: `The retarget off`). So the comment is a snapshot from before the retarget, and
its complaint about a missing base report was true when it was written.

## What is established, and what is not

**The upload half works.** `codecov/codecov-action@v5` is still wired in
`.github/workflows/maven.yml` in three jobs, each passing `secrets.CODECOV_TOKEN`, and the most
recent successful master run's `Upload coverage to Codecov` step succeeded with Codecov's own
`Upload queued for processing complete` in the log, against a `master` commit. Nothing is broken on
this side of the wire.

**The comment half is unexplained, and that is the open question.** PR
astubbs/parallel-consumer#29's `test` job runs the upload on every push, yet the PR carries exactly
one Codecov comment, from April. Whether Codecov is failing to refresh because it is still anchored
to the retired base, because the fork's project settings did not survive the
confluentinc→astubbs migration, or because commenting is switched off server-side, **cannot be
determined from the Actions logs** - they only show the upload succeeding. It needs somebody with
access to app.codecov.io for this repository.

`codecov.yml` exists at the repo root and configures only project/patch status targets. It carries no
`comment:` block and names no branch, so nothing in the tree pins the base to `master-confluent`.

## The decision this needs, which is not a fix

Ask whether Codecov is wanted here at all. If it is, somebody has to look at the Codecov project
settings and confirm PR comments are enabled and the default branch is `master`. If it is not,
**removing the three action steps and `codecov.yml` is cleaner than nursing a service whose output
nobody has read in months** - and that is an operator call, not an agent one. Either way the stale
comment on astubbs/parallel-consumer#29 needs deleting by hand.

One loose end worth closing whichever way it goes: `7894373cc` added `CODECOV_TOKEN` **and**
documented it in `AGENTS.md`; that documentation did not survive the `AGENTS.md` restructuring and
was not relocated to `docs/ci.md`. Codecov is currently mentioned in no topic doc.

## Prior art searched, and what it returned

`docs/plans/`, `docs/solutions/`, `docs/inflight/` greps for `codecov` - **no note or write-up is
about Codecov at all**; every hit is the `master-confluent` branch-retarget bookkeeping cited above.
Issues `--state all` - **not one issue mentions Codecov or coverage tooling.** Merged PRs touching
`codecov|maven.yml` - several, none about the comment or the base branch.
`git log --all -S'codecov' -- .github/` returns the original setup commit, the token commit, and one
other; **no commit has ever addressed this comment.** Reading
astubbs/parallel-consumer#29's thread top to bottom, no human has ever mentioned it either.
