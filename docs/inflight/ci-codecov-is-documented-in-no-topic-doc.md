# Codecov is documented in no topic doc, and now there is much more of it to document

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

`grep -ci codecov docs/ci.md` returns zero. `7894373cc` added `CODECOV_TOKEN` **and** documented it
in `AGENTS.md`; that documentation did not survive the `AGENTS.md` restructuring and was never
relocated. The doc that owns CI has never mentioned the service.

That was a small gap when Codecov was one upload step nobody read. It is a larger one now, because
several things were wired on top of it and none is written down where an operator would look:

- coverage uploads, per suite, carrying `flags` - and the fact that **every flag except `default`
  reads 0% on the default branch, correctly**, because the per-suite `test:` job is
  `pull_request`-only while `build:` (which carries `default`) is `push`-only. A reader who does not
  know that files a bug against the uploader.
- a separate **test-results** upload (`codecov-action@v5`, `report_type: test_results`), which is a
  different Codecov product from coverage and feeds Test Analytics.
- the `codecov/*` statuses that now appear on every PR, and `codecov.yml` at the repo root, which
  configures only `coverage.status` and carries no `comment:` block.
- `bin/inflight.mjs codecov`, which reads Codecov's API **unauthenticated** because this repo is
  public - see [`docs/inflight-tool.md`](../inflight-tool.md), which owns those commands.

## The question this note used to ask, and how it was settled

This note previously asked why `codecov-commenter` had posted nothing since April, and recorded that
it could not be answered from Actions logs because those only ever show the upload succeeding.

**It was the GitHub App's repository access.** Codecov's own dashboard showed commit messages
unavailable and patch coverage as `-` while the uploads were landing - the symptom was visible only
there, never in CI. Granting the App access to the repository was followed immediately by Codecov
posting on a live PR and by its statuses appearing on PRs again.

**Stated as supported rather than proven, deliberately.** The confirming experiment - toggle access
off, observe commenting stop, toggle on, observe it resume - is not one anybody is going to run on a
live repository, and granting access destroyed the evidence that would have made the diagnosis
airtight. It is the best-supported explanation and the observation is consistent with it; it is not a
controlled result, and this note says so rather than rounding it up.

The old note also asked whether Codecov was wanted here at all. That is answered by what has since
been built on it, and the answer is yes.

## Not worth doing

The April `codecov-commenter` comment on astubbs/parallel-consumer#29 was listed here as needing
manual deletion. That PR has since merged, so the stale comment is now a historical artefact on a
closed thread rather than misinformation on live work. Delete it if it bothers you; nothing depends
on it.
