# `codecov/project` is red for most of a run, and clears itself

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- post-merge: checked-begin - the measurements below name astubbs#207 as where they were taken,
     in the past tense, which stays true once that PR has landed -->

**A required-looking check is red for most of every CI run and turns green on its own.** Seen three
times in one afternoon on astubbs#207, each time going green once the run finished. Nobody has been
misled by it yet only because nobody looked while a run was in flight.

## The mechanism

Coverage is uploaded per suite, from several jobs: `.github/workflows/maven.yml` uploads with
`flags: ${{ matrix.suite }}` from the matrixed lane, and separately with `flags: default` from the
master-only full build. `codecov/project` compares TOTAL coverage against the base, so until the last
suite's upload lands it is comparing a partial head against a complete base.

The integration suite is the one that matters here, because it covers a lot that unit tests do not.
While it is still running, project coverage reads several points below base and the check is
**FAILURE**. When it finishes, the numbers move back and the check flips to **SUCCESS** with no
intervention.

Measured on astubbs#207: project reported 76.60% against a base of 80.09%, with hits DOWN by 120 on a
diff that added 63 lines and deleted no tests. That arithmetic is the tell - a PR that only adds lines
cannot subtract hits, so the difference is a missing upload rather than a coverage loss.

## Two things that look like causes and are not

- **The `default` flag showing `?`.** It does that on every PR by construction: the job carrying
  `flags: default` is the master-only full build, which is `SKIPPED` on a PR. It is not evidence of a
  failed upload.
- **A cancelled run.** A run cancelled by the next push never completes its integration upload, so
  `codecov/project` stays red at that head with nothing coming to fix it. That red is stale rather
  than transient, and it is cleared by the next head rather than by waiting.

## What to do about it

Nothing, while a run is in flight - **wait for `checks_terminal` before reading `codecov/project` at
all**. `codecov/patch` is the one that answers "is the code this PR added covered", it is computed
from the patch rather than from totals, and it does not swing during a run.

Worth fixing properly if it ever costs someone real time: the honest fix is a codecov config that
waits for all expected flags before posting a project status (`after_n_builds`), not a threshold
widening, which would hide a real regression to silence a timing artefact.
<!-- post-merge: checked-end -->
