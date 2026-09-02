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

## It is three checks, not one

The context is `codecov/project` for the total, and `codecov/project/<flag>` per flag -
`codecov/project/unit` and `codecov/project/integration` here. **They go red at different moments and
recover at different moments**, because each waits on its own suite's upload, so seeing one clear tells
you nothing about the others and the names are different enough to read as unrelated problems.

The flag-scoped pair is the more alarming to look at, because the total can be **up** while both flags
report a drop. Measured on astubbs#207 at one point: project **+0.59%** (hits +78, misses -14) with
`unit` at -4.14% and `integration` at -16.32%. Sixty-one added lines out of ~4,800 cannot move a flag
four points, so a flag delta that large next to a positive total is the baseline talking, not the diff.
All three were green once every upload had landed.

## What to do about it

Nothing, while a run is in flight - **wait for `checks_terminal` before reading `codecov/project` at
all**. `codecov/patch` is the one that answers "is the code this PR added covered", it is computed
from the patch rather than from totals, and it does not swing during a run.

Worth fixing properly if it ever costs someone real time: the honest fix is a codecov config that
waits for all expected flags before posting a project status (`after_n_builds`), not a threshold
widening, which would hide a real regression to silence a timing artefact.
<!-- post-merge: checked-end -->
