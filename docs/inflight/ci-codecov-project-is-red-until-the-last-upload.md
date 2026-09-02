# A codecov project check going red usually is not your PR - two causes, and only one clears itself

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- post-merge: checked-begin - the measurements below name astubbs#207 as where they were taken,
     in the past tense, which stays true once that PR has landed -->

**A required-looking check goes red on a PR that did nothing wrong.** Four times in one afternoon on
astubbs#207. **Do not read that as "it always clears itself"** - this note said exactly that until the
fourth one survived to a terminal run, and a heading promising self-recovery is how a reader dismisses
the one occurrence that is telling them something.

There are **two causes with the same symptom**, and they need different responses:

| | mid-run | at a terminal run |
|---|---|---|
| What is partial | the **head** - a suite has not uploaded yet | the **base** - master's own report is incomplete |
| Clears itself? | **yes**, when the last upload lands | **no**, it stays red |
| What to do | wait for `checks_terminal` | run the files check below |

The `Files` count separates them in one line, so start there rather than deciding which case you are
in from the percentages.

## Cause 1, mid-run: the head is partial

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

## Cause 2, at a terminal run: the BASE is partial - and the one-line tell

Every other symptom here needs an argument. This one does not. The report's diff block prints
`Files` for base and head, and **the difference must equal the number of files the PR adds**. When it
does not, the base report is incomplete and every percentage in the comment is comparing against a
partial measurement.

Measured on astubbs#207 at a terminal run: base **83** files, head **93**, on a PR that adds **3**.
The other seven are master's own files missing from the base report - which also accounts for
`Lines +446` on a diff of about sixty. Project read -0.11%, `integration` -16.99% and `unit` -5.43%
off that base; none of it was this PR.

Check that before reading any percentage, including at a terminal run - it is faster than reasoning
about hits and misses, and unlike them it cannot be argued with.

## What to do about it

**Mid-run: nothing.** Wait for `checks_terminal` before reading a project check at all.

**Still red at a terminal run: run the files check.** If the base is short, the percentages are
measuring master's missing report and there is nothing here to fix on the PR - say so and move on. If
the files line up, the number is real and worth reading.

**Either way, `codecov/patch` is the check that answers "is the code this PR added covered".** It is
computed from the patch rather than from totals, so it does not swing during a run and it does not
inherit a partial base. On astubbs#207 it stayed green through all four reds.

Worth fixing properly if it ever costs someone real time: the honest fix is a codecov config that
waits for all expected flags before posting a project status (`after_n_builds`), not a threshold
widening, which would hide a real regression to silence a timing artefact.
<!-- post-merge: checked-end -->
