# The coverage uploads no longer hand Codecov an inert `**` glob - the proof is still outstanding

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

`codecov/project/unit` and `codecov/project/integration` reported large negative deltas on branches
that changed no Java. They are required checks, so they read as "the branch under review dropped
coverage".

**Cause, measured from the upload logs.** The uploader's CLI does not expand `**`, and nothing routed
`files:` through a shell, so the pattern arrived literally, matched nothing, and the CLI fell back to
a tree-wide search:

- On master's full build that fallback found EVERY jacoco report - both halves, every module - so the
  `unit` and `integration` flags each received the whole tree. The tell is that both flags reported an
  identical figure, which can only happen if both hold the same data.
- On a pull request each suite job has produced only its own half, so the same fallback found only
  that half.

A PR's `integration` flag was therefore compared against a master `integration` flag that silently
contained the unit half too. The delta measured that, not the branch. [`docs/ci.md`](../ci.md)'s
Codecov section **owns the measured table**; what is here is the state of the repair.

It was the same defect this repository had already fixed once, for the test-results upload: a
`files:` line that reads as configuration and does nothing, whose fallback produces a plausible
answer instead of an empty one - which is what let it survive review.

## What changed

<!-- post-merge: checked-begin -->
Every `codecov/codecov-action` call in `.github/workflows/maven.yml` is now handed a real,
comma-joined file list built by a preceding `find` step and passed through `$GITHUB_OUTPUT`, with
`disable_search: true` so the fallback cannot silently re-widen the set, and the upload skipped
outright when the collector finds nothing rather than handing over an empty `files:`.

The master `build` job collects the two halves into two separate step outputs, because one half per
flag is exactly the split the per-flag gates compare. The PR `test` matrix and the currently disabled
`ak-experimental` job each collect both patterns, because a suite job produces only what it produces
- narrowing which half reaches which flag is the separate, still-open question recorded in
[`ci-codecov-flags-not-like-for-like.md`](ci-codecov-flags-not-like-for-like.md), and expanding a
pattern that matched nothing neither answers it nor depends on it.

The work landed on `fix/coverage-uploads-inert-glob`.
<!-- post-merge: checked-end -->

## What is still open, and it is the point of the whole exercise

**The repair is unverified, and it cannot be verified locally.** What was checked locally is that the
collector emits the paths it should: the shell was executed against a simulated multi-module tree
with both halves present, with only one half present, and with neither. That says the step produces
the right list. It says nothing about whether Codecov then measures like against like, because the
comparison only exists on the server and only after both sides have re-uploaded.

**The proof is the first per-flag comparison built from real file lists on both sides**: master's
`unit` and `integration` flags no longer reporting an identical figure, and a branch that changes no
Java no longer showing a delta.

Until master has run the `build` job once with these uploads, a pull request is still compared
against a base assembled the old way. **A red per-flag gate inside that window is the old defect
still being measured, not a new one** - the same shape of expected-red the sibling note above
records for the flag split, and for the same reason.

## A second, independent way the same gates go red - and the one-line tell that separates them

Seen from the other side while astubbs/parallel-consumer#207 was being compared against master, and
it is not the glob. Master's run at `beb01e1ce` was `completed/cancelled` - superseded by the next
push - so nothing uploaded for that commit at all and Codecov holds no report for it. Every PR
comparing against it falls back to older master data: that PR's diff block read base 83 files /
4448 lines against head 93 / 4894, on a change that adds three files, and `node bin/inflight.mjs
codecov` agreed that master was still recorded at 83 files. The same PR then saw both per-flag gates
green on one head and red on the next with no code change between them - the difference being
whether the base master commit's run had completed. So the gates are neither reliably self-clearing
nor permanently broken, and a red one says nothing about the branch until the files count has been
read.

The files count separates the two causes in one line:

| | the inert glob (this note) | a cancelled master run |
|---|---|---|
| Files, base vs head | **equal** - the flags hold unlike sets, not unlike files | **base is short** by more than the PR adds |
| Clears when | this lands and master re-uploads | the base moves to a master commit whose run completed |

This note does not fix the second cause and does not close on it. It is recorded here because the
time it cost was spent attributing its deltas to the first, and the tell above is what would have
separated them immediately.

## A correction worth keeping

The first diagnosis recorded here was wrong in the opposite direction - it claimed the PR lane
uploaded BOTH halves under one flag while master uploaded one file per flag. The upload logs say the
reverse. It was corrected before it left the branch, and the wrong version is noted because the
evidence that settles it (a `Found N coverage files` line far exceeding what a one-file glob could
match, and master's two flags reporting the same number) was available the whole time and was
explained away as an unrelated API quirk.

## Delete when

A pull request that changes no Java shows `codecov/project/unit` and `codecov/project/integration`
comparing cleanly, against a master base whose two flags report different figures.
