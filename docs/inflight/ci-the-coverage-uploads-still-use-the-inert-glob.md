# Both coverage uploads still use the inert `**` glob, and it is what breaks the per-flag gates

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

`codecov/project/unit` and `codecov/project/integration` report large negative deltas on branches
that change no Java. They are required checks, so they read as "the branch under review dropped coverage".

**Cause, measured from the upload logs.** The uploader's CLI does not expand `**`, and nothing routes
`files:` through a shell, so the pattern arrives literally, matches nothing, and the CLI falls back to
a tree-wide search:

- On master's full build that fallback finds EVERY jacoco report - both halves, every module - so the
  `unit` and `integration` flags each receive the whole tree. The tell is that both flags report an
  identical figure, which can only happen if both hold the same data.
- On a pull request each suite job has produced only its own half, so the same fallback finds only
  that half.

A PR's `integration` flag is therefore compared against a master `integration` flag that silently
contains the unit half too. The delta measures that, not the branch. [`docs/ci.md`](../ci.md)'s
Codecov section carries the measured table.

**It is the same defect this repository already fixed once**, for the test-results upload: a `files:`
line that reads as configuration and does nothing, whose fallback produces a plausible answer instead
of an empty one - which is what makes it survive review.

## The fix, and why it is its own change

Expand both globs before handing them over and set `disable_search: true`, exactly as the
test-results upload now does, so the fallback cannot silently re-widen the set.

Not folded into the change that diagnosed it: this alters what a REQUIRED merge gate measures, and it
should be the only thing in its diff so the before/after comparison is the proof. Expect the first
clean per-flag comparison to be that proof.

## A correction worth keeping

The first diagnosis recorded here was wrong in the opposite direction - it claimed the PR lane
uploaded BOTH halves under one flag while master uploaded one file per flag. The upload logs say the
reverse. It was corrected before this note left the branch, and the wrong version is noted because
the evidence that settles it (`Found 13 coverage files` against a one-file glob, and master's two
flags reporting the same number) was available the whole time and was explained away as an unrelated
API quirk.
