# The mirror fixes in flight, and whether they close their issues

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

Four of the mirrors swept in the 2026-08-20 triage already had a fix, so they were checked for a
different thing than the other ten: does the PR actually close the issue, and if it deliberately does
not, is that reasoning anywhere a reader will find it? Two need an edit. The companion note for the
ten without fixes is [`upstream-mirror-bodies-are-stale.md`](upstream-mirror-bodies-are-stale.md).

## astubbs#201 should close astubbs#155, and does not say so

The PR carries no closing reference, and its body describes itself as fixing "the log-noise half" of
astubbs#155. That framing implies a remainder, and there is not one.

Both the issue's own `## Fork status` and this PR's body say the other half - the stall - was already
fixed by confluentinc#547 and confluentinc#606, and further by the confluentinc#857 family here
(astubbs#119), with "nothing in this PR touches it". So once astubbs#201 lands, astubbs#155 has
nothing open left in it.

**Add `Fixes astubbs/parallel-consumer#155` to the body before merge.** Without it the issue outlives
its own fix, and the next person to read it inherits the "half" framing and goes looking for the
missing half.

## astubbs#203 closes astubbs#169 and astubbs#170, in a form the convention forbids

The links resolve, so GitHub will close both. But the body writes `Closes #169. Closes #170.` -
bare - and [`docs/issue-references.md`](../issue-references.md) requires the fully qualified
`owner/parallel-consumer#NN` for anything posted to GitHub, because that is the only form that both
names the repo and auto-links. confluentinc#169 and confluentinc#170 both exist and mean other
things, so a reader of that body is guessing.

Cosmetic against the gate, which passes, and not cosmetic against a human. Worth correcting while the
PR is open.

## astubbs#204 deliberately does not close astubbs#177, and that is right

Merged with no closing reference, on purpose. Its body has a section headed "What this closes, and
what it does not", reading confluentinc#833 as one symptom sitting on three independent defects:
astubbs#100 and astubbs#80 landed, astubbs#204 took the reporting and the retry budget, and the third
<!-- post-merge: checked -->
- an AB-BA deadlock between the poll and control threads on the commit path - is astubbs#29, whose fix
had not been observed working at the time of writing: its reproducer runs in a commit mode the cycle
cannot reach. `bug-857-family.md` owns that finding and its current state.

No change needed to the PR. **The gap is on the issue**, which has no comments at all, so the
reasoning for why it survived its own fix exists only inside a merged PR body. Anyone scanning the
tracker sees an open issue whose fix merged and draws the obvious wrong conclusion. The remaining
scope is recorded here and in [`core-commit-failure-seam.md`](core-commit-failure-seam.md); it is
not recorded where a reader of the issue will meet it.

## Delete when

astubbs#201 carries its closing reference, astubbs#203's is qualified, and astubbs#177 says on its
own face what is left in it.
