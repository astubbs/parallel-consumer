# The mirror fixes in flight, and whether they close their issues

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

Four of the mirrors swept in the 2026-08-20 triage already had a fix, so they were checked for a
different thing than the other ten: does the PR actually close the issue, and if it deliberately does
not, is that reasoning anywhere a reader will find it? The companion note for the ten without fixes is
[`upstream-mirror-bodies-are-stale.md`](upstream-mirror-bodies-are-stale.md).

<!-- post-merge: checked-begin -->
Three of the four are settled. astubbs#201 now carries `Fixes astubbs/parallel-consumer#155`, and
astubbs#203 carries `Closes astubbs/parallel-consumer#169. Closes astubbs/parallel-consumer#170.` in
the qualified form the convention requires - both verified against GitHub's own resolved links, not
the PR bodies alone. Confirm with `gh pr view <n> -R astubbs/parallel-consumer --json
closingIssuesReferences` rather than trusting this sentence; the point of recording it is that the
question was asked and answered, not the answer itself.
<!-- post-merge: checked-end -->

## astubbs#204 deliberately does not close astubbs#177, and that is right

Merged with no closing reference, on purpose. Its body has a section headed "What this closes, and
what it does not", reading confluentinc#833 as one symptom sitting on three independent defects:
astubbs#100 and astubbs#80 landed, astubbs#204 took the reporting and the retry budget, and the third
<!-- post-merge: checked -->
- an AB-BA deadlock between the poll and control threads on the commit path - is astubbs#29, whose fix
had not been observed working at the time of writing: its reproducer runs in a commit mode the cycle
cannot reach. `bug-857-family.md` owns that finding and its current state.

No change needed to the PR. **The gap is on the issue**, whose only comment is a heads-up about the
`InternalRuntimeException` rename, so the reasoning for why it survived its own fix exists only inside
a merged PR body. Anyone scanning the tracker sees an open issue whose fix merged and draws the
obvious wrong conclusion. The remaining scope is recorded here and in
[`core-commit-failure-seam.md`](core-commit-failure-seam.md); it is not recorded where a reader of the
issue will meet it.

## Delete when

astubbs#177 says on its own face what is left in it.
