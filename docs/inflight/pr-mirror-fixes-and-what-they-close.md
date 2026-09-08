# The mirror fixes in flight, and whether they close their issues

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
<!-- inflight-vetted: 2026-09-07 - `gh pr view --json closingIssuesReferences` confirms astubbs#201 closes astubbs#155 and astubbs#203 closes astubbs#169 and astubbs#170, and that astubbs#204 still closes nothing, so the first section holds. The astubbs#177 section is rewritten: that mirror was closed as completed on 2026-09-01 and astubbs#29 merged on 2026-09-02, but the issue's only comment is still the rename heads-up, so the reasoning gap it records is intact -->

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
- an AB-BA deadlock between the poll and control threads on the commit path - is astubbs#29, which
merged on 2026-09-02. `bug-857-family.md` owns that finding and its current state.

No change needed to the PR. **The gap is on the issue**, whose only comment is still a heads-up about
the `InternalRuntimeException` rename, so the reasoning for why it survived its own fix exists only
inside a merged PR body. astubbs#177 was closed as completed on 2026-09-01 with no closing comment, so
the wrong conclusion a reader draws has changed shape rather than gone: the tracker now shows a
three-defect report closed with nothing on it saying which fix answered which half. The remaining
scope is recorded here and in
[`core-commit-failure-seam.md`](core-commit-failure-seam.md); it is not recorded where a reader of the
issue will meet it. Telling the *upstream* reporters is a different task and is owned by
[`upstream-tell-809-833-the-hang-is-fixed.md`](upstream-tell-809-833-the-hang-is-fixed.md).

## Delete when

astubbs#177 says on its own face which fix answered which half of it.
