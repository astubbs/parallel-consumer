# astubbs#337 - draft issue responses, written now because context peaks at merge, not at release

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
<!-- post-merge: exempt-file -->

**The rule this file applies (operator, 2026-08-25): an inflight note that maps to a GitHub issue
carries a DRAFT response to that issue before its PR merges.** The agents who fixed the thing hold
the best context at merge time; by release time it has to be re-mined from commit logs. Drafts are
posted (after operator review) when the merge lands or with the release note, whichever the
operator chooses - **nothing in this file is posted without an explicit instruction.** Delete this
file in the PR that posts them, or with this PR at merge prep if posting moves elsewhere.

---

## Draft 1 - for astubbs#121 (fork mirror of confluentinc#894)

> Fixed by astubbs/parallel-consumer#337, shipping in 0.6.0.0.
>
> **What it was.** Each commit carries an encoded note listing which offsets above it are still
> outstanding, written relative to the committed offset. The code read that offset twice - once to
> encode the note, once to commit - so a record completing between the reads filed the note under a
> higher base than it was written against. After a rebalance everything in it decoded shifted:
> the reported `auto.offset.reset`, and in a second, quieter mode, records dismissed as
> already-processed. The fix samples once and carries the value with the payload, so the two cannot
> disagree by construction.
>
> **The reproduction requested upstream in October 2025 now exists** - deterministic, ending on the
> reporter's own out-of-range step, with controls in both directions (the tests fail on the unfixed
> code and pass with the fix; with the injected race disarmed they pass on the unfixed code too, so
> the failure comes from the race, not the fixture). The full mechanism write-up ships in-repo:
> `docs/solutions/logic-errors/commit-offset-read-twice-shifts-every-encoded-incomplete-offset.md`.
>
> **What was done beyond this one fix, because one instance of a defect class is rarely alone:**
> - A systematic hunt for the whole class (multiple reads of moving state combined as one snapshot)
>   using multiple frontier AI models - it found four sibling defects, all fixed
>   (astubbs/parallel-consumer#344, #345, #346, #349), with ~50 further candidates examined and
>   dismissed with recorded reasons.
> - Every fix was adversarially reviewed by an independent second model, and every reproduction was
>   mutation-checked - reverting each fix turns its tests red.
> - JetBrains **Lincheck** was calibrated against the pre-fix code and refound four of the bugs
>   unaided; it is being adopted as a standing concurrency lane so the class stays detectable.
> - OpenJDK **jcstress** probes measured the remaining memory-model residuals on real hardware;
>   the one that mattered is closed by astubbs/parallel-consumer#349.
>
> Credit for the fix itself is unchanged - it is @sangreal's confluentinc#893, carried as-is.

## Draft 2 - comment for upstream confluentinc#894

> This is now fixed in the maintained fork, releasing in 0.6.0.0:
> astubbs/parallel-consumer#337 carries confluentinc#893 (credit unchanged - @sangreal's fix,
> approved here by @rkolesnev in November 2025) together with the deterministic reproduction that
> was asked for on that PR in October 2025 and, until now, did not exist anywhere.
>
> For anyone landing here later: the flow PDF attached to confluentinc#893 turned out to contain
> both the production logs and a working "how to make it happen" recipe - it was the key to the
> reproduction.
>
> Nothing in this repository is closed or changed by this comment.
>
> <!-- pc-mirror:issue-894 -->

## Draft 3 - comment for upstream confluentinc#893

> This PR is released in the maintained fork's 0.6.0.0 via astubbs/parallel-consumer#337, carried
> as-is with authorship unchanged, now accompanied by a deterministic behavioural reproduction
> (the request from 2025-10-26) and regression tests at three levels. The "another edge case"
> suspicion raised during review here was chased separately: the sibling window one call below this
> fix was found, reproduced and fixed as astubbs/parallel-consumer#344.
>
> Nothing in this repository is closed or changed by this comment.
>
> <!-- pc-mirror:pr-893 -->
