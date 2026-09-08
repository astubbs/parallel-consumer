# Draft response to astubbs#248 - posted by the pre-release sweep, not by this PR

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
<!-- inflight-state: deferred - until the pre-release sweep, or an explicit instruction to post -->
<!-- post-merge: exempt-file - a drafted issue reply, held until the sweep posts it. It deliberately
     outlives the PR that wrote it, so it cannot be written in post-merge terms. -->

Not posted. Post only on explicit instruction; delete this file when it is posted, not when its PR
merges.

The issue is the fork mirror of confluentinc/parallel-consumer#203 (nioertel, 2022-03). It stays
OPEN after astubbs/parallel-consumer#470 - the paragraph explaining why is the point of this draft,
and must not be dropped if the reply is shortened.

---

Part of this is now fixed, in astubbs/parallel-consumer#470, and part of it is not. Both halves are
worth stating, because the part that is fixed is not the part the original report was mainly about.

**What is fixed: the async commit path no longer records an offset as committed at the moment the
request is SENT.**

The `todo keep work in limbo until async response is received?` marker this issue quotes verbatim is
deleted by that PR, and what stood behind it is now implemented. Under
`PERIODIC_CONSUMER_ASYNCHRONOUS` - the shipped default commit mode - `commitAsync` returns as soon
as the request is handed to the client, and PC was marking the partition clean on the next line. The
consequence was worse than a missing retry: marking clean ends the story, so
`collectCommitDataForDirtyPartitions()` returned empty from then on and **no further `commitAsync`
was ever issued for those offsets**. A callback then arriving with the `RetriableCommitFailedException`
you reported had nothing left to retry - "retriable" described a retry nothing would perform - and a
callback that never arrived was indistinguishable from success. The broker's committed offset stayed
behind PC's belief, silently and permanently, so whoever owned the partition next resumed from a
position the broker never recorded.

Now an offset is recorded as committed only when the broker acknowledges it. A failed or dropped
acknowledgement leaves the offsets dirty, and the next commit cycle re-sends them - the same handling
the synchronous path's rejections already get: logged, not fatal, still owed. The failure line is one
WARN saying exactly that, rather than the previous ERROR that promised a re-commit it could not
always deliver.

**What is NOT fixed, and why this issue stays open.**

Your actual symptom was a throughput collapse with endless retries after the broker restart, and
nothing in that work reproduced it. What the fix removes is the silent-loss path your report
exposed - a real defect, and one that would have made recovery worse - but it is not established
that it is the cause of the slowdown you saw. It is entirely possible that the retries you observed
were the coordinator genuinely being unavailable for as long as they lasted, with PC's handling of
them a secondary matter.

So this stays open rather than being closed by that PR, deliberately. The deciding experiment is a
reproduction against the broker-restart harness, and until someone runs it the honest position is
that one mechanism the report touched has been fixed and the reported symptom has not been explained.

**A note on this issue's history**, since it may look stranger than it is: it was closed upstream as
*completed* in a July 2023 administrative sweep while it was still labelled `wait for info`, with
three diagnostic questions unanswered. That closure carried no fix and no triage. This fork mirrors
it as open because it was never actually resolved.

If you still have the broker-restart scenario to hand - particularly the consumer configuration and
roughly how long the retry storm lasted relative to the outage - that is the missing input, and it
would settle the remaining half.
