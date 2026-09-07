# The produce-lock lifecycle is spread across four classes, and none of them can state its invariant

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->

The invariant is one sentence: **exactly one produce lock per `PollContextInternal`, released exactly
once, at `cleanUpContext`.** No class owns it. The lifecycle runs:

| Step | Where | What |
|---|---|---|
| owns the lock | `ProducerManager` | holds `producerTransactionLock`; issues via `beginProducing`, releases via `finishProducing` |
| acquires | `ParallelEoSStreamProcessor#processAndProduceResults` | twice, in two branches mutually exclusive on `isAllowEagerProcessingDuringTransactionCommit` |
| stores and guards | `PollContextInternal#setProducingLock` / `#takeProducingLock` | a field, plus the only enforcement of "one per context" |
| releases | `AbstractParallelEoSStreamProcessor#cleanUpContext` | claims it back out of the context and returns it |

Acquire in one class, store in a second, release in a third, against a lock owned by a fourth. Each
sees a quarter of the contract, so **none of their javadocs can state it** - and every one of them
carries a partial restatement instead.

## How this surfaced, which is the part worth keeping

<!-- post-merge: checked-begin - a dated account of how the finding surfaced, in the past tense -->
Not by reading the code. `dups: similarity` failed astubbs#262 on
`ProduceLockReleaseTest` <-> `ProducerManagerTest`, and the engine compares whole files *including
comments*. (Those two are now one class: the tests were merged into `ProducerManagerTest`, which is
the cohesive answer to the symptom while the ownership problem below stays open.) Two rounds of extracting genuinely duplicated code moved it about a point; deleting the
smaller file's entire class javadoc - every word of explanation - still did not clear the cap.

The first reading was that this is a measurement artefact of an engine that tokenises English. **That
reading was wrong, and the better one is Antony's:** if two test classes have to carry the same
explanation, that is evidence they are explaining the same thing, and the explanation has nowhere
single to live. The duplicated English is a *true* signal about structure. The metric found a design
problem the way a smoke alarm finds a fire - not by understanding it, but by being sensitive to
something real.
<!-- post-merge: checked-end -->

## The evidence that it is a real defect class, not just inelegance

**Every defect found in this mechanism on 2026-09-01 lives in a seam between those four classes**, not
inside any one of them:

1. **The orphaned lock.** `beginProducing` returns a lock and the caller passes it straight to
   `setProducingLock`. If the setter refuses, the caller holds the only reference and drops it -
   because acquisition and hand-over are separate steps owned by different classes, with the caller
   bridging the gap. Found by Codex review.
2. **The release that opened an EoS window.** The fix for (1) had `PollContextInternal` call
   `unlock()` - doing `ProducerManager`'s job without `ProducerManager`'s knowledge that
   `ProducingLock` is not a token but a wrapper over one shared `ReadLock`. Releasing the wrong one
   silently freed the commit thread mid-send.
3. **`setProducingLock(Optional.empty())` orphaning a held lock.** The setter is a plain field
   assignment carrying invariant work it does not own, so its guard was written against the wrong
   half of the state.

Three defects, one week, all in the joins. That is what a missing owner produces.

## What the shape of a fix looks like

<!-- post-merge: checked - names the PR that declined to do it, which stays true afterwards -->
Not proposed for astubbs#262 - recorded so it is decided deliberately rather than re-derived the next
time someone touches this.

- **Make the lock a real token.** `ProducingLock` currently wraps the shared `ReadLock`, so any
  instance can release any hold of the calling thread. A token that knows whether it has been
  released, and refuses twice, removes defect classes (2) and (3) by construction.
- **Give the lifecycle one owner.** Either `PollContextInternal` acquires and releases through
  `ProducerManager` itself (so the caller never holds a bare lock across a gap), or a small
  lifecycle type owns acquire/store/release and the three current classes delegate to it.
- **Then the explanation has one home**, and the tests stop having to restate it - which is the
  observable that started this.

## Do not "fix" this by deleting the comments

That was measured and rejected: removing the entire class javadoc from the smaller file leaves the
similarity essentially where it was, and the repo has already learned this lesson once -
`f5586337b` established that the obvious response to a high reading from this engine (delete the
prose) was the wrong one. The prose is not the problem; what it is forced to describe is.
