# Async commit is marked successful before the broker acknowledges it

<!-- inflight-type: bug -->
<!-- inflight-impact: data-loss -->
<!-- inflight-labels: concurrency -->
<!-- inflight-vetted: 2026-09-07 - still true: `ConsumerOffsetCommitter.commitOffsets` `PERIODIC_CONSUMER_ASYNCHRONOUS` branch still returns as soon as `consumerMgr.commitAsync` is handed its callback, the callback still only logs, and `AbstractOffsetCommitter.retrieveOffsetsAndCommit` still calls `onOffsetCommitSuccess(offsetsToCommit)` straight after `commitOffsets(...)` returns - so an async commit that never lands is marked durable. One citation has drifted: the marker is the lowercase `// todo keep work in limbo until async response is received?`, and the notes uppercase `grep -rn "TODO"` now finds only two unrelated `TODO(refactor)` lines -->

Surfaced by the torn-read hunt of 2026-08-24 as an out-of-family finding, and split out of
[`bug-torn-read-family.md`](bug-torn-read-family.md) so it is not deleted with that dossier when the
family's work closes.

`ConsumerOffsetCommitter` carries an existing `TODO` on this: in the asynchronous commit path the
offset is treated as committed at the point the request is *sent*, not when the broker acknowledges
it. A failed or dropped async commit therefore leaves state believing an offset is durable when it
is not, which is the wrong-commit family's shape even though the mechanism is not a torn read.

`grep -rn "TODO" parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/ConsumerOffsetCommitter.java`
is the marker. Not reproduced, not measured - recorded so it is not rediscovered a third time.
