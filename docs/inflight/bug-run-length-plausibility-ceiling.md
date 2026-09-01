# A structurally valid but implausibly large run length is still accepted

<!-- inflight-type: bug -->
<!-- inflight-impact: data-loss -->
<!-- post-merge: checked-begin -->

A `RunLengthV2` entry of `Integer.MAX_VALUE` moves the highest-seen offset about two billion forward.
That marks the whole range as already succeeded, so `PartitionState#isRecordPreviouslyCompleted` skips
every record in it - silent non-processing, not replay.

**Why astubbs#207 did not fix it while fixing its neighbours.** That PR rejects what the payload itself
proves wrong: a negative length or run, a declared bit length with no bytes behind it, a run-length body
that is not a whole number of entries, an empty run-length body. A long run of *completed* offsets is
exactly what run-length encoding is **for**, so nothing in the bytes distinguishes an absurd run from a
legitimate one, and the decoder has no partition end offset to check it against.

**The decision.** Bounding it means choosing a plausibility ceiling - a number with no principled
derivation, which would reject a real offset map if set too low. That is a product call, not a
correctness one, and guessing it is worse than leaving the gap recorded.

Reachable today by anything that can write offset metadata for the consumer group, so it is not purely
theoretical - but it needs metadata that is well-formed as far as the encoder is concerned.

<!-- post-merge: checked-end -->
