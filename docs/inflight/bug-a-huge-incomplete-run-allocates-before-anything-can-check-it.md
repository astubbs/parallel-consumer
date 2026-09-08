# A run-length payload can ask for two billion boxed longs before any check runs

<!-- inflight-type: bug -->
<!-- inflight-impact: crash -->

`OffsetRunLength.runLengthDecodeToIncompletes` walks an *incomplete* run one offset at a time into a
`TreeSet`, so a five-byte payload whose first run is `Integer.MAX_VALUE` asks for two billion boxed
longs and exhausts the heap during the rebalance callback. Same family as the `BitSetV2` case
astubbs#207 closed by requiring the declared bit length to be backed by bytes that are present - but
run-length is compact by design, so nothing in the payload bounds what one entry may demand.

**The plausibility check does not cover this one, by construction.** That check compares the
*decoded* claim against the partition's log end offset, and the log end offset is not knowable until
the first fetch delivers it - by which time this decode has already happened. The claim-side defect
(a completed run marking a range as done, so PC silently skips it) is what it closes.

**A decode-side bound would need a number nobody can derive**, which is exactly what
`bound-a-guard-with-ground-truth-not-a-plausible-number-2026-09-08.md` argues against for the claim
case. Two shapes worth considering when this is picked up, neither costed yet:

- Cap the *materialisation* rather than the claim: decode incompletes lazily, or refuse to build a
  set larger than the partition could deliver in flight, which is a memory-safety limit rather than
  a plausibility one and can be stated as such.
- Carry the claim without expanding it, so a range only becomes a set when records arrive to justify
  it.

Reachable by anything that can write offset metadata for the consumer group. It fails loudly (an
`OutOfMemoryError`, not a silent skip), which is why it is not a v6 gate on its own.
