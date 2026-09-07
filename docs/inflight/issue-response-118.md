# Draft response to astubbs#118 (confluentinc#326) - the corrupt-body half

<!-- inflight-type: task -->
<!-- inflight-impact: stranded-work -->
<!-- post-merge: checked-begin - every astubbs#207 reference below is past tense, which stays true
     once that PR has landed; the draft itself is addressed to the issue, not to the branch -->

**Not posted.** Post only on explicit instruction, and delete this file when it is posted - never when
the PR that wrote it lands. The issue is CLOSED and already carries a comment opening the paper trail
to astubbs#207; what follows is the half that comment could not contain, because the work it describes
came after it was written.

**What is already said on the issue, and must not be repeated:** that astubbs#217 fixed it, that
astubbs#207 revisited the same defect independently, that astubbs#207 routes every unreadable payload
through the policy instead of making the ungoverned path benign, and that the default moves to
`IGNORE` so this reporter stays covered. All of that is in place.

---

## Draft

Following up on this once more, because the fix grew after the comment above was written - and the
part that grew is the part that would have affected you more than the crash did.

**The crash you reported was the visible half.** Your Kafka Streams metadata could not be decoded, and
the exception escaped `onPartitionsAssigned`, which Kafka turns into
`User rebalance callback throws an error`. That is fixed, and with the default now `IGNORE`, pointing
Parallel Consumer at a consumer group Kafka Streams previously owned no longer stops it: the
unreadable map is discarded and the partition starts from its committed offset.

**The half nobody had looked at is what happened to metadata that decoded *successfully* but wrongly.**
Probing the decoders with malformed payloads found that they trusted their own header fields. A
`BitSet` payload declaring 32767 bits with an empty body produced 32767 fabricated incomplete offsets
and no error at all. One declaring `Integer.MAX_VALUE` bits exhausted the heap. A negative length, or a
negative run length, produced a highest-seen offset *below* the committed one. A run-length body one
byte too long had that byte silently dropped and the rest decoded.

None of those throw. They return an offset map that nothing downstream can distinguish from a real one,
and the two outcomes are mass replay or mass silent skipping of records that were never processed. A
crash is the better failure of the two, which is why this is worth a second comment on a closed issue.

Those payloads now go through the same policy your case does, so they are discarded rather than
believed. The checks added are only ones the payload can settle against itself - a declared bit length
must be backed by bytes actually present, a run length is a count so it can never be negative, and a
run-length body must be a whole number of entries. Nothing plausible-but-wrong is rejected, and there
is a companion test that round-trips real encoder output through the new validation so a future change
that made it over-strict would fail rather than quietly discard working offset maps.

**One thing to know if you ever set the policy to `FAIL` deliberately.** `FAIL` now means what it says
for every unreadable payload, not only for recognisably-Kafka-Streams bytes. It stops rather than
discarding a map, because discarding one replays records that completed but were not committed. It
currently stops by throwing out of the rebalance callback - the same surfacing you originally saw - and
whether that is a clean stop or leaves commits blocking is recorded as open
(`docs/inflight/core-fail-policy-escapes-the-rebalance-callback.md`). The default is `IGNORE`, so you
reach none of that without asking for it.
<!-- post-merge: checked-end -->
