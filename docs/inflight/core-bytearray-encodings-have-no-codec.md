# `ByteArray` / `ByteArrayCompressed` claim two magic bytes and have neither encoder nor decoder

<!-- inflight-type: task -->
<!-- inflight-impact: refactor -->
<!-- post-merge: checked-begin -->

Two `OffsetEncoding` constants reserve magic bytes that nothing can produce and nothing can read.

- **No live encoder.** `OffsetSimultaneousEncoder` has the call commented out, with the reason inline -
  grep `no advantage over BitSet encoding`.
- **No decoder.** They are absent from `EncodedOffsetPair#decodeBody` and fall to the `default` arm.

**Nothing is unsafe today.** Since astubbs#207 they fail as a typed `UnsupportedOffsetEncodingException`
routed through `invalidOffsetMetadataPolicy`, rather than the bare `UnsupportedOperationException` they
raised before, so an old reader meeting one degrades the way every other unreadable payload does.

**The open decision is the wire format, which is why this is not a tidy-up.** Deleting them frees two
magic bytes for a future encoding; implementing them commits to a format nobody has asked for. Freeing
a magic byte is only safe if no deployed PC ever wrote one - true as far as the encoder shows, but it
is a claim about every version ever shipped, not about this tree. That makes it release-gated: it
belongs with the breaking-change queue in `docs/refactoring.md`, not an ad-hoc cleanup.

<!-- post-merge: checked-end -->
