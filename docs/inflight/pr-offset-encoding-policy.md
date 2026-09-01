# Offset-encoding forward compatibility - `invalidOffsetMetadataPolicy` reachability

<!-- inflight-type: bug -->
<!-- inflight-impact: config-lie -->
<!-- post-merge: checked-begin -->

Non-blocking finding on astubbs#197, fixed on its own branch: `invalidOffsetMetadataPolicy` was
**unreachable for the case it exists to handle**. `EncodedOffsetPair.unwrap` resolved the magic byte
via `OffsetEncoding.decode`, which threw a bare `RuntimeException("Unexpected magic: ...")` *before*
any code that knew the policy ran - so an older PC reading a commit written by a newer PC died on
partition assignment no matter how the option was set. The `default` branch of the decode switch
(an encoding that is in the enum but has no decoder - `ByteArray`) bypassed it the same way.

Worth recording because the fork now **owns this wire format**: every encoding we add makes the old
behaviour reachable by more users, and only a version that already has the fix can degrade
gracefully. The fix has to ship *before* the encoding that would trigger it, which is why it is not
being folded into a later encoding change.

## Master fixed the same defect first, differently - astubbs#217

**astubbs#207 and astubbs#217 (`ed78d6cc9`, `Fixes astubbs#118`) are independent fixes for the same
defect.** astubbs#217 merged first and deleted astubbs#207's item from the release ledger
(`docs/inflight/release-0.6.0.0.md` item 5, "an `OffsetEncoding` magic-byte hazard, was fixed in
astubbs#217"). Neither PR knew about the other. The two disagree about what the option *means*, so
the merge of master into astubbs#207 was a design decision, not a textual resolution:

| | astubbs#217 (master) | astubbs#207 |
|---|---|---|
| Unreadable metadata | `decode` throws checked `OffsetDecodingError`, caught by `loadPartitionStateForAssignment`, map dropped | every path routes through `EncodedOffsetPair#handleUnreadableMetadata`, which applies the policy |
| What the policy governs | recognisably-Kafka-Streams bytes only; the javadoc was **narrowed** to say undecodable metadata "is never fatal under either policy" | every unreadable payload, uniformly |
| `IGNORE` resume offset | `HighestOffsetAndIncompletes.of(baseOffset)` - the off-by-one below | `of(baseOffset - 1)` |

**astubbs#207's routing was kept.** astubbs#197's item 5 recorded the defect as "an older PC ...
dies **regardless of how the policy is configured**" - the complaint is that the policy does not
govern. astubbs#217 made the ungoverned path benign and documented the inconsistency rather than
removing it; under its default `FAIL`, Kafka Streams metadata shut the consumer down while every
other unreadable byte silently recovered. One option, two opposite behaviours, chosen by which
flavour of unreadable you happened to hit.

Two changes from astubbs#217 are **not** part of that competing fix and were re-applied on top when
resolving: the `InternalRuntimeException` -> `PCInternalRuntimeException` rename (`6aae29989`, the
old class is deleted, so references do not compile), and the astubbs#121 sample-the-high-water-mark-
once fix in `encodeOffsetsCompressed` (`7eeecc920`) - dropping that one silently loses records.

## Corrupt bodies now go through the policy too, and used to answer rather than fail

The policy originally reached two cases: an unrecognised magic byte, and a known encoding with no
decoder. A third was left, and probing it found something worse than the escape it was filed as -
the decoders trusted their own header, so metadata PC did not write produced **answers**:

| Payload | Before | Now |
|---|---|---|
| `BitSet` declaring 32767 bits, empty body | 32767 fabricated incomplete offsets, no error | policy |
| `BitSetV2` declaring `Integer.MAX_VALUE` bits | `OutOfMemoryError` | policy |
| `BitSetV2`/`BitSet` negative length | highest-seen offset *below* the committed one | policy |
| `RunLengthV2` negative run | highest-seen offset below the committed one | policy |
| `RunLength` body one byte too long | trailing byte silently dropped, rest decoded | policy |
| Truncated length field | `BufferUnderflowException` escaped `onPartitionsAssigned` | policy |
| Compressed body that is not a zstd frame | `ZstdIOException` escaped `onPartitionsAssigned` | policy |

A wrong offset map is worse than a failed one: nothing downstream can tell it from a real one, and
the two outcomes are mass replay or mass silent skipping. The checks added are only ones the buffer
can settle by itself - a declared bit length must be backed by bytes that are actually present, a run
length is a count so it is never negative, and a run-length body must be a whole number of entries
(`asShortBuffer()` silently drops a trailing partial element, which is what let the odd-length body
decode). Everything they reject is routed through `handleUnreadableMetadata` as
`CorruptOffsetMetadataException`, so it is the same user-visible event as the other two cases.

## The default is now `IGNORE`, changed from `FAIL`

Forced by the merge, not incidental to it. Once the policy genuinely governs every unreadable path,
the default decides what happens to someone who configures nothing - and that person is the
astubbs#118 / confluentinc#326 reporter, who pointed PC at a consumer group a Kafka Streams app had
previously owned. Leaving the default at `FAIL` would have made that exact report fatal again.

`FAIL` stays available and now means what it says: stop rather than silently discard an offset map,
because discarding it replays records that completed but were not committed.

astubbs#217's `ForeignOffsetMetadataOnAssignmentTest` asserted the old contract directly - that an
unknown magic byte does not escape `onPartitionsAssigned` **under an explicit `FAIL`**. That
assertion is the thing astubbs#207 overturns, so the test was rewritten rather than deleted: the
astubbs#118 regression is now pinned under the *default* policy (the reporter's actual
configuration), and a second test pins `FAIL` as genuinely fatal.

## Cross-branch context

- **Collides with anything touching the offsets decode path** - `EncodedOffsetPair`,
  `OffsetEncoding`, `OffsetMapCodecManager`. Textual conflicts are likely and fine; the semantic
  point to preserve on any merge is that *every* "this build cannot read this payload" path goes
  through `EncodedOffsetPair#handleUnreadableMetadata`, not a direct `throw`.
- **`OffsetMapCodecManager.errorPolicy` is no longer a mutable static.** It was written by the
  constructor, so the last codec manager constructed in a JVM decided the policy for every other
  one (and leaked between tests). It is now a final field read from `PCModule` options. Branches
  that still reference the static will not compile - read it from the module instead. astubbs#217
  fixed this too, as a non-final per-instance field; the final field here supersedes it.
- The static `decodeCompressedOffsets(long, byte[])` / `deserialiseIncompleteOffsetMapFromBase64(long,
  String)` helpers still exist for tests, and now default explicitly to `FAIL`.
- **`IGNORE` now resumes from `committedOffset - 1`, not `committedOffset`.** Found while wiring the
  two new paths through the shared helper: the pre-existing Kafka Streams `IGNORE` branch returned
  `HighestOffsetAndIncompletes.of(baseOffset)`, but `baseOffset` is the *committed* offset, i.e. the
  next one to be polled. Claiming to have *seen and succeeded* it makes
  `PartitionState#isRecordPreviouslyCompleted` skip that record and pushes the next commit to
  `baseOffset + 1`. The no-metadata-at-all branch of `decodeCompressedOffsets` two lines away always
  got this right (`nextExpectedOffset - 1`); the `IGNORE` branch did not. All three unreadable-metadata
  paths now agree with it. Any branch merging its own `IGNORE` handling must use `baseOffset - 1`.
  **Still live on master** via astubbs#217's `of(baseOffset)`, so astubbs#207 is what fixes it.

## Left open

- `ByteArray` / `ByteArrayCompressed` remain encoder-less and decoder-less enum constants. They now
  fail cleanly rather than with an `UnsupportedOperationException`, but whether to implement or
  delete them is a wire-format decision - queued in `docs/refactoring.md`
  (*offsets/OffsetEncoding.java*), not here.
- **`FAIL` stops the consumer by letting a checked exception escape Kafka's rebalance callback**,
  which surfaces as a generic `KafkaException: User rebalance callback throws an error` with the
  real cause nested. That opaque failure is what astubbs#118 was filed about; it is now opt-in
  rather than the default, but a deliberate stop should still go through PC's own fatal-error path
  with a message naming the partition, the magic byte and the option that caused it. Queued in
  `docs/refactoring.md`, not fixed here - it reaches beyond the offsets package.
- **A run length that is structurally valid but implausibly large is still accepted.** A `RunLengthV2`
  entry of `Integer.MAX_VALUE` moves the highest-seen offset about two billion forward, which marks
  that whole range as already succeeded. Unlike the shapes now rejected, nothing in the payload
  proves it wrong - a long stretch of completed offsets is what run-length encoding is *for*, and the
  decoder has no partition end offset to check it against. Capping it means choosing a plausibility
  ceiling, which is a product decision rather than a correctness one, so it is recorded rather than
  guessed at. Queued in `docs/refactoring.md`.
- **Back-links to add when astubbs#207 merges** (none existed when this note was written, in either
  direction):
  astubbs#207 -> astubbs#118 / astubbs#217 / confluentinc#326; astubbs#118 -> astubbs#207;
  astubbs#217 -> astubbs#197; and astubbs#197's own "Also found while triaging" bullet, which still
  carries item 5 unticked and unlinked even though the ledger file's copy was deleted.

<!-- post-merge: checked-end -->
