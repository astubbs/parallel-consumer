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

## Also in this PR, found at merge prep rather than by a gate

- **`docs/features/invalid-offset-metadata-policy.yaml` had been falsified by this work** and is
  corrected. Four claims were untrue - most bluntly "FAIL is the default and stops the application",
  and a `boundaries` entry saying unrecognizable metadata "is not made fatal by this option", which
  is astubbs#217's contract this PR overturns. `check-docs-data.sh` and the `docs data: audit` lane
  both stayed green over it, because they validate structure and not truth. The PR checklist box for
  `docs/features/` had been answered "N/A - no user-facing feature added", which was wrong.
- **Javadoc lost in the merge, restored.** Taking this branch's side of `OffsetMapCodecManager`
  dropped master's parameter and `@throws` documentation on both `deserialiseIncompleteOffsetMapFromBase64`
  overloads and both `decodeCompressedOffsets` overloads. Restored as *coverage*, not text: master's
  wording asserted the astubbs#217 contract ("Metadata that cannot be decoded at all raises
  `OffsetDecodingError` under either policy"), which is exactly what this PR changes. This is the
  failure mode `AGENTS.md` warns about for whole-file conflict resolution - prose vanishes and
  nothing goes red.
- **Two dangling javadoc citations**, in the two exception classes this branch adds: both said "see
  `InternalRuntimeException` for why", and that class was renamed and deleted on master in
  astubbs#267. `check-file-refs.sh` validates cited paths, not class names in comments.
- **SpotBugs `MOM_MISLEADING_OVERLOAD_MODEL`**: this branch gave the private instance
  `deserialiseIncompleteOffsetMapFromBase64` a new signature, so a pre-existing instance-plus-static
  overload shape landed as a new-line finding. Renamed to `decodeOffsetMapForPartition`, which is
  also the more honest name - the instance form is the only one that consults the user's configured
  policy.

## Harvested from a parallel re-cut

A second session independently re-cut this work as `recut/207-offset-policy-bypass`, then stood down
(pushed only so its handoff pointed at real commits; no PR; never force-pushed over this head). Its
design disposition - that astubbs#217 supersedes this routing - is **not** adopted. Two things from
it are folded in:

- the `magicByteOfAnEncodingThatDoesNotExistYet()` helper was defined here but not applied in
  `ForeignOffsetMetadataOnAssignmentTest`, which still hard-coded `(byte) 42` in the test whose whole
  subject is the unknown-magic path. Now derived from the enum there too.
- a `JStreamParallelEoSStreamProcessorTest` flake sighting that would otherwise have gone with the
  abandoned branch. Carried into `docs/inflight/test-untracked-ci-flakes.md` - and, chasing it,
  **astubbs#116 turns out to both explain and fix it**, so it is recorded there with a mechanism and
  an owner rather than as an undiagnosed flake.

## Left open

Each of these outlived this PR and now has its own note, so nothing is restated here:

- [`core-bytearray-encodings-have-no-codec.md`](core-bytearray-encodings-have-no-codec.md) - the two
  encoder-less, decoder-less `OffsetEncoding` constants, and the wire-format decision about them.
- [`bug-run-length-plausibility-ceiling.md`](bug-run-length-plausibility-ceiling.md) - a structurally
  valid but implausibly large run length, which nothing in the payload proves wrong.
- [`core-fail-policy-escapes-the-rebalance-callback.md`](core-fail-policy-escapes-the-rebalance-callback.md)
  - `FAIL` stopping via Kafka's generic callback wrapper rather than PC's own fatal path.

- **The back-links are DONE - do not re-add them.** This entry previously listed four as outstanding
  and all four now exist, so the list was work nobody needed to repeat: astubbs#207's body names
  astubbs#118, astubbs#217 and confluentinc#326; astubbs#118 carries a comment opening the paper
  trail to astubbs#207; astubbs#217 carries one naming both astubbs#197 and astubbs#207, including
  which of its decisions astubbs#207 overturns; and astubbs#197's item 5 is ticked, with the
  fixed-twice-independently explanation inline. Verified 2026-09-02.

  What is **not** on any of them is what this PR found after those comments were written - the
  corrupt-body class, where a decoder trusting its own header returned a fabricated offset map rather
  than failing. `issue-response-118.md` drafts that half; per this directory's rules it is posted only
  on explicit instruction, and it outlives this PR rather than being deleted with it.

<!-- post-merge: checked-end -->
