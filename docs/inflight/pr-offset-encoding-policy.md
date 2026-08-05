# Offset-encoding forward compatibility - `fix/offset-encoding-policy-bypass`

Non-blocking finding on #197, fixed on its own branch: `invalidOffsetMetadataPolicy` was
**unreachable for the case it exists to handle**. `EncodedOffsetPair.unwrap` resolved the magic byte
via `OffsetEncoding.decode`, which threw a bare `RuntimeException("Unexpected magic: ...")` *before*
any code that knew the policy ran - so an older PC reading a commit written by a newer PC died on
partition assignment no matter how the option was set. The `default` branch of the decode switch
(an encoding that is in the enum but has no decoder - `ByteArray`) bypassed it the same way.

Worth recording because the fork now **owns this wire format**: every encoding we add makes the old
behaviour reachable by more users, and only a version that already has the fix can degrade
gracefully. The fix has to ship *before* the encoding that would trigger it, which is why it is not
being folded into a later encoding change.

## Cross-branch context

- **Collides with anything touching the offsets decode path** - `EncodedOffsetPair`,
  `OffsetEncoding`, `OffsetMapCodecManager`. Textual conflicts are likely and fine; the semantic
  point to preserve on any merge is that *every* "this build cannot read this payload" path goes
  through `EncodedOffsetPair#handleUnreadableMetadata`, not a direct `throw`.
- **`OffsetMapCodecManager.errorPolicy` is no longer a mutable static.** It was written by the
  constructor, so the last codec manager constructed in a JVM decided the policy for every other
  one (and leaked between tests). It is now a final field read from `PCModule` options. Branches
  that still reference the static will not compile - read it from the module instead.
- The static `decodeCompressedOffsets(long, byte[])` / `deserialiseIncompleteOffsetMapFromBase64(long,
  String)` helpers still exist for tests, and now default explicitly to `FAIL`.

## Left open

- `ByteArray` / `ByteArrayCompressed` remain encoder-less and decoder-less enum constants. They now
  fail cleanly rather than with an `UnsupportedOperationException`, but whether to implement or
  delete them is a wire-format decision - queued in `docs/refactoring.md`
  (*offsets/OffsetEncoding.java*), not here.
