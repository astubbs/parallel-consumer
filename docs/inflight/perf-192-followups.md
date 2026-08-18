# Deferred follow-ups from the density-work review (astubbs#192)

> Extracted from `origin/perf/192-offset-encoding-density` @2a31b0a74, `docs/inflight/perf-192-followups.md`.

Findings the `lfg192-rev1` code-review run of the offset-encoding density branch
(`perf/192-offset-encoding-density`, astubbs#192) verified but deliberately deferred - none block
that PR, and none are resolved by it.

- **Extract the density benchmark's report renderer.** `OffsetEncodingDensityBenchmarkTest` mixes
  ~250 assertion-free report-rendering lines into an already very large test file. Extraction is
  safe under the existing byte-identical gate: `bin/offset-encoding-density-report.sh --check`
  fails on any behavioural drift, so a pure move is fully verified.
- **Unchecked-decode escapes now funnel through one choke point - keep it that way.** The review
  found corrupt DeltaList bodies escaping the `OffsetDecodingError` recovery as unchecked
  exceptions; the same class pre-existed for `OffsetRunLength` and `OffsetBitSet` (truncated
  payloads throw `BufferUnderflowException`) and for the zstd `IOException` sneaky-thrown through
  `EncodedOffsetPair#getDecodedIncompletes`. The fix converts them all in
  `OffsetMapCodecManager#decodeCompressedOffsets`, which is the ONLY production caller of
  `EncodedOffsetPair.unwrap`/`getDecodedIncompletes` - so the whole class is mitigated for every
  encoding, but only for as long as that choke point stays the sole decode entry: a new direct
  caller of `EncodedOffsetPair` reopens it.
- **No Java-API-compatibility gate.** The library has no japicmp/revapi (or equivalent) check, so a
  source- or binary-breaking change to the public API rides through CI unflagged. Needs a decision
  on baseline version and gate placement.
- **`EncodedOffsetPair.SIZE_COMPARATOR` drops size-ties from `sortedEncodings`.** The comparator
  compares only buffer capacity, and `OffsetSimultaneousEncoder.sortedEncodings` is a `TreeSet`, so
  two encodings of equal size collapse to one - which encoding "wins" (and gets the usage-metric
  increment in `OffsetMapCodecManager#encodeOffsetsCompressed`) is then nondeterministic across
  runs. Attribution noise only - the payload chosen is the same size either way. The benchmark
  already works around it by reading `getEncodingMap()`.

## SpotBugs: DeltaListEncoder CT_CONSTRUCTOR_THROW is accepted, convention-consistent

astubbs#306's SpotBugs pass flags `CT_CONSTRUCTOR_THROW` on `DeltaListEncoder` (constructor throws
`DeltaListEncodingNotSupportedException` on range overflow). This mirrors `BitSetEncoder`'s identical
constructor-throw pattern, whose same finding sits latent in the baseline (see
`static-spotbugs-latent-findings.md`). Restructuring one encoder to a static factory while its
siblings keep the throw would be worse than the finding; if the pattern is ever fixed, fix it across
all encoders at once.
