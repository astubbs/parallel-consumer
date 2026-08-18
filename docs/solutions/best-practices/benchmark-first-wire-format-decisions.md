---
title: "Benchmark-first for forever wire formats: measure real byte layouts through the real pipeline before freezing them"
date: 2026-08-18
category: best-practices
module: parallel-consumer-core
problem_type: best_practice
component: development_workflow
related_components:
  - testing_framework
severity: medium
applies_when:
  - "designing or extending a serialization/wire format that must stay decodable forever"
  - "encoded output size is a deterministic function of the input, so candidates can be measured before being built"
  - "a size cap or back-pressure threshold determines where density actually matters"
  - "comparing candidate encodings against incumbents that go through compression or string-encoding stages"
tags: [wire-format, offset-encoding, benchmarking, measurement-first, decision-gating, deterministic-tests, zstd, compression]
---

# Benchmark-first for forever wire formats: real byte layouts through the real pipeline

> Extracted from `origin/perf/192-offset-encoding-density` @2a31b0a74, `docs/solutions/best-practices/benchmark-first-wire-format-decisions.md`.

## Context

Parallel Consumer commits its incomplete-offset map as a string in the Kafka offset metadata
field, under a hard cap (`OffsetMapCodecManager.DefaultMaxMetadataSize` = 4,096 chars) with
back-pressure engaging at 0.75 of it (3,072 chars). Every encoding ever written to that field
must remain decodable forever - the reader has no way to know which PC version wrote a given
commit, so the format family is append-only. Adding a new wire format is therefore close to
irreversible: the encoder can be removed later, but the decode obligation and the reserved
magic bytes cannot.

Issue [astubbs#192](https://github.com/astubbs/parallel-consumer/issues/192) (mirror of
confluentinc#903) asked why PC uses custom run-length/bitset encoders rather than RoaringBitmap,
and Base64 rather than a denser text codec. The tempting failure modes were both available:
adopt the "obviously better" library on reputation, or argue from closed-form arithmetic
("a delta list is ~1 byte per incomplete, a bitset is 1 bit per offset, therefore...").
Nobody had ever measured either alternative.

The session (PR astubbs#306, branch `perf/192-offset-encoding-density`) instead measured three
candidate wire formats before committing to any of them:

- **`chunked-bitset`** - a Roaring-style chunked bitset (2^16-bit chunks, each stored as the
  smallest of array/bitmap/run containers). This IS Roaring's container model, implemented
  dependency-free, so the library question (KTD1) got a measured answer too.
- **`delta-list`** - a sparse delta-list: first incomplete's relative offset, then
  unsigned LEB128 varint gaps.
- **`u-run-length`** - unsigned-short run lengths (the existing optimisation TODO in
  `OffsetSimultaneousEncoder` made measurable: 2 bytes per run like v1, but reaching 65,535
  like neither v1 nor v2 manages at that width).

The key insight that makes this workable: **encoded density is a deterministic function of the
input** (the incompletes distribution and the range length). Unlike a timing benchmark, a size
benchmark is exact, reproducible, cheap, and its output is committable and diffable. So the
near-irreversible decision could be gated on measurement *before* any format was frozen.

## Guidance

The method, as applied in `OffsetEncodingDensityBenchmarkTest`
(`parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/offsets/OffsetEncodingDensityBenchmarkTest.java`)
and the plan (`docs/plans/2026-08-17-001-perf-offset-encoding-density-plan.md`, KTD4/KTD5):

1. **Give each candidate a minimal layout writer, not a full encoder** (KTD4). The three
   candidates are static methods in the test (`chunkedBitSetLayout`, `sparseDeltaListLayout`,
   `unsignedRunLengthLayout`) that emit real bytes in the candidate's wire shape - but they are
   not registered `OffsetEncoder`s, have no enum entries, and no decode path. Real bytes are the
   point; a decode path is not. Building a full encoder for a candidate that turns out to lose
   would be exactly the abandoned experimental code the plan forbids.

2. **Run candidate bytes through the IDENTICAL post-processing pipeline as the incumbents.**
   Incumbents are measured through the real `OffsetSimultaneousEncoder.invoke()` (reading
   `getEncodingMap()`, not `sortedEncodings`, whose size-only `TreeSet` comparator silently
   drops size-ties). Candidate layouts go through the same
   `OffsetSimpleSerialisation.compressZstd()` and the same Base64/Z85 string arithmetic, with
   the same 1-byte magic prefix, so both sides are compared on the same footing. Closed-form
   arithmetic about a format is NOT comparable to the incumbents' post-zstd measured numbers -
   the whole comparison must happen after the shared pipeline.

3. **Measure at the engagement points, and gate shipping on a threshold there** (KTD5). The
   report finds, per scenario family, the range size at which the winning payload crosses the
   3,072-char back-pressure threshold - the point where density buys real headroom. The ship
   rule: a candidate ships only if it beats the best incumbent by >= 10%, post-compression and
   post-string-encoding under the production compression rule, on at least one scenario whose
   incumbent payload is already at or above 3,072 chars. Raw percentage wins on payloads of a
   few dozen bytes do not count; "the bar is for carrying a new wire format forever" (KTD5).
   At most the best candidate ships - the benchmark itself asserts this
   (`assertWithMessage("KTD5 allows at most one candidate to ship")`).

4. **Make the output committed, deterministic, and gated.** The test generates
   `docs/offset-encoding-density-benchmark.md`; `bin/offset-encoding-density-report.sh --check`
   fails the PR checklist when the committed report no longer matches the encoders. Determinism
   is a hard requirement (seeded `Random` keyed only on family identity and range size, no
   timestamps, `Locale.ROOT`), so the report is byte-identical across runs and machines.

5. **Record the case-against for rejected candidates where the next asker will find it.** The
   two losing candidates got named `VERDICT ... case-against` lines with the measured numbers
   in the committed report, magic-byte pairs reserved anyway (`'r'/'z'`, `'u'/'U'` per KTD7),
   and the design reasoning in the offsets package javadoc
   (`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/offsets/package-info.java`).
   A measured case-against is a result, not a failure - it is the permanent answer to
   astubbs#192.

6. **Show the decision's full input, and flag where the rule's letter and spirit diverge.** The
   report lists every qualifying row the verdict was decided on, and states plainly that 5 of
   the 6 rows at or above the back-pressure threshold were already PAST the 4,096-char cap -
   where the partition blocks whichever encoding wins - so whether the delta-list's win
   satisfies KTD5's *spirit* ("real headroom where density matters") was explicitly handed to
   the human deciding the build milestone, not quietly settled by the benchmark.

The plan's milestone structure enforced the gate: U1 (benchmark) had to produce its verdict
lines before U4 (the winning encoder) could start - "Do not start this unit before U1's verdict
line exists; if the verdict is case-against for all candidates, this unit and its files are
skipped entirely." U5 then closed the loop: the shipped encoder's real output replaced the
layout-writer column and was asserted byte-for-byte equal to it
(`DeltaListEncodingTest.matchesTheBenchmarkLayoutWriterByteForByte()`), validating the KTD4
layouts retroactively.

## Why This Matters

**Shipped wire formats are forever.** The reader must accept every format ever written, so a
format that ships on intuition and turns out mediocre is not removable - it is a permanent
decode obligation, a reserved magic byte, and a maintenance surface. The benchmark cost one
test class and a committed report; the alternative was potentially building two production
encoders (with decode paths, enum entries, golden vectors, and forever-compatibility) that the
measurement shows would have lost. The obligation also runs forward: a prior session had
already settled that PC owns the commit-metadata string outright and the format is earmarked to
grow an optional embedded opaque byte-blob field for clients (session history) - one more
reason a frozen layout is a commitment, not an experiment.

**Closed-form arithmetic and post-zstd measurements are incommensurable.** The report states
the mechanism: "zstd on a sparse bitmap is the incumbent's strongest move. A BitSet of a
low-density incompletes set is a long run of zeros with a few ones, and zstd takes it close to
its entropy - which is why the pre-compression size advantage of a sparse format shrinks so
much by the time it reaches the string." A layout's apparent redundancy is exactly what the
compressor removes, so the naive arithmetic ("1 byte per incomplete beats 1 bit per offset by
8x at 1% density") measures an advantage most of which does not survive to the metadata string.
The delta-list still won on sparse-to-mid uniform families - but because zstd works on a much
smaller input, not by the naive margin, and the advantage *reverses* at 20% density where gaps
are short and numerous. None of that is derivable without running the real pipeline. The same
run also surfaced a pipeline defect no closed-form model contained: the all-or-nothing
compression gate, where one tiny encoding suppressed every other encoder's zstd twin, costing
real characters (fixed as KTD8/R11 - per-encoder twins - independent of any candidate).

**A recorded case-against prevents re-litigating.** "Why not RoaringBitmap?" is the kind of
question that returns every year or two. With the measured verdict committed (and regenerated
by a gate whenever the encoders change), the next asker gets numbers, not folklore - and the
next proposer of a chunked bitset finds +0.03% waiting for them.

## When to Apply

- Designing any persisted or wire format with forever-decode obligations: offset metadata,
  message headers, on-disk state, protocol frames - anywhere old writers' output must stay
  readable and formats accumulate rather than replace.
- Any time a size/density claim is about to be decided by arithmetic or intuition, and the
  real pipeline includes a compressor, an outer codec, framing overhead, or a threshold rule -
  anything that makes the paper number and the shipped number diverge.
- When an "obviously better" well-known library format (RoaringBitmap here) is proposed
  against bespoke incumbents: implement its *layout* as a throwaway writer and measure it on
  the real corpus before taking the dependency. Here the library's container model scored
  +0.03% at best on qualifying scenarios, against a ~450KB jar and a four-dependency policy
  (KTD1).
- When the property being decided is a deterministic function of the input. Size is; latency
  is not - this method deliberately excludes timing (no JMH), which needs different machinery.
- The gate matters most when a threshold or cap creates distinct operating regimes: measure at
  the engagement points where behavior changes (here, where back-pressure engages), not on
  averages over regions where the win is irrelevant.

## Examples

From the committed report (`docs/offset-encoding-density-benchmark.md`) and the session's
commits on PR astubbs#306:

- **`delta-list`: ship.** Verdict as recorded: "best incumbent 58,783 chars -> 45,421 chars on
  uniform-random 5% @ range 1,000,000 (delta-list+zstd), 22.73% denser; clears the 10% bar on a
  payload already at or above the 3,072-char back-pressure threshold." Across the qualifying
  rows it delivered +19.8% (uniform 1% @ 1M), +20.9% (uniform 5% @ 100k), and +22.7%
  (uniform 5% @ 1M) - the "+20-23% of metadata characters on the uniform-sparse scenarios" the
  shipping commit ("feat(offsets) astubbs#192: sparse delta-list encoder ships, and zstd twins
  register per encoder") cites. It became `DeltaListEncoder` (`'d'/'D'`), the only candidate
  built into production.
- **`chunked-bitset` (Roaring's container model): case-against.** Best qualifying result
  +0.03% - 11,550 chars vs the incumbent's 11,553 (`BitSetV2Compressed`) on uniform-random 20%
  @ range 100,000. Production-view winner on 1 of 32 scenarios. Never built as an encoder;
  magic bytes reserved, reasoning recorded in the package javadoc.
- **`u-run-length`: case-against.** Best qualifying result -0.14% - 3,579 chars vs the
  incumbent's 3,574 (`RunLengthCompressed`) on clustered-bursts @ range 1,000,000. Production-
  view winner on 0 of 32 scenarios: the incumbent run-length families were already effectively
  optimal in their home scenarios (4-20 chars for a trailing run, where the candidates' 5-byte
  `[magic][rangeLength]` header alone is decisive).
- **The threshold-honesty example.** 5 of the 6 rows the ship rule qualified on were already
  past the 4,096-char cap, where the partition blocks regardless of winner. The benchmark
  applied KTD5's rule exactly as written, shipped the report saying so, and explicitly deferred
  the spirit-of-the-rule judgement to the human gate before U4 - the ship decision was made on
  the visible engagement-point table, not on a verdict line alone.
- **The free win the measurement surfaced in passing:** Z85 over Base64 converges on ~6%
  shorter for every encoding at once - shipped as the length-competitive outer codec (writer
  emits the shorter form, with a small-payload Base64 floor; reader accepts both forever),
  independent of any candidate format.

One caveat the report itself carries and any reuse of this method should copy: the scenario
distributions are assumed, not observed - no workload telemetry of
`ratioMetadataSpaceUsedDistributionSummary` was available - and the report says to read the
verdicts with that in mind. A benchmark-first gate is only as representative as its corpus, so
state the corpus's provenance in the committed output.

## Related

- PR astubbs#306 - the offset-encoding density PR this method was developed on
- Issue astubbs#192 (mirror of confluentinc#903) - the driving question; the committed report is its permanent answer
- `docs/offset-encoding-density-benchmark.md` - the committed, regenerated benchmark report (the numbers live there)
- `docs/plans/2026-08-17-001-perf-offset-encoding-density-plan.md` - KTD4 (layout-first benchmark) and KTD5 (candidate-neutral ship rule)
- `docs/inflight/perf-192-followups.md` - deferred follow-ups from the same branch (report-renderer extraction, decode invariants)
- Issue astubbs#237 (exact continuous offset encoding) and astubbs#129 (serialisation versioning strategy) - forward-looking consumers of this rule: future candidate formats should go through the same gate
- `docs/solutions/best-practices/sneaky-thrown-checked-exceptions-defeat-spotbugs-dataflow.md` - sibling learning from the same PR (different lesson: static analysis vs sneaky-thrown exceptions)
- `docs/solutions/logic-errors/boundary-claim-tested-only-on-friendly-samples.md` - sibling learning from the same PR: verifying formula claims already in prose (post-decision), where this doc is about measuring candidates before freezing them (pre-decision)
- `docs/solutions/logic-errors/all-or-nothing-conditional-registration-suppresses-competitors.md` - the full writeup of the all-or-nothing compression gate this benchmark surfaced in passing (KTD8/R11)
