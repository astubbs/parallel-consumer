---
title: "All-or-nothing conditional registration: one small encoder suppressed every encoder's compressed twin"
date: 2026-08-18
category: logic-errors
module: parallel-consumer-core
problem_type: logic_error
component: service_object
related_components:
  - testing_framework
severity: medium
symptoms:
  - "Winning offset-map payloads were larger than necessary: a quiteSmall run-length encoding (a handful of bytes) suppressed the zstd twin of a bitset thousands of bytes long, because twin registration was gated on NO encoder being small"
  - "The cost was silent - no failure, no error, just extra characters against the offset-metadata cap; the '!'-marked rows of docs/offset-encoding-density-benchmark.md are its measurement"
  - "The cost only appeared in scenarios where the small encoder was not itself the winner, so winner-focused checks never surfaced it"
  - "Latent amplifier: registering a new often-tiny encoder (the delta-list) would have flipped the global gate in almost every scenario, disabling compression twins for ALL encoders - adding a better competitor would have made committed payloads LARGER"
root_cause: logic_error
resolution_type: code_fix
tags: [all-or-nothing-gate, conditional-registration, per-item-gating, competitive-selection, offset-encoding, compression, zstd, no-regression-property-test]
---

# All-or-nothing conditional registration: a small competitor suppressed everyone's compressed twins

> Extracted from `origin/perf/192-offset-encoding-density` @2a31b0a74, `docs/solutions/logic-errors/all-or-nothing-conditional-registration-suppresses-competitors.md`.

## Problem

`OffsetSimultaneousEncoder` runs several offset encoders simultaneously over the same incompletes data and commits whichever encoding packs smallest (`packSmallest()`, `parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/offsets/OffsetSimultaneousEncoder.java`). Each encoder can also register a zstd-compressed twin of its plain output, adding another candidate to the competition.

Whether twins registered at all was decided by a single **global** gate over the whole field:

```java
// pre-fix
boolean noEncodingsAreSmallEnough = encoders.stream().noneMatch(OffsetEncoder::quiteSmall);
if (noEncodingsAreSmallEnough || compressionForced) {
    encoders.forEach(OffsetEncoder::registerCompressed);
}
```

(before-side of the diff in commit "feat(offsets) astubbs#192: sparse delta-list encoder ships, and zstd twins register per encoder", PR astubbs#306)

If ANY one encoder's plain output was `quiteSmall()` (under `LARGE_ENCODED_SIZE_THRESHOLD_BYTES` = 200), NO encoder got a compressed twin. The per-item question "is THIS encoder's output worth compressing?" was implemented as an all-or-nothing predicate over the whole field.

Why this matters in a smallest-wins competition: the gate couples competitors. A run-length encoding of a long complete run is a handful of bytes; its mere existence then suppressed the compressed twin of a bitset thousands of bytes long. Whenever the small encoder was not itself the winner, the committed payload was larger than it needed to be - real characters against Kafka's offset-metadata cap, which is exactly the budget the density work (issue astubbs#192, plan `docs/plans/2026-08-17-001-perf-offset-encoding-density-plan.md`) exists to protect.

The plan made the latent defect acute. The new delta-list encoder is small in precisely its target scenarios, so registering it would have flipped the global gate off across far more of the input space - adding a *better* competitor would have made winning payloads *larger*. The plan's KTD8 entry states it directly: "A new candidate that is small in exactly its target scenarios would flip that gate and stop the incumbents' compressed twins from registering, making winning payloads larger - the inverse of this plan's goal."

## Symptoms

None, and that is the point. No error, no warning, no failed test - just committed offset payloads silently larger than they should be, and getting larger when an improvement lands. The observable effect would have been density regression: scenarios where some other encoder's compressed form would have won committing its plain form instead, burning metadata headroom and engaging back-pressure earlier.

It was caught before it shipped a regression, during the density work itself:

- The plan's known-tricky-details audit identified the gate (KTD8, with requirement R11: "adding a small candidate must never suppress other encoders' zstd twins, and for every benchmark scenario the chosen payload with the new encoder registered is no larger than without it"). The plan also records that the inefficiency was **pre-existing** for the incumbent set - a small RunLength could already suppress a larger BitSet's smaller zstd twin - the delta-list would only have widened it.
- The benchmark measured it. `docs/offset-encoding-density-benchmark.md` marks every scenario where at least one registered encoding was too small to earn a twin with a `!` in the twins column, noting that under the all-or-nothing gate this report was first generated with, one such encoding suppressed them all - and the per-candidate verdicts explicitly discount `!`-row margins for it.

## What Didn't Work

The original global-gate reasoning was locally plausible: "if some encoding is already small, the payload fits comfortably, so the compression step is pointless overhead - skip it." That holds only under an unstated assumption: that the small encoding is the one that will be committed. The competition breaks the assumption. `packSmallest()` picks the minimum over *all registered candidates*, so a small non-winning encoding says nothing about whether the eventual winner would benefit from a twin. The gate turned one competitor's private property (my plain output is tiny) into a field-wide decision (nobody compresses), coupling candidates that a smallest-wins selection needs to be independent.

## Solution

Shipped in PR astubbs#306, commit "feat(offsets) astubbs#192: sparse delta-list encoder ships, and zstd twins register per encoder". Twin registration became per-encoder, each gated only on that encoder's own plain size:

```java
private static void registerCompressedTwins(final Set<? extends OffsetEncoder> encoders) {
    for (OffsetEncoder encoder : encoders) {
        if (compressionForced || !encoder.quiteSmall()) {
            encoder.registerCompressed();
        }
    }
}
```

(`OffsetSimultaneousEncoder.registerCompressedTwins`; its javadoc carries the KTD8 rationale, the `!`-row citation, and the can-only-shrink invariant: "A compressed twin can only ever be chosen by `packSmallest()` if it is smaller, so registering more of them cannot make a commit bigger.")

The invariant is pinned by `parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/offsets/OffsetEncoderRegistrationTest.java`:

- `aSmallEncodingNoLongerSuppressesAnotherEncodersCompressedTwin` rebuilds the shape that made this necessary - 10,000 offsets with a 500-offset incomplete tail, where run-length encodes in a handful of bytes and the bitsets need one bit per offset. It asserts run-length gets no twin (compression cannot help it), the large encodings keep theirs, and, for every incumbent pair, a twin is registered *exactly when* the plain form is not `quiteSmall`.
- The R11 no-regression property test, `theShippedPayloadIsNeverLargerThanBeforeTheDeltaList`, phrases the guarantee as `shippedBytes` `isAtMost(before)`: for each of eight benchmark scenarios (trailing-incomplete-run, four uniform-random densities, clustered-bursts, alternating-xo, all-incomplete) crossed with three range sizes (1k, 10k, 100k), the payload chosen now - delta-list registered, per-encoder twins - must be no larger than what the pre-change pipeline would have committed. The "before" side is reconstructed from a forced-compression pass by replaying the old all-or-nothing gate over incumbent plain sizes (`smallestBeforeThisChange`), which is valid because forcing compression only *adds* entries and the old gate was a pure function of plain sizes.
- `forcedCompressionStillRegistersEveryTwin` pins that the testing override still registers every twin regardless of size.

## Why This Works

In a pick-the-smallest competition, correctness of "add more candidates" rests on monotonicity: each candidate's presence can only improve or leave unchanged the outcome. Registration is where that property lives. A twin registered per-encoder is inert unless it wins, and it can only win by being smaller - so every registration decision keyed on the item itself preserves monotonicity. Any gate keyed on the *field's* state (does ANY encoder satisfy X? do NONE?) couples candidates: adding one changes what others are allowed to enter, and an improvement anywhere can become a regression elsewhere. Per-item gating restores independence; the R11 property test pins the invariant itself - "adding an encoder never makes the chosen payload larger" - rather than any specific size, so it keeps holding as encoders, thresholds, and zstd versions drift.

## Prevention

Name the defect class: **conditional registration keyed on a global predicate over the field instead of the item**. A per-participant decision (should this one get a variant / be entered / be skipped?) implemented as an all-quantified condition (`noneMatch`, `anyMatch`, `allMatch`, "if nothing is small", "if any is ready") silently couples competitors, and stays latent until a new participant shifts the predicate's truth value.

- **Grep-able smells**: registration or enrollment inside `if (collection.stream().noneMatch(...))` / `anyMatch(...)` blocks; a boolean derived from the whole set (`noEncodingsAreSmallEnough`, `allCandidatesReady`) guarding a `forEach(register)`. When the guarded action is per-item, ask whether the predicate should be per-item too.
- **The property-test pattern for competitions**: whenever a system selects the best of N candidates, guard it with a result-with-vs-result-without regression test - for every representative scenario, `outcome(candidates + newcomer) <= outcome(candidates)` (or `>=` for maximizing selections). Test the monotonicity invariant, not specific sizes.
- **When adding a competitor to any winner-take-best system**, sweep the existing scenarios and assert none degrades. The delta-list work did this via the committed benchmark (`docs/offset-encoding-density-benchmark.md`) plus the R11 test; the `!`-marked rows are what the audit looked like when it found the coupling.
- **Write the audit down before implementing**: the gate was caught because the plan's known-tricky-details pass (KTD8) asked "what in the existing selection machinery changes meaning when a new candidate arrives?" - a question worth asking of any registry, chain-of-responsibility, or scoring pipeline before extending it.

## Related Issues

- PR astubbs#306 - the offset-encoding density PR carrying the fix ("...zstd twins register per encoder")
- Issue astubbs#192 (mirror of confluentinc#903) - the driving offset-encoding issue
- `docs/plans/2026-08-17-001-perf-offset-encoding-density-plan.md` - KTD8 (the mechanism writeup) and R11 (the no-regression requirement)
- `docs/offset-encoding-density-benchmark.md` - the measured cost of the old gate (the `!`-marked rows) and the per-encoder rule now in force
- `docs/solutions/best-practices/benchmark-first-wire-format-decisions.md` - same-PR sibling; its benchmark run is what surfaced this defect in passing
- `docs/solutions/logic-errors/boundary-claim-tested-only-on-friendly-samples.md` - same-PR sibling: a different latent logic defect, same strengthen-to-a-property remedy
- `docs/inflight/perf-192-followups.md` - adjacent deferred quirk in the same competition machinery (`SIZE_COMPARATOR` drops size-ties; attribution noise only)
