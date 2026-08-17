---
title: Docs claimed a codec crossover the code didn't implement - and the tests sampled only confirming sizes
date: 2026-08-18
category: logic-errors
module: parallel-consumer-core
problem_type: logic_error
component: testing_framework
related_components:
  - documentation
severity: medium
symptoms:
  - "Docs and package javadoc claimed Base64 is used below 22 payload bytes, but the writer emitted sentinel+Z85 from n=1 whenever it was strictly shorter"
  - "Crossover test passed while sampling only payload sizes consistent with the false claim; the contradicting sizes n=1, 4, 7, 13 were never exercised"
  - "Small steady-state offset-metadata payloads lost the old-reader Base64 compatibility the docs promised"
  - "Adversarial full-domain table (n=1..30) falsified the documented 'Z85 wins from 22 bytes' crossover arithmetic"
root_cause: logic_error
resolution_type: code_fix
tags: [boundary-testing, adversarial-testing, test-sampling, formula-claims, doc-code-drift, z85, base64, offset-encoding]
---

# Docs claimed a codec crossover the code didn't implement - and the tests sampled only confirming sizes

## Problem

During the offset-encoding density work (PR astubbs#306, issue astubbs#192), the outer string codec gained a per-payload choice: emit the offset map metadata as either Base64 or sentinel-prefixed Z85, whichever string is shorter. The prose - the plan document, the offsets package javadoc, the `OffsetMapCodecManager` javadoc, and the javadoc on `encodeShorterOfBase64OrZ85` in `parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/offsets/OffsetSimpleSerialisation.java` - all told the same story about where the boundary falls:

> "sentinel+Z85 is longer below 12 payload bytes, ties from 12 through 21, and wins from 22 bytes up" (pre-fix javadoc)

That is a formula claim: a universally-quantified statement over every payload size. The code, meanwhile, implemented something simpler - pure strict-shorter-of:

```java
// pre-fix
if (Z85Codec.encodedLength(src.length) + 1 < base64Length(src.length)) { /* emit Z85 */ }
```

And the arithmetic does not support the story. Base64 emits `4*ceil(n/3)` characters (padded); sentinel+Z85 emits `1 + 5*floor(n/4) + (n%4 == 0 ? 0 : n%4 + 1)` (Z85's partial-tail rule, `Z85Codec.encodedLength`, plus one sentinel character). At n=1 Base64 pads a mostly-empty final block to 4 characters while sentinel+Z85 needs only 3. The same padding asymmetry recurs at n=4 (6 vs 8), n=7 (10 vs 12), n=13 (18 vs 20), and other sizes below 22. So the strict-shorter writer was emitting Z85 from **one payload byte** at many small sizes - contradicting every doc's claim that small payloads stayed Base64 (and stayed readable by older PC releases, which was the whole point of the claim).

This was a three-way triangle failure, and each leg looked fine from the other two:

- **Prose** asserted a boundary ("Base64 below 22, Z85 from 22").
- **Code** implemented a different rule (shorter-of, which crosses at n=1).
- **Tests** sampled only the sizes where the two happened to agree.

The pre-fix crossover test in `parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/offsets/WorkManagerOffsetMapCodecManagerTest.java` checked payload sizes `{3, 12, 21, 22, 24, 64}`. Check those against both stories: at 3 Z85 is longer, at 12 and 21 the lengths tie (and Base64 wins ties), at 22/24/64 Z85 wins. Every sampled point is consistent with **both** the false prose and the actual code - the sample was (accidentally) drawn entirely from the intersection. The falsifying sizes, 1, 4, 7, 13, were simply never sampled.

## Symptoms

From inside the session, the false story was completely convincing:

- Every document agreed, because each had inherited the claim from the previous one - the plan stated the design decision (KTD6), the method javadoc restated the plan, and the package javadoc and the `OffsetMapCodecManager` javadoc restated it again. Repetition across documents reads as corroboration, but it was one unchecked derivation copied four times. The plan even disagreed with itself ("wins ... from 24 bytes up" in one sentence, ">= 22 bytes" two sentences later - both survive in `docs/plans/2026-08-17-001-perf-offset-encoding-density-plan.md` as a historical record) and nothing tripped over it.
- The crossover test was green, and it genuinely exercised the formulas and the writer's output - at six sizes that all happened to sit where prose and code agree.
- Nothing in CI, review tooling, or the doc pipeline compares a prose formula against code behavior. Only arithmetic can.

## What Didn't Work

- **Trusting a repeated claim because a test "covers" it.** The test covered the claim in the weakest possible sense: it confirmed it at a friendly sample. A boundary claim is falsified at the boundary and at the exceptional points, not at the sizes where every story agrees.
- **Sampling convenient sizes.** `{3, 12, 21, 22, 24, 64}` reads like a thorough spread - below, at-tie, at-crossover, well-above. It was chosen (in good faith) to illustrate the documented story, so of course it confirmed the documented story.
- **Treating cross-document agreement as verification.** Four documents saying "22" is one claim with four copies, not four checks.

## Solution

An adversarial review pass refused to sample and instead computed the full length table for small n - every payload size from 1 up through the claimed boundary and beyond - from the two formulas. The table falsified the claim immediately: sentinel+Z85 is strictly shorter at n=1, 4, 7, 13 and other sizes below 22. (The n=1..30 table itself lives in the review run's working notes, not the tree; what the tree carries is its consequences.)

That exposed a **decision fork, not a doc fix**. Two consistent resolutions existed:

1. Rewrite the docs to match the accidental behavior ("Z85 from 1 byte, whenever shorter") - cheap, and silently gives up the small-payload old-reader compatibility the docs had promised.
2. Make the code implement the documented intent - a floor below which the writer always emits Base64.

The fix commit ("fix(review) astubbs#192: corrupt delta-list metadata recovers instead of crash-looping, and the Z85 floor makes the docs true", on the PR astubbs#306 branch) chose the intent. The rationale: the sizes where Z85 wins below the floor save only one to three characters each, on payloads nowhere near the 4,096-character metadata cap - a character there buys no headroom, while Base64 there keeps every payload an older PC release will ever see in steady state readable for free. The documented boundary was the *right* design; the code was wrong.

The implementation, in `OffsetSimpleSerialisation`:

```java
static final int Z85_MIN_PAYLOAD_BYTES = 22;

static String encodeShorterOfBase64OrZ85(final byte[] src) {
    if (src.length >= Z85_MIN_PAYLOAD_BYTES
            && Z85Codec.encodedLength(src.length) + 1 < base64Length(src.length)) {
        // emit sentinel + Z85
```

From 22 bytes up, sentinel+Z85 is always strictly shorter than Base64 (at 22: 29 characters vs 32), converging on ~6% shorter; the strict-shorter comparison is kept behind the floor as a semantic guard on that claim rather than trusted arithmetic.

The tests were then repointed at the sizes that had falsified the old story. `writerPicksTheShorterOuterCodecAtTheCrossover` in `WorkManagerOffsetMapCodecManagerTest` now pins, per size, the expected Base64 length, the expected sentinel+Z85 length, and the writer's actual choice:

- **n=1, 4, 7, 13** - Z85 would be a character or two shorter, and the writer must still emit Base64 (these rows are what pin the floor; they fail loudly if anyone reverts to pure shorter-of);
- **n=3** - Z85 genuinely longer; **n=12, 21** - equal length, 21 being the last size below the floor;
- **n=22** - the floor: Z85 fires (29 vs 32) and is strictly shorter from here on; **n=24, 64** - above.

Three hardening details in the test design are worth keeping:

- The expected lengths are **restated from first principles** in the test (`expectedBase64Chars`, `expectedSentinelZ85Chars`) rather than calling the production formulas, and the floor is a literal `22` rather than a read of `Z85_MIN_PAYLOAD_BYTES` - so a mutant in production cannot move both sides of the comparison at once.
- The shared helper `assertChosenOuterCodec` is also applied to every encoding round-tripped in the long-running codec tests, so real offset-map payloads of arbitrary sizes are continuously checked against the floored rule, not just the hand-picked table.
- `Z85CodecTest.encodedLengthMatchesTheDensityFormula` sweeps the *length formula itself* over the full domain n=0..64, asserting both actual encoded output and `encodedLength(n)` against an independently restated formula - the crossover arithmetic is only as true as this formula, so it is asserted directly rather than inferred.

Every doc was corrected in the same commit: the README (via `src/docs/README_TEMPLATE.adoc`) now reads "Base64 for small payloads (below 22 bytes, keeping them readable by older releases), the denser Z85 from 22 payload bytes up, where it is always shorter"; `docs/offset-encoding-density-benchmark.md` now models "the writer's floored rule" explicitly and its affected result rows were regenerated (a handful of tiny-payload winners changed once the model told the truth); the javadoc on `Z85_MIN_PAYLOAD_BYTES` and `encodeShorterOfBase64OrZ85` states the floor and *why the below-floor exceptions exist*.

## Why This Works

- **A boundary claim is universally quantified; a sample can only confirm it.** "Z85 wins from 22 bytes" means "for all n < 22, Base64 is shorter or equal" AND "for all n >= 22, Z85 is shorter". No finite sample of agreeing points proves either half - but the domain where the halves could fail is small and cheap (the boundary region, plus the periodic padding pattern), so an exhaustive table over it is a complete falsification instrument. The review's full table did in minutes what six green sample points had failed to do across the whole session.
- **Implementing the intent, not the accident, kept every doc true at once.** Because the fix moved the code to the documented boundary, the doc corrections were refinements ("here is why the floor exists") rather than a sweep of retractions across four documents - and the compatibility property the docs had promised became real instead of being quietly withdrawn.
- **Pinning the falsifying sizes converts the incident into a permanent tripwire.** n=1, 4, 7, 13 are exactly the points where "shorter-of" and "floored" disagree. Any regression to the old behavior re-falsifies at those rows with a message naming the floor, instead of passing silently the way the original sample did.

## Prevention

- **When prose states a formula or boundary, test the domain, not a sample.** If the domain is finite and cheap (here: payload sizes around a crossover), sweep it exhaustively, as `encodedLengthMatchesTheDensityFormula` does for n=0..64. If it is not, test the exact boundary and both neighbors (21, 22, and the sizes just past) - the one place a boundary claim can be wrong quietly.
- **Choose test points adversarially, not illustratively.** Points chosen to demonstrate the documented story will confirm the documented story. Ask "at which inputs would the claim and the code disagree?" and pin those. If you cannot name such a point, you have not analyzed the formula.
- **Restate formulas independently in tests.** A test that calls the production formula to compute its own expectation verifies nothing; both sides move together. Duplicate the arithmetic from first principles (and literal constants like the floor) so a production mutation breaks the comparison.
- **Cross-document repetition is not corroboration.** When several docs agree on a number, find the single derivation they all inherit and check *it*. The pre-fix plan even carried "22" and "24" in different paragraphs - internal inconsistency in a repeated claim is a strong hint no one has done the arithmetic.
- **Doc-vs-code disagreement is a design decision, not a doc fix.** When the table falsifies the prose, there are always two consistent worlds: docs follow code, or code follows docs. Deciding between them is choosing the product's contract (here: old-reader compatibility for small payloads). Make the choice explicitly, record why, and encode it in tests - do not default to editing whichever artifact is cheaper to change.

## Related Issues

- PR astubbs#306 - the offset-encoding density PR carrying the fix commit ("...the Z85 floor makes the docs true")
- Issue astubbs#192 (mirror of confluentinc#903) - the driving offset-encoding issue
- `docs/solutions/workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md` - the class this belongs to: an instrument that could not have said no carries no information. This is the sampling-bias instance - the test ran and asserted real things, but its sample was drawn entirely from the confirming region
- `docs/solutions/test-flakiness/vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md` - prior use of the arithmetic-impossibility falsification technique
- `docs/solutions/documentation-gaps/competitor-comparison-docs-must-cite-the-primary-spec.md` - same defect family on the docs side: claims authored from awareness, never checked against the authoritative source (there a spec; here the arithmetic itself)
- `docs/solutions/best-practices/benchmark-first-wire-format-decisions.md` - same-PR sibling: measuring candidates before freezing them (pre-decision); this doc is about verifying formula claims after (post-decision)
- `docs/solutions/best-practices/sneaky-thrown-checked-exceptions-defeat-spotbugs-dataflow.md` - same-PR sibling, unrelated lesson (static analysis vs sneaky-thrown exceptions)
- `docs/plans/2026-08-17-001-perf-offset-encoding-density-plan.md` - KTD6 preserves the falsified pre-fix arithmetic as a historical record
