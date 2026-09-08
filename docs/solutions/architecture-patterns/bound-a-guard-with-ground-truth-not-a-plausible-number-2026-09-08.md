---
title: "Bound a guard with ground truth, not a plausible number - the run-length ceiling that stayed open for a year"
date: 2026-09-08
category: architecture-patterns
module: parallel-consumer-core/offsets
problem_type: architecture_pattern
component: offset_encoding
root_cause: the_only_available_bound_was_a_guess_so_the_guard_was_never_written
resolution_type: code_fix
severity: high
applies_when:
  - A validator must reject an input that is structurally legal but semantically impossible
  - The obvious bound is a constant somebody has to choose, and choosing it wrong rejects real data
  - A guard has been parked as "a product call" because nobody could justify the number
  - Deciding whether a check may make a network call, and what it should do when that call fails
  - A validator wants a fact the process does not have yet, but will have shortly, for free
related_components:
  - PartitionState
  - ConsumerManager
  - EpochAndRecordsMap
  - OffsetRunLength
  - EncodedOffsetPair
tags:
  - guard-predicate
  - ground-truth
  - fail-open
  - lazy-validation
  - rebalance-path
  - offset-encoding
  - input-validation
  - wire-format
  - issue-197
---

# Bound a guard with ground truth, not a plausible number

## Context

PC stores its offset map in the commit metadata, and one encoding is run-length: a list of counts of
completed and incomplete offsets. astubbs#207 hardened that decoder against everything the *bytes*
can settle - a negative run, a body that is not a whole number of entries, a declared bit length with
no bytes behind it - and routed every rejection through the user's `invalidOffsetMetadataPolicy`.

One case was left, and recorded as parked: a run length of `Integer.MAX_VALUE` is structurally
perfect. It moves the highest-seen offset about two billion forward, and `PartitionState`'s
`isRecordPreviouslyCompleted` then reads every real record in that range as already succeeded. Not
replay - **silent non-processing**, from metadata anything sharing the consumer group can write.

The note that tracked it said, correctly, that a long run of completed offsets is exactly what
run-length encoding is *for*, so nothing in the payload proves an absurd one wrong; and it concluded
that bounding it "means choosing a plausibility ceiling - a number with no principled derivation",
which is a product call rather than a correctness one. That framing is what kept the defect open: it
treats "pick a constant" as the only available shape for the guard.

## Guidance

**When a check needs a bound, ask what in the system already knows the answer before you invent a
number.** Three candidate bounds were considered here, and only one is not a guess:

- **A configured window** - concurrency, the in-flight target, "nobody legitimately gets this far
  ahead". Refuted by the product itself: PC's back-pressure keys off the *encoded payload size*, and
  a run-length map with one stuck offset stays three entries wide however far the partition runs
  ahead of it. A single poison record therefore produces a legitimately enormous range, so any such
  ceiling eventually discards a true offset map - and discarding one replays every record it covers.
- **A round number** ("no partition legitimately runs a billion ahead of its commit"). The same
  objection with less honesty about it, and it fails silently: the day it is wrong, a real map is
  thrown away and nothing distinguishes that from the corrupt case it was written for.
- **The partition's log end offset** - the one bound a legitimate map provably cannot cross. PC only
  ever encodes offsets it has polled, an offset the partition does not hold cannot have been polled,
  and Kafka's end offset only grows, so a bound read *now* still holds for a map written earlier.
  There is no false-positive case to trade off, which is what makes it a correctness bound rather
  than a tuning parameter.

**Having found the authority, ask what it costs to consult - and whether the answer is already in
the building.** The first implementation of this fix bought the end offset with a `ListOffsets`
request at assignment, batched across the partitions carrying metadata. It worked, and it was
rejected on review for two reasons worth keeping:

- **It put a blocking broker round trip inside the rebalance callback**, which in this codebase is
  the most fragile path there is - the one whose stalls and deadlocks have consumed more
  investigation than anything else. A check against corrupt metadata is not worth a new way for a
  rebalance to hang.
- **It failed open exactly when the broker was unhealthy.** An unreachable leader means no end
  offset, which means the guard stands down - so the check was reliably absent in precisely the
  conditions that make rebalances storm and offset maps get rewritten.

**The same fact was already arriving for free, slightly later.** Every fetch response carries the
partition's high watermark, and the consumer exposes it as `Consumer.currentLag` (KIP-695) - no
request of its own, computed from the last fetch. `position + currentLag` reconstructs the watermark
exactly when both are read in one breath on the thread that owns the consumer. So the check moved
off the wire and onto the first batch: the decode accepts the map and *retains what it claims*, and
the claim is settled before any record is consulted against it. Nothing is lost by waiting, because
nothing consults the map until a record arrives to be consulted about.

**"Later, for free" beats "now, at a cost" whenever the deadline is not real.** The instinct is to
validate at the point of parsing, and here that instinct is what forced the wire call: the data
needed to judge the payload simply is not in the process yet at decode time. Ask when the judgement
is actually *needed* - the first consult, not the parse - and the cost can disappear.

**A guard with no ground truth must stand down, not guess - and standing down is deferral, not
acceptance.** `currentLag` is empty until a fetch has happened, and the position read is asked for
with a zero timeout so it raises rather than sends a request. Every such outcome means *not
established*, and the check simply looks again at the next batch. The batch's own records serve as a
floor in the meantime - a record that arrived certainly exists - and a floor can only ever *confirm*
a claim, never refute one, which is why it is safe to use with no watermark at all. Failing closed
would discard honest offset maps on a broker hiccup, a far more likely event than the corrupt
payload the guard exists for.

**Check the producing side, and pin it at the boundary.** A bound like this is only safe if the
encoder can never write what it rejects. Here it cannot, because the encoder's range top is an
offset PC actually saw - but the argument is worth a test rather than a paragraph: every encoder
round-trip case is measured against the rule with the tightest honest bound there is, a partition
ending exactly on the last offset encoded. Anything that made the encoder run one offset past what
it saw surfaces as a false rejection instead of as silence.

**A check moved to where the meaning lives often covers more than the one it replaced.** The
decode-side version had to be threaded into each decoder and applied per encoding; the lazy one
reads the *decoded claim*, so run-length and bitset are covered by the same line, and any encoding
added later is covered without being told. The bitset case is worth naming because it is easy to
dismiss: its declared bit count must be backed by bytes that are present, so a full 4KB metadata
field buys tens of thousands of skipped records rather than two billion. Bounded is not true.

## Why This Matters

The defect was correctly diagnosed, correctly filed, and then sat open - not because it was hard to
fix but because the fix had been framed as a number nobody could justify. That framing is the durable
lesson: **"this needs a constant we cannot derive" is a signal to go looking for an authority, not a
reason to park the work.** The authority here was one method call away on an object the code already
held.

It also inverts the usual reason a guard is refused. The objection to bounding a run length was that
the check might be wrong; the check that was finally written *cannot* be wrong in the expensive
direction, and the only thing traded away is a round trip on a path that already makes several.

## When to Apply

- Writing any validator for a value whose legality depends on something outside the payload: a
  length, an offset, a count, a timestamp, an id range.
- Reviewing a parked bug whose blocker is "we would have to pick a threshold".
- Deciding what a guard does when the fact it needs is unavailable - name the direction it fails in
  and say why that direction is the cheaper wrong answer.
- Being tempted to buy a fact with a network call on a latency- or liveness-critical path: ask what
  already arrives carrying it, and when the answer is first *needed* rather than first available.

## Related

- [a-guard-must-assert-what-it-means-not-what-is-easy-to-check.md](a-guard-must-assert-what-it-means-not-what-is-easy-to-check.md) -
  the sibling rule, and the step before this one: this document assumes you already know what the
  guard should mean, and is about where the number it compares against comes from.
- [`docs/inflight/pr-207-offset-encoding-policy.md`](../../inflight/pr-207-offset-encoding-policy.md) -
  the policy this guard routes through, why the default is `IGNORE`, and the corrupt-payload family
  it already covers.
- [`docs/inflight/bug-no-metric-for-discarded-offset-metadata.md`](../../inflight/bug-no-metric-for-discarded-offset-metadata.md) -
  still open: every discard here is logged and nothing counts it.
