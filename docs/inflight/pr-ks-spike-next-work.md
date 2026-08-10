# Kafka Streams spike: the next moves, ranked

For the `parallel-consumer-streams-spike` work (astubbs#255). Detail and measurements live in
`docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md`; this file is the ranked worklist.

## 1. Triage the 33 failing StreamTaskTest cases - do this first

With the seam **on**, 33 of Kafka's 101 `StreamTaskTest` cases fail. Nobody has read the list.

**Classify, do not fix.** The question is which of three piles each failure lands in, and the answer
decides whether "stateless, at-least-once, semantically equivalent to stock" is a week away or a
quarter:

- **Plumbing** - error wrapping, close/suspend ordering, consumer pausing. Wiring, not invention.
- **PC's home turf** - anything downstream of offset accounting. See item 2.
- **Semantics** - stream time, windowing, joins, suppression, EOS. Design questions, not bugs.

Cheap to do and it de-risks every estimate that follows, which is why it is first.

## 2. Take commit data from PC, not from Streams' `consumedOffsets`

**Now planned as U9 in the plan document, governed by KTD-S7 and R10 - the detail below is the
rationale; the plan carries the executable unit.**

**The only shortcoming on the list that can lose data.** `consumedOffsets.put` happens when
`doProcess` returns, so with workers completing out of order a commit can cover records still in
flight. Crash there and they are gone.

**It is not a race, so no amount of locking fixes it.** Stock keeps a single `Long` per partition - a
high-water mark - which is *sufficient* under sequential processing, because when record N completes,
1..N-1 are provably done. Under concurrent dispatch that guarantee is gone: dispatch 10, 11, 12, let 12
finish first, and `consumedOffsets` becomes 12 while 10 and 11 are still running. One `Long` cannot
express "12 done, 10 and 11 in flight". The data structure is the defect.

`WorkManager.collectCommitDataForDirtyPartitions()` returns exactly that state: the offset is the
lowest *incomplete* offset - the genuinely safe resume point - and the metadata carries the completed
but non-contiguous offsets beyond it, encoded. Frontier plus holes. On restart, resume at the frontier
and skip what is already done.

So this is deleting the wrong mechanism in favour of the one already underneath it. Out-of-order
completion tracking is what Parallel Consumer *is*.

**One thing to get right when implementing:** `checkpointableOffsets()` merges
`recordCollector.offsets()` (changelog, producer-side) with `consumedOffsets`. Only the input-partition
half should come from PC - the changelog half is already correct and must not be disturbed.

## 3. Wake on work, so there is no penalty when PC cannot parallelise

Measured, confirmed by a one-term experiment: the `StreamThread` poll wait costs ~74ms per record
whenever PC can only dispatch one at a time, and throttles every workload besides. With `poll.ms = 1`
the single-key penalty falls from ~1695ms to ~24ms and experiment A's p50 goes from 8.0x to 19.1x.

Design and its trap are in the plan. Short version: build wake-on-work - poll briefly, then block on
our own condition for the rest of the budget - rather than repurposing `wakeup()`, which Kafka Streams
already uses for shutdown. Adaptive timeout is a legitimate interim.

**This gates a promotional claim**, so it is not merely an optimisation: "no penalty when you fall
back to traditional Kafka Streams usage" is false while the single-key case measures 0.69x.

## 4. Remove the APIs that do not work, rather than documenting them

Windowed operators, joins, suppression and EOS are broken or unsupported on the PC path, and today
nothing stops a user reaching for them - they get silently wrong results.

**We are patching Kafka Streams anyway.** So make the unsupported surface refuse to be used - by
annotating and throwing, **not** by deleting the methods:

1. **`@DoNotCall` plus `@Deprecated`** on `join`, `windowedBy`, `suppress` and friends. ErrorProne's
   `@DoNotCall` reports a call as a compile **error**, and it is already on the dependency tree at
   2.41.0 as an annotations-only artifact. `@Deprecated` gives a plain warning to everyone else.
2. **Throw `UnsupportedOperationException` from the body, guarded on `PcDispatchSwitch.isEnabled()`** -
   so a seam-off run stays identical to stock, and a seam-on run refuses immediately with a message
   naming the construct.
3. **A `ProcessorTopology` check at task construction** - the backstop, because the Processor API can
   build a `WindowStore` without touching `KStream`. No new patched class: `StreamTask` is already
   patched and its constructor holds both topology and config.

**The signatures must survive.** Kafka's own suite calls these methods heavily; deleting them stops
that suite compiling, which would forfeit the 188-test evidence and block ever running more of it.

Needs the item 1 triage first, to know exactly what the boundary is.

## 5. Stream time under concurrency - a design question, not a bug

Stream time advances in `PartitionGroup.nextRecord()`, which the PC path bypasses. But the deeper
problem is that with several records in flight there is no single obvious answer to "what is the
current stream time", and windowing, joins and suppression all inherit whatever it is.

**Promising direction:** the safe value is a *low-water mark* - advance stream time only to the point
where no earlier record is still in flight. That is structurally the same problem as tracking which
offsets are complete when work finishes out of order, which PC already solves and encodes. Worth
testing whether one mechanism can serve both before designing a second one.

## Promotional material to keep

Two lines that survived scrutiny and belong in whatever gets written for the release:

- **"The one remaining correctness question is offset commit - which is the single thing Parallel
  Consumer is already best in the world at."** True, and it reframes the largest open risk as the
  project's core competency rather than a gap.
- **"Evolutionary for the workloads it targets, with no penalty when you fall back to traditional
  Kafka Streams usage."** Accurate *only once item 3 lands* - see above.
- **"Try doing this with your Share Groups."** The sharpest line available, and it is not just a jab -
  it names a real structural difference. KIP-932 Share Groups decouple consumption from partition
  count, which is the same problem PC solves, but they operate at the *consumer* layer: nothing in
  KIP-932 gives Kafka Streams or Kafka Connect key-concurrent processing inside a task, because those
  frameworks build their own execution model on top of the consumer. Patching that execution model is
  precisely what these two modules do. The repo already has the comparison written up
  (`STRATEGY.md`, the KIP-932 section) - so the claim can be made without hand-waving, which is the
  only way it lands rather than reading as noise. Check the section is still accurate before quoting
  it, and keep the tone light: the substance carries it, and a swipe that turns out to be wrong is
  worse than no swipe.

Pair both with their caveats, per `next-fork-packaging-docs-and-licensing.md`.

## Delete when

The triage is done and its outcome recorded, and items 2 and 3 have either landed or become their own
plans.
