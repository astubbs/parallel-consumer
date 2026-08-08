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

**The only shortcoming on the list that can lose data.** `consumedOffsets.put` happens when
`doProcess` returns, so with workers completing out of order a commit can cover records still in
flight. Crash there and they are gone.

The fix is to stop maintaining Streams' `consumedOffsets` on the PC path and take commit data from
`WorkManager.collectCommitDataForDirtyPartitions()` - which tracks exactly this: completed offsets,
out of order, with the incomplete ones encoded into the commit metadata.

This is not new work so much as deleting the wrong mechanism in favour of the one already underneath
it. Out-of-order completion tracking is what Parallel Consumer *is*.

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

**We are patching Kafka Streams anyway.** So make the unsupported surface *unavailable* when the seam
is on: fail at build or topology-construction time rather than at 3am in production. A user who cannot
express the broken thing cannot be hurt by it, and an alpha that refuses what it cannot do honestly is
worth more than one that documents the same list in a README nobody reads.

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

Pair both with their caveats, per `next-fork-packaging-docs-and-licensing.md`.

## Delete when

The triage is done and its outcome recorded, and items 2 and 3 have either landed or become their own
plans.
