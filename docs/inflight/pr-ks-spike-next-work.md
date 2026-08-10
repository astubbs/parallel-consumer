# Kafka Streams spike: the next moves, ranked

For the `parallel-consumer-streams` work (astubbs#255). Detail and measurements live in
`docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md`; this file is the ranked worklist. Presentation
work - the example demo, the API stability tag, and dropping "spike" from the name - is in
`next-streams-module-graduation.md`.

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

**Invert the default, and let Kafka's own suite be the gate for reinstatement.** Windowing, joins,
suppression and EOS are what we know is *broken*, which is not the same set as what we know *works*.
The owner's ask is the stronger rule: every public Kafka Streams API this fork exposes starts refused
unless we have proven it, and an API comes back only when Kafka's own test suite exercises it with the
seam **on** and passes - not when someone reads the code and judges it fine. That makes the supported
surface grow with evidence rather than with optimism, and it couples this item to item 6: the more of
Kafka's suite we actually run, the more surface the gate can open.

Needs the item 1 triage first, to know exactly what the boundary is.

## 5. Stream time under concurrency - a design question, not a bug

Stream time advances in `PartitionGroup.nextRecord()`, which the PC path bypasses. But the deeper
problem is that with several records in flight there is no single obvious answer to "what is the
current stream time", and windowing, joins and suppression all inherit whatever it is.

**Promising direction:** the safe value is a *low-water mark* - advance stream time only to the point
where no earlier record is still in flight. That is structurally the same problem as tracking which
offsets are complete when work finishes out of order, which PC already solves and encodes. Worth
testing whether one mechanism can serve both before designing a second one.

## 6. Run Kafka Streams' entire test suite, not only the classes we patched

The evidence today comes from `StreamTaskTest`, `RecordCollectorTest` and `ProcessorContextImplTest` -
the classes covering what the patch touches, and the narrowest defensible sample. The harness already
unpacks and patches Kafka's own *test* sources, so aiming it at the rest of the Streams suite is
largely configuration, and anything it finds is a divergence we currently cannot see at all.

Two payoffs, and other items depend on both. The seam-**off** arm becomes a far stronger claim: the
whole suite passing against the patched classes says "behaviour preserved" in a way that a handful of
classes cannot. The seam-**on** arm produces exactly the evidence item 4's gate needs before any
refused API can be reinstated.

Expect it to be slow, and expect failures that are Kafka's own environmental flakiness rather than
ours. So the first pass is a triage exercise like item 1, not a pass/fail number.

## 7. At the Kafka 4 upgrade, carry patches for several Kafka versions and test them in parallel

The patch is generated against whatever `kafka.version` resolves to, which makes a single-version patch
set hostage to one Kafka release: the module supports exactly the version it was cut against. When the
repo moves to Kafka 4 (astubbs#53, `pr-53-java-baseline-kafka4.md`), the harness should instead hold a
patch per supported Kafka version and build and test each of them in parallel, rather than migrating
the one patch forward and stranding the previous version's users.

Deliberately not now. It only pays for itself once there is a second version to support, and the Kafka
4 upgrade is where that arrives.

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
  precisely what these two modules do.

  **Where the comparison actually lives** (an earlier draft of this note sent readers to the wrong
  place): the substantive table is in the generated README, "When to use this library (vs KIP-932
  Share Groups)" - edit `src/docs/README_TEMPLATE.adoc`, never `README.adoc`. `STRATEGY.md` carries a
  single sentence of it under Target problem. So the claim can be made without hand-waving, which is
  the only way it lands rather than reading as noise. Check the table is still accurate before quoting
  it, and keep the tone light: the substance carries it, and a swipe that turns out to be wrong is
  worse than no swipe.

  **This line is also strategy input, not only promotion.** The README's table compares Share Groups
  and PC as two ways to consume; the framework point is a level up from that, and it is the argument
  for PC as an execution engine underneath other frameworks. It has been fed into `STRATEGY.md`
  rather than living only here.

Pair both with their caveats, per `next-fork-packaging-docs-and-licensing.md`.

## Delete when

The triage is done and its outcome recorded, items 2 and 3 have either landed or become their own
plans, and the later items have moved on too - item 7 belongs with the Kafka 4 work in
`pr-53-java-baseline-kafka4.md` once that starts.
