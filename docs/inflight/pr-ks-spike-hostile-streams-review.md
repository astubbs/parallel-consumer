# Wanted before this alpha is taken seriously: a hostile Kafka Streams core-developer review

For the `parallel-consumer-streams-spike` PR (astubbs#255).

## The brief

Review the module **from the point of view of a Kafka Streams core developer who starts from the
belief that this cannot work.** Not a balanced review - an attempt to falsify.

The output that matters is not an opinion. It is **a failing test**: find a hole and write the test
that exposes it. A hole nobody can demonstrate is a hypothesis; a hole with a red test is a finding.

## Why this specific shape

Every review so far has been done by people (and agents) who already believed the seam works, checking
whether it was built correctly. That is a different question from whether it is *sound*. The strongest
evidence in the spike came from exactly this move applied narrowly - un-confining one field and
watching a `ClassCastException` fall out of the topology graph - and nobody predicted that failure
before the control arm produced it.

Someone who knows `StreamThread`, `PartitionGroup`, `RecordCollector` and the store stack from the
inside will know where else to push.

## Where to aim, based on what the spike already admits

These are the known-weak spots. A reviewer should not be limited to them, but they are the cheapest
starting points, and each should have a test written against it:

- **Stream time never advances on the PC path.** `streamTime` moves in `PartitionGroup.nextRecord()`,
  which the PC path bypasses. What breaks besides `STREAM_TIME` punctuators?
- **Offsets commit optimistically.** Workers finish out of order and `consumedOffsets` takes whatever
  finished last, so a commit can cover records still in flight. Kill the process mid-run and show the
  loss.
- **Consumer pausing is gone.** `StreamTask.addRecords`'s buffer-based pause never fires, so PC's own
  limits are the only inflow control. Show unbounded growth, or show the consumer starving.
- **Failures surface a pump cycle late**, not synchronously. Show a case where that lag matters.
- **The `record`/`recordInfo` field handoff between threads is still racy** - U4 fixed `recordInfo`
  allocation but `StreamTask.record` has the same defect, untouched and known.
- **`commitNeeded` and `partitionsToResume` still have read-modify-write races.** Concurrent
  collections fixed corruption, not atomicity.
- **33 of Kafka's own `StreamTaskTest` cases already fail with the seam on.** That list is a map of
  where the semantics diverge - a hostile reviewer should be able to turn several of them into
  minimal, targeted reproductions.

## What would count as a result

- A red test in this repo demonstrating a hole - best outcome, whatever it costs the claim.
- A demonstrated hole that cannot yet be tested, with the reason.
- A reasoned argument that a named limitation is worse than the write-up admits.
- "I tried to break X and could not, here is what I tried" - genuinely useful, and the only form of
  reassurance worth anything here.

Do not accept the module's own test suite as evidence. It was written by the same people who wrote the
implementation.

## Delete when

The review has run and its findings are either fixed or recorded in the result document.
