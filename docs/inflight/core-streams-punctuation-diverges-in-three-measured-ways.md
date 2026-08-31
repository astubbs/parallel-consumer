# Punctuation on the PC dispatch path: warned, not refused, and three divergences remain

<!-- inflight-type: bug -->
<!-- inflight-impact: config-lie -->
<!-- inflight-labels: concurrency -->

Replaces `streams-stream-time-punctuation-is-unsupported-and-not-refused.md`, whose headline claim -
*"the punctuator registers successfully and never fires"* - stopped being true when stream time
started advancing on this path (astubbs#255, U13). The shape-gap reasoning that note carried (a
punctuator is a call on the processor context, so none of the three refusal layers can see it) now
lives in the module README under **What is still unsupported and NOT refused**, which is its durable
owner.

## What changed, so this note is not read as the old one

Stream time advances: `PcTaskDispatcher` publishes a low-water mark over work in flight, and the
patched `StreamTask` reads it from `maybePunctuateStreamTime`, `canPunctuateStreamTime` and
`streamTime()`. Both punctuation types now warn once per task at registration, which is the only
channel available - `PunctuatorWarningTest` is the gate on that, with a seam-off control.

**Refusing punctuation was considered and rejected**: both types fire and produce correct output for
the common shapes, so refusing would break topologies that run today.

**The `hasUncommittedWork() || commitNeeded` candidate was also rejected**, and this is the part most
likely to be re-proposed by someone reading only the handover. Its stated benefit did not survive
measurement - punctuator effects reach the broker independently of any commit, and `postCommit` runs
normally under load - so what it buys is a little less changelog replay after a crash landing in an
idle window, against a commit-cadence change for every PC-path caller and a `TaskMigratedException`
on a clean close after a punctuate-only interval. The three classes that measured it are
`PunctuatorEffectSurvivalTest`, `PostCommitCheckpointGapTest` and `PunctuatorCommitCoverageTest`;
read the last one's javadoc before picking an observable, because it names two that look decisive and
are not.

## Still open

- **`STREAM_TIME` punctuators re-fire over covered event time after a restart against a group THIS
  module committed.** Measured, with a seam-off control that fires at the restored mark:
  `StreamTimePunctuatorRefireOnRestartTest`. Cause is named at `seedStreamTime`'s call site in
  `pc-streams.patch` - a PC-written commit carries PC's frontier payload, not Streams'
  `TopicPartitionMetadata`, so `decode` yields UNKNOWN and there is nothing to seed from. The PC arm
  asserts the defective behaviour so the defect stays visible in a green suite, and its message says
  how to invert the assertion on fix. **Fix direction: populate PC's own opaque rider (KTD-S7). Do not
  reintroduce Streams' `TopicPartitionMetadata` as a second writer.** The pattern behind that ruling is
  written up on a branch this one does not carry - read it with
  `git show daa43d212:docs/solutions/architecture-patterns/one-owner-per-metadata-field-with-an-opaque-rider.md`.
  A mixed stock-to-PC handoff is a
  different path and is still untested.
- **Punctuate and process overlap, which stock never does**, and a plain `KeyValueStore` is supported
  here - so a punctuator that iterates a store races the processors writing it. Warned about, not
  prevented. **The open question is whether a punctuator should be *refused* when the topology also
  declares a state store.** A warning is right while the punctuator is the only thing that can reach
  the hazard; it stops being right if anyone finds a way to hit it without registering one.
- **`STREAM_TIME` punctuations can be SKIPPED, not merely delayed.** `PunctuationSchedule.next`
  collapses every interval crossed in one jump into a single firing, and on this path such jumps are
  the normal case rather than a data gap. Said in the warning; nothing measures how often.

## Two things the divergence measurements still owe

- **How far punctuation lags stock, split by cause.** `getDispatchesBehindStreamTime()` is the
  instrument and is read only by a unit test - the dispatch-order term and the not-yet-fetched term
  are not separated, and that split is what a decision about reinstating any windowed operator turns
  on.
- **A seam-ON run of Kafka's own `shouldReadCommittedStreamTime*` cases.** Those exercise
  `seedStreamTime`'s call site and `pcAwareStreamTime()`'s PC branch, and the module's upstream
  execution pins the seam OFF, so both are measured by hand rather than gated. It belongs with the
  seam-on evidence lane rather than here.
  <br>**And that run does NOT cover the second half of the seed - measured, not assumed.** Removing
  only the `pcRecordQueues` seed and re-running the same two cases seam-on leaves them green,
  case-for-case. So the line that makes a `UsePartitionTimeOnInvalidTimestamp` extractor recover after
  a restart rests on reading `RecordQueue.updateHead`'s signature, and Kafka's own suite has no case
  for that extractor to borrow. Anyone adding the seam-on lane should add one rather than assume the
  existing cases reach it.

## One claim left standing that this work did not verify

`PcUnsupportedConstruct.WINDOWED_AGGREGATION` and the `windowedBy` refusal strings in the patch argue
from `observedStreamTime`, which is the window aggregator's own field and not the task's stream time -
so U13 did not falsify them and they were left alone apart from dropping a "never advances" clause
that read as if it named the task's value. Whether `observedStreamTime`'s behaviour on this path is
correctly described is unchecked. The corruption clause beside it - a non-volatile long updated
read-modify-write from every worker - is the part that is established.

## Delete when

The re-fire gap is closed by KTD-S7's rider, or the store-race question is answered by a refusal
rather than a warning. The divergence measurements are the evidence lane's, not this note's, and can
be struck from here once that lane owns them.
