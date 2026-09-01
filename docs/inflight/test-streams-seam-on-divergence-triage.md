# Seam-on divergences in Kafka's own suite: what each one is, and who owns it

<!-- inflight-type: register -->
<!-- inflight-impact: blind-spot -->

For `parallel-consumer-streams` (astubbs#255). **This is the machine-readable half of the seam-on
evidence lane**: `bin/ci-streams-seam-on-evidence.sh` runs Apache Kafka's own suite against the
patched classes twice - seam off, then seam on - and classifies every case that passes in the first
and fails in the second. Three of its classes are derived from the code at run time and need nothing
from here. The rest are attributions that no amount of reading a stack trace can produce, and this
note is where they live.

**Why the attributions are here and not in the lane's own source.** An attribution is a judgement
about *why* a case diverges, it is reviewed as prose, and it stops being true when somebody changes
the semantics under it. Putting it in the classifier would bury it where nobody edits it and hide it
from the reviewer who could tell it was wrong. Putting it here means the sibling rung that closes one
of these mechanisms edits the ledger entry in the same PR that closes it.

**Re-derive, never quote.** Which cases diverge, and how many, move with the branch, the Kafka
version and the seam - so no count is written down anywhere in this note or in the lane. The command
is:


```bash
JAVA_HOME=<a JDK 17> bin/ci-streams-seam-on-evidence.sh
```

and the whole measurement lands in `parallel-consumer-streams/target/seam-on-evidence-report.txt`.
<!-- file-refs: N/A - that report is written by the run, under target/, so it exists only after the lane has been run -->

## What the lane derives without help

- **`refused-construct`** - read out of `PcUnsupportedConstruct` at run time: the marker is the
  common prefix of every message that enum can produce, and the attribution is whichever constant the
  message names. A construct added to or removed from the refusal envelope changes what the lane
  recognises with no edit anywhere.
- **`commit-frontier-encoding`** - the case renders an `OffsetAndMetadata` in its assertion, so it is
  asserting the offset and commit-metadata encoding this module deliberately diverges from: PC's
  frontier is not Streams' `consumedOffsets`. **Its limitation is stated rather than hidden** - an
  offset *regression* inside these unit tests would land in this class too. What actually defends
  offset correctness is the broker-backed commit-frontier law, not this lane.
- **`ledgered-flake`** - matched from a `flaky-case:` marker in any note in this directory. It is what
  lets a flaky control arm be named instead of re-run.

## The attributed classes

Each is defined by a `seam-on-divergence-class:` marker below and applied by
`seam-on-divergence:` markers, keyed by method - never by parameterisation, because which parameter
loses a race is not a property of the diagnosis.

<!-- seam-on-divergence-class: asynchronous-dispatch = Kafka's test drives the task synchronously - addRecords, then process(), then assert - and asserts state that only a record processed BEFORE process() returns can produce. Under PC dispatch the record is registered with the WorkManager and run on a worker, so within the test's synchronous window the partition group is empty, the processor has not run, and whatever it would have thrown has not been thrown yet. -->
<!-- seam-on-divergence-class: split-poll-wait = wake-on-work split the poll wait into a short poll plus a wait on our own condition, which changes what one iteration of the main loop is and what timeout the consumer is polled with. -->
<!-- seam-on-divergence-class: stream-time-lags-in-flight-work = stream time on the PC path is a low-water mark that deliberately never passes a record still in flight, so a punctuation cannot close a window over work still inside the chain. Kafka's test adds records, calls process() once and asserts the punctuation stock produces because stock finished processing inside that call. The mark is therefore behind by the records the workers have not finished, and the punctuation is late rather than lost. -->
<!-- seam-on-divergence-class: processing-threads-mode = Kafka's private processing-threads config, where DefaultTaskExecutor calls task.process from its own thread rather than the StreamThread. Named as out of scope in PcTaskDispatcher's threading contract since the seam landed, and unreachable by default. Only the parameterisations that enable it diverge. -->

**`asynchronous-dispatch`** is the largest and the least interesting: it is the seam working. These
cases are not evidence of a defect and they are not going to be fixed - Kafka's unit tests assume
synchronous processing because stock Kafka Streams is synchronous, and the whole point of this module
is that it is not. The broker-backed arms in this module are where the same properties get asserted
in a way that survives asynchrony. One sub-shape is worth naming because it looks different:
`shouldThrowExceptionOnCloseCleanError` gets a `TaskMigratedException` where it expected a
`ProcessorStateException`, which is the task-lifecycle rung's `validateClean` correctly noticing work
still in flight.

## Two classes were retired at the reconciliation, and the retirement was a MEASUREMENT

`stream-time-never-advances` and `exception-type-lost-in-the-worker` were owned by the stream-time
rung (astubbs/parallel-consumer#396) and the error-surfacing rung
(astubbs/parallel-consumer#395). Both are gone from the list above, and what replaced them was
established rather than assumed.

**The method.** The entries were deleted FIRST and the lane re-run, so a mechanism that had not
actually been closed would match nothing and be reported as unexplained instead of quietly keeping
its old label. Then, because a failure seen on one branch says nothing about that branch, a
**control arm** was run: the same seam-on suite at astubbs/parallel-consumer#396's own tip, in a
detached worktree - which carries the stream-time work and NOT the error-surfacing work, so the one
term that differs from the reconciled tree is the reconciliation itself.

| Case | Control arm (the sibling's own tip) | Reconciled tree | Reading |
|---|---|---|---|
| `StreamThreadTest#shouldReinitializeRevivedTasksInAnyState` | fails on **all three** parameters | passes on `[1]` and `[2]`, fails on `[3]` | the error-surfacing fix is present and did exactly what its rung claimed; `[3]` is the processing-threads mode |
| `StreamTaskTest#shouldRecordBufferedRecords` | fails, `2.0` against `0.0` | **passes** | the backpressure work closed it - so its entry above is deleted, on this evidence rather than on one stale-report line |
| `StreamThreadTest#shouldPunctuateActiveTask` | passes | passes | closed on the stream-time rung already |
| `StreamThreadTest#shouldPunctuateWithTimestampPreservedInProcessorContext` | passes | passes | same |
| `StreamTaskTest#shouldPunctuateOnceStreamTimeAfterGap` | fails, `7` against **`0`** | fails, `7` against **`6`** | the clock is no longer stuck; the mark is one record behind |
| `StreamTaskTest#shouldRespectPunctuateCancellationStreamTime` | fails, `false` against `true` | fails, `true` against `false` | the failing assertion **changed sides** - a different line of the test, so a different behaviour |

**Nothing was lost by the merge**: every case is the same or better than at the sibling's own tip.
The two that still diverge are a NARROWED residue with a different mechanism, which is why they are
re-attributed to `stream-time-lags-in-flight-work` above rather than left under a name that is no
longer true. Reading `0` punctuations as "the clock never moves" and `6 of 7` as the same thing
would have hidden the whole of the stream-time rung's effect.

**`stream-time-lags-in-flight-work` is the seam's own guarantee, seen from Kafka's suite.** It is
not a defect to fix, and the design would be worse without it - a punctuation that closed a window
over records still inside the processor chain is exactly what the low-water mark exists to prevent.
What it IS is a user-visible divergence in the direction the module's own documentation did not
previously state: astubbs/parallel-consumer#396 pinned the mark *overtaking* stock as a known
divergence, and this is the mark *lagging* it. That gap is what
[`core-streams-punctuation-diverges-in-three-measured-ways.md`](core-streams-punctuation-diverges-in-three-measured-ways.md)
now owns - that note already listed the lag as a measurement it owed, and this run supplied it - and
it is the named trigger the dispatch default is waiting on.

**`split-poll-wait`** has never had a seam-on measurement of its own. Both entries are consistent
with the mechanism - one asserts a main-loop iteration count, the other asserts the duration the
consumer was polled with - but neither has been run against a control arm with wake-on-work ablated,
which is what would settle it. That is the strongest open item in this note.

<!-- seam-on-divergence: org.apache.kafka.streams.processor.internals.StreamTaskTest#shouldProcessInOrder = asynchronous-dispatch -->
<!-- seam-on-divergence: org.apache.kafka.streams.processor.internals.StreamTaskTest#shouldBeProcessableIfAllPartitionsBuffered = asynchronous-dispatch -->
<!-- seam-on-divergence: org.apache.kafka.streams.processor.internals.StreamTaskTest#shouldPauseAndResumeBasedOnBufferedRecords = asynchronous-dispatch -->
<!-- seam-on-divergence: org.apache.kafka.streams.processor.internals.StreamTaskTest#shouldRecordE2ELatencyOnSourceNodeAndTerminalNodes = asynchronous-dispatch -->
<!-- seam-on-divergence: org.apache.kafka.streams.processor.internals.StreamTaskTest#shouldProcessRecordsAfterPrepareCommitWhenEosDisabled = asynchronous-dispatch -->
<!-- seam-on-divergence: org.apache.kafka.streams.processor.internals.StreamTaskTest#shouldResumePartitionWhenSkippingOverRecordsWithInvalidTs = asynchronous-dispatch -->
<!-- seam-on-divergence: org.apache.kafka.streams.processor.internals.StreamTaskTest#shouldRespectCommitNeeded = asynchronous-dispatch -->
<!-- seam-on-divergence: org.apache.kafka.streams.processor.internals.StreamTaskTest#shouldMaybeReturnOffsetsForRepartitionTopicsForPurging = asynchronous-dispatch -->
<!-- seam-on-divergence: org.apache.kafka.streams.processor.internals.StreamTaskTest#shouldWrapKafkaExceptionWithStreamsExceptionWhenProcess = asynchronous-dispatch -->
<!-- seam-on-divergence: org.apache.kafka.streams.processor.internals.StreamTaskTest#shouldThrowOnTimeoutExceptionAndBufferRecordForRetryIfEosDisabled = asynchronous-dispatch -->
<!-- seam-on-divergence: org.apache.kafka.streams.processor.internals.StreamTaskTest#shouldThrowExceptionOnCloseCleanError = asynchronous-dispatch -->
<!-- seam-on-divergence: org.apache.kafka.streams.processor.internals.StreamThreadTest#shouldLogAndRecordSkippedMetricForDeserializationException = asynchronous-dispatch -->
<!-- seam-on-divergence: org.apache.kafka.streams.processor.internals.StreamThreadTest#shouldLogAndRecordSkippedRecordsForInvalidTimestamps = asynchronous-dispatch -->



<!-- seam-on-divergence: org.apache.kafka.streams.processor.internals.StreamThreadTest#shouldRespectNumIterationsInMainLoopWithoutProcessingThreads = split-poll-wait -->
<!-- seam-on-divergence: org.apache.kafka.streams.processor.internals.StreamThreadTest#shouldRespectPollTimeInPartitionsAssignedStateWithStateUpdater = split-poll-wait -->

<!-- seam-on-divergence: org.apache.kafka.streams.processor.internals.StreamTaskTest#shouldPunctuateOnceStreamTimeAfterGap = stream-time-lags-in-flight-work -->
<!-- seam-on-divergence: org.apache.kafka.streams.processor.internals.StreamTaskTest#shouldRespectPunctuateCancellationStreamTime = stream-time-lags-in-flight-work -->

<!-- seam-on-divergence: org.apache.kafka.streams.processor.internals.StreamThreadTest#shouldReinitializeRevivedTasksInAnyState = processing-threads-mode -->

## The finding this triage produced, and it contradicts an inherited one

`StreamThreadTest.shouldLogAndRecordSkippedRecordsForInvalidTimestamps` is the case
`test-streamthreadtest-invalid-timestamps-flake.md` diagnoses, and an earlier triage on the branch
forest counted its seam-on failure as that flake firing. **It is attributed here to
`asynchronous-dispatch` instead**, on evidence the earlier triage did not have: its sibling
`shouldLogAndRecordSkippedMetricForDeserializationException` fails seam-on in exactly the same
parameterisation with exactly the same assertion shape, and only in that parameterisation, in every
seam-on run taken here - while the seam-off control arm was clean in the same runs. A coin-flip flake
does not land on two sibling tests at the same parameter in consecutive runs. The flake is real and
its own note stands; what is being corrected is attributing *this* seam-on failure to it, which would
have hidden a systematic divergence behind a known-flaky name.

That is also why the lane consults the ledgered-flake registry **last** among the divergence classes
and **first** on the control arm. A flake explains a dirty control; letting it explain a divergence is
how a real one disappears.

## An entry reported stale on one run is not necessarily stale

The lane reports a `seam-on-divergence` entry that matched nothing, which is usually a rung having
fixed the thing. There is one benign way to get the same report: **a case that fails in the CONTROL
arm falls out of the divergence set entirely**, because a divergence is green-off-and-red-on and that
case was not green off. `shouldLogAndRecordSkippedRecordsForInvalidTimestamps` does exactly this
whenever its ledgered flake fires seam-off - it moves to "failing in both arms", and its attribution
here goes quiet for that run without having stopped being true.

So check which list the case is in before deleting an entry the lane called stale. Two runs with the
case green in the control and still not diverging is the signal to act on; one run with a dirty
control is not.

## Delete when

Every class above has either been closed by the rung that owns it or moved to a durable home. The
`asynchronous-dispatch` entries will outlive the others: they describe the seam working as designed,
so they leave this note only if somebody decides Kafka's synchronous unit tests should be excluded
from the seam-on arm outright, which is a decision nobody has taken.
