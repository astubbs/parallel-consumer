# The dispatch seam's named trigger is closed - the flip is a reconciliation decision, not this rung's

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

`parallel-consumer-streams` (astubbs#255). **The seam still defaults OFF, and this note exists so
nobody reads that as an open defect.**

## What changed

The third and last named reason for the default being off - a typed control-flow exception raised
inside a processor never reaching Kafka's recovery - is closed, with both halves fixed (the type and
the delivery) and a commit fence added so nothing commits past a failure while it is in flight. The
knowledge is in
[`docs/solutions/architecture-patterns/an-async-seam-owes-a-control-flow-exception-both-its-type-and-its-timing.md`](../solutions/architecture-patterns/an-async-seam-owes-a-control-flow-exception-both-its-type-and-its-timing.md).

Measured with a seam-on run of Kafka's own suite before and after, on one machine, nothing else
changed: `StreamThreadTest.shouldReinitializeRevivedTasksInAnyState` goes green on both parameter
combinations this module supports, `StreamTaskTest.shouldRecordBufferedRecords` goes green with the
backpressure work, **nothing regresses**, and the seam-off oracle is byte-identical.

## Why the default did not move with it

Three things, and only the first is about this rung:

- **Its third parameter combination stays red, for a reason that is not this defect.** That
  combination is Kafka's private processing-threads mode, where `DefaultTaskExecutor` calls
  `task.process` from its own thread - named as out of scope in `PcTaskDispatcher`'s threading
  contract since the seam landed, and unreachable by default.
- **Stream-time punctuation is still unsupported and not refused**, tracked separately in
  [`streams-stream-time-punctuation-is-unsupported-and-not-refused.md`](streams-stream-time-punctuation-is-unsupported-and-not-refused.md)
  and owned by the stream-time rung rather than this one. It was already priced in when the refusal
  reason was closed, and it is a different item, not a rediscovery of this one.
- **The measurement that decides a flip has to be taken on the reconciled branch.** Three sibling
  rungs of astubbs#255 are in flight against the same base, and each moves the seam-on numbers. A
  flip argued from any one of them is arguing from a state that will not exist by the time it merges.

## What to do, and when

At consolidation, once the rungs are reconciled: re-run the seam-on measurement on the merged tree,
read the per-class numbers out of `target/surefire-reports-kafka-upstream/` (deleting the report
directories first), and decide. Expect the pattern this decision has followed three times already -
the measurement names the next reason - so treat "no reason left" as a finding to be shown, not
assumed.

`PcDispatchSwitch`'s javadoc, the module README and the pom's module description all state the
current position; they are the three sites that move together when it changes.

## Delete when

The default has been re-decided against a fresh seam-on measurement on the reconciled branch, whether
or not it moves.
