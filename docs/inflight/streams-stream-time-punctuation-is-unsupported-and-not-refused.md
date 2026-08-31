# Stream-time punctuation is unsupported on the PC dispatch path, and the refusal envelope does not catch it

<!-- inflight-type: bug -->
<!-- inflight-impact: config-lie -->

A topology that calls `context.schedule(interval, PunctuationType.STREAM_TIME, callback)` registers
the punctuator successfully and it never fires under PC dispatch. No exception, no warning, nothing
in the log - just periodic output that does not arrive. `WALL_CLOCK_TIME` punctuation is unaffected.

**Mechanism.** Stock Kafka Streams advances stream time inside `PartitionGroup.nextRecord()`. The PC
dispatch path does not go through it - records go from the task's register call straight to a worker
- so the clock the punctuator is scheduled against never moves.

**Why the refusal envelope does not cover it, and this is a shape gap rather than an oversight.** All
three refusal layers key on something structural: a DSL method call, a state store type reaching the
task, or a config key. A punctuator is none of those. It is a call on the processor context, made
from inside `init()` while the task is already running, long after the envelope check in
`StreamTask`'s constructor has passed. Refusing it needs a different hook - either intercepting
`schedule()` on the patched processor context, or walking the topology for processors that register
one, which is not something the topology graph exposes.

It is therefore the one item of the module's original unsupported list that is documented rather than
refused, and the module README says so in its own words under "What is still unsupported and NOT
refused". Anyone reading the refusal envelope and concluding the surface is now fully guarded is
wrong by exactly this one construct.

**Where it was raised.** As an open review thread on astubbs/parallel-consumer#271, the Kafka Streams
feasibility study, alongside the other correctness threads that unit work has since taken over. That
thread predicted the failure mode ("a common, non-windowed pattern ... silently never fires") before
any of the refusal work existed, and it survived the refusal work intact.

**Related but not the same thing:** the punctuators that *do* fire, on wall-clock time, set
`commitNeeded` and the PC commit path discards it - so a punctuate-only interval does no flush and no
checkpoint. That is a commit-cadence question owned by the task-lifecycle work, not a refusal
question, and closing this note does not close that.
