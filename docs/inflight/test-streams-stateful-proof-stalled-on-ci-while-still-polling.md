# The stateful proof stalled once on CI - and it was polling briskly while it did

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->

`PcDrivenStatefulProofTest.pcDrivenAggregationMatchesTheStockBaseline` timed out on repetition 2 of 3
in the `Integration Tests` lane of astubbs/parallel-consumer#398, the run that first put that class on
CI. Repetitions 1 and 3 passed in the same JVM. **One sighting, not a rate**, and it is written down
because the alternative was to re-run and learn nothing.

**Do not loosen the drain timeout, shorten the repetitions, or serialise the class to make this go
away.** What the evidence rules out is exactly the reading that would justify any of those.

## What the run actually said

The StreamThread's own progress line, logged when the drain gave up:

```
Processed 14 total records, ran 0 punctuators, polled 1207 times and
committed 0 total tasks since the last update 120093ms ago
```

**1207 polls and 14 completions in two minutes is not a starved topology.** A thread short of CPU
polls *less*; this one was going round its loop briskly and finishing almost nothing. That is the
shape of work that was dispatched and did not complete, not of a machine that never got to it - and it
is why "the runner was busy" is not an available explanation, however busy the runner was.

The ambient probe reported clean: no rebalance dwell, no lag stagnation, no frozen partitions. So the
consumer group was making progress at the broker while the task was not making progress at the
processor.

Two more lines from the same job log, **both unattributed on purpose**: an
`IllegalStateException: Blocker interrupted while parked` out of `PcTaskDispatcher`'s dispatch lambda
with an `InterruptedException` cause, and Kafka's `ProcessorNode` reporting that the processing
exception handler is set to fail. The lane runs four surefire forks into one interleaved log and
another class (`CommitFrontierCrashRestartTest`, which interrupts workers by design) was running in
the same window, so neither line can be pinned to this fork from the log alone. They are recorded
because if they *are* this fork's, the rest follows immediately: retries are disabled on the PC path,
so a record that fails blocks its KEY shard permanently, and a permanently blocked shard is a
topology that polls forever and completes nothing.

## The control arm, and what it could not vary

| Arm | Shape | Result |
|---|---|---|
| Uncontended, local | `forkCount=1`, three separate runs | green every time |
| **Contended, local** | `forkCount=4 -DreuseForks=true`, whole streams integration suite plus core's, matching the lane's own settings | **green, whole suite** |
| CI | same fork settings, GitHub-hosted 2-core runner | one repetition of three timed out |

The instrumentation is confirmed to have reached the contended arm rather than assumed: the failsafe
plugin pins no `<forkCount>` of its own - unlike surefire, where the root pom's
`<forkCount>${surefire.forkCount}</forkCount>` makes a bare `-DforkCount` a silent no-op - so the
property arrives, and the autopsy dump from the CI failure shows the lane running with the same two
values.

**So the contended arm did not reproduce it, and that does not settle it.** The one term the local arm
could not vary is the core count: four JVMs and four brokers on a developer machine is not four JVMs
and four brokers on two cores. Calling this contention would be asserting the term that was never
tested.

## The experiment that would settle it

Vary core count and nothing else. Either run the streams integration lane in a two-core cgroup locally
at `forkCount=4`, or run it on a CI runner at `forkCount=1` - the second is cheaper and is the better
arm, because it holds the machine fixed and changes only the concurrency. If it goes green at
`forkCount=1` on the same runner, the stall needs that load to appear and the question becomes which
of the two unattributed lines above belongs to this fork. If it still stalls, the load is a red
herring and this is a defect in completion on the PC path, which is the error-surfacing rung's ground.

Before either, add a second sighting cheaply: the lane re-runs on every push to that branch, so the
rate is being collected whether or not anybody asks for it.

## Delete when

The core-count arm has been run and the outcome recorded - or the stall is diagnosed and owned by the
rung that fixes it.
