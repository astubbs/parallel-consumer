# The chaos suite cannot catch a duplicate that happens in a QUIET run

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->

**This is the only route left to settling astubbs#178** (mirror of confluentinc#843, "record being
picked up by multiple threads simultaneously"). That issue offers two ways forward and one of them is
now closed for good.

## Why the other route is dead

The issue is marked `wait for info`, waiting on richer logs from the reporter - partition and offset
at pickup - to establish whether the symptom was **one record dispatched twice** or **two upstream
productions of the same payload**. Those logs are not coming: the report is from 2024, on
`confluentinc/parallel-consumer`, which is no longer maintained and may be archived. Nobody is going
to answer.

So the choice is to leave the issue open forever on a condition that cannot be met, or to build the
instrument that answers it without the reporter.

## The structural gap

The Chaos Pain Suite asserts **bounded duplicates per disturbance**. That is the right assertion for
what it does - a rebalance or an induced fault is *allowed* to redeliver, and the suite's job is to
bound how much.

**A quiet run has no disturbances, so there is nothing to bound.** No retry, no rebalance, no
injected fault means the suite's central assertion is vacuously satisfied while a duplicate could
pass straight through it. The scenario this issue describes is exactly that shape: an ordinary
steady-state run, no disturbance, one record seen twice.

That is a blind spot in the suite rather than a bug in it, and it is invisible from inside: every
existing scenario is built around a disturbance, so nothing ever exercises the quiet case.

## What the scenario has to do

- **No disturbance at all** - no rebalance, no retry, no injected fault, no chaos conductor.
- **Assert ZERO duplicate deliveries**, not a bound. With nothing to justify a redelivery, one is a
  defect.
- **Control production, and record the offset dispatched to each thread.** This is the part that
  earns the scenario: it eliminates the "two upstream productions of the same payload" explanation by
  construction, which is the ambiguity the reporter's logs were wanted for. A scenario that produces
  each payload exactly once can distinguish what the field report could not.
- Run long and wide enough that the quiet path is genuinely exercised - the whole point is that the
  interesting window is not near a disturbance.

## What it can and cannot prove

**A green run is weak evidence and must be reported as a rate**, not a verdict: N runs, this
duration, this concurrency, zero duplicates. Absence over a sampled window does not establish
absence, and this repo has already written up what happens when a clean instrument is read as a clean
system.

**A red run is strong.** With production controlled and no disturbance, a second dispatch of the same
offset is a defect with nowhere else to hide - which is why this is worth building even though the
prior is that it stays green.

## What the prior actually is

astubbs#178's own fork-status analysis says a second concurrent submission is **not reachable by
reading the code**: selection runs on the control loop, completion on the control thread, and
`WorkContainer.inFlight` is written and read by that one thread only. So the expected outcome is
green.

Two reasons that does not make it pointless. The reachability argument is an invariant nothing
enforces - the same shape as the registration-order window in astubbs#370, which is closed today only
because two things happen to run on one thread. And a direct-pull engine moves selection onto worker
threads, at which point the argument stops holding and this scenario becomes the thing that notices.
