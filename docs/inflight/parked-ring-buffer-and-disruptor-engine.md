# Parked: ring-buffer / Disruptor engine, and the per-thread-queue variants

<!-- inflight-type: parked -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement -->

Opened 2026-08-21, after profiling put the largest parking site in the engine on the mailbox
([`next-mailbox-contention.md`](next-mailbox-contention.md)) and the owner recalled having tried ring
buffers years ago. **They exist, they are already registered, and the register calls them dead ends.**

## The prior art, and where it is recorded

[`docs/refactoring.md`](../refactoring.md) already lists these under *"Perf: engine & queue
experiments -> mostly dead-ends; ideas for confluentinc#884"*, each pinned to a SHA:

| Branch | What it was |
|---|---|
| `origin/features/disrupter` @9473ab39 | **LMAX Disruptor engine experiment** - "START: Disrupter engine experiments" |
| `origin/direct-ringbuffer` @c247d89c | "Direct ring buffer approach for work publishing - avoid intermediate buffer which must be managed. A new Thread blocks putting work into the buffer, which feeds directly into the ExecutorService." (2020-12-02) |
| `origin/ringbuffer-batch` @ee942830 | ring-buffer engine, batched |
| `origin/refactor/worker-queues` @a616de9e | **worker-queue rework** - the per-thread-queue idea |
| `origin/refactor/double-ended-queue` @58a2b997 | block on submission rather than on results (backpressure) |
| `origin/refactor/gpt3-central-queue-direct-pull` @7e775a11 | central queue, direct pull - *noted: poller-throttling issue, didn't help* |

**So every structural idea raised in this session has been attempted before**: the ring buffer, the
Disruptor, per-thread worker queues, and a central queue with direct pull. That is worth knowing before
anyone starts, and it is the reason this note exists rather than a fresh design.

## What the register does not say

`docs/refactoring.md` calls them "mostly dead-ends" **without recording why**, which is the gap. A
dead end because it was slower, because it broke a guarantee, because it was abandoned mid-way, or
because the author ran out of evening are four very different pieces of information, and only the first
two should stop a retry.

**The open branch-and-issue accounting PR is astubbs#327**, *"assess ten untriaged upstream mirrors,
correct the records that were wrong, and give the fork's branches one accounting"*. **These branches
deserve the same treatment there**: each one read, and its outcome recorded as a reason rather than a
verdict.

## What this session adds to the question

**Measurements the 2020 experiments did not have**, and they lower the expected payoff considerably:

- **The mailbox lock is real but cheap.** Replacing it with a lock-free queue: **+3.3% at a 100ms
  handler, -2.7% at 0ms**, both outside run-to-run spread. A trade, not a win.
- **Three other lock removals returned nothing or hurt** - the shard scan twice, and the worker pool's
  own queue at **-69%**. See [`perf-hypothesis-register.md`](perf-hypothesis-register.md).
- **The ceiling is not ours.** In-flight plateaus near 2,750 for PC and 2,848 for a bare
  `KafkaConsumer` with a thread pool. Below that ceiling PC's internals have slack.

**So a ring buffer would be replacing a structure measured to cost about 3%**, in an engine whose
throughput is set by something outside it. That is the opposite of the situation in 2020, when none of
this was known.

## The variant that has not been tried

**A mailbox sharded per producer thread** - the owner's suggestion, and the only idea here without a
branch. Each worker writes to its own single-producer queue; the control loop drains all of them. **No
write contention at all**, because no two producers share a structure - the same trick `LongAdder` uses
for counters, applied to a queue.

It is more attractive than a ring buffer for this specific shape, because it needs no dependency and no
bounded-capacity policy. **The cost is a drain that visits every shard**, which is the thing this
session has repeatedly found does not matter at these depths - and a per-thread structure keyed by a
pool thread that can die and be replaced.

**Still bounded by the same 3%.** Worth doing only alongside a redesign that is happening anyway.

## If it is restarted

1. **Read the six branches first** and record why each stopped. That is the missing input.
2. **Do not change a structure and its `size()` behaviour together.** That confound made one experiment
   here uninterpretable and cost a full round of measurement.
3. **Measure at 0ms, 2ms and 100ms with repeats.** Three of this session's wrong conclusions came from
   single runs at a point with 21% spread.
