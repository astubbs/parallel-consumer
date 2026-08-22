# WorkManager as its own thread: workers ask, it replies, and nothing inside is thread-safe

<!-- inflight-type: feature -->
<!-- inflight-impact: architecture -->
<!-- inflight-labels: needs-measurement -->

**Antony's proposal, 2026-08-22.** Rather than N workers scanning the shards themselves, give
`WorkManager` its own thread. A worker **asks** for work; the manager **replies** with a work unit.
Because only one thread ever touches the state, **every collection inside becomes a plain
non-thread-safe one** - `HashMap` and `TreeMap` instead of `ConcurrentHashMap` and
`ConcurrentSkipListMap`.

## It was built in 2022, and the verdict that buried it is already retracted

`origin/refactor/gpt3-central-queue-direct-pull` - see
[`parked-2022-central-queue-rework.md`](parked-2022-central-queue-rework.md):

| Commit | What it is |
|---|---|
| **`4e78c4b68`** | **"Central distribution via actor msg, batch 100 from central"** - this proposal, verbatim |
| `58826c349` | "Central queue facade over Shards" - the selectable-shard queue |
| `5dcd39bb3` | `Map<Long, BlockingQueue<ProcessingShard>>` - per-worker shard queues |

The "1/3 as fast" figure attached to that family **does not measure it**: `QueuedShardManager.inner()`
was a busy-spin with the blocking wait commented out beneath it and a `todo` about an unresolved
race, so N idle workers burned N cores. Do not cite it.

The actor framework itself also exists and is small - **537 lines in 4 files, coupled to PC by one
16-line marker interface** ([`core-actor-revival.md`](core-actor-revival.md)), whose top-ranked next
step is a one-day falsification pipeline.

## What it actually buys, and what it does not

**It does not buy new safety, because `core` already has single-threaded ownership.** The control
loop is the only thread that scans `WorkManager` today. **Direct pull is what gave that property up**,
and this proposal restores it while keeping the pull shape. That is worth being clear about: the
today's-default engine is not unsafe, and this is not a fix for it.

**What it does buy:**

- **The concurrent collections go.** `TreeMap` beats `ConcurrentSkipListMap` on constant factor and
  on cache behaviour, and the coherence traffic from thousands of readers over constantly-mutated
  structures - a real cost in the direct-pull collapse, with no lock anywhere in it - disappears
  entirely.
- **The claim CAS goes.** One thread decides who gets a record, so there is no race to lose. The
  check-then-act defect fixed today in `2e8318504` **could not exist** in this design.
- **The buffer-sizing question goes, and this is the point.** With request/reply there is no executor
  queue to keep at the right depth, so `DynamicLoadFactor` and `isPoolQueueLow()` have nothing to do.
  That is the wart this whole line of thinking is aimed at.
- **It composes with the shard queue rather than competing.** Under single-threaded ownership the
  selectable-shard queue in [`next-selectable-shard-queue.md`](next-selectable-shard-queue.md) needs
  no atomics and no double-enqueue guard - it is just a `LinkedList` the owner maintains. **This
  proposal makes that one simpler, not redundant.**

## The two objections, and 2022's answer to the first

**1. Request/reply reintroduces the lag Antony objected to** - "threads waiting for work is wasted
time" - because a worker that asks has already finished and is idle until the reply lands.

**2022's answer is in the commit title: "batch 100 from central".** A worker asks for a *batch*, not
a record, so the round trip is amortised across 100 records and the worker holds a local buffer to
work through. The buffering question does not vanish so much as move: from "how deep should the
shared executor queue be" - a global quantity nobody can observe - to "how many should a worker hold
ahead", which is local, bounded, and observable by the worker itself.

**Under `KEY` ordering a batch can hold at most one record per key**, so batch size is bounded by the
number of selectable shards rather than chosen freely. Under `UNORDERED` it is free. That asymmetry
needs measuring before a batch size is picked.

**2. The manager thread becomes the ceiling.** Every record now passes through one thread. At 25,000
msg/s with batch 100 that is 250 requests/sec, which is nothing; at batch 1 it is 25,000/sec, which a
good queue handles but which makes that thread the throughput bound. **So batching is not an
optimisation here, it is what makes the design viable** - and that should be established before
anything else is built.

## How to settle it

Take the ideation's top pick: **falsify cheaply before betting**. Prototype the request/reply path
alone, no Kafka, in the shape of `bench/threads/ThreadCeiling.java` - N workers asking one owner
thread for batches of k, with plain collections - and find where it bends as k varies. That answers
both objections with one experiment and costs a day.

If it holds, the sequence is: manager thread first (it is what makes everything else simple), then
the shard queue as its internal structure, then delete the load factor.

See also: [`next-selectable-shard-queue.md`](next-selectable-shard-queue.md),
[`next-starvation-is-the-signal-not-queue-depth.md`](next-starvation-is-the-signal-not-queue-depth.md),
[`next-pre-rendered-work-order-list.md`](next-pre-rendered-work-order-list.md),
[`parked-2022-central-queue-rework.md`](parked-2022-central-queue-rework.md),
[`core-actor-revival.md`](core-actor-revival.md).
