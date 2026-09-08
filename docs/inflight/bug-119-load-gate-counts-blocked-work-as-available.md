# The record-intake gate counts work it cannot advance, and an instance that fills with permanent failures stops fetching

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->
<!-- inflight-labels: concurrency -->
<!-- inflight-state: open - the accounting gap is measured and pinned; whether it is worth changing needs the design decision below -->

**The filename carries astubbs#119, the fork mirror of confluentinc#857**, per this directory's rule
that a note's number is always the fork's. `bug-857-family.md` carries an upstream number because it
predates the rule; do not read the two prefixes as naming different things.

This note is about the **intake stall**, which is not the report the soak that found it was hunting.
[`bug-177-commit-response-timeout-unreproduced.md`](bug-177-commit-response-timeout-unreproduced.md)
**owns astubbs#175's question** - the commit-response timeout, the candidate list and the
discriminator - and it records the two runs that turned this up as a by-product. What is here is the
mechanism those runs left named-but-untested, and the design question it opens.

## The accounting gap, which is measured rather than argued

`WorkManager#isSufficientlyLoaded()` is `inShards - parkedForRetry > targetAmountOfRecordsInFlight *
loadingFactor`, and it gates the broker poller's pause and resume. Its own javadoc states why the
subtraction is there: records in retry back-off "occupy the buffer but no amount of worker capacity
can advance them".

**A record queued behind a failing shard head under `KEY` ordering meets that description exactly,
and is counted anyway.** `ProcessingShard#getWorkIfAvailable` breaks out of an ordered shard after
its head, so nothing behind the head is selectable however much worker capacity exists; only the head
is ever in the retry queue, so only the head is ever subtracted.

`WorkManagerTest#theLoadGateCountsRecordsQueuedBehindABlockedKeyHeadAsWorkable` pins it: three
records of one key with a failing head, and the gate reads **two workable while nothing at all is
selectable**, then **three while one is**. The over-read is the whole queue behind the blocked head,
and it grows with that queue.

## What the gap is NOT, which is the part that decides the fix

A permanently failing head is **itself workable** - it is retried forever, it holds a worker for the
length of the user function every time, and it counts on its own account. So a buffer that has filled
with poisoned heads latches the gate whether or not anything is queued behind them, and closing the
head-of-line gap alone would not necessarily change what an instance does. Only a soak arm separates
those two, and the arms are recorded in `CommitResponseTimeoutSoakIT`'s `Calibration status` block
alongside their predictions.

## The design decision this opens, and why no small fix is available

"Count only what is selectable" is the obvious repair and it is wrong. Under `KEY` or `PARTITION`
ordering at most one record per shard is selectable at any instant, and a shard whose head is out at
a worker has **none** - so a healthy, fully loaded instance would read close to zero, call itself
under-loaded, and fetch without bound. The gate is not measuring "what can I run right now"; it is
measuring "have I enough queued that a worker finishing will find something", and a record behind a
*busy* head satisfies that while a record behind a *permanently failing* head does not.

The distinguishing property is therefore **liveness of the shard head, not a count** - and "this head
will never succeed" is not decidable from the shard's state. That is what makes this a decision
rather than a tidy-up. The directions, in the order they look worth taking:

1. **Bound the failures instead of the buffer.** A record that has failed *n* times is diverted or
   skipped, so the buffer cannot fill permanently with records that will never leave it. This is
   already the project's stated direction - `docs/data/roadmap.yaml`'s `dead-letter-queue` entry says
   in as many words that "retrying forever is the only built-in answer today, and it is the wrong one
   for a poison record" (astubbs#149, confluentinc#310). If that lands, this gap stops being reachable
   in the shape that produced it.
2. **Make the latch loud rather than silent.** The state is already exported - the
   `NUM_PAUSED_PARTITIONS` gauge - and says nothing in the log. A gate that has read `true` across
   many consecutive ticks while nothing retired is a report an operator can act on, and it changes no
   semantics. Cheap, safe, and it does not need this decision settled.
3. **Exclude a shard whose head has failed more than *n* times from the gate's figure.** It closes
   the gap without unbounding the fetch, at the price of a heuristic constant and an O(shards) read
   the gate does not currently take.

## The same shape elsewhere - swept, with what was ruled out

`isRecordsAwaitingProcessing()` reads `getNumberOfWorkQueuedInShardsAwaitingSelection()`, the sum of
the per-shard selection-claim counters netted against the retry queue. A record queued behind a
blocked head holds its claim, so that figure counts it too - and `drain()` gates the transition to
closing on it. A `KEY`-ordered instance holding one permanently failing record therefore has work
"awaiting processing" for as long as it holds the record, which is the same over-read in a second
consumer. **Measured, not reasoned**: the last assertion of
`WorkManagerTest#theLoadGateCountsRecordsQueuedBehindABlockedKeyHeadAsWorkable` is
`isRecordsAwaitingProcessing()` reading true with nothing selectable. The consequence differs (a close
that waits out its drain timeout, rather than a poller that stays paused), which is why it is recorded
here rather than folded into the gate question.

Ruled out, checked and not affected: `isWorkInFlightMeetingTarget()` and `hasWorkInFlight()` read
`numberRecordsOutForProcessing`, which counts records actually dispatched to a worker and cannot
include a queued one; `checkPipelinePressure()` reads the executor's own queue depth rather than any
shard figure; `getNumberOfRecordsInShards()`'s other callers are diagnostics and tests.

## Related

- [`bug-177-commit-response-timeout-unreproduced.md`](bug-177-commit-response-timeout-unreproduced.md) - owns astubbs#175, and the soak whose runs found this
- [`bug-857-family.md`](bug-857-family.md) - the register of what sits behind upstream's one "paused consumption" symptom; astubbs#336 fixed the gate's *drift*, which is a different defect in the same expression
