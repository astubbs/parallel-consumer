# A poison record stops the whole instance fetching, and the record-intake gate is what stops it

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->
<!-- inflight-labels: concurrency -->
<!-- inflight-state: open - measured and settled; what remains is a product decision, not a diagnosis -->

**The filename carries astubbs#119, the fork mirror of confluentinc#857**, per this directory's rule
that a note's number is always the fork's. `bug-857-family.md` carries an upstream number because it
predates the rule; do not read the two prefixes as naming different things.

This note is about the **intake stall**, which is not the report the soak that found it was hunting.
[`bug-177-commit-response-timeout-unreproduced.md`](bug-177-commit-response-timeout-unreproduced.md)
**owns astubbs#175's question** - the commit-response timeout, the candidate list and the
discriminator. What is here is the mechanism its first runs left named-but-untested, now settled, and
the product decision that is all that remains.

## What an operator sees

A single instance, `KEY` ordering, records that throw on every attempt, no rebalance and no
misconfiguration. Within the first second the instance stops fetching from the broker **permanently**
- every partition paused - while its workers stay busy retrying the records it already holds. From
outside it looks alive and loaded; nothing is logged, nothing is thrown, throughput is zero, and only
`pc_num_paused_partitions` says so. confluentinc#833's reporter showed
`pc_processed_records_total` flat across the window their timeout fired in, which is this state.

## Settled: the gate is the mechanism, and head-of-line blocking is not why

Three six-minute arms on 2026-09-08, each differing from the first by exactly one term, with
`WorkManager` at DEBUG and the logging config that carried it verified in the log rather than assumed.
Full conditions and numbers are in `CommitResponseTimeoutSoakIT`'s `Calibration status` block, which
owns them; what matters here is what each settled.

- **`KEY`, the experimental arm.** Reproduced astubbs#471's thirty-minute result in six minutes and at
  the same success count. The gate read `true` on all but the first four of ~37k evaluations, every
  partition paused throughout, and it latched **on the first fetch, 0.8s in** - `inShards=500` against
  a threshold of 28 - and never unlatched.
- **`UNORDERED`, the control on ordering.** No shard head can block anything behind it, so every held
  record is genuinely selectable. **It stalled the same way.**
- **`messageBufferSize=20000`, the control on the gate itself** - the threshold moves from 42 to
  20,006 and nothing else moves. **The outcome flips**: the gate never read `true`, zero partitions
  were paused at any sample, records kept arriving, and successes roughly doubled.

**So the gate is what stops intake** (arm 3 is the positive control), **and the stated mechanism is
wrong** (arm 2). Arm 1's own arithmetic agrees more directly: the records it held came from a single
burst over 1000 distinct keys, so there was **at most one record per key and nothing queued behind
any blocked head at all**. What latches the gate is records that are perfectly workable - they are
retried continuously, saturating the workers - and that never retire.

## What is still true about the accounting gap, and what it is not

The gate is `inShards - parkedForRetry > targetAmountOfRecordsInFlight * loadingFactor`, and its own
javadoc says why the subtraction is there: retry-parked records "occupy the buffer but no amount of
worker capacity can advance them". A record queued behind a failing `KEY` head meets that description
exactly - `ProcessingShard#getWorkIfAvailable` breaks out of an ordered shard after its head - and is
counted anyway. `WorkManagerTest#theLoadGateCountsRecordsQueuedBehindABlockedKeyHeadAsWorkable` pins
it: three records of one key with a failing head, and the gate reads two workable while nothing is
selectable, then three while one is.

**That gap is real and it is not this stall.** It makes a stall arrive sooner where records DO queue
behind blocked heads; it did not participate in the runs above. Do not cite it as the cause.

## The decision: no gate change fixes this

Two bounds are in play and only one of them is the gate.

1. **The intake bound.** The buffer fills with records that never retire, so the poller pauses for
   good. Arm 3 lifts it.
2. **The worker bound.** The same records re-offer themselves every retry delay and consume the entire
   worker budget. Arm 3 does **not** lift it: successes doubled, then plateaued by minute three while
   the held population climbed linearly with the producer.

So raising the threshold converts a hard stall into an **unbounded-memory slow starve**, and any
"count only what is selectable" rule is worse still - under `KEY` or `PARTITION` ordering at most one
record per shard is ever selectable, and a shard whose head is out at a worker has none, so a healthy
fully-loaded instance would call itself under-loaded and fetch without bound. The gate is not
measuring "what can I run now"; it is measuring "have I enough queued that a finishing worker finds
something", and a record behind a *busy* head satisfies that while a record behind a *permanently
failing* one does not. The distinguishing property is **liveness of the head, not a count**, and
"this will never succeed" is not decidable from the shard's state.

**The fix therefore has to bound the FAILURES, not the buffer** - which is the project's stated
direction already: `docs/data/roadmap.yaml`'s `dead-letter-queue` entry says in as many words that
"retrying forever is the only built-in answer today, and it is the wrong one for a poison record"
(astubbs#149, confluentinc#310). Until that lands:

- **Make the latch loud.** The state is exported as the `NUM_PAUSED_PARTITIONS` gauge and says nothing
  in the log. A gate that has read `true` across many consecutive ticks while nothing retired is a
  report an operator can act on, it changes no semantics, and it does not need this decision settled.
  This is the cheapest available improvement and the one worth taking first.
- **Do not** raise the default buffer, add a "selectable" count, or special-case the ordered shard
  head. The arms above show what each of those buys.

## The same shape elsewhere - swept, with what was ruled out

`isRecordsAwaitingProcessing()` reads `getNumberOfWorkQueuedInShardsAwaitingSelection()`, the sum of
the per-shard selection-claim counters netted against the retry queue. A record queued behind a
blocked head still holds its claim, so that figure counts it too - and `drain()` gates the transition
to closing on it. **Measured, not reasoned**: the last assertion of
`WorkManagerTest#theLoadGateCountsRecordsQueuedBehindABlockedKeyHeadAsWorkable` is
`isRecordsAwaitingProcessing()` reading true with nothing selectable. The consequence differs (a close
that waits out its drain timeout, rather than a poller that stays paused), which is why it is recorded
here rather than folded into the gate question.

Checked and ruled out: `isWorkInFlightMeetingTarget()` and `hasWorkInFlight()` read
`numberRecordsOutForProcessing`, which counts records actually dispatched to a worker and cannot
include a queued one; `checkPipelinePressure()` reads the executor's own queue depth rather than any
shard figure - and that is why the load factor barely moved in the stall, since the pool was never
starved; `getNumberOfRecordsInShards()`'s remaining callers are diagnostics and tests.

## Related

- [`bug-177-commit-response-timeout-unreproduced.md`](bug-177-commit-response-timeout-unreproduced.md) - owns astubbs#175, and the soak whose runs found this
- [`bug-857-family.md`](bug-857-family.md) - the register of what sits behind upstream's one "paused consumption" symptom; astubbs#336 fixed the gate's *drift*, which is a different defect in the same expression, and this is not a fourth instance of it
