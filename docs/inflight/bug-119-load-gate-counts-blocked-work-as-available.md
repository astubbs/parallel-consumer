# Retry-forever plus any FRACTION of never-succeeding records eventually stops the instance fetching, at a threshold you can compute

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->
<!-- inflight-labels: concurrency -->
<!-- post-merge: checked -->
<!-- inflight-state: open - mechanism settled, latch point derived, and the latch now reports itself (astubbs/parallel-consumer#497); waiting on astubbs#149 for the real fix -->

**The filename carries astubbs#119, the fork mirror of confluentinc#857**, per this directory's rule
that a note's number is always the fork's. `bug-857-family.md` carries an upstream number because it
predates the rule; do not read the two prefixes as naming different things.

This note is about the **intake stall**, which is not the report the soak that found it was hunting.
[`bug-177-commit-response-timeout-unreproduced.md`](bug-177-commit-response-timeout-unreproduced.md)
**owns astubbs#175's question** - the commit-response timeout, the candidate list and the
discriminator. What is here is the mechanism its first runs left named-but-untested: now settled, with
the latch point derived and measured, and the mitigations that are open.

## What an operator sees

A single instance, no rebalance and no misconfiguration, with records that throw on every attempt.
The instance stops fetching from the broker **permanently** - every partition paused - while its
workers keep retrying the records it already holds. From outside it looks alive and loaded; nothing
is logged, nothing is thrown, throughput is zero, and only `pc_num_paused_partitions` says so.
confluentinc#833's reporter showed `pc_processed_records_total` **flat** across the window their
timeout fired in, which is this state and not a busy one - so this is the best explanation anyone has
produced for confluentinc#809 and confluentinc#833.

**This is not a pathological-workload result.** It does not need a 50% failure rate, it does not need
`KEY` ordering, and it does not need the workers to be busy. At 1% poison the instance ran normally
for a minute, delivering ~3,900 records, and then latched for good with **11 of its 14 workers idle**.
Any long-lived instance that retries forever while a non-zero *fraction* of its stream never
succeeds arrives here; the only question is when, and that is computable - see the latch point below.

## Settled: the gate is the mechanism, and head-of-line blocking is not why

Four arms on 2026-09-08, each differing from the first by exactly one term - three of six minutes and
one of ten, because that one's whole question is *when* the latch arrives - with `WorkManager` at DEBUG
and the logging config that carried it verified in the log rather than assumed.
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
- **`failureFraction=0.01`, the control on the poison rate**, ten minutes. **The gate oscillated and
  then stopped.** Successes rose steadily to 3,902 while it oscillated, then froze there for the
  remaining nine minutes; the final unbroken `true` run is nearly nine minutes long. This is the arm
  that says the stall is not an artefact of the reporter's 50% rate.

**So the gate is what stops intake** (arm 3 is the positive control), **and the stated mechanism is
wrong** (arm 2). Arm 1's own arithmetic agrees more directly: the records it held came from a single
burst over 1000 distinct keys, so there was **at most one record per key and nothing queued behind
any blocked head at all**.

## The latch point, and why every long-lived instance reaches it

The gate is `inShards - parkedForRetry > target * loadingFactor`, and **`parkedForRetry` is not a
property of the population**. By Little's law it is `retry throughput * retryDelay`. So with `P`
permanently-failing records held:

    unparked = P - (retry throughput * retryDelay)
    latch when unparked > targetAmountOfRecordsInFlight * loadingFactor

Under retry-forever `P` only grows, while the term subtracted from it is **bounded**: retry
throughput cannot exceed `maxConcurrency / userFunctionDuration`, so the parked term cannot exceed

    maxConcurrency * retryDelay / userFunctionDuration

which at this scenario's defaults (14 workers, a 1s static `defaultMessageRetryDelay`, a 100ms user
function) is **140** records. The latch is therefore an eventual certainty, at a ceiling of
`140 + 42 = 182` held poison records here.

**The `42` there is the gate's threshold term, not a second measured population** -
`targetAmountOfRecordsInFlight(14) * loadingFactor(3)`, read off the arms' own gate lines. The two
addends are different units on purpose: one bounds how many held records are parked, the other is the
line the remainder has to cross. `loadingFactor` is `DynamicLoadFactor#getCurrentFactor`, which starts
at `DEFAULT_INITIAL_LOADING_FACTOR` of 2 and steps up one at a time, so arm 1 latched against a
threshold of **28** before it had stepped at all; 182 is stated at the value the longer arms settled
at, and a run that had stepped further would latch later, not never - see the step-up condition in the
sweep below.

**Measured, and the measurement beats the bound in the dangerous direction.** `parkedForRetry` has
median 135 and hard max **140** across arms 1-3 while the population ranges from 549 to 17,103 - a 31x
population change with an unchanged parked count, which is only possible if parked is set by
throughput. Arm 1's predicted `unparked` of `549 - 140 = 409` is exactly its observed minimum. But
arm 4 latched at **98** held records, about 64 seconds in, because its retry throughput settled at
30.6/s and so its parked term was ~30 rather than 140. **A slower retry service latches the gate
sooner**, because fewer records are in back-off and more therefore read as workable. 182 is a
ceiling, not an estimate.

**Saturation is not a precondition - the instance stalls while idle.** At arm 4's latch the pool was
doing 30.6 failures/s, about 3 of its 14 workers: **22% utilisation**, against arm 1's 95%. It
stopped fetching from the broker while 78% idle. What limits the retry cadence to ~3.2s per record
against a *static* 1s delay (confirmed static - no `retryDelayProvider` is set and there is no
progressive backoff in `WorkContainer#computeRetryDueAt`) is **not measured**, and it is the first
arm in the scenario's list, because the latch point is a function of that number.

## What is still true about the accounting gap, and what it is not

The gate is `inShards - parkedForRetry > targetAmountOfRecordsInFlight * loadingFactor`, and its own
javadoc says why the subtraction is there: retry-parked records "occupy the buffer but no amount of
worker capacity can advance them". A record queued behind a failing `KEY` head meets that description
exactly - `ProcessingShard#getWorkIfAvailable` breaks out of an ordered shard after its head - and is
counted anyway. `WorkManagerTest#theLoadGateCountsRecordsQueuedBehindABlockedKeyHeadAsWorkable` pins
it: three records of one key with a failing head, and the gate reads two workable while nothing is
selectable, then three while one is.

**That gap is real, and it has a role - just not the one astubbs#471 gave it.** It is not what
latches the gate: arm 2 latches identically with no ordering constraint at all, and arm 1 held one
record per key. What it does is stop the residue draining *after* the latch. Arm 4's `inShards` fell
103 -> 98 and then sat at exactly 98 for 6,201 consecutive gate evaluations, nothing retiring for
nine minutes: those 98 are ~40 poison plus ~58 healthy records queued behind poisoned heads on their
own keys, because at 1% over four bursts a key holds several records where arm 1's single burst gave
each key exactly one. **Eleven idle workers sat beside 58 deliverable records they were not allowed
to reach.** So: not the cause of the latch, and the reason the latch is unrecoverable.

## Correction, 2026-09-09 - a FRACTION, not "any poison"; one bad record is not enough

<!-- post-merge: checked -->
Written into astubbs/parallel-consumer#497, and applied in place above rather than left standing,
because this note is the live record rather than a dated one. **The phrase "any poison at all" was
inherited from astubbs/parallel-consumer#487 and overstates the result; its own text on master is
left alone.**

**A single record that never succeeds does not latch the gate, and cannot.** The gate is
`inShards - parkedForRetry > target * loadingFactor`: one held record, minus one parked while it
waits out its back-off, is nowhere near a threshold of tens. Its offset map encodes a single gap
compactly, the commit sits below it, and the instance runs indefinitely with that one record
retrying beneath a healthy stream that keeps retiring.

**What latches the gate is a non-zero FRACTION of a live stream that never succeeds.** The two
properties that matter are both about the population, not about any one record: healthy records
retire and leave the shards, never-succeeding ones do not, so their share of what is held rises
monotonically while the stream keeps arriving. The parked term subtracted from it is bounded by
throughput rather than by population, so the unparked remainder crosses the threshold eventually -
at 1% in the measured arm, and sooner when the retry service is slower. "Any fraction" is the
correct claim and it is still a strong one; "any poison" is not, and reads as if one bad record
were enough.

## Open, from the review of the latch warning - the pass count is calibrated against ONE commit interval

<!-- post-merge: checked -->
`WorkManager#LATCHED_PASSES_BEFORE_WARNING` is derived from the loop's two cadences: latched passes
are fast because failure results arrive in the mailbox continuously, and healthy-but-slow passes are
slow because the mailbox is empty and each one blocks for the commit interval. The asymmetry is what
makes a pass count safer than an elapsed-time bound - **at the ordinary commit-interval default.**

**It is not safe at every default, and the constant's javadoc now says so. `getTimeToBlockFor()` has
two branches, and the derivation described only one.**

- **The commit interval, which is not always five seconds.** Under `PERIODIC_TRANSACTIONAL_PRODUCER`
  the default is `DEFAULT_COMMIT_INTERVAL_FOR_TRANSACTIONS`, two orders of magnitude shorter than the
  ordinary one, so the healthy grace collapses to roughly the same order as the latched cadence and a
  fully-loaded transactional instance inside a long user function can be reported. A short commit
  interval set by hand does the same on any mode.
- **The retry-delay branch, which needs no unusual configuration at all.** When
  `isWorkInFlightMeetingTarget()` is false - dispatch below full concurrency, which is the ordinary
  state under `KEY` or `PARTITION` ordering whenever fewer keys are active than `maxConcurrency`
  allows - and any record is in retry back-off, the pass blocks for
  `min(commitInterval, max(defaultMessageRetryDelay, lowestScheduled))`. At the stock one-second retry
  delay that is a one-second cadence, so the grace is roughly a hundred seconds rather than eight
  minutes, on stock defaults. **This one is a static trace of the two branches rather than a measured
  arm** - it wants a calibration run before anything is decided on it.

**What is not decided.** The line carries its own discriminator today - it prints `parkedForRetry`,
which the state this exists for holds continuously and a merely-slow instance reads as zero - so the
operator can tell the two apart from the report itself. Narrowing the *trigger* on that term is the
obvious next move and is **not** taken here, because it changes what the report fires on and wants its
own measured arm: a transient zero in the parked count would suppress a real latch, and no arm has
measured how often that happens. Scaling the count from the configured commit interval is the other
candidate, and reintroduces the clock the design avoided.

<!-- post-merge: checked -->
Raised by review on astubbs/parallel-consumer#497, which corrected the constant's javadoc rather than
changing the trigger.

## Not a product decision to be weighed - an eventual certainty to be bounded

The earlier close called this "a decision", which understates it. There is no configuration of the
existing code in which a long-lived instance with retry-forever and a non-zero fraction of
never-succeeding records does **not** end up here; the only variables are how long it takes and how idle the machine is when it happens. What is
open is which of the mitigations below is taken, not whether the state is reachable.

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

<!-- post-merge: checked -->
- **Make the latch loud - DONE, astubbs/parallel-consumer#497.** The state was exported as the
  `NUM_PAUSED_PARTITIONS` gauge and said nothing in the log; a gate reading `true` across
  `LATCHED_PASSES_BEFORE_WARNING` consecutive control-loop passes with nothing retiring is now a WARN
  naming the operands, once, with an INFO when it clears. No semantics changed - the gate's decision,
  the poller's pausing, the retry service and every counter are untouched. Arm 4 is the argument that
  made it first: an instance can be 78% idle, look healthy, and be permanently stopped, and until
  then the only thing that would tell an operator was a gauge nobody is alerting on. **The count is
  passes, not elapsed time**, and `WorkManager#LATCHED_PASSES_BEFORE_WARNING`'s javadoc owns the
  derivation - the two cadences it separates and the fifty-fold grace the count gives the healthy
  one.
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

**Three adjacent mechanisms ruled out, each with evidence rather than by reading.**

- **A dead poll thread** - astubbs#477's class. Not this: the soak classifies a terminal failure by
  its own message and reported no finding in any of the four arms - the poller is alive throughout,
  it is paused.
- **A lost wakeup from a stale pause cache**
  (`docs/solutions/performance-issues/paused-poll-wakeup-lost-to-stale-pause-cache-2026-09-01.md`).
  Not this either, and the discriminator is decisive: `maybeWakeupPoller()` is gated on
  `!wm.isSufficientlyLoaded()`, so with the gate continuously `true` **no wakeup is ever attempted** -
  its `Found Poller paused` line appears zero times in every arm. Nothing can lose a wakeup that was
  never sent.
- **The eager-sync stall withdrawn by astubbs#478** - "a bound the processor count crosses, not a
  defect". Do not collapse this note into that one. That symptom **drained** (`inFlight=0`,
  250,000 consumed) and needed a processor-count cap to appear; this one never drains -
  `inShards` sat at exactly 98 for 6,201 consecutive evaluations on an uncapped box - and it is
  reached at 22% CPU utilisation with no cap at all.

Checked and ruled out: `isWorkInFlightMeetingTarget()` and `hasWorkInFlight()` read
`numberRecordsOutForProcessing`, which counts records actually dispatched to a worker and cannot
include a queued one; `getNumberOfRecordsInShards()`'s remaining callers are diagnostics and tests.

**And the dynamic threshold does not grow its way back out - the step-up is conditioned on the very
thing the latch stops.** A rising `loadingFactor` raises the gate's threshold, so in principle the
factor could climb toward its ceiling of `DEFAULT_MAX_LOADING_FACTOR` and unlatch the gate on its own.
It cannot, and the reason is a source condition rather than a coincidence of these runs:
`checkPipelinePressure()` steps the factor only when `isPoolQueueLow() && lastWorkRequestWasFulfilled`,
and that second term is set by `retrieveAndDistributeNewWork` as `gotWorkCount >= delta`, where
`delta` is the shortfall of dispatched records against the loaded target. Once the shards have nothing
selectable left, every pass hands back less than the shortfall, the flag stays `false`, and the factor
is pinned wherever it stood when the latch arrived. **The idle pool does not rescue it either**:
`isPoolQueueLow()` reads the executor's own queue depth, which was low throughout arm 4 - it is the
fulfilment term that holds the factor down, not the pressure term. Which is what arm 4 shows from the
outside: `loadingFactor(3)` across an unbroken `true` run of nearly nine minutes, eleven workers idle
and the executor queue empty the whole time.

## Related

- [`bug-177-commit-response-timeout-unreproduced.md`](bug-177-commit-response-timeout-unreproduced.md) - owns astubbs#175, and the soak whose runs found this
- [`bug-857-family.md`](bug-857-family.md) - the register of what sits behind upstream's one "paused consumption" symptom; astubbs#336 fixed the gate's *drift*, which is a different defect in the same expression, and this is not a fourth instance of it
