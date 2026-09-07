# astubbs#177 / astubbs#175: `Timeout waiting for commit response` - never reproduced, and nobody owns it

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->
<!-- inflight-labels: concurrency -->

**This file exists because the work had no home.** `bug-857-mirror-attributions-unconfirmed.md`
correctly says the honest options are "reproduce and diagnose, or close on their own merits" - but it
owns the *attribution* question, not the investigation, so "reproduce and diagnose" has sat as a
sentence nobody could pick up. Two field reports have now been open for months with no reproduction
attempt: a grep of `docs/plans/` and `docs/solutions/` finds nothing targeting either. The only
adjacent record is `unforceable-trigger-commit-lock-timeout-2026-08-07.md`, which is a *test* flake
on the same lock and unrelated to the reporters' scenario.

## The two reports

- **astubbs/parallel-consumer#177** (confluentinc/parallel-consumer#833) - PC runs for a while, then
  exits with `InternalRuntimeException: Timeout waiting for commit response PT30S`, with **~50% of
  records failing across 1000 keys**.
- **astubbs/parallel-consumer#175** (confluentinc/parallel-consumer#809) - the same exception,
  sporadically, in production on GKE. 22 comments upstream; reported still present on the newest
  version at the time.

## Why they are not closed, in one paragraph

Both were attributed to astubbs/parallel-consumer#100 - an unhandled `RebalanceInProgressException`
killed the broker-poll thread, and that thread is the only producer of commit responses, so every
waiter then times out. The story fits. It is not the only story that fits: `maybeDoCommit()` is
called **only** from the poll loop, so ANY reason that loop stops servicing the queue produces the
identical symptom. **A dead poller and a wedged-but-alive poller are indistinguishable from
outside**, and astubbs#100 only fixed the dead one.

## A THIRD candidate arrived 2026-08-19 - and it is not a fix for these reports

<!-- post-merge: checked -->
astubbs#29 hardened metrics teardown, which had been able to kill the broker-poll thread: meter
de-registration runs inside `onPartitionsRevoked`, on the poll thread inside `poll()`, and the meter
registry is usually the USER'S, so an exception from third-party code escaped the rebalance callback
and took out the only producer of commit responses. Every later commit then blocks until it times
out - **the exact symptom these two reports describe**.

**This does not close either report, and must not be recorded as doing so.** The mechanism requires a
user-supplied `MeterRegistry` that throws on `remove`; PC's default when none is configured is an
empty `CompositeMeterRegistry`, a no-op that cannot throw. Nothing in either report says the reporter
configured metrics at all, let alone a registry that failed. Attributing on "the mechanism fits"
is precisely the error corrected on astubbs/parallel-consumer#44, which sat attributed to
<!-- post-merge: checked -->
astubbs/parallel-consumer#29 for months in a commit mode where that fix cannot run.

So the candidate list is now three, all producing one trace:

1. **Poller died** from an unhandled `RebalanceInProgressException` - astubbs#100, landed.
2. **Poller wedged but alive** - uncharacterised, and still nobody's.
<!-- post-merge: checked -->
3. **Poller died from a throwing metrics registry** - found on astubbs#29 and landing on
   astubbs#57, which owns `PCMetrics`; **neither has merged**, so master still carries the exposure.
   Only reachable by a
   user who configured one.

**The useful part is that candidate 3 is self-identifying from now on.** With astubbs#204's change,
a poller death releases the waiter carrying the poller's own exception - so if this was ever the
cause, a future occurrence names the metrics failure in the cause chain rather than presenting as a
bare PT30S timeout. Combined with the fix, that means this candidate should now either disappear or
announce itself.

## What would discriminate, and why it is easier now than it was

astubbs/parallel-consumer#204 releases a waiter immediately on poller **death**, with the poller's own
exception as the cause. So on current master the two cases have finally separated:

- timeout arrives carrying a poller exception -> the poller **died** (astubbs#100's class)
- timeout arrives with **no** poller exception, PT30S elapsed -> the poller is **wedged but alive**,
  which is a defect nobody has characterised

That is the whole experiment. It cannot retro-diagnose the original reports, but it means a
reproduction on current code answers the question immediately rather than needing thread dumps.

## A concrete reproduction to try, because "reproduce it" is not a plan

The astubbs#177 reporter's shape is unusually specific and looks buildable:

- **1000 keys**, so `KEY` ordering with a wide key space
- **~50% of records failing**, which is the part no existing test does - a user function that fails
  roughly half the time drives sustained retry traffic through the commit path
- runs *for a while* before dying, so it is an accumulation, not a startup race

Suggested first attempt, as a soak rather than a unit test: `KEY` ordering, 1000 keys, a user function
failing ~50% with the project's retry behaviour, `PERIODIC_CONSUMER_SYNC` (the mode that blocks on the
response queue), run 30+ minutes, and assert only that no `Timeout waiting for commit response`
occurs. Then read whether any timeout carries a poller exception, per the discriminator above.

Reuse before building: `ChaosScenarioBase` already provides a fleet, a failing-work harness exists in
the retry tests, and the chaos suite's `ProgressProbe` plus the new `INSTANCE_STALL` detector will say
whether an instance is wedged while it happens. **Do not start a parallel harness** - see
`docs/testing.md`.

## The experiment now exists, and its first two runs measured the experiment rather than the defect

`CommitResponseTimeoutSoakIT` (the `soak` lane, `docs/testing.md`) is the reproduction attempt this
note asked for. **2026-09-07, two runs, 0 timeouts in 2 x 30 minutes - and neither is a
sighting-ledger entry, because in both the assertion could not have failed after the first minute.**

Conditions common to both runs, so the numbers are interpretable:

| Term | Value |
|---|---|
| Duration | 30 min each (`-Dsoak.duration=PT30M`) |
| Shape | 1 instance, no churn, `KEY` ordering, `PERIODIC_CONSUMER_SYNC`, 1s commit interval |
| Scale | 1000 keys over 20 partitions, `maxConcurrency` 14, ~100ms user function |
| Poisoning | per-record and permanent (a poisoned record throws on every attempt) |
| Produce | 1000 records every 20s - 90,000 produced per run |
| Broker | the suite's Testcontainers Kafka on Docker |
| Machine | maintainer's macOS arm64 workstation |
| Assertion | exactly one - no `Timeout waiting for commit response`, and no other terminal failure |

| Arm | Seed | Succeeded | Failed | Findings |
|---|---|---|---|---|
| `failureFraction` 0.5 (the reporter's) | `3747722682837130843` | 451 | 237,006 | none |
| `failureFraction` 0.03 | `5055695573431537469` | 2,372 | 81,114 | none |

Run command (the second adds `-Dsoak.failureFraction=0.03`):

```
./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
  -Dincluded.groups=soak -Dexcluded.groups= -Dit.test=CommitResponseTimeoutSoakIT \
  -Dfailsafe.failIfNoSpecifiedTests=false -Dsoak.duration=PT30M
```

### What both runs measured is a total intake stall, not the absence of a timeout

Successes froze - at 451 and at 2,372 - within the first ~60 seconds of each run and **never moved
again** across the remaining 29 minutes, while the producer kept publishing and the failure count
climbed at a rate that then held exactly constant. A constant retry rate against a frozen success
count means no new record is being taken as work at all: the instance has stopped, not merely slowed.

**A stalled instance cannot reach the exception being hunted:**

- only `PartitionState#onSuccess` calls `setDirty`; `onFailure` in the same file is an explicit
  no-op, so a failing record never marks its partition dirty;
- the control loop gates on `shouldTryCommitNow` in `AbstractParallelEoSStreamProcessor` -
  `isTimeToCommitNow() && wm.isDirty() && !isRebalanceInProgress.get()`;
- so with no success anywhere, nothing is dirty, no commit request is enqueued, and
  `ConsumerOffsetCommitter#commitAndWait` - the only place `Timeout waiting for commit response` is
  thrown - is never entered.

A green run therefore cannot distinguish "no timeout occurred" from "no commit was attempted". This
is exactly the `dirty` asymmetry `upstream-tell-809-833-the-hang-is-fixed.md` names for this same
workload; these runs are the measurement of it rather than evidence about the reports.

**Lowering the poisoned fraction does not fix it, and that is a run rather than a guess:** 0.03 bought
about 1.5 extra bursts and stalled identically, which rules out "too many poisoned keys" and makes the
fraction the wrong knob. Duration is the wrong knob too - the stall arrives in minute one of thirty.

### What stops intake is not the documented back pressure - and the candidate is named

`PartitionState#updateBlockFromEncodingResult` logs on every transition (`Offset map data too large`,
`not allow further messages`). **Neither string appears once in either run's log**, so offset-encoding
back pressure is eliminated.

The untested candidate is the load gate. `WorkManager#isSufficientlyLoaded` compares
`workable = inShards - parkedForRetry` against `targetAmountOfRecordsInFlight * loadingFactor`, and
`inShards` counts records queued **behind** a blocked shard head - records that can never be worked -
while only the failing head itself is `parkedForRetry`. A shard set full of unworkable queued records
would therefore read as "sufficiently loaded", the broker poller would stay paused, and nothing would
ever arrive to change it. That is the silent-stall shape the gate's own comment names against
confluentinc#857. **This is a hypothesis, not a result.**

### What to run next, in order

1. **Re-run either arm with `WorkManager` at DEBUG and read the `isSufficientlyLoaded=` line at the
   moment successes freeze.** It prints its own operands (`inShards`, `parkedForRetry`, the threshold)
   for exactly this purpose. It either confirms or eliminates the load gate, and until it is read the
   other arms are guesswork. One run settles it.
2. **Per-attempt rather than per-record failure**, so records eventually succeed, the shards drain and
   the instance keeps committing for the whole run. On this evidence it is the only shape that keeps
   the commit path alive indefinitely - promoted from "a different mechanism" to "the first arm that
   can falsify the assertion at all".
3. **`gtassone`'s configuration from confluentinc#809** - 128 partitions, concurrency 64, user
   function 100ms to minutes. `upstream-175-sporadic-commit-timeouts.md` nominates him as the better
   wedge candidate and says why; this scenario transcribes the *other* reporter, whose defect
   `upstream-tell-809-833-the-hang-is-fixed.md` says is already fixed.

### The stall may be the better lead than the timeout

confluentinc#833's reporter showed `pc_processed_records_total` **flat** across the window in which
their timeout fired - which is this state, not a busy one. Whoever picks this up should consider
whether the reported timeout is a *consequence* of an intake stall rather than a peer of it.

### This note's candidate list needs reconciling with two notes on master

Neither was cited when this note was written, and both change what is left to hunt:

- **`upstream-tell-809-833-the-hang-is-fixed.md`** - four `astubbs#177` commits landed 2026-08-19 and
  close candidate 1's class outright: the poller now publishes its own death
  (`notifyPollerDied`), waiters are released with its exception, and the message reports the budget it
  actually waited on. The astubbs#177 mirror is closed; confluentinc#833 is still open and still
  unanswered.
- **`upstream-175-sporadic-commit-timeouts.md`** - confluentinc#809 is *not* the same defect as
  confluentinc#833 despite the shared message, and its strand-by-strand table leaves exactly one
  thing reachable at HEAD: **the poll thread alive but wedged, the AB-BA cycle, owned by astubbs#29**.

So candidate 2 - "poller wedged but alive" - is the only live target, and it is not unowned. The
branch `test/177-commit-response-timeout` (unmerged) is further prior art: it confirmed the
astubbs#100 trigger reproduces when the catch is removed, and records one **falsified** experiment
(moving `brokerPollSubsystem.supervise()` earlier in `controlLoop()` does not help - measured).

## Do not

- Do not attach a closing keyword from any PR on present evidence - see
  `bug-857-mirror-attributions-unconfirmed.md`, which owns that rule.
- Do not treat a release shipping as confirmation.
- Do not close as unreproducible without *having tried*, and without naming both candidate mechanisms
  in the closing comment.

## Related

- `docs/inflight/bug-857-mirror-attributions-unconfirmed.md` - owns the attribution question
- `docs/inflight/bug-857-family.md` - which defects sit behind the one upstream symptom
- `docs/solutions/architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md`
