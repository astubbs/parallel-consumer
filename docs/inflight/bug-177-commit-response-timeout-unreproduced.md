# astubbs#175: `Timeout waiting for commit response` - never reproduced, and nobody owns it

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->
<!-- inflight-labels: concurrency -->
<!-- inflight-vetted: 2026-09-08 - applied: shrunk to the astubbs#175 half; candidate 3 closed against the tree, and the astubbs#177 closure cross-referenced to the note that owns attribution rather than restated; checked: astubbs#175 is still OPEN and still unreproduced, a grep of `docs/plans/` and `docs/solutions/` still finds nothing targeting it, astubbs#29 and astubbs#57 are MERGED, and `PCMetrics.removeQuietly` now catches every exception from `meterRegistry.remove` under a stated never-throws teardown contract -->

**This file exists because the work had no home.** `bug-857-mirror-attributions-unconfirmed.md`
correctly says the honest options are "reproduce and diagnose, or close on their own merits" - but it
owns the *attribution* question, not the investigation, so "reproduce and diagnose" has sat as a
sentence nobody could pick up. The field report sat open for months with no reproduction attempt at
all - **the attempt now exists**, as `CommitResponseTimeoutSoakIT` in the `soak` lane, and its runs
are recorded at the bottom of this note - the latest, 2026-09-18, on the released v0.6.0.0 with the
intake latch held open, is the first in which the assertion could have failed for the whole thirty
minutes; it found nothing. A grep of `docs/plans/` and `docs/solutions/` still
finds nothing targeting the report; the only adjacent record is
`unforceable-trigger-commit-lock-timeout-2026-08-07.md`, which is a *test* flake on the same lock and
unrelated to the reporter's scenario.

**The filename carries astubbs#177 for history only.** That issue was closed on 2026-09-01 without
the reproduction or the closing comment this note asked for;
[`bug-857-mirror-attributions-unconfirmed.md`](bug-857-mirror-attributions-unconfirmed.md) owns that
outcome and records it once. Renaming this file would break every citation of it, so the name stays
and the subject is astubbs#175.

## The report

**astubbs/parallel-consumer#175** (confluentinc/parallel-consumer#809) - `InternalRuntimeException:
Timeout waiting for commit response PT30S`, sporadically, in production on GKE. 22 comments upstream;
reported still present on the newest version at the time.

## Why it is not closed, in one paragraph

It was attributed to astubbs/parallel-consumer#100 - an unhandled `RebalanceInProgressException`
killed the broker-poll thread, and that thread is the only producer of commit responses, so every
waiter then times out. The story fits. It is not the only story that fits: `maybeDoCommit()` is
called **only** from the poll loop, so ANY reason that loop stops servicing the queue produces the
identical symptom. **A dead poller and a wedged-but-alive poller are indistinguishable from
outside**, and astubbs#100 only fixed the dead one.

## The candidate list, and where each stands

1. **Poller died** from an unhandled `RebalanceInProgressException` - astubbs#100, landed.
2. **Poller wedged but alive** - uncharacterised, and still nobody's. This is the open one.
3. **Poller died from a throwing metrics registry** - **closed.** Meter de-registration runs inside
   `onPartitionsRevoked`, on the poll thread inside `poll()`, and the meter registry is usually the
   USER'S, so an exception from third-party code escaped the rebalance callback and took out the only
   producer of commit responses - the exact symptom this report describes. astubbs#29 and astubbs#57
   have both merged, and `PCMetrics.removeQuietly` now swallows everything `meterRegistry.remove`
   throws under a stated never-throws teardown contract, so master no longer carries the exposure.
   It was never a fix for this report in any case: the mechanism needs a user-supplied
   `MeterRegistry` that throws, and nothing in the report says the reporter configured metrics at
   all. Attributing on "the mechanism fits" is precisely the error corrected on
   astubbs/parallel-consumer#44, which sat attributed to
   <!-- post-merge: checked -->
   astubbs/parallel-consumer#29 for months in a commit mode where that fix cannot run.

## What would discriminate, and why it is easier now than it was

astubbs/parallel-consumer#204 releases a waiter immediately on poller **death**, with the poller's own
exception as the cause. So on current master the two cases have finally separated:

- timeout arrives carrying a poller exception -> the poller **died** (astubbs#100's class)
- timeout arrives with **no** poller exception, PT30S elapsed -> the poller is **wedged but alive**,
  which is a defect nobody has characterised

That is the whole experiment. It cannot retro-diagnose the original report, but it means a
reproduction on current code answers the question immediately rather than needing thread dumps.

## A concrete reproduction to try, because "reproduce it" is not a plan

The shape below comes from the now-closed astubbs#177 report, not from astubbs#175, and it is kept
because it is unusually specific and looks buildable - it is the cheapest route to the wedged-poller
question either report poses:

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

**This section is now a record of the design rather than an outstanding request**: it was built as
`CommitResponseTimeoutSoakIT`, and what its runs actually measured is the next section. Do not build
it again.

## The experiment now exists, and its first two runs measured the experiment rather than the defect

`CommitResponseTimeoutSoakIT` (the `soak` lane, `docs/testing.md`) is the reproduction attempt this
note asked for, built from the astubbs#177 shape above for the reason stated there - it is buildable,
and it is the cheapest route to the wedged-poller question. **2026-09-07, two runs, 0 timeouts in
2 x 30 minutes - and neither is a sighting-ledger entry, because in both the assertion could not have
failed after the first minute.**

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
| `failureFraction` 0.5 (astubbs#177's reporter's) | `3747722682837130843` | 451 | 237,006 | none |
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

- only `PartitionState#onSuccess` calls `recordCompletion` (`setDirty`, before dirty became derived);
  `onFailure` in the same file is an explicit no-op, so a failing record never marks its partition
  dirty;
- the control loop gates on `shouldTryCommitNow` in `AbstractParallelEoSStreamProcessor` -
  `isTimeToCommitNow() && wm.isDirty() && !isRebalanceInProgress.get()`;
- so with no success anywhere, nothing is dirty, no commit request is enqueued, and
  `ConsumerOffsetCommitter#commitAndWait` - the only place `Timeout waiting for commit response` is
  thrown - is never entered.

A green run therefore cannot distinguish "no timeout occurred" from "no commit was attempted". This
is exactly the `dirty` asymmetry the astubbs#177 write-up
(`docs/solutions/logic-errors/the-only-producer-of-commit-responses-died-so-every-waiter-reported-a-timeout-2026-09-18.md`) names for this same workload; these runs are the measurement of it rather than evidence about the report.

**Lowering the poisoned fraction does not fix it, and that is a run rather than a guess:** 0.03 bought
about 1.5 extra bursts and stalled identically, which rules out "too many poisoned keys" and makes the
fraction the wrong knob. Duration is the wrong knob too - the stall arrives in minute one of thirty.

### What stops intake is not the documented back pressure - and the candidate is named

`PartitionState#updateBlockFromEncodingResult` logs on every transition (`Offset map data too large`,
`not allow further messages`). **Neither string appears once in either run's log**, so offset-encoding
back pressure is eliminated.

The candidate named here was the load gate. `WorkManager#isSufficientlyLoaded` compares
`workable = inShards - parkedForRetry` against `targetAmountOfRecordsInFlight * loadingFactor`, and
`inShards` counts records queued **behind** a blocked shard head - records that can never be worked -
while only the failing head itself is `parkedForRetry`. That is the silent-stall shape the gate's own
comment names against confluentinc#857.

**It has since been run, and it is half right.** The gate IS what stops intake; head-of-line blocking
is NOT why. Three six-minute arms on 2026-09-08 settle it, and a fourth at a low poison rate on
2026-09-09 turns the verdict into a threshold: the gate latches on an idle instance once the poison
population outgrows what the retry service holds in back-off, so any retry-forever instance that
meets poison gets there eventually.
[`bug-119-load-gate-counts-blocked-work-as-available.md`](bug-119-load-gate-counts-blocked-work-as-available.md)
**owns that question from here** - the arms, the latch point, the accounting gap that is real but was
not the cause, and the fix that bounds failures rather than the buffer. What matters to *this* note is only the
consequence: **the intake stall is not going to be fixed by a change to the gate**, so the arms below,
which exist to make this scenario able to falsify its own assertion, are unchanged by it.

### What to run next, in order

1. **DONE, 2026-09-08 - `WorkManager` at DEBUG.** The gate read `true` on all but the first four of
   ~37k evaluations with every partition paused; a control arm on ordering stalled identically, and a
   control arm on the gate's threshold flipped the outcome. Verdict and evidence in the note named
   above, and in `CommitResponseTimeoutSoakIT`'s `Calibration status`.
2. **Per-attempt rather than per-record failure**, so records eventually succeed, the shards drain and
   the instance keeps committing for the whole run. On this evidence it is the only shape that keeps
   the commit path alive indefinitely - promoted from "a different mechanism" to "the first arm that
   can falsify the assertion at all". *2026-09-18: the same property was obtained with `UNORDERED`
   plus a buffer the run cannot fill (arm C below), and the assertion held for thirty falsifiable
   minutes; this arm stays the way to get the property under `KEY`.*
3. **`gtassone`'s configuration from confluentinc#809** - 128 partitions, concurrency 64, user
   function 100ms to minutes, `PERIODIC_CONSUMER_SYNC`. This is astubbs#175's own shape, and the
   scenario does not have it: the workload transcribes the now-closed astubbs#177 report instead,
   whose defect is fixed (`docs/solutions/logic-errors/the-only-producer-of-commit-responses-died-so-every-waiter-reported-a-timeout-2026-09-18.md`).
   `upstream-175-sporadic-commit-timeouts.md` no longer nominates it as a *wedge* candidate - see
   the section below - but it remains the closest recorded configuration to the live report, which is
   what this arm buys.

### 2026-09-18: re-run on v0.6.0.0 with the latch held open - zero timeouts in the first run that could have found one

Since the 2026-09-07 runs the 857 family's stall fixes shipped in v0.6.0.0, and the question left is
whether a *bare* `Timeout waiting for commit response` - the poller wedged but alive - ever appears on
the released code once the workload is arranged so the intake gate does not latch and commits keep
happening. Three arms on the tree at v0.6.0.0 plus two docs commits (`2e6f13ef1`), same seed
`3747722682837130843`, `failureFraction` 0.5, retry-forever, `PERIODIC_CONSUMER_SYNC` at 1s, the
suite's Testcontainers `confluentinc/cp-kafka:7.9.0`, a Linux x86_64 workstation with every JVM pinned
to 8 processors, no other Maven JVM on the box at any arm's start. Commit activity was read from the
broker's `__consumer_offsets` (one row per partition per acknowledged commit, timestamped) rather than
from the product, whose commit-path lines are all DEBUG - the command is in the scenario's
`Calibration status`, which owns the full numbers.

| Arm | Change | Gate | Commit path | Result |
|---|---|---|---|---|
| A, control, 6 min | none | astubbs#497 WARN 13.2s after the banner (`inShards=549 parkedForRetry=140 workable=409 vs 42, pausedPartitions=20`); latched at all 11 samples | 8 commit instants, all in the first 7.3s, then none | succeeded 451 frozen, failed 49,710; no findings |
| B, 30 min | `messageBufferSize=20000`, `KEY` | open for 7m06s, then latched at `inShards=20993 vs 20006` for the remaining 22m54s | 27 commit instants, the last 3m06s in; none after | succeeded 897 (frozen from 3.5 min), failed 250,067; no findings |
| C, 30 min | `messageBufferSize=100000`, `UNORDERED` | open at all 59 samples; `inShards` peaked 51,949 vs 100,002; no WARN | a commit in every one of the 30 minutes - 619 instants, the last 1794.8s in | succeeded 37,641 and still rising, failed 213,533; **no findings** |

**Arm B settles that the buffer is the wrong knob for this question under `KEY`, for a reason the
gate arms did not isolate.** Its commit path went quiet at minute three with the gate open and no
partition paused: under `KEY` each key retires records until its first poisoned one and never again,
so with permanent poison successes are bounded by the key space (897 observed, twice), and dirty is
derived from completions (`PartitionState#isDirtyAt`), so once successes stop nothing is ever dirty
and `commitAndWait` is never entered. A 100,000 buffer would have held the gate open for the full
thirty minutes and commits would still have stopped at minute three. `UNORDERED` is what lifts that
bound - it is a departure from astubbs#177's reporter's ordering, chosen because it is the only
combination of the existing knobs under which the assertion can fail at all after minute three, and
the 2026-09-08 arm 2 had already shown ordering does not decide the latch.

**What this is, and is not.** On v0.6.0.0, one instance, a quiet box: zero bare commit-response
timeouts and zero poller deaths across 654 acknowledged commit instants, 619 of them in thirty
minutes of a continuously exercised commit path. One run at one shape; it says the shape did not
reproduce once, never that it cannot. It says nothing about astubbs#175's own configuration (128
partitions, concurrency 64, a user function of minutes - item 3 above, still unrun), a loaded box, a
rebalance, more than one instance, or durations past thirty minutes.

### The stall may be the better lead than the timeout

confluentinc#833's reporter - astubbs#177's, whose shape this scenario transcribes - showed
`pc_processed_records_total` **flat** across the window in which their timeout fired, which is this
state, not a busy one. Whoever picks this up should consider whether the reported timeout is a
*consequence* of an intake stall rather than a peer of it.

### Where the two adjacent notes leave candidate 2, and why that matters to the next arm

The candidate list at the top of this note already reflects both of these. They are repeated here
because they are what the remaining soak arms are aimed at, and each is owned by the note named:

- **`docs/solutions/logic-errors/the-only-producer-of-commit-responses-died-so-every-waiter-reported-a-timeout-2026-09-18.md`** - four `astubbs#177` commits landed 2026-08-19 and
  close candidate 1's class outright: the poller now publishes its own death
  (`notifyPollerDied`), waiters are released with its exception, and the message reports the budget it
  actually waited on. astubbs#177's mirror is closed; confluentinc#833 is still open upstream and
  still unanswered, which is why its reporter's shape is still worth running here.
- **`upstream-175-sporadic-commit-timeouts.md`** - confluentinc#809 is *not* the same defect as
  confluentinc#833 despite the shared message, and its strand-by-strand table now closes its last
  named strand too: **the poll thread alive but wedged - the AB-BA cycle - was fixed by astubbs#29,
  merged 2026-09-02**, and that note records the strand as closed for this report.

So candidate 2 stays the open one, with **no named mechanism left inside it**: the single wedge the
record ever described has been fixed, so a reproduction on current code would be characterising
something nobody has yet described. That is what makes the intake stall these runs found the more
promising lead of the two. The branch `test/177-commit-response-timeout` (unmerged) is further prior
art: it confirmed the astubbs#100 trigger reproduces when the catch is removed, and records one
**falsified** experiment (moving `brokerPollSubsystem.supervise()` earlier in `controlLoop()` does not
help - measured).

## Do not

- Do not attach a closing keyword from any PR on present evidence - see
  `bug-857-mirror-attributions-unconfirmed.md`, which owns that rule and records what it cost when it
  was ignored.
- Do not treat a release shipping as confirmation.
- Do not close as unreproducible without *having tried*, and without naming both candidate mechanisms
  in the closing comment.

## Related

- `docs/inflight/bug-857-mirror-attributions-unconfirmed.md` - owns the attribution question
- `docs/inflight/bug-857-family.md` - which defects sit behind the one upstream symptom
- `docs/solutions/architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md`
