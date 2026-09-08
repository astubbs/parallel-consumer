# astubbs#175: `Timeout waiting for commit response` - never reproduced, and nobody owns it

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->
<!-- inflight-labels: concurrency -->
<!-- inflight-vetted: 2026-09-08 - applied: shrunk to the astubbs#175 half; candidate 3 closed against the tree, and the astubbs#177 closure cross-referenced to the note that owns attribution rather than restated; checked: astubbs#175 is still OPEN and still unreproduced, a grep of `docs/plans/` and `docs/solutions/` still finds nothing targeting it, astubbs#29 and astubbs#57 are MERGED, and `PCMetrics.removeQuietly` now catches every exception from `meterRegistry.remove` under a stated never-throws teardown contract -->

**This file exists because the work had no home.** `bug-857-mirror-attributions-unconfirmed.md`
correctly says the honest options are "reproduce and diagnose, or close on their own merits" - but it
owns the *attribution* question, not the investigation, so "reproduce and diagnose" has sat as a
sentence nobody could pick up. The field report has been open for months with no reproduction
attempt: a grep of `docs/plans/` and `docs/solutions/` finds nothing targeting it. The only
adjacent record is `unforceable-trigger-commit-lock-timeout-2026-08-07.md`, which is a *test* flake
on the same lock and unrelated to the reporter's scenario.

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
