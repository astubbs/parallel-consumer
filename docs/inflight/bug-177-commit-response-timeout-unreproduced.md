# astubbs#177 / astubbs#175: `Timeout waiting for commit response` - never reproduced, and nobody owns it

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
astubbs/parallel-consumer#29 for months in a commit mode where that fix cannot run.

So the candidate list is now three, all producing one trace:

1. **Poller died** from an unhandled `RebalanceInProgressException` - astubbs#100, landed.
2. **Poller wedged but alive** - uncharacterised, and still nobody's.
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
