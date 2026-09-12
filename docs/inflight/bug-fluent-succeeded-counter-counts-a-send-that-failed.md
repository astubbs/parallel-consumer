# A producing route's succeeded counter climbs while its records never commit, so a broken produce reads as healthy

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

<!-- post-merge: checked-begin -->
Open against the fluent API's outcome counters, under issue astubbs#504; found by the independent
cross-model review on astubbs#502, which asked for the produce path's counting to be driven from the
engine. Both references stay resolvable once that pull request has merged, which is why they are named
rather than described.
<!-- post-merge: checked-end -->

It is a defect in a **published meter**, not only in a counter the tests read, which is why it is here
and not only on the field: the increment sits beside a
`meters.recordOutcome(..., OutcomeTag.SUCCEEDED)` that registers through the engine's `PCMetrics` into
the user's own registry (R19, KTD8).

**This is a known defect class in this repository, already named**: state advanced at request time rather
than at acknowledgement time -
[`docs/solutions/logic-errors/an-async-commit-was-recorded-on-send-not-on-acknowledgement-2026-09-07.md`](../solutions/logic-errors/an-async-commit-was-recorded-on-send-not-on-acknowledgement-2026-09-07.md)
carries the worked instance on the commit path and the repair shape. The produced-record total on this
same class was repaired that way and is now exact; this counter is the instance left.

## What an operator sees

A producing route whose sends are failing - an unreachable broker, a rejected topic, a quota that keeps
refusing. The source record is never completed, so its offset never commits and it is delivered again.
Each delivery serialises fine, reaches the outcome mapping, and is counted. The `outcome=succeeded`
counter therefore climbs for a record the broker has taken no times at all, and climbs faster the worse
the failure is.

On a dashboard that reads as throughput. It is the counter an operator checks to decide the pipeline is
moving, and here it moves precisely when nothing is.

**The sibling meter now disagrees with it, which makes the reading worse rather than better.** Produced
records are counted from the engine's acknowledgement callback - grep `produceAcknowledged` in
`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/fluent/RouteDispatcher.java` - so
succeeded and produced diverge on exactly this failure, and the divergence reads as a bug in the produce
path rather than in the counting.

## Where it actually is - narrower than the field's own note says

Only the **produce arm** of the outcome mapping. In that same file, grep `case PRODUCE`: the increment
happens after serialisation and before the engine has sent anything. The plain `case SUCCEEDED` arm above
it has no send to fail and is exact, so `Outcome.succeeded()` on a producing route is never over-counted -
it is `Outcome.produce(...)` alone.

Checked and **not** instances of the same class, so nobody re-audits them: `filtered` completes with
nothing to send; `parked` is a hand-back with no broker round-trip, and it already guards the case it has
(an increment skipped when the partition's claim was revoked while the record ran).

## Why it is not simply moved

The mechanism and both rejected seams are recorded on the field - grep
`private final LongAdder succeeded` in that file. In short: the produce callback fires per produced
*record* and carries the poll context, never the one source record, so it cannot say "this source record
completed"; and `WorkManager.addSuccessfulWorkListener` is per record but cannot tell a success from a
filtered record, because the engine completes both identically - which is the entire reason this facade
counts them apart.

Driving it from the engine therefore needs the facade to remember each record's reported outcome until
the engine completes it. **That is per-record facade state, which KTD14 in
`docs/plans/2026-09-09-002-feat-ux-modernisation-plan.md` says this package does not keep** - so closing
it is a decision about that decision, not an implementation choice somebody can just make. That is the
part a comment on a field cannot surface, and the reason this note exists.

## What it is waiting on

An owner call, between:

- **Accept the over-report and say so where a user reads the meter.** Cheapest, and honest only if the
  meter's documentation admits it: a counter documented as "successes the function reported" is a
  different promise from "records that succeeded", and today nothing tells a user which it is.
- **Let the facade hold per-record outcome state, narrowing KTD14.** Closes it exactly, at the cost of
  the state that decision exists to keep out.
- **Ask the engine to distinguish a completed record's outcome.** The KTD14-shaped answer - the engine
  completes success and filtered identically, and a seam separating them would serve this counter and R8
  at once. Engine work, so not this milestone.

## Scope, so nobody over- or under-reads it

The fluent package has never shipped, so no user is misled today. Milestone A is the release candidate
and this counter ships with it, so the window to settle it closes at that release rather than after.

There is no test pinning the over-report, deliberately: a test asserting the wrong number would have to
be rewritten by whichever option above is taken, and it would read as the behaviour being intended.
