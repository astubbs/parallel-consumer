# A commit-failure seam - astubbs/parallel-consumer#317

<!-- inflight-priority: low -->

**Priority: high.** Ranked top of `next-candidates.md`. The demand signal is not a request in a
tracker - it is a user running a **patched build of the library** in production to get this
behaviour. That is the loudest signal a missing feature can produce, and it is on the record.

Feature request, spun out of astubbs#177 / confluentinc#833 rather than duplicating it.
**astubbs/parallel-consumer#317** owns the design questions; this file is why it exists and what the
research behind it found, so the next session does not re-derive it from the issue thread.

## The gap

A commit that exhausts its budget takes the whole instance down: the exception escapes `controlLoop`,
`supervisorLoop` records it as the failure cause, PC closes, and the application learns about it
afterwards through `getFailureCause()`. There is no "the commit failed, you decide" hook.

## What the upstream thread actually says

Both halves are on confluentinc#833, and they disagree - which is the point.

- **`ndqvinh2109` already worked around it in production**: *"PC has been closed unintentionally. We
  have applied a work-around to try catch the `controlLoop` function so that the exception won't
  propagate to `supervisorLoop`"*. A user patching library internals to get a decision PC does not
  offer is the clearest possible signal for this feature.
- **`rkolesnev` argued the opposite**: *"if it cannot commit data - then polling will fail as well and
  processing anything that was previously polled is just going to cause more side-effects /
  duplicates... I don't think that indefinite retry is an answer there either."* That is a good case
  for shutdown being the **default**. It is not a case for it being the only option.

Note who is speaking: this is a colleague's read on an abandoned upstream tracker, not a maintainer
decision binding this fork. Weigh it on its merits.

## What Kafka does, which is the model to borrow

Kafka's client throws `TimeoutException` - **retriable** in its taxonomy - and hands the choice to the
caller. Two-level budgets (`default.api.timeout.ms` over `request.timeout.ms`, `delivery.timeout.ms`
over `request.timeout.ms + linger.ms`), honest reporting, application decides.

astubbs#204 borrowed the first two of those: `offsetCommitTimeout` is now a whole-operation budget in
the `default.api.timeout.ms` role, and the failure is reported as
`OffsetCommitBudgetExceededException` naming the knob that ran out. **The seam is the third part, and
the only one PC still lacks.** The exception's message points here, so a user meeting it learns the
shutdown is a known limit rather than an accident.

## Why fixing the defect does not close this

astubbs#177 is the reported *defect* - a commit timeout reporting the wrong cause, and the causes
behind it - addressed by astubbs#204 (reporting, retry budget) and astubbs#29 (the AB-BA deadlock).
Even with a correct, honestly-reported commit failure, PC still terminates and the application still
has no say. That is what `ndqvinh2109` was patching around, and it survives both fixes.

## Delete this file when

astubbs/parallel-consumer#317 lands, or is closed as won't-do with the reasoning recorded there.
