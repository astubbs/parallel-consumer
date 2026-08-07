# Open: does PC recover from a poisoned transaction, or is it wedged until close?

Surfaced by astubbs#261's CI review, which reproduced the control arm independently. Not a regression
from that fix - the fix is what makes the state *reachable and visible* rather than silently wrong.

## What is observed

Once a terminally failed send correctly moves the transaction to abortable-error, every subsequent
record fails with:

```
KafkaException: Cannot execute transactional method because we are in an error state
```

In `TransactionalPartialResultSetIT` all 10 follow-on records failed that way across a 20s window
while the instance stayed up, dying only at close (`PC closed due to error`).

**Before astubbs#261 this did not happen** - the transaction was never poisoned, so PC carried on and
committed a partial result set. The new behaviour is strictly better: an instance that visibly stops
beats one that silently publishes corrupt data. But "visibly stops" is still a liveness failure, and
this project has a whole suite dedicated to that class.

## Why it is not answered by the test that found it

`defaultMessageRetryDelay` is 120s, far longer than the test's window, so the failed records never
reach a retry inside the run. The test therefore cannot distinguish:

- PC recovers on its own once the retry delay elapses - a new transaction is begun and the work
  proceeds; or
- PC is wedged for the life of the instance and only a restart clears it.

Nobody has established which. The test deliberately asserts only the guarantee it was written for -
that nothing partial becomes visible - and does not assert recovery, because a correct implementation
could legitimately abort and retry rather than fail fast.

## Why it is worth someone's time

This is the *alive but not progressing* shape the chaos pain suite hunts: rebalance-dwell zombies,
lag stagnation, drain overruns. If PC is wedged until close after any terminal produce failure, a
single oversized record could stop a partition indefinitely, and the symptom would look exactly like
the stalls in the `confluentinc#857` family rather than like a produce error.

It is also a natural candidate for the deferred transactional chaos scenario (Phase B of
`docs/plans/2026-08-07-001-test-transactional-eos-battle-test-plan.md`): a scenario that injects a
terminal produce failure and then asserts the fleet still makes progress would settle it, and would
do so under the churn where it matters.

## How to settle it

Run one instance past `defaultMessageRetryDelay` after a terminal send failure and watch whether the
source offset advances. Report the rate and the conditions, not a verdict. The distinguishing evidence
is whether a *new* transaction is ever begun after the poisoned one, which the producer state machine
makes observable.

## Related

- astubbs#261 - the fix that makes this state reachable, and its `TransactionalPartialResultSetIT`
- `docs/solutions/test-issues/transactional-batching-stall-produce-lock-released-per-record-2026-08-08.md` -
  a different defect with the same user-visible shape, where the cause was that no commit was ever
  *attempted*. Worth reading first: it is the closest prior art for telling these apart.

## Answered from the code, 2026-08-08: there is no recovery path short of close

The review settled this without needing the experiment proposed above.

The only `abortTransaction()` call site in the codebase is `ProducerManager#close(Duration)`. And
`ProducerWrapper#isTransactionOpen()` is `producerState.equals(BEGIN)`, which stays `BEGIN` after a
poisoned send - so `lazyMaybeBeginTransaction` never begins a replacement transaction, and nothing in
band ever aborts the poisoned one.

So the instance either:

- dies at the next commit attempt, when the abortable-state `KafkaException` propagates out of
  `commitOffsets` and kills the control thread; or
- if nothing is dirty, so no commit is attempted, stays alive and stuck exactly as observed.

Neither is recovery. The question above is closed; what remains is the design decision, which is
genuinely open: abort-and-reopen on abortable error, versus fail fast. That belongs with the deferred
Phase B chaos work rather than bolted onto a produce callback.

Two things would make the wedge cheaper to live with in the meantime, if the design decision takes a
while:

- `log.error("Error producing result message", exception)` now fires once per record per retry for the
  life of a wedged instance, unthrottled.
- PC's failure reason after poisoning is a generic `KafkaException` with no hint that a restart is the
  only cure.

Until that decision is taken, a small unit test asserting that a poisoned transaction is never
re-begun would at least freeze today's behaviour, so a future recovery fix shows up as a visible
change rather than a silent one.
