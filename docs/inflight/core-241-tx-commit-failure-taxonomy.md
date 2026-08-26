# Transaction commit failures: the record says "undifferentiated", the code has not been since 2022

<!-- inflight-type: feature -->
<!-- inflight-impact: reliability -->


astubbs#241 (confluentinc#144). **The premise both the mirror body and the manifest entry
`sweep-2023-tx-failure-taxonomy` rest on is false at HEAD**, and correcting it is the point of this
note - a future session reading either would design against code that stopped existing in 2022.

## What the record claims, and what the code does

The claim is "one generic retry loop"; the loop's own `catch` clause is
`catch (TimeoutException | InterruptException e)` in
`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/ProducerManager.java`, sitting
under a comment block that classifies every exception `KafkaProducer#commitTransaction` documents and
ends `Only catch and retry the retriable ones, others fail fast the control thread`. `IllegalState`,
`UnsupportedVersion`, `Authorization`, `KafkaException` and `InvalidProducerEpoch` are therefore not
retried at all - they propagate on first occurrence. A coarse taxonomy already exists.

It arrived in confluentinc#355 (2022-09-29), which replaced `catch (Exception e)` outright. The issue
predates it by more than a year and nobody re-read the code afterwards.

## What is actually still open

- **The retry set contradicts Kafka's own taxonomy, in both directions.** `TimeoutException extends
  RetriableException`; `InterruptException extends KafkaException` directly and is *not* retriable by
  Kafka's marker. PC retries the interrupt anyway, without honouring or restoring the interrupt.
  Whether to key off `RetriableException` instead of an enumerated list is the first design question.
- **`arbitrarilyChosenLimitForArbitraryErrorSituation` is a count, not a budget.** Each attempt blocks
  up to `max.block.ms`, so the worst case is that many multiples of it. astubbs#204 gave the *consumer*
  path a whole-operation budget, but `offsetCommitTimeout` is read only by `ConsumerManager` /
  `ConsumerOffsetCommitter`, and its javadoc says so - "Only relevant if using
  `PERIODIC_CONSUMER_SYNC`". Transactional mode has no user-visible commit budget at all. That
  asymmetry is the obvious fix shape.
- **The retry arm can report success without committing.** On a retry it calls `commitTransaction()`
  only when `isTransactionCompleting()`; the other branch merely logs, then `committed = true` is set
  unconditionally. Probably unreachable, because a timed-out commit leaves the manager COMMITTING -
  but nothing establishes that, which is the next bullet.
- **Nothing tests any of it.** No test in either source tree names `InterruptException`,
  `Retired too many times` or the retry limit. This is unasserted behaviour, not verified behaviour.
- **Fencing arrives by two different routes with two different types**: wrapped in
  `InternalRuntimeException` from `sendOffsetsToTransaction`, raw from `commitTransaction`.
  `core-recoverable-producer-fencing.md` and astubbs#225 describe only the first.

## Start from the better prototype, not the one the manifest names

The manifest points at `origin/tx-commit-failure`, a single 2021 WIP commit that retried
`IllegalStateException` and predates confluentinc#355 entirely.

`origin/bugs/prod-tx-manager-retries` (2022-11) is the one to read: it deletes the arbitrary constant
for a `RetrySettings` option (`FailureReaction` of `FAIL_FAST` or `RETRY_UP_TO_MAX_RETRIES`), adds
retry around `sendOffsetsToTransaction`, and introduces `PCCommitFailedException` and
`PCTimeoutException`. Not cherry-pickable - it calls `commitTransaction()` twice on the retry path and
dereferences the saved error before its own null check - but it is a design, not a sketch. **This
settles the open question in `branch-audit-orphans.md`**, which lists that branch as "plausibly
belongs to `sweep-2023-tx-failure-taxonomy`; verify before attaching": it does. Its exception types
are unrecorded input to `core-exception-hierarchy-cleanup.md`.

## What to put on the issue

Keep it open - the taxonomy question survives even though its stated premise does not - but the body
is what a future reader trusts, so replace `## Fork status` with something like:

> **Partly overtaken by events.** The "one generic retry loop" this issue describes was replaced in
> confluentinc/parallel-consumer#355 (2022-09-29): `commitOffsets` now retries only `TimeoutException` and
> `InterruptException` and fails fast on everything else. What remains is narrower. The retry set does
> not match Kafka's own marker in either direction - `TimeoutException` *is* a `RetriableException`,
> `InterruptException` is not and is retried anyway without honouring the interrupt. The limit is a
> count rather than a time budget, and `offsetCommitTimeout` covers only the consumer path, so
> transactional mode has no commit budget a user can set. None of it is covered by a test.

Relabel `bug` to `feature`: no defect is demonstrated here, and what is left is a policy design.

## Collisions

`ProducerManager` is contended. astubbs#262 and the `fix/transactional-produce-callback-abort` branch
both carry the produce-callback change that astubbs#261 already landed on master, so both need
reconciling against master before anything else touches the file; astubbs#257 does not go near
`commitOffsets`. None of them changes the commit retry loop, so a taxonomy change conflicts textually
at worst - but it must not land before astubbs#262, whose own notes record two further produce-path
mishandlings that belong in the same design.

Co-design partners, all three: astubbs#225 (fencing should abort and rejoin), astubbs#317 /
`core-commit-failure-seam.md` (let the application decide when a commit fails), and this. They are one
question - what PC does when a commit fails - and answering them separately will produce three
policies.

`docs/refactoring.md` carries a one-line `brute-force transaction-commit retry` triage item for the
same code; fold it into this note when the work starts.
