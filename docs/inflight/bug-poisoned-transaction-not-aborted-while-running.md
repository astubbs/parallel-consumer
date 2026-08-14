# A terminally failed send leaves the transaction abortable but unaborted

Opened by astubbs#261, which fixed the data-correctness half of this and deliberately left the
liveness half. Read that PR's "what this does not fix" section first; this note is the follow-up it
owes.

## What is open

Under `PERIODIC_TRANSACTIONAL_PRODUCER`, a result record that can never be sent - the client rejects
it outright, e.g. `RecordTooLargeException` - now correctly moves the transaction into an abortable
error state. Nothing then aborts it while PC is running:

- `abortTransaction()` is reached only from `ProducerManager#close`.
- The commit that would surface the error is gated on `wm.isDirty()`, and only a SUCCESS sets dirty.
  A failure does not.

So when the failing record is the only uncommitted work, nothing is dirty, no commit is attempted,
no error surfaces, and the instance keeps running with a dead transaction open until close. It looks
healthy. Under ordinary traffic the next commit attempt fails loudly and PC shuts down, which is why
astubbs#261's regression test has to inject "commit nudges" to reproduce anything at all - without
other traffic there is nothing to observe.

**No partial result set is published.** The all-or-none guarantee holds either way. This is a
liveness and observability gap, not corruption.

## The decision pending

Either accept it - it is strictly better than the silent partial commit it replaced - or make PC act
on a poisoned transaction rather than wait for close. Two shapes were considered in review:

1. Record the terminal send failure and have the controller abort the open transaction directly.
2. Treat it as a commit trigger that bypasses the `isDirty` gate, so the failing commit surfaces.

Both widen `ProducerManager`'s contract with the controller, which is why astubbs#261 did not take
them on. Whichever is chosen, it should be settled alongside the retry/DLQ work below rather than
in isolation - the three answers interact.

## The workaround users have today

Wrap the user function so a record that can never be sent never reaches the producer: validate or
size-check results and handle them yourself, rather than returning them for PC to send. This is the
same answer the README already gives for poison messages generally - "in your code: a configurable
retry delay function, record skipping, or a circuit breaker" - as opposed to Share Groups' broker
side `group.share.delivery.attempt.limit`.

## Why this belongs with dead-letter queues and retries

This is a poison record on the **output** side. The familiar poison pill is an **input** record whose
processing always fails; this is a result record that can never be produced. They share a root: PC
has no terminal-failure concept, so nothing can ever decide a record is undeliverable and route it
away.

- **No DLQ exists.** `docs/refactoring.md` records the deleted `poisonPillGoesToDeadLetterQueue`
  stub and confirms zero DLQ occurrences in `src/main/java`. Tracked as astubbs#149
  (`confluentinc#310`), with `confluentinc#366` the closed prior attempt. `docs/inflight/next-candidates.md`
  ranks it the most-demanded missing feature.
- **Retry is unbounded and purely time-based.** `defaultMessageRetryDelay` / `retryDelayProvider`
  set *when* to retry, never *whether to stop*. There is no max-attempt count and no terminal
  outcome, so a permanently unsendable record retries forever. `confluentinc#196` asks for exactly
  that missing max-retries-plus-callback.
- **The retryable half exists; the terminal half does not - and neither covers sends.** A user
  function can already signal retry by throwing the public `PCRetriableException`, which
  `AbstractParallelEoSStreamProcessor` recognises on the user-function failure path. What was never
  built is its opposite: no exception says *stop, this will never succeed*, so nothing can classify
  a failure as terminal. `confluentinc#242` (issue, closed) asked for the retry half and got it; its
  PR `confluentinc#291`, which added explicit terminal *and* retry types, is closed unmerged. Both
  concern exceptions the user's **code** throws, so neither classifies a *send* failure - read them
  as precedent for the shape, not as cover for this. Both are already accounted for and must not be re-mirrored: `confluentinc#291`
  fell in the 2023-06-15 swept-PR half of `sweep-2023-admin-closure` and is recorded in
  `upstream-map.yaml`; `confluentinc#242` is not sweep-affected, having been closed as completed by
  astubbs in 2022.
- **The send failure's own exception is misnamed**, which costs a rediscovery every time: the
  non-transactional path throws `InternalRuntimeException` for what is an expected operational
  state. Tracked in `docs/refactoring.md` under `internal/ProducerManager.java`; naming only, no
  behaviour change, and it does not wait on the retry work.
- **The failure-history control is inert.** See `bug-max-failure-history-is-inert.md` - a related
  decision that has to be made in the same area.

A DLQ or a max-attempt terminal outcome would give this defect somewhere to go: today the only
terminal states are "retry forever" or "kill the transaction".

## Do not re-derive

"poison" IS discussed in this repo and upstream - a search that misses it wastes a session. The
concept lives in `README.adoc` (the Share Groups comparison table), `docs/refactoring.md`, and
`src/docs/development/upstream-pr-analysis.adoc`. What did not exist before astubbs#261 is any
record of the transactional **send**-side case, which is this note.
