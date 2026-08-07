# Two pre-existing EoS holes on the produce path, found by review not by test

Both surfaced by the code review of the transactional battle test. Neither is introduced by that work,
and neither is fixed there - a main-code correctness fix belongs in its own PR with its own diagnosis,
not smuggled into a test branch. Recorded so they are not lost.

## 1. An InvalidPidMappingException marks the whole batch SUCCEEDED

`ParallelEoSStreamProcessor#processAndProduceResults` catches `InvalidPidMappingException`, calls
`closeOnException(e)` - and does **not** rethrow. Control falls through to `return results;` with a
partial, possibly empty, result list.

`runUserFunctionInternal` then calls `onUserFunctionSuccess` and `addToMailBoxOnUserFunctionSuccess`
for **every** `WorkContainer` in the batch. So records whose output records were never produced are
marked succeeded, and their offsets become commit payload.

That is an exactly-once violation by the same shape as the partial-result-set defect astubbs#261
fixed: output missing, offset advanced. The trigger is real rather than theoretical -
`pm.produceMessages(...)` is inside the try, so an `InvalidPidMappingException` raised by
`lazyMaybeBeginTransaction` / `beginTransaction` lands here directly. A send-ack failure arrives
wrapped in `ExecutionException` and correctly falls to the generic arm instead, which is why this has
stayed hidden.

**Why it matters more now.** astubbs#261's rationale is that not throwing from the produce callback
is safe *because* "the failure still reaches the work container either way - `processAndProduceResults`
waits on each returned `Future`". That holds for every arm except this one.

**Fix shape:** rethrow after closing, so the batch fails rather than reporting success. If some caller
genuinely depends on the quiet close, unwrap `ExecutionException` causes before the type check and
still rethrow. Silently returning `results` for a batch that did not produce must not remain an
option.

**Test shape:** a unit test with a producer whose `beginTransaction` throws `InvalidPidMappingException`,
asserting the `WorkContainer`s are NOT marked succeeded.

## 2. A throwing abortTransaction skips closing the producer

`ProducerManager#close(Duration)` aborts an open transaction inside a `try { ... } finally {
releaseCommitLock(); }`, and calls `closeProducer(timeout)` *after* that block. The `finally` releases
the lock but does not contain the abort's exception, so a throwing `abortTransaction()` skips
`closeProducer` entirely.

`abortTransaction()` throws precisely in the states this project now deliberately creates: a fenced
producer (`ProducerFencedException` / `InvalidProducerEpochException`) or a poisoned one ("we are in
an error state"). And `AbstractParallelEoSStreamProcessor#innerDoClose` calls
`producerManager.ifPresent(x -> x.close(timeout))` **without** the try/catch its three neighbouring
shutdown steps all have.

Net effect: after any fencing or poisoned-transaction event, `pc.close()` leaves the KafkaProducer's
IO thread, sockets and buffers alive, while `doClose`'s finally still marks the instance CLOSED. A
host that restarts PC instances leaks a producer per fenced shutdown.

**Fix shape:** make the producer close unconditional - wrap the abort in try/catch, log, do not
rethrow, so control always reaches `closeProducer(timeout)`. Optionally give the `innerDoClose` call
the same try/catch(log.warn) the preceding steps use.

**Test shape:** `ProducerManagerTest` already builds a `ProducerManager` over a controllable wrapper;
a throwing-abort variant asserting `close` was still called on the producer fits there.

## Why neither is fixed in the battle-test branch

That branch's scope boundary is explicit: a red test implicating main code is the deliverable, and the
fix is a separate PR. It made one exception - astubbs#261 - and split it out precisely so the fix
could be reviewed on its own evidence rather than buried under 5,000 lines of test. These two deserve
the same treatment, and the first of them - the `InvalidPidMappingException` hole - in particular
deserves a reviewer looking only at it.
