# Should the produce callback still throw in non-transactional mode?

<!-- inflight-type: task -->
<!-- inflight-impact: reliability -->

`ProducerManager`'s `sendCallback` throws from `Callback#onCompletion` when the producer is **not**
transactional, and does not when it is. The asymmetry is deliberate and documented; whether it should
exist at all is an open decision about the library's error contract, and it is nobody's to take
inside a test PR.

<!-- post-merge: checked - past tense about what that PR found; a PR number is permanent -->
Raised as a review finding on astubbs#262 and answered there, but it is not a defect that PR
introduced or can close - so it is recorded here rather than left as an open review thread gating a
merge indefinitely.

## What the code does

```java
boolean usingTransactions = producerWrapper.isConfiguredForTransactions();
this.sendCallback = (RecordMetadata metadata, Exception exception) -> {
    if (exception != null) {
        log.error("Error producing result message", exception);
        if (!usingTransactions) {
            throw new PCInternalRuntimeException("Error producing result message", exception);
        }
    }
};
```

Grep `Installed on every send` in `ProducerManager` for the javadoc that owns the reasoning.

## Why the transactional half had to change

`KafkaProducer#doSend` invokes the callback from inside its own `catch (ApiException)` handler and
only *afterwards* calls `transactionManager.maybeTransitionToErrorState(e)`. A throw escapes before
that runs, so a terminally failed send never moved the transaction into an abortable state: the
records already accepted stayed in it, the next commit succeeded, and a `read_committed` consumer saw
a PARTIAL result set for one source offset. astubbs#261 fixed that by not throwing under transactions.

## The open question

The reviewer's point is that the justification for *not* throwing does not depend on the mode.
`processAndProduceResults` blocks on every returned `Future`, and an exceptionally-completed send
fails the record for retry - in both modes. So two error-signalling mechanisms coexist, selected by a
boolean, when one may be sufficient.

**What narrows it, and was not established when the question was first asked:** the throw was only
ever observable on the **synchronous** pre-accumulator path, in both modes. When a send fails
asynchronously, Kafka's own `ProducerBatch.completeFutureAndFireCallbacks` catches whatever a callback
throws and logs `"Error executing user-provided callback on message for topic-partition"`. So on the
async path there is already one mechanism, not two. What remains is local, synchronous failures -
serialization, buffer exhaustion, `max.request.size` - where a non-transactional user does still get a
throw out of `doSend`.

## The options

- **(a) Remove the throw unconditionally**, leaving the failed `Future` as the single mechanism.
  Coherent, one error path. It is a **behaviour change to the non-transactional error contract**:
  today a synchronous failure unwinds the send loop, so later records in the batch are never
  attempted; without the throw they are all sent and the failure surfaces at the first `.get()`. That
  is observable and wants its own test.
- **(b) Keep the gate and document why the modes differ.** Effectively done - the javadoc now carries
  the transactional reasoning *and* the async-inertness fact. If (b) is the answer, this note is the
  record of it and can be deleted.
- **(c) Leave as-is** without deciding, which is where it currently sits.

## What would settle it

Not a reading. A test that fails a send **asynchronously** and asserts the callback throw never
surfaces would confirm the inertness claim above, which is currently read from Kafka's bytecode rather
than demonstrated. Then (a) becomes a small, testable change in its own PR.

## Relationship to other work

Adjacent to **astubbs#241 / confluentinc#144** ("ProducerManager should handle different types of
transaction failures appropriately"), which is the umbrella for exactly this class of question -
worth folding into that design rather than answering alone.

**Not the same as astubbs#225**, which is `ProducerFencedException` during a *commit* killing the
instance when it should abort and rejoin. Different exception, different path, different question;
they are neighbours in `ProducerManager` and nothing more.

There is a `TODO(refactor)` on `sendCallback` about `PCInternalRuntimeException` misnaming a failed
send - a separate, smaller item, tracked in `docs/refactoring.md`.
