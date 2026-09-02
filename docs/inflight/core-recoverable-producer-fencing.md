# Next: make producer fencing recoverable instead of fatal

<!-- inflight-type: feature -->
<!-- inflight-impact: crash -->
<!-- inflight-state: deferred - after v6, nothing breaks by shipping without it -->


Scoped. `docs/plans/2026-09-02-001-feat-recoverable-producer-fencing-plan.md` owns the requirements,
the decisions and their rationale; this note keeps only what a reader needs before opening it.
Tracked as astubbs#225.

## Read the plan, not this note's history

This note previously carried the proposal from astubbs#225 verbatim: raise a recoverable exception,
abort the transaction, let the consumer rejoin. **Two of those three steps do not work**, and the
plan replaces them. Anyone designing from the older shape will build the wrong thing, which is why
the corrections are named here rather than left in the plan alone:

- **Abort is not available on a fenced producer.** `KafkaProducer#abortTransaction` documents
  `ProducerFencedException` as a fatal `@throws`, exactly as `commitTransaction` does. Kafka Streams
  calls it anyway and swallows the throw, because the broker has already aborted.
- **Rejoin is not the hard part.** Recovery needs a *new producer*, and PC cannot build one:
  `ParallelConsumerOptions` holds a finished instance, `ProducerWrapper` assigns it once from
  `options.getProducer()`, and a `KafkaProducer`'s configuration cannot be read back out. The plan's
  first requirement is therefore an ownership change, not an exception swap.
- **"Whether rejoin is expressible in PC's lifecycle" is answered, and the answer is yes.** No
  state-machine addition is needed on the commit path: the produce/commit lock pair already gives
  the control thread exclusive access at the moment fencing is detected. Only the produce path needs
  a worker-to-controller escalation.

## Two things this note is the only home for

- **The trigger with a real user report is on the produce path, not the commit path.** astubbs#411
  (`confluentinc#830`) hit `InvalidPidMappingException` after two days of producer inactivity and
  asked for precisely this feature; `confluentinc#839` answered it by shutting the instance down,
  which is the behaviour the plan reverses. The mirror was created on demand: the 2026-08-05 bulk
  import covered open upstream issues and the 2023 sweep cohort, and an issue closed in 2024 by a
  genuinely merged fix was in neither. `confluentinc#839` gets no mirror and no manifest entry - it
  is an upstream PR already carried in this fork's history, and the manifest keys on *fork* work.
- **It sequences after astubbs/parallel-consumer#352**, which merges first and unchanged. That PR's
  R6 is true of the behaviour it was written against; this work makes it untrue and owns the update,
  including the two fencing tests in its `ProducerManagerCommitBudgetTest`.

## Related, and not duplicated here

- `bug-857-transactional-revoke-wait.md` owns the unbounded wait in `onPartitionsRevoked`. It
  reduces how often the commit path reaches a fencing condition; it does not remove it, and the two
  are independent.
- `core-241-tx-commit-failure-taxonomy.md` owns the wider classification of transaction commit
  failures. This work adds one recoverable condition and deliberately does not widen further.
