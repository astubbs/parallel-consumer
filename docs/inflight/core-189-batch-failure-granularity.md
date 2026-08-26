# One poison record starves its batch companions permanently - astubbs/parallel-consumer#189

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->


Mirror of [confluentinc issue #887](https://github.com/confluentinc/parallel-consumer/issues/887).
The design work is already done and ranked: the **poison-isolation ladder** in
[`docs/ideation/2026-08-17-batching-enhancements-ideation.html`](../ideation/2026-08-17-batching-enhancements-ideation.html)
- jitter, then batch bisection, then a per-record failure manifest. This note exists because that
ideation is filed under *batch composition* in
[`core-batching-enhancements.md`](core-batching-enhancements.md), whose deferral is gated on the
`confluentinc#915` composition-API decision - and **the first rung is not gated on it**. Separating
the two is the point.

## Verified at HEAD

The reporter's reading is correct, and the consequence is worse than "surprising". Four mechanisms
compose, all under `parallel-consumer-core/src/main`:

- `AbstractParallelEoSStreamProcessor.java`, the `for (var wc : workContainerBatch)` loop in the
  `catch` around `runUserFunctionInternal` - one thrown exception calls `onUserFunctionFailure` on
  **every** container. The user function is invoked before anything is marked succeeded, so no
  innocent record is ever distinguishable from the poison one.
- `WorkContainer.java`, `updateFailureHistory` - the retry due time is
  `lastFailedAt.get().plus(retryDelay)`, and `ParallelConsumerOptions.java`'s
  `DEFAULT_STATIC_RETRY_DELAY` is a flat `Duration.ofSeconds(1)` with no randomisation. The whole
  batch becomes due at the same instant.
- `AbstractParallelEoSStreamProcessor.java`, `makeBatches` - a plain fixed-size `partition` over the
  shard scan, and `ProcessingShard.getWorkIfAvailable` walks `entries` in offset order. The failed
  records hold the lowest available offsets in their shard, so the identical batch re-forms.
- **There is no retry ceiling** - the same finding astubbs#149 records as the DLQ's prerequisite - so
  it never ends. The innocent records' side effects re-execute once a second, unbounded, past what
  at-least-once implies.

The line the reporter quotes, "There is no guarantee that the messages will be retried again in the
same batch" (`src/docs/README_TEMPLATE.adoc`, and the generated `README.adoc`), is literally true and
practically inverted: in steady state it is always the same batch.

Nothing merged since the mirror was written has changed this. The merged PRs touching
`AbstractParallelEoSStreamProcessor.java` or `WorkContainer.java` are the package rename, the
issue-reference sweep, astubbs#177's commit-error reporting and astubbs#209's pool hardening.

## The seam is already half-built

`runUserFunctionInternal` declares `intermediateResults`, comments *"capture each result, against the
input record"*, never populates it, and returns it empty. Per-record result correlation was started
and abandoned, so **batch atomicity is an unfinished feature, not a design invariant** - which is what
makes the manifest rung "finish a seam" rather than hot-path surgery.

## Pending decision, maintainer-only

**Does the default retry delay get jitter?** Small change, and the existing per-record
`retryDelayProvider` (`WorkContainer.java`, `getRetryDelayConfig`) is both the workaround to give the
reporter today and the proof that desynchronising breaks the lockstep. But it changes retry timing
for every existing deployment, so it wants a go/no-go rather than silent shipping. Nothing else in
the ladder waits on that answer.

## Draft answer to the reporter, ready to post

astubbs#189 stays open - the ladder is real work - but the reporter has been waiting since 2025-08
and the workaround exists today. Post something like:

> Your reading is right, and it is not bad luck. On a batch failure every record in the batch is
> marked failed, the retry delay is a flat one second with no randomisation, and batches are cut as
> fixed-size runs over the shard's records in offset order. So the three records fail together,
> become due together, sit adjacently, and re-form the same batch - and because there is no retry
> ceiling, that repeats indefinitely. The README's "no guarantee they will be retried in the same
> batch" is literally true, but it reads as though re-batching were random when in steady state it
> is not, so you were right to expect otherwise.
>
> **What unblocks you now:** set a `retryDelayProvider` on `ParallelConsumerOptions`. It is invoked
> per record with a `RecordContext`, so returning a jittered or attempt-scaled delay (for example a
> base delay plus a random component, or one scaled by `getNumberOfFailedAttempts()`) desynchronises
> the three records. Only the due records are eligible when a batch is formed, so the good ones get
> taken alone and commit, and the poison record ends up retrying by itself.
>
> **What we intend to do about it:** ship default-on jitter, then bisect a failed batch to isolate
> the poison record in a bounded number of rounds, then let a batch function report which records
> failed so only those re-enter retry. Tracked here.

Nothing above is a promise about dates. Do not claim the manifest is close; only the first rung is.

## Overlaps to hold together

- **DLQ (astubbs#149, brainstorm astubbs#313, draft astubbs#8).** Both want a terminal disposition
  for a record that cannot succeed. A DLQ needs the retry *ceiling*; this needs failure
  *attribution*, and the per-record manifest is the natural place a DLQ hand-off fires from. Scope
  the contract once rather than twice.
- **`confluentinc#915` batch construction strategy.** It changes what a batch contains on first
  construction; bisection and the manifest change what it contains on retry, and bisection has to
  respect shard boundaries. The two collide at ordering. Jitter touches neither.

## Delete when

The jitter go/no-go is recorded, its rung has shipped or been declined, and astubbs#189 carries the
answer to the reporter.
