# Commit-failure seam for the async commit mode

<!-- inflight-type: feature -->
<!-- inflight-impact: reliability -->
<!-- inflight-state: deferred - needs the async dirty-tracking refactor, and no demonstrated demand yet -->

The commit-failure seam (astubbs#317, landed for the sync and transactional commit modes) cannot
operate under `PERIODIC_CONSUMER_ASYNCHRONOUS` - the shipped default commit mode - and the exclusion
was a deliberate, user-directed scoping: making it work there is a project of its own, not a wiring
job.

## Why the seam cannot reach async today

Two independent gaps, both structural:

- **No budget, no exhaustion event.** `ConsumerManager#commitAsync` is a thin wrapper with no retry
  loop and no timeout; nothing can ever throw the exhaustion event the handler triggers on.
- **Offsets are marked clean optimistically.** The async path runs `onOffsetCommitSuccess` the
  moment the request is *fired*, before the broker answers; a later failure arrives in a log-only
  callback with no dirty state left to retry. The seam's CONTINUE semantics ("offsets stay dirty,
  recommit next cycle") have nothing to attach to.

So the work is: give async commits a whole-operation budget (the `offsetCommitTimeout` shape the
sync and transactional paths share since astubbs#177), and defer clean-marking until the broker's
callback confirms - a dirty-tracking refactor of the async commit path. rkolesnev proposed the
retry half upstream on confluentinc/parallel-consumer#833 ("add X number of retries for async
commit ... making it configurable") before the fork.

## What guards the gap meanwhile

`ParallelConsumerOptions#validate()` rejects a non-default `commitFailureHandler` or continue mode
combined with the async commit mode, naming the supported modes - the misconfiguration fails fast
instead of being silently inert. The README's feature material and
`docs/features/commit-failure-seam.yaml` state the limitation.

## Delete this file when

The seam operates under the async commit mode, or the gap is closed as won't-do with the reasoning
recorded on an issue.
