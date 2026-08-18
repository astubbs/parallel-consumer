# Truth probes: tests that can tell internal state from reality

Candidate work, unowned. Ranked here rather than in `docs/refactoring.md` because it is a testing
capability rather than a code refactor.

## The gap

Nothing in this repo could distinguish *"the counter says 0"* from *"there are 0 records in flight"*
until 2026-08-18, when one had to be built to settle a question. That absence is what let a fix be
written in April 2026 for counter drift **that does not exist** - and the fix itself then drove the
counter to -20 while records were genuinely in flight. Two independent defects, both from reasoning
about a symptom with no instrument to check the premise.

The two that now exist, and are the model:

- `OutForProcessingCounterDriftProbeTest` - runs the real engine on a `MockConsumer`, gates the user
  function so records are genuinely out with the pool, drives a revoke through the engine's own
  listener, waits for quiescence, then compares `numberRecordsOutForProcessing` against ground truth.
- `Rebalance857CommitSyncDeadlockProbeIT` - forces the revoke-during-commit overlap deterministically
  rather than waiting for a rebalance to draw it.

Both share the shape worth generalising: **arrange the state deliberately, then assert the internal
view against an independently computed truth** - not against itself, and not against "did it stall".

## Candidates, roughly by how much they gate behaviour

1. **Shard and queue depth** - `getNumberOfWorkQueuedInShardsAwaitingSelection()` feeds
   `isSufficientlyLoaded()` alongside the counter that already drifted. Same failure mode available.
2. **Incomplete-offset tracking** - the offset map decides what gets committed and therefore what is
   redelivered. Truth is derivable from what the harness produced and what the user function saw.
3. **Paused-partition state** - now derived from the consumer rather than mirrored, so a probe would
   pin that it stays derived; the mirror it replaced desynced silently under cooperative rebalancing.
4. **Epoch fencing** - that records from a revoked assignment are dropped and *their new owner
   receives them*, rather than merely "dropped".

## Why this is not just "more tests"

The existing suite asserts outcomes - records processed, offsets committed. A truth probe asserts
that PC's own *bookkeeping* matches reality, which is the thing that silently degrades: every
confluentinc#857-family stall is a stall with no error, and every one of them was visible in some
internal number before it was visible in behaviour.

Background:
`docs/solutions/workflow-issues/prove-the-problem-exists-before-writing-the-fix.md`,
`docs/solutions/architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md`.
