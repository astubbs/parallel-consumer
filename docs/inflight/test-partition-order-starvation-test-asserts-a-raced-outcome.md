# The PARTITION-order starvation test asserts a raced outcome, not a contract

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->

`PartitionOrderProcessingTest.allPartitionsAreNotProcessedInParallel` is the NEGATIVE half of a pair
(the positive half is `allPartitionsAreProcessedInParallel`). It asserts that under PARTITION ordering
with the default buffer, by the time 500 records have been processed at least one of five partitions
<!-- post-merge: checked - astubbs#517 is named as the merged PR the sighting happened on -->
has been served nothing. It went red on the astubbs#178 fix (astubbs#517) at a head that differed from a
passing head by three javadoc lines, and the re-run of the same head passed; the sighting is in
[`test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md) and this note is what that row points at.
Opened on the owner's question on that PR: if a comment-only change can turn it red, what does the
pair actually prove, and is the ordered scan under PARTITION covered by something that cannot?

## What the pair proves, and what it does not

- **The positive test is sound.** With `messageBufferSize(5000)` it waits up to 120s until every
  partition has been served at least once. That is a liveness claim with a bound, and it can only go
  red if some partition is never served - a real defect.
- **The negative test asserts an outcome, not a mechanism.** `pc.subscribe` precedes the 10000-record
  produce, so the consumer's first fetch races the producer. If the fetch lands after every partition
  holds 500 or more records, the poll returns one partition's batch, the 10-500 buffer fills with it,
  and the intake gate (`WorkManager#isSufficientlyLoaded`, read by `BrokerPollSystem`'s throttle)
  pauses the poller with one partition resident - starvation, test green. If the fetch lands while
  partitions hold ~100 records each, the first polls carry all five partitions - no starvation, test
  red. The failing run's `{0=109, 1=109, 2=109, 3=108, 4=109}` is the second shape. Which shape a run
  gets is producer-versus-fetch timing on the runner. Same code, either verdict.
- **The README states starvation as a possibility, not a promise.** The passage beginning "As default
  buffer size is calculated as `maxConcurrency * batchSize * loadFactor`" says threads *can* be
  starved *if* the incoming rate outruns processing *as* the small buffer fills from a subset of
  partitions. The negative test turns each of those conditionals into a fact it expects the broker to
  produce on cue. It is a demonstration of a documented limitation, written as an assertion.
- **Neither test exercises the shard scan's choice.** What decides which partitions get served is
  which partitions have records IN THE BUFFER, and that is decided upstream of `ShardManager` by the
  fetch composition and the intake gate. The scan only chooses among what is resident. So the pair
  says nothing about `ProcessingShard#getWorkIfAvailable`'s ordered loop, and a change to that loop
  (astubbs#178's per-candidate departure gate, for one) is neither caught nor exonerated by it.

## Where the ordered scan under PARTITION is actually covered, deterministically

- `WorkManagerTest.resumesFromNextShard` under PARTITION: three partitions resident, no revoke, one
  record drawn from each in turn, and a fourth draw returns nothing because each shard has one in
  flight. That is the direct proof that with no departure recorded every populated shard is served -
  the gate is inert.
- `WorkManagerTest.basic` under every ordering, and the confluentinc#236 starvation test in the same
  class (shards are not starved when the first shard alone can satisfy the request).
- `KeyOrderAcrossRebalanceTest`: `aRedeliveredKeyWaitsForItsOldEpochFlightToEnd` under both KEY and
  PARTITION (the gate withholds), `anotherPartitionIsNotHeldBackByThisPartitionsOldEpochFlight` (the
  <!-- post-merge: checked - past tense: the test landed with that PR -->
  gate withholds ONE shard's work, and the other partition goes out - added by astubbs#517 for this
  question), and `unorderedMakesNoPromiseAndIsNotHeldBack` (UNORDERED records nothing).

That set is the answer to "is the coverage good": the scan's drawing across shards under PARTITION,
with and without a departure in flight, is pinned by single-threaded tests that cannot flake. The
integration pair does not add to it.

## What to do with the negative test

Three options, and this is the decision the note is open on:

1. **Assert the mechanism, at unit level.** Build a `WorkManager` under PARTITION with the default
   options, register one poll holding one partition's 500 records, and assert two things with fixed
   inputs: `isSufficientlyLoaded()` is true (the buffer is full of one partition, so the poller would
   be paused), and `getWorkIfAvailable` hands out exactly one record (one shard, one in flight). That
   is the README's claim stated as what the code does, with no broker and no race. Then the
   integration test becomes redundant and is deleted, with the README passage still owning the advice.
2. **Keep the integration test but remove the race it depends on.** Produce BEFORE subscribing and
   wait for the acks, so every partition holds 2000 records at the first fetch. This narrows the
   window but does not close it: the Fetcher's completed-fetch order and `max.poll.records` still
   decide whether the first poll is one partition or several, and that is broker and client
   behaviour, not a contract PC makes.
3. **Delete it as a demonstration that never belonged in the suite.** It documents a tuning problem,
   which the README already does with numbers.

Lean: option 1, then option 3 for the integration test - the positive half stays. Sized at one unit
<!-- post-merge: checked - past tense: says what that merged PR chose not to do -->
test and one deletion; deliberately not done by astubbs#517, which was a key-ordering fix while this
test's subject is buffer tuning, and because the delete is the kind of change that should be its own
reviewable commit with the README cross-reference checked.

Provenance: the pair arrived with upstream PR confluentinc/parallel-consumer#682 (configurable
message buffer size), which added the README passage in the same change - the test was written to
demonstrate the passage. Prior art checked: `docs/test-hardening/inactive-tests-audit-2026-08-08.md`
counts the class as coverage for `messagesCanBeProcessedOptionallyPartitionOffsetOrder` (it is, for
the positive half);
[`../solutions/test-flakiness/parallel-integration-tests-flaky-under-concurrency-2026-07-28.md`](../solutions/test-flakiness/parallel-integration-tests-flaky-under-concurrency-2026-07-28.md)
lists the class among those that rotate red under suite parallelism, on timing signatures, which is
consistent with the mechanism above; `bin/inflight.mjs prior-art` found no plan or note on the test
across every ref.
