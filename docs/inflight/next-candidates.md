# Next candidates, ranked

Collisions are in `pr-blockers-and-collisions.md`. The ranked backlog and full verdicts live in
`src/docs/development/upstream-pr-analysis.adoc`; these are the ready picks:

- **`confluentinc#912` vertx leak** - branch done, needs rebase + PR (`branch-912-vertx-leak.md`). Best
  immediate pick.
- **Logging-verbosity cleanup** - batch `confluentinc#629` / `#631` / `#640` into one PR
  (`ConsumerOffsetCommitter`, `RemovedPartitionState`, `AbstractParallelEoSStreamProcessor`). Low
  effort, high return.
- **Contributor-friction build fixes** - `confluentinc#162` (mvn compile without test-jar),
  `confluentinc#861` (`ManagedTruth` not found), `confluentinc#906` (pom version mismatch).
- **Security dependency bumps** - `confluentinc#851` (postgres), `confluentinc#913` (assertj); pom-only.
- **[#40](https://github.com/astubbs/parallel-consumer/issues/40)** - dedup the `MockConsumer*` test
  classes (test-only; the duplication bot keeps flagging them).
- **`confluentinc#915` batch construction strategy** - cherry-pick, closes the 4-year-old
  `confluentinc#266`. Medium effort.
- **DLQ** (`confluentinc#310`, or revive `confluentinc#366`) - the most-demanded missing feature. Large, and
  spec-stage only.

## Three instances of the deferral-priced-at-a-blocking-wait defect, found in core

Found by the same-defect-class sweep after wake-on-work landed in
`parallel-consumer-streams` (astubbs#255 item 3). The class: **work is deferred to "the next time the loop
runs", and that deferral is silently priced at a blocking wait's full duration.** All three are in
`parallel-consumer-core`, all three pre-date that work, and none was touched by it. The adapter modules
(vertx, reactor, mutiny) were checked and are **clean** - they route completions into core's mailbox rather
than owning a wait, and all three get the publish-then-signal ordering right.

1. **`BrokerPollSystem` ↔ `ConsumerOffsetCommitter`: the wake that shortens a 2s long poll is droppable.**
   `ConsumerManager.wakeup()` only calls `consumer.wakeup()` `if (pollingBroker.get())`, and that flag is
   false for the whole stretch between polls - registering work, committing, pausing/resuming, cache
   updates. A commit request landing in that window has its wakeup swallowed and waits out the next full
   2s poll. In `PERIODIC_CONSUMER_SYNC` this stalls the entire control loop, not just the commit.
2. **The intended mitigation for (1) is dead code, and is the edge-triggered form the streams work
   rejected.** `ConsumerManager.commitRequested` is a **non-volatile** boolean, set by
   `onCommitRequested()` - which has **zero callers** - and cleared by the waiter. Wired up as written it
   could lose the signal both ways: set-before-arm, and no happens-before at all. The level-triggered
   alternative is already there: read `commitRequestQueue` directly. Either delete it or wire it up
   level-triggered.
3. **`getTimeToBlockFor` rounds a retry deadline up to the full static retry delay.**
   `AbstractParallelEoSStreamProcessor:1243` discards `lowestScheduled` (a time *remaining*) whenever it is
   below `defaultMessageRetryDelay`, so a record retriable in 10ms is waited on for 1s. Nothing can end
   that wait early - retries are a timer, with no thread and no signal - and the branch is only reached
   when the pool is idle, so no completion is coming either. The `// todo can sleep for less than this
   time?` at `:1238` is asking exactly this.

Worth noting `PcWorkSignal`'s javadoc already anticipates (3) for the streams path: a retry timer is the
second thing that can make work dispatchable, and it must raise the signal when retries are re-enabled
there.
