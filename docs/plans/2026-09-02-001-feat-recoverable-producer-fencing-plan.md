---
title: Recoverable Producer Fencing - Plan
type: feat
date: 2026-09-02
topic: recoverable-producer-fencing
artifact_contract: ce-unified-plan/v1
artifact_readiness: requirements-only
product_contract_source: ce-brainstorm
execution: code
---

# Recoverable Producer Fencing - Plan

## Goal Capsule

**Objective.** Give Parallel Consumer a transactional producer it owns, so that when the broker tells it the producer is no longer usable, PC closes that producer, builds a replacement, and carries on — instead of spinning forever on the produce path or shutting the instance down on the commit path. Tracked as astubbs#225.

**Product authority.** `STRATEGY.md` > `AGENTS.md` > this plan > implementer judgement. The recovery shape is modelled on Kafka Streams' `TaskMigratedException` handling, but Streams is a reference and not a constraint: where PC can do better, it does.

**Open blockers.** None that stop planning starting. R13 and R14 state outcomes rather than mechanisms because every concrete mechanism proposed so far failed against PC's locking and thread confinement; planning must validate a mechanism for each before other units are committed, and a failed candidate reopens the recovery shape rather than merely the requirement.

**Execution profile.** Public API change to `ParallelConsumerOptions` with a deprecation, plus core changes in `ProducerManager`, `ProducerWrapper` and `ParallelEoSStreamProcessor`. Reverses one shipped behaviour (confluentinc#839).

---

## Product Contract

### Summary

PC accepts producer configuration and builds its own transactional producer through an overridable factory, rather than being handed a finished `Producer` instance. Owning the producer is what makes recovery possible: when the broker reports the producer invalid, PC discards it, builds a fresh one, and continues from redelivered records. Handing PC a producer instance stays supported and deprecated, without recovery.

### Problem Frame

A transactional producer can stop being usable for reasons that have nothing to do with the application being wrong. Under KIP-447, losing a rebalance race invalidates the producer's group generation. A broker can expire a producer id after a quiet period. A stale epoch can arrive from a commit that overran its generation. None of these mean the data is unsafe, and none of them mean the instance should stop.

PC has two answers today and both are wrong, in opposite directions.

On the **commit path**, `ProducerManager.commitOffsets` catches `ProducerFencedException` and rethrows it as `PCInternalRuntimeException`. Nothing between there and the supervisor loop catches it, so it becomes `failureReason`, triggers `doClose`, and the instance is gone. Offsets stay dirty and the records are redelivered, so the data is correct; only the liveness is wrong.

On the **produce path**, `ParallelEoSStreamProcessor` catches `InvalidPidMappingException` around the produce-and-ack block and calls `closeOnException`. That shutdown was itself a fix. Before it, PC retried the same record forever. A user hit exactly that in production and reported it as astubbs#411 (`confluentinc#830`), running `PERIODIC_TRANSACTIONAL_PRODUCER` with a per-instance UUID `transactional.id`, after two days of producer inactivity. Their own requested remedy was "close the producer that is causing this error and create a new producer instance". **Their configuration supplies a `Producer` instance** — today the only option — so it stays on the path that keeps its terminal behaviour, and the fix reaches them only once they move to the PC-built path. What shipped instead was `confluentinc#839`, which converted the spin into a shutdown, chosen on the maintainer's stated uncertainty about whether a transaction could be restarted without losing in-flight work.

**That catch fires only on a synchronous throw from the produce call, not on the ack wait.** `FutureRecordMetadata.valueOrError` raises `new ExecutionException(exception)`, so the future's `get` can only surface an `ExecutionException`, which falls past the typed catch into the generic handler that wraps it as `PCInternalRuntimeException("Error while waiting for produce results", e)` — verbatim the stack trace in the report. The regression test mocks the synchronous throw, so it passes without covering the reported path. R9 therefore has to unwrap before matching, and the reported spin may still be reachable on master; that is a defect in its own right, not something this work can assume already handled.

**The maintainer's doubt was better founded than it looks, and the obvious answer to it is wrong.** Offsets do stay dirty on a failed commit, but dirty is not the same as incomplete: `PartitionState.onSuccess` removes the offset from the incomplete set and raises the succeeded watermark at success time, while `onOffsetCommitSuccess` only records the committed offset and clears the dirty flag. Redelivery today is a consequence of the instance *dying* and rebuilding its offset state from the broker. An instance that survives keeps that in-memory completion, so the next commit would carry offsets whose output the abort discarded. Recovery therefore owes a mechanism for making that work processable again — the same pairing Kafka Streams makes between resetting its producer and closing its tasks dirty. Restoring the offset alone does not do it: `PartitionState.onSuccess` drops the `ConsumerRecord` with the offset and `ProcessingShard.onSuccess` retires the `WorkContainer`, so an offset restored without its work can never complete.

The library exists to keep processing under conditions that would stall a plain consumer. Dying when the broker hands back a routine, recoverable condition is the opposite of that.

The reason PC cannot recover today is structural rather than incidental. `ParallelConsumerOptions` holds a finished `Producer` instance and `ProducerWrapper` assigns it once from `options.getProducer()` into a final field. PC cannot read a `KafkaProducer`'s configuration back out, so it cannot construct a replacement. `initTransactions()` is called once from the `ProducerManager` constructor and never again, and `abortTransaction()` is reachable only from the close path. That same opacity is why `ProducerWrapper.discoverIfProducerIsConfiguredForTransactions` reaches into `KafkaProducer`'s private `transactionManager` field, under a comment that names the alternative and doubts it — "Nasty reflection but better than relying on user supplying a copy of their config, maybe".

### Key Decisions

- **PC builds its own producer from configuration, through an overridable factory.** (session-settled: user-directed — chosen over adding recovery around a user-supplied `Producer` instance: PC cannot read a finished producer's configuration, so it cannot build a replacement, and recovery is impossible without one.) Governs R1, R2, R3, R7.
- **PC derives the `transactional.id` wherever PC builds the producer.** (session-settled: user-directed — chosen over letting the user set it on the PC-owned path: a `transactional.id` shared by two live instances makes re-initialisation fence the other holder, which re-initialises and fences back.) Governs R4, R5, R6, R20.
- **The producer-instance option stays, deprecated, without recovery, and its removal is queued for the next major.** (session-settled: user-directed — chosen over removing it outright now: keeping it lowers the barrier for existing users, while queueing the removal stops producer handling being written twice indefinitely.) Governs R16, R17, R19, R21.
- **One recoverable condition, not a per-exception taxonomy.** (session-settled: user-approved — chosen over classifying each transaction failure separately: Kafka Streams collapses the same four exceptions into a single migration signal, and the response PC needs is identical for all of them.) Governs R8, R9, R10, R13, R14.
- **The produce-path case is absorbed rather than left to a separate change.** (session-settled: user-directed — chosen over scoping this to the commit path: it is the only trigger with a real user report, and recovery is better than both behaviours it replaces.) Governs R11, R18.
- **Recovery attempts are unbounded.** (session-settled: user-approved — chosen over a bounded attempt count: Kafka Streams has no migration counter at all, and PC owning the `transactional.id` removes the failure mode a bound would guard against.) Governs R12, R15.
- **Recovery is not observable to the application beyond logs and metrics.** (session-settled: user-approved — chosen over a public exception, a listener callback, or reuse of the commit-failure handler surface: recovery is PC's own business, like a retry, and nothing here forecloses adding a hook later.) Governs R22, R23, R24.
- **This work supersedes astubbs/parallel-consumer#352's R6 rather than amending it.** (session-settled: user-directed — chosen over rewording R6 before that PR merges: R6 was true of the behaviour when written, and a later change making an earlier true statement untrue is the ordinary order, not an error to correct pre-emptively.) Governs R10, R11.
- **Kafka Streams is the reference, not the specification.** Its shape is copied where it fits and diverged from where PC can do better; every divergence is named at the requirement it affects.

### Requirements

**Producer ownership and configuration**

- R1. `ParallelConsumerOptions` accepts producer configuration for every produce flow, from which PC constructs the producer. Recovery applies only where PC is transactional; the configuration path itself is not restricted to those flows.
- R2. Producer construction goes through a factory that callers may override, so a caller can wrap, instrument, or substitute the producer without supplying a finished one. PC passes the resolved configuration including the `transactional.id` it derived under R4, and each invocation returns a newly constructed producer rather than a cached or previously returned one.
- R3. The factory has a default that requires no caller action; supplying producer configuration alone is sufficient to use the produce flows.
- R4. Where PC builds the producer, PC sets the `transactional.id`. The value is unique per running instance, is stable for that instance's life, and is reused by every replacement producer so that re-initialisation fences the producer it replaces.
- R5. A caller-set `transactional.id` on the PC-built path does not take effect, and PC says so at WARN naming the value it derived instead.
- R6. The derived `transactional.id` uses a documented prefix derived from `group.id`, with only the uniqueness suffix varying, so a single prefixed TransactionalId ACL authorises it. No group's prefix is a string prefix of another's, since Kafka matches prefixed ACLs literally and an aliasing prefix would authorise fencing another application's producers.
- R7. Producer configuration values are never rendered raw into logs, exception messages, or any `toString()`. A diagnostic that renders configuration shows only keys on an explicit non-secret allow-list and redacts every other value unconditionally, with no mode that reveals them. Kafka's password typing is not sufficient on its own: it does not cover serializer, interceptor or Schema Registry secrets such as `basic.auth.user.info`.

**Detection and recovery**

- R8. PC treats the producer as invalid on `ProducerFencedException`, `InvalidProducerEpochException`, `InvalidPidMappingException`, `OutOfOrderSequenceException` (whose subclass `UnknownProducerIdException` it therefore also covers) and `CommitFailedException`, and responds to all of them identically. Only `CommitFailedException` is commit-path only; every other condition is reachable on both paths, because kafka-clients stores an abortable error and rethrows it wrapped in `KafkaException` from the next transactional call. Detection matches the full set on both paths.
- R9. Detection unwraps `ExecutionException` and any `KafkaException` wrapper before matching the conditions in R8, so a condition surfacing from a send future is recognised.
- R10. On detecting an invalid producer, PC attempts to abort the open transaction, closes the discarded producer under a bounded timeout tolerating a close that fails, builds a replacement through the factory, and resumes processing.
- R11. Recovery is reachable from both the commit path and the produce path.
- R12. Recovery repeats as often as the condition recurs, with no attempt limit and no terminal state reached by counting.
- R13. No offset from an aborted transaction is committed, and every record whose output that abort discarded is processed again. Returning the offset to incomplete does not by itself achieve this: `PartitionState.onSuccess` drops the `ConsumerRecord` along with the offset and `ProcessingShard.onSuccess` retires the `WorkContainer`, so an offset restored without its work can never complete and the partition stalls. What state PC retains to make the record processable again is a build decision.
- R14. Recovery ends with PC a full member of the consumer group, and the first commit after it carries group metadata the broker accepts for the group's live generation — metadata from a generation the group has moved past is refreshed rather than committed with. Where the generation never changed, as when a broker expires an idle producer id, the existing metadata is already live and nothing needs refreshing. Whether an explicit rejoin is required at all, and which thread performs it, is a build decision — PC's consumer is confined to the broker-poll thread and `onPartitionsRevoked` waits on the same write lock recovery holds, so the mechanism cannot be assumed.
- R15. A replacement producer that fails to build or initialise is retried on the next commit cycle with backoff rather than inline; a failure retrying cannot fix, such as an authorization denial, is terminal and names the `transactional.id` that was refused. While no usable producer exists, producing is suspended rather than left to fail records against a discarded producer. Suspension does not surface to the user function as produce-lock-acquisition or send timeouts — the produce path's existing bounded waits pause or are exempted while the window is open — and a transition to the terminal state releases suspended work so shutdown does not wait on records that can never be produced.

**Compatibility and migration**

- R16. Supplying a `Producer` instance continues to work for every flow it works for today, and is marked deprecated.
- R17. Supplying both producer configuration and a `Producer` instance fails options validation rather than resolving silently to one of them.
- R18. On the PC-built path, `InvalidPidMappingException` on the produce path recovers rather than closing the instance, replacing the behaviour `confluentinc#839` intended.
- R19. On the producer-instance path, every condition in R8 keeps its current behaviour — which for a condition arriving wrapped from a send future is the spin the Problem Frame identifies, not a terminal close. PC logs a single WARN at options validation — not when a condition first arises — naming the configuration-plus-factory replacement, the absence of recovery, and the release the removal is queued for.
- R20. The upgrade notes carry the whole move from a `Producer` instance to configuration plus factory, including re-granting any TransactionalId ACL against the prefix in R6 and the loss of any caller-set `transactional.id` that operational tooling keys on.
- R21. The `Producer`-instance option's deprecation javadoc names the major *after* the release that ships the deprecation as the one its removal is queued for, matching the entry in `docs/refactoring.md`. Naming the current release would remove the option in the same version that deprecates it, contradicting R16.

**Observability**

- R22. Each recovery emits a log record identifying the triggering condition and the outcome.
- R23. Recoveries are counted in the metrics surface, distinguishably from ordinary commit failures.
- R24. Consecutive recoveries with no successful commit between them are counted separately and raise the log level, so an instance that is alive but not progressing is distinguishable from one that recovered once.

### Actors

- A1. The application — supplies producer configuration, and optionally a factory; does not manage producer lifecycle.
- A2. The user function — runs on a worker thread and returns records for PC to produce; sees no new failure type as a result of this work.
- A3. Parallel Consumer — owns producer construction, transaction lifecycle, detection, and recovery on the PC-built path.

### Key Flows

The two arrival paths differ in which thread observes the condition and which lock it holds, and that difference is what recovery has to bridge.

```mermaid
flowchart TB
  A[Broker reports the producer invalid] --> B{Which path observed it}
  B -->|Commit path| C[Control thread, write lock held]
  B -->|Produce path| D[Worker thread, read lock held]
  C --> E[Recover in place]
  D --> F[Signal the control thread]
  F --> G[Control thread takes the write lock, readers drain]
  G --> E
  E --> H[Abort what can be aborted, discard producer, rebuild under the same transactional id]
  H --> I[Release the lock; discarded work restored before any further commit]
  I --> J[Reprocess; no aborted-transaction offset is committed; membership refreshed]
```

- F1. Commit-path recovery
  - **Trigger:** A condition in R8 is raised while PC is committing offsets inside a transaction.
  - **Actors:** A1, A3
  - **Steps:** The control thread already holds the commit write lock, so no worker can be producing. PC aborts what can be aborted, discards the producer, builds a replacement under the same `transactional.id`, releases the lock, and ensures the work whose output the abort discarded is reprocessed before any subsequent commit. Whether that restoration happens inside the locked region or after release is part of R13's build decision, and R14's membership outcome is reached after release, not inside this step.
  - **Outcome:** The instance keeps running and the work is redelivered.
  - **Covers R8, R10, R11, R13, R14.**

- F2. Produce-path recovery
  - **Trigger:** A condition in R8 surfaces from a send future on a worker thread, wrapped in `ExecutionException`.
  - **Actors:** A1, A2, A3
  - **Steps:** The worker unwraps the condition from its `ExecutionException`. It holds the produce read lock, so it cannot replace the producer itself: it reports the condition and releases its lock. The control thread acquires the write lock, which drains the remaining readers, then recovers as in F1.
  - **Outcome:** The instance keeps running; the affected records are redelivered.
  - **Covers R8, R9, R10, R11, R13, R14, R18.**

- F3. Recovery unavailable
  - **Trigger:** A condition in R8 arises while PC is using a caller-supplied `Producer` instance.
  - **Actors:** A1, A3
  - **Steps:** PC cannot build a replacement, so it behaves as it does today. The single WARN naming the remedy was already emitted at options validation under R19; the failure itself adds no new WARN.
  - **Outcome:** As today, and as today it is not uniformly terminal — a wrapped send-future condition still spins. The reason and the remedy are stated at startup.
  - **Covers R16, R19.**


### Acceptance Examples

- AE1. **Covers R8, R10, R11, R13, R14.** Given PC is running with PC-built producer configuration and a transaction is open, when the broker raises any of the recoverable conditions during offset commit, then the instance is still running afterwards, no offset from that transaction is committed, every record in it is processed again, and the first commit after recovery carries group metadata the broker accepts for the live generation.
- AE2. **Covers R9, R11, R18.** Given the same configuration, when `InvalidPidMappingException` surfaces from a send future on a worker thread, then PC builds a replacement producer, the affected record is processed again, and its offset is committed within a bounded number of cycles. Asserting only that the instance stays alive would pass on master today, where the outcome is the original spin.
- AE3. **Covers R12.** Given the condition recurs on several consecutive cycles, when each recovery completes, then PC recovers again each time and reaches no terminal state through repetition alone.
- AE4. **Covers R4, R5, R6.** Given a caller sets a `transactional.id` in producer configuration on the PC-built path, when PC constructs the producer, then the value PC derived is used, a WARN names the override, every derived id begins with the documented `group.id`-derived prefix with only the uniqueness suffix differing, and two instances of the same application never share an id.
- AE5. **Covers R16, R19.** Given a caller supplies a `Producer` instance, when options are validated, then a single WARN names the unavailable recovery, what enables it, and the release the removal is queued for; and when a condition in R8 later occurs, behaviour matches today's with no further WARN.
- AE6. **Covers R17.** Given a caller supplies both producer configuration and a `Producer` instance, when options are validated, then construction fails with a message naming both.
- AE7. **Covers R7.** Given producer configuration carrying SASL and TLS secrets, when PC starts and logs its options, then no credential material appears in the output.
- AE8. **Covers R13.** Given a record completed successfully earlier in a transaction that is then aborted by recovery, when the next commit runs, then that record's offset is not committed and the record is processed again.
- AE9. **Covers R15.** Given the replacement producer cannot reach the transaction coordinator, when recovery runs, then PC retries on a later cycle with backoff rather than looping inline; and given the `transactional.id` is not authorised, then PC fails terminally naming that id.
- AE10. **Covers R24.** Given recovery fires repeatedly with no successful commit between attempts, when the pattern continues, then the consecutive count is observable and the log level rises above the single-recovery case.

### Scope Boundaries

**Deferred for later**

- The wider transaction-failure taxonomy from astubbs#241 (`confluentinc#144`) — the arbitrary retry limit in `ProducerManager.commitOffsets` and the undifferentiated use of `PCInternalRuntimeException` for expected operational states. This plan introduces one recoverable condition; classifying the rest is separate.
- The consumer remains a caller-supplied instance. The same argument for configuration-plus-factory applies to it, and is not this problem.
- The unbounded wait at `onPartitionsRevoked` for an in-flight transaction to finish, owned by `docs/inflight/bug-857-transactional-revoke-wait.md`. astubbs#29 bounded the `PERIODIC_CONSUMER_SYNC` revoke path by declining the commit lock; the transactional wait is untouched. It reduces how often the commit path reaches a fencing condition without removing it.
- The commit-failure seam in astubbs/parallel-consumer#352, which gives the application a decision when a commit exhausts its retry budget. Dependencies / Assumptions owns the relationship and the sequencing.
- Closing the wrapped-send-future spin on the *producer-instance* path, owned by `docs/inflight/bug-411-wrapped-send-failure-spins-forever.md`. R9's unwrapping applies where PC builds the producer; the deprecated path keeps the behaviour the Problem Frame identifies as a live master defect, and closing it there is its own item rather than something this work silently absorbs.
- Actually removing the `Producer`-instance option. R21 only requires the deprecation to name its release; the removal is queued in `docs/refactoring.md` under the next-major section and lands there, not here.
- The remaining reflection in `ProducerWrapper` for transaction state (`isCompleting`, `isReady`). Owning the configuration removes the need to *discover* whether the producer is transactional; it does not remove these.

**Outside this work**

- The Kafka Streams execution seam (astubbs#255). `PcTaskDispatcher` builds PC with a stub consumer and no producer, taking only the work manager, so PC has no producer on that path and nothing here reaches it.
- Any change to the guarantee that a partial result set is never published. Recovery preserves it rather than relaxing it.

### Dependencies / Assumptions

- Assumes aborting a transaction on an already-invalid producer may itself raise, and that this is expected rather than fatal — Kafka Streams' `StreamsProducer.abortTransaction` swallows exactly these exceptions on the grounds that the broker or transaction coordinator has already aborted the transaction.
- The consumer is confined to the broker-poll thread by `ThreadConfinedConsumer`, and `onPartitionsRevoked` opens by spinning on `isTransactionCommittingInProgress()`, which is the write lock recovery holds. Any group-membership action taken from the control thread, or inside the write-locked region, deadlocks or throws. This is why R14 states an outcome rather than a mechanism.
- Zombie fencing comes from every transaction carrying `sendOffsetsToTransaction(offsets, ConsumerGroupMetadata)` with a live generation, not from the `transactional.id` — R4 makes the id unique per instance, so it fences only the producer it replaces. No derivation chosen at planning time may be relied on for cross-instance fencing.
- R4's uniqueness is scoped to a single run, so a restart does not fence its predecessor: a crashed instance's open transaction waits out `transaction.timeout.ms`, blocking `read_committed` consumers, and each run leaves a distinct id in the coordinator's state. Kafka Streams avoids this with an identity persisted across restarts; PC has no equivalent today. Recorded as an accepted cost, not a defect.
- Assumes PC's existing produce/commit lock pair provides the exclusion recovery needs on the commit path: producing takes the read lock of `producerTransactionLock` and committing takes the write lock, with `preAcquireOffsetsToCommit` acquiring and flushing before `commitOffsets` runs. No new quiescing mechanism is assumed.
- Assumes `group.id` is available to derive the R6 prefix from, matching how Kafka Streams derives one from `application.id`. This plan adds no separate prefix option; R6 makes the derived prefix documented and stable so a single prefixed ACL covers it.
- Does **not** assume redelivery is already correct. Dirty offsets alone do not achieve R13: `PartitionState.onSuccess` clears an offset from the incomplete set at success time, long before any commit, so an instance that survives the abort keeps that completion. Un-completion is a mechanism this work owes, not a property it inherits.
- Sequenced after astubbs/parallel-consumer#352, which merges first and unchanged. That PR's R6 — a fenced transactional producer stays immediately fatal without consulting its handler — is a true statement of the behaviour it was written against, held by `producerFencedOnSendOffsetsStaysFatalAndNeverReachesTheCommitLoop` and `recoveryAbortFailureStaysFatalAndHandlerFree` in `ProducerManagerCommitBudgetTest`. This work makes it untrue and therefore rewrites both, along with that PR's own R6 line and the corresponding statements in its commit-failure-seam feature file and README section. Every "R6" elsewhere in this plan is this plan's own R6, the derived-prefix requirement. Per `AGENTS.md`, the reasoning being overridden is recorded where it is overridden, so the change does not read as an oversight. Both PRs edit `ProducerManager` and `ProducerWrapper`, so textual conflicts are expected independently of this.
- The two remain complementary in what they own: astubbs/parallel-consumer#352 decides who chooses when a commit exhausts its budget; this decides what PC does when the producer itself is unusable. Only the fencing branch inside `ProducerManager.commitOffsets` is shared.

### Outstanding Questions

**Deferred to planning**

- Whether the recoverable condition is represented as an internal exception type, a state on `ProducerManager`, or a signal on the existing commit-response channel. R8 fixes what is detected; how it travels is a build decision.
- How a worker signals the control thread on the produce path without adding a second wait with its own deadline.
- What state PC retains so a record whose output was discarded can run again (R13) — retaining each `WorkContainer` until the commit covering it succeeds, versus a seek-based re-fetch — and what bounds its memory, given shard back-pressure accounting.
- How R14's group-membership outcome is reached: whether an explicit rejoin is needed at all once R13's restoration exists, which thread issues it, and what channel carries the request. Note that `enforceRebalance` is a no-op under the KIP-848 consumer protocol, so a caller on that protocol would silently get nothing.
- Whether `State` gains a member for the window in which the producer is being replaced, or whether the write lock alone is sufficient to make that window invisible.
- The exact `transactional.id` derivation, given that the integration harness in `parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/utils/KafkaClientUtils.java` already invents random ids to stop tests fencing each other, and that workaround becomes product behaviour. No plain prefix-plus-`group.id`-plus-delimiter scheme satisfies R6, since group ids may contain the delimiter and `app` then yields a literal prefix of `app-x`; a prefix-free encoding such as a length-prefixed or digested group id is required, at the cost of the raw id's greppability.

### Sources / Research

- astubbs#225 is the tracking issue. `docs/data/roadmap.yaml` carries it as `survive-producer-fencing`, horizon `next-0x`, `blocks_1_0: false`.
- astubbs#411 (`confluentinc#830`) is the only field report: `PERIODIC_TRANSACTIONAL_PRODUCER`, UNORDERED, per-instance UUID `transactional.id`, triggered by two days of producer inactivity. `confluentinc#839` is the shutdown that answered it, merged upstream and carried in this fork's history, still visible as the `InvalidPidMappingException` catch in `ParallelEoSStreamProcessor`.
- `docs/inflight/core-recoverable-producer-fencing.md` is this work's in-flight note; it names the corrections this plan makes to the issue's own proposal and carries the facts that live nowhere else.
- astubbs#241 (`confluentinc#144`) records that `ProducerManager.commitOffsets` treats every commit failure identically, and says astubbs#225 and it should be designed together. `docs/inflight/core-241-tx-commit-failure-taxonomy.md` corrects that premise: a coarse taxonomy has existed since confluentinc#355, and the retry loop already catches only `TimeoutException` and `InterruptException`.
- Kafka Streams 3.9.2 is the reference implementation: `StreamsProducer.commitTransaction` collapses `ProducerFencedException`, `InvalidProducerEpochException`, `CommitFailedException` and `InvalidPidMappingException` into `TaskMigratedException`; `StreamThread.handleTaskMigrated` closes tasks and resubscribes; `TaskManager.handleLostAll` calls `reInitializeThreadProducer` under EOS-v2, reaching `StreamsProducer.resetProducer`, which closes the producer and asks `KafkaClientSupplier` for another. `DefaultKafkaClientSupplier.getProducer` is the one-line default that makes the supplier free to callers. `StreamsConfig` lists `TRANSACTIONAL_ID_CONFIG` in `NON_CONFIGURABLE_PRODUCER_EOS_CONFIGS` and warns-and-drops a caller-set value.
- KAFKA-14567 ("Kafka Streams crashes after ProducerFencedException") is the reason Streams' shape is copied without assuming it is airtight.
- `TransactionManager`'s `TxnOffsetCommit` handler in kafka-clients 3.9.2 is where four of the broker error codes behind R8's conditions diverge in the client: `UNKNOWN_MEMBER_ID` and `ILLEGAL_GENERATION` become an abortable `CommitFailedException`, while `INVALID_PRODUCER_EPOCH` and `PRODUCER_FENCED` become a fatal `ProducerFencedException`. PC catches only the latter today.
- No test exercises producer fencing, transaction abort, or recovery. One test does pin the behaviour R18 replaces: `ParallelEoSStreamProcessorTest.closePCWhenInvalidPidMappingException` asserts the produce-path shutdown, and must be rewritten under R18 — though it mocks a synchronous throw from the produce call rather than the wrapped failure the field report shows, so it does not cover the reported path. The only other occurrence of the concept in the test tree is the comment in `KafkaClientUtils` explaining why test producers are given random transactional ids.
- The defect class does not recur outside the core module: `produceMessages` is referenced only in `parallel-consumer-core`, and the vertx, reactor and mutiny processors inherit the core produce path through `ExternalEngine` rather than duplicating it.
