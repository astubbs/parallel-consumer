# Shrink the five next-largest classes: `PartitionState`, `WorkContainer`, `ParallelConsumerOptions`, `ShardManager`, `BrokerPollSystem`

<!-- inflight-type: task -->
<!-- inflight-impact: refactor -->

Recorded 2026-09-08 alongside
[`core-decompose-abstract-parallel-eos-stream-processor.md`](core-decompose-abstract-parallel-eos-stream-processor.md),
which owns the God class, the merge-base measurement script, and the rules that bind any cut in the
engine. This note is the same question for the five classes below it in size: what each one holds,
which open PRs rewrite it and so must land first, which notes already track a defect inside it, and
where a cut would go. The per-file lines `docs/refactoring.md` keeps for these classes are the small
tidy-ups; they stay there, and nothing here restates them.

**Which five, and why not others.** Rank main code by `wc -l` and take the five below the God class:

```bash
git ls-files 'parallel-consumer-*/src/main/java/**/*.java' | xargs wc -l | sort -rn | head -12
```

`ProducerManager`, `PartitionStateManager`, `WorkManager` and `ProcessingShard` sit just under these
and share their blockers; they are named where a cut reaches them but get no section of their own.

**The order across the five, by what is blocked on least:** `ParallelConsumerOptions` and
`BrokerPollSystem` are cuttable once astubbs#352 is decided; `WorkContainer` once astubbs#468 and
astubbs#359 land; `ShardManager` once astubbs#431 and astubbs#361 land; `PartitionState` last, behind
three fresh PRs and the whole astubbs#225 stack.

## `PartitionState`

**What it holds.** Two things wearing one class: the per-partition **offset ledger** (the incomplete
offsets map, highest seen and highest succeeded, the bootstrap truncation in
`maybeTruncateBelowOrAbove`, the epoch check) and the **commit-payload encoder** (`tryToEncodeOffsets`,
`updateBlockFromEncodingResult`, the pressure threshold, the `dirty` flag and `getCommitDataIfDirty`),
plus a metrics block of gauges and distribution summaries that is a third concern on its own.

**The seam.** The encoder half talks to `OffsetMapCodecManager` and nothing else in the class; the
ledger half is what the control loop and the shard manager read. The `refactors/offsets-class` and
`refactors/refactor-psm-and-ps` branches of 2022 attempted exactly this split (`docs/refactoring.md`,
"Offsets/state classes", tied to confluentinc#233, mirror astubbs#117) and are the prior art to read.

**Merge first.**
- astubbs#470 (open, on today's master): async commit success waits for the broker's answer. Small,
  and it changes when `onOffsetCommitSuccess` may run.
- astubbs#469 (draft, on today's master): the two flags that cross threads, `allowedMoreRecords` and
  `fencedForRevocation`, measured then fenced or redesigned. **This is the thread-model decision for
  the class**; splitting before it means deciding it twice.
  [`bug-allowed-more-records-crosses-threads-unfenced.md`](bug-allowed-more-records-crosses-threads-unfenced.md)
  is the defect it answers.
- astubbs#460 (draft, ten commits behind): the offset-metadata rider adds roughly half the class's
  current size to the encoder half. Decide it before drawing the encoder seam, because it decides
  what the encoder's output type is.
- The astubbs#225 stack (astubbs#472 onward) adds the completed-but-uncommitted ledger to this class
  and `PartitionStateManager`. `docs/refactoring.md`'s `PartitionState` entry already records the
  replay-order trap astubbs#410 reintroduces; a cut that moves `addNewIncompleteRecord` must keep
  the register-then-publish order that entry explains.

**Also on this class.**
[`a-throwing-meter-registry-kills-the-poll-thread-and-strands-close.md`](../solutions/runtime-errors/a-throwing-meter-registry-kills-the-poll-thread-and-strands-close.md)
is why `initMetrics` and `deregisterMetrics` are shaped as they are; a metrics extraction must keep
that shape.

## `WorkContainer`

**What it holds.** The record and its epoch, the **execution-state machine** (`onQueueingForExecution`,
`endFlight`, `isClaimableFrom`, `ExecutionState`, the atomic claim astubbs#335 added), the **retry
schedule** (`getDelayUntilRetryDue`, `computeRetryDueAt`, the user-supplied delay provider and its
broken-provider guard), the failure history, and identity (`equals`, `hashCode`, `compareTo`).

**The seam.** The retry schedule is a pure function of the failure history and the options, and is
the cut with no thread-model content. The execution state is the part every other class touches and
should stay.

**Merge first.**
- astubbs#468 (open, one commit behind): equality becomes identity so the stale sweep removes only the
  container it inspected. This decides what `equals` means, which any extraction of the identity
  methods must build on, and it also reaches `ProcessingShard`.
- astubbs#359 (draft, three weeks behind): record residence time. Adds a third of the class's current
  size, all timing; if it lands, the timing fields are their own cut.
- astubbs#295 (open) and the astubbs#242 stack above it add the verdict-free return, an additive
  change to the execution state. Cut after or around it; it does not conflict with a retry-schedule
  extraction.

**Also on this class.**
[`a-guard-outlives-the-claim-that-motivated-it.md`](../solutions/best-practices/a-guard-outlives-the-claim-that-motivated-it.md)
was written against a guard here; read it before keeping or moving any of the defensive checks in
the execution-state methods.

## `ParallelConsumerOptions`

**What it holds.** The public builder, the validation family (`validate`, `producerSourceValidation`,
`transactionsValidation`, `loadFactorValidation`), the deprecated fields and their shims
(`setCommitInterval`, `defaultMessageRetryDelay`, `isUsingTransactionalProducer`, the reflective
auto-commit work-around flag), and a growing set of engine switches.

**The seam.** Validation into a `ConfigurationValidator`, which is the class the 2022
`origin/refactor/control-loop` branch already cut for the God class's half of it - the two
validators belong together. The deprecated members are release-gated removals, listed under
*Breaking changes queued for next major version* in `docs/refactoring.md`, and are not a refactor.

**Merge first.**
- astubbs#360 and astubbs#361 each add an engine switch here (`useVirtualThreads`,
  `directPullEngine`) with a system-property default. Take them before touching the builder, or the
  validators and the new switches conflict.
- astubbs#352 adds the commit-failure options and reaches the validation methods.
- astubbs#333, astubbs#392 and astubbs#456 add the largest block of options on any branch - the
  admission and rate-limiting surface. Sequence after the cut; they inherit it.

**Nothing else tracks a defect in this class.** It is the one of the five where a cut is a pure
mechanical move.

## `ShardManager`

**What it holds.** The shard map and its three counters (`getNumberOfWorkQueuedInShardsAwaitingSelection`,
`getNumberOfRecordsInShards`, `getNumberOfRecordsParkedForRetry`), **work selection**
(`getWorkIfAvailable`, `getWorkableRecords`, the iteration resume point), the success and failure
paths that retire or re-queue a container, the stale sweep, and a metrics block.

**The seam.** Selection is the hot path and the thing astubbs#361 rewrites; the counters are the
conservation figures astubbs#336 introduced; retirement is the retry-queue pairing astubbs#431 is
about. Three concerns, three PRs, and each PR is the reason not to cut its concern yet.

**Merge first.**
- astubbs#431 (draft, one commit behind, conflicting on a fresh conflict): the rebalance callbacks
  decline the retry queue's write lock. It rewrites the lock discipline across this class and
  `ProcessingShard`, and
  [`pr-431-must-pair-its-queue-removal-with-the-shard-removal.md`](pr-431-must-pair-its-queue-removal-with-the-shard-removal.md)
  says what it still owes. Cut retirement after it.
- astubbs#468: the stale sweep, as above.
- astubbs#361: selection. `ShardOccupancy` and the O(1) scan replace the walk in
  `getWorkIfAvailable`; a selection extraction before it would be extracting the code that PR
  deletes.
- [`core-shard-selection-counter-can-now-be-derived-by-scan.md`](core-shard-selection-counter-can-now-be-derived-by-scan.md)
  is deferred and says the selection counter can become derived; that is a counter-half decision to
  take at the counter cut.

**Constraint no PR removes.** `processingShards` is pinned by a test on purpose - the engine's
`AGENTS.md` owns the reason. A cut may move the field only by moving the pin with it.
[`bug-shard-displacement-orphans-the-retry-queue-entry.md`](bug-shard-displacement-orphans-the-retry-queue-entry.md)
and [`bug-processing-shard-available-work-undercount.md`](bug-processing-shard-available-work-undercount.md)
are open defects in the retirement and counter paths; fix or carry them, do not obscure them.

## `BrokerPollSystem`

**What it holds.** The poll thread's own control loop (`controlLoop`, `handlePoll`,
`pollBrokerForRecords`), **back-pressure by pausing the subscription** (`managePauseOfSubscription`,
`doPause`, `resumeIfPaused`, `shouldThrottle`, the paused-partition diagnostics), the poller's copy of
the **close sequence** (`doClose`, `closeAndWait`, `transitionToClosing`, `maybeCloseConsumerManager`),
and the consumer-commit-mode commit path (`retrieveOffsetsAndCommit`, `maybeDoCommit`).

**The seam.** Pause-and-resume is a self-contained policy over `ConsumerManager` and the work
manager's load figure; it is the cut with no thread-model content. The close sequence and the commit
path are the two-thread contract itself and must not move without the thread-model decision:
[`two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md`](../solutions/architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md)
explains why the two `isResponsibleForCommits` methods look contradictory and are not, and
[`a-mirror-of-state-another-component-owns-is-a-contract-nobody-wrote.md`](../solutions/architecture-patterns/a-mirror-of-state-another-component-owns-is-a-contract-nobody-wrote.md)
was written against this class's `runState` mirror.

**Merge first.**
- astubbs#352 touches the commit path lightly and is the only open PR that changes this class beyond
  the package rename.
- Nothing else. The open defects here are the blockers instead:
  [`bug-poller-death-leaves-the-consumer-open-in-consumer-commit-modes.md`](bug-poller-death-leaves-the-consumer-open-in-consumer-commit-modes.md)
  and [`bug-shutdown-teardown-race.md`](bug-shutdown-teardown-race.md) are both in the close sequence,
  and a cut that moves it should fix them on the way or leave them legible.

**The larger question this class is the subject of.** confluentinc#200 (mirror astubbs#142) proposes
removing the poll thread altogether, and `origin/improvements/poller-bus-actor` tried the poller as an
actor; both are catalogued in `docs/refactoring.md` under "Thread model". A pause-policy extraction is
compatible with either outcome. Anything larger is that decision, not a refactor.

## Prior art checked

- `bin/inflight.mjs prior-art` for each of the five class names: the hits are the defect notes cited
  above and the 2022 `refactors/offsets-class` family; no note proposes a split of any of the five.
- `gh pr list --state merged` by file for the last month: every merged change to these classes was a
  fix inside it (astubbs#335, astubbs#336, astubbs#451, astubbs#467); none extracted.
- `git log --all --diff-filter=A` since 2023 for a `*Validator`, `*Encoder`, `*Policy` or
  `*Schedule` beside these classes in main code: nothing.
