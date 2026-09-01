# The control loop scanned every shard once per pass, to build a log line nobody was reading

<!-- inflight-type: bug -->
<!-- inflight-impact: throughput -->

## What was wrong

`AbstractParallelEoSStreamProcessor`'s control loop - grep `Control loop: blocking on mailbox` -
passed `wm.getNumberOfWorkQueuedInShardsAwaitingSelection()` as a plain argument to `log.trace`.
SLF4J defers *formatting*, not *argument evaluation*, so that call ran on every control-loop pass at
every log level, including the levels production runs at.

The call is not cheap. It reaches `ShardManager.sumOfShardAvailableCounters()`, which sums a counter
across every processing shard. Under `KEY` ordering the shard map is keyed per record key, so the
scan grows with in-flight key cardinality - and under saturation `timeToBlockFor` collapses toward
zero, so the loop spins at its fastest exactly when the scan is at its largest. The two costs peak
together.

The line arrived with astubbs/parallel-consumer#29; master has nothing at that point in the loop.
Both neighbouring log statements that use the same accessor - one in the same method, one in
`WorkManager` - are correctly guarded, which is what marks this as a slip rather than a habit.

## What is OPEN: whether this accounts for the throughput shortfall

`MultiInstanceHighVolumeTest.multiInstance` is down ~39% on astubbs/parallel-consumer#29's CI runs
against a passing baseline, with the neighbouring performance classes within 5% on the same runs.
That selectivity fits this defect - the neighbours are `UNORDERED` (shards track partitions, so the
scan is tiny) or carry very large messages (few concurrent records), while the failing test is
`KEY`-ordered over 3,000,000 small records. It also fits the failure to reproduce locally: a
development box has the headroom to absorb control-thread work that a two-core hosted runner does
not.

**Fitting is not the same as established.** Nobody has measured the fix against the shortfall, and
the instrument's own spread on identical code is 1.54x, which is wide enough to produce this
appearance by chance. `docs/handoffs/perf-lane-throughput-shortfall.md` carries the
full investigation and the two explanations already ruled out by measurement (lane composition, and
runner speed).

**What would settle it** is the pair that handoff names and nobody has run:
`MultiInstanceHighVolumeTest` alone, on CI, on both trees, more than once per side. Note while
planning it that the performance lane is `if: github.event_name == 'pull_request'` and master's push
build excludes `performance`, so **this test has never run on master at all** - the master side of
that pair has to be manufactured with a throwaway PR from a branch at master's tip, not found in
history.

## The fix, and why it is enforced twice

The call is now a supplier: `log.atTrace().addArgument(() -> ...).log(...)`. `Logger#atTrace()`
returns the NOP builder when trace is disabled, and the NOP builder never invokes the supplier.

One mechanism guards it, and the gap left by the other is stated rather than papered over:

- `HotPathLogArgumentsAreDeferredTest` - asserts the SLF4J behaviour the fix rests on, at a pinned
  level, with the eager form as its control arm. A source check cannot see runtime behaviour, so an
  SLF4J upgrade that evaluated suppliers eagerly would break the fix while leaving the source reading
  correctly.
- **Nothing automatically catches somebody writing the eager form again.** A bespoke source gate for
  that was written and then removed: PMD's `GuardLogStatement` is the standard rule for this exact
  pattern, and a private scanner discriminating on a hand-maintained list of five accessor names goes
  stale the first time someone adds a scanning accessor without thinking of it - passing while covering
  less, which is the failure it existed to prevent. Adopting PMD is the open option; its known
  noisiness is the honest reason nobody has.

The consequence is still caught downstream: a regression of this shape shows up in the throughput gate,
which measures the effect rather than the pattern.

## Where the fix is, and where the defect still is

<!-- post-merge: checked - the branch is NAMED rather than called "this one", and the sentence is about where a fix sits relative to two branches, so it reads identically after either of them merges -->
**The fix is on `handoff/enable-large-number-of-instances`. astubbs/parallel-consumer#29 still has the
eager form**, and that is a deliberate operator decision (2026-09-01), not an oversight or a missed
cherry-pick. Do not read this note as clearing astubbs#29.

Verify rather than trust either half of that sentence - both branches move:

```
git show origin/bugs/857-paused-consumption-multi-consumers-bug:parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/AbstractParallelEoSStreamProcessor.java \
  | grep -A4 'Control loop: blocking on mailbox'
```

Two consequences follow, and the second is the one that decides what a next session can do:

- **The fix cannot reach master on its own.** The branch holding it carries astubbs#29's whole stack, so
  merging it means merging astubbs#29.
- **The hypothesis cannot be tested where the failure appears.** The ~39% shortfall shows on astubbs#29's
  performance lane, and that lane runs astubbs#29's tree - which still evaluates the shard scan every
  control-loop pass. So a green or red run there says nothing about this fix until the fix is on that
  tree. Anyone planning the like-for-like CI pair should establish which tree each side is running
  before reading its numbers; that confusion is what the companion handoff spent a session on.

The defect was introduced by `1479f73ff wip(#857): provisional fixes for silent stall under <!-- issue-refs: exempt - quoted commit subject; qualifying it would misquote the commit -->
rebalance`, which is on astubbs#29 and not on master.
