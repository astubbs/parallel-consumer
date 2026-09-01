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

<!-- post-merge: checked - names the PR that introduced the line, which stays true once that PR merges -->
The eager form was introduced by astubbs/parallel-consumer#29 and was never on master before it.
Both neighbouring log statements that use the same accessor - one in the same method, one in
`WorkManager` - are correctly guarded, which is what marks this as a slip rather than a habit.

## What is OPEN: whether this accounts for the throughput shortfall

<!-- post-merge: checked - a historical record of what that PR's runs showed; still readable after it merges -->
`MultiInstanceHighVolumeTest.multiInstance` was down ~39% on astubbs/parallel-consumer#29's CI runs
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

Two mechanisms guard it, because neither can see what the other sees:

- `bin/check-hot-log-args.sh` - a source gate. It sees the guard; ArchUnit cannot, because a guard is
  control flow and ArchUnit reads the call graph, so an ArchUnit rule would flag the correctly-guarded
  sites identically. Its header carries that reasoning and the denylist of scanning accessors.
- `HotPathLogArgumentsAreDeferredTest` - asserts the SLF4J behaviour the fix rests on, at a pinned
  level, with the eager form as its control arm. A source check cannot see runtime behaviour, so an
  SLF4J upgrade that evaluated suppliers eagerly would break the fix silently.

The gate's own self-test, `bin/test-check-hot-log-args.sh`, exists because the check's first draft
used gawk's `ENDFILE` on a box whose `awk` is mawk: it parsed, ran nothing, and printed its success
line over a file containing this exact defect.

## Where the fix is now

<!-- post-merge: checked-begin -->
**The fix was merged into astubbs/parallel-consumer#29 on 2026-09-01**, from
`handoff/enable-large-number-of-instances`, with both of its guards. That reverses the earlier
operator decision to keep that PR on the eager form deliberately - a decision taken when the fix sat
on a branch that could not merge without dragging the whole stack with it. Merging in the other
direction removed the obstacle, so the reasoning expired rather than being overruled.

**`largeNumberOfInstances` was NOT re-enabled by that merge.** The source branch had it enabled; the
annotation holding it disabled was kept, because the rate that branch measured - one failure in ten
consecutive runs, failing as a stall rather than an overload - is not a rate a required check can
carry. `docs/inflight/test-largenumberofinstances-residual-failures-unmeasured.md` owns that thread.

Verify rather than trust, since both branches move:

```
git show origin/bugs/857-paused-consumption-multi-consumers-bug:parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/AbstractParallelEoSStreamProcessor.java \
  | grep -A4 'Control loop: blocking on mailbox'
```

**The merge turns the next performance run into the measurement this note was waiting for.** The lane
runs that PR's own tree, and the tree now carries the supplier form, so `MultiInstanceHighVolumeTest`
there becomes a like-for-like test of the hypothesis for the first time - the previous section's
"cannot be tested where the failure appears" no longer holds.

Read the result against the instrument's 1.54x spread on identical code, which is wide enough to
produce either answer by chance. A single recovery toward the ~73,000 rec/s baseline is *consistent
with* this defect having been the cause and does not establish it; a single run still low does not
clear it. The pair named above - the same test alone, both trees, more than once per side - is still
what would settle it, and one lucky run is the likeliest way to stop looking too early.

The defect was introduced by `1479f73ff wip(#857): provisional fixes for silent stall under <!-- issue-refs: exempt - quoted commit subject; qualifying it would misquote the commit -->
rebalance`.
<!-- post-merge: checked-end -->
