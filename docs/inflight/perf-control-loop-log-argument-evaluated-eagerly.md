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

## Was OPEN, now ANSWERED: whether this accounts for the throughput shortfall

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
appearance by chance. `docs/inflight/test-perf-lane-asserts-a-deadline-on-a-varying-machine.md`
carries the full investigation, including the two explanations ruled out by measurement (lane
composition, and runner speed) and the wrong paths already taken.

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

**All three capacity profiles were then re-enabled, same day, by operator decision.** The merge
initially held `largeNumberOfInstances` disabled on the grounds that one failure in ten is not a rate
a required check can carry. That was reversed once the ordering was checked: the ten-run measurement
predates this fix, so the rate it produced is a rate for the *unfixed* tree. Whether it survives the
fix is an open question, and the failure mode makes the connection plausible rather than idle - the
failure was a rebalance stall with a member not answering, and a control thread scanning every shard
every pass is a candidate reason a member answers late. Enabling them is how that gets tested.
`docs/inflight/test-largenumberofinstances-residual-failures-measured-not-explained.md` owns it.

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

## 2026-09-01: MEASURED. One controlled run, and the outcome flipped

The hypothesis above was tested by the next `Performance Tests` run after the fix merged, and this is
the like-for-like comparison the whole investigation lacked - not because anyone constructed it, but
because merging the fix made the lane run it.

| | failing run `b42ab61d7` | passing run `92c5d5b70` |
|---|---|---|
| `VeryLargeMessageVolumeTest` | 53.86 s | 53.28 s |
| `LargeVolumeInMemoryTests` | 39.45 s | 38.28 s |
| `LoadTest` | 41.34 s | 40.32 s |
| `MultiInstanceRebalanceTest` | 0.020 s, all skipped | 170.9 s, 3 tests, all passed |
| **`MultiInstanceHighVolumeTest`** | **FAILED, 43,552 rec/s, 2,638,050 of 3,000,000** | **PASSED, 76,950 rec/s, all 3,000,000 in 38,986 ms** |

**Why this is a control arm and not just a good run.** The only main-code difference between those two
heads is this fix - verify with `git diff b42ab61d7 92c5d5b70 -- '*/src/main/java/*'`, which returns
that one file. The three neighbouring classes land within 1-3% of the failing run, so the machine was
not materially faster. And lane composition moved the *wrong* way: the capacity profiles were skipped
in the failing run and ran for 170.9 s of churn ahead of the throughput test here, in the same reused
JVM - the carryover previously measured at about 5,000 rec/s. The confound was re-added, and the
number still rose about 77%.

**What it does not establish.** One run per side. The instrument's spread on identical code is 1.54x,
so a single pair cannot be a verdict - though it is worth noting the passing band observed to date is
71,387 to 109,898 rec/s and the failing band 39,684 to 44,992, and this result sits in the first.
What would still strengthen it is repetition, which now costs nothing but time: the lane runs this
test on every push to the PR.
