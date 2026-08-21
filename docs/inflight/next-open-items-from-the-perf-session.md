# Next: what this performance session left open, ranked

<!-- inflight-type: next -->
<!-- inflight-impact: correctness -->
<!-- inflight-labels: needs-measurement -->

Opened 2026-08-22. **The open items from the session, in the order they deserve attention**, so the
next person does not have to reconstruct them from forty commits.

## 1. SETTLED - there was no UNORDERED dispatch regression

This entry used to claim one, on the strength of `OrderingModeDispatchParityTest` failing. **It was
the test, not the code.** It timed both modes in a suite that runs test methods in parallel, and the
two arms are not equally sensitive to contention, so the comparison moved without the code moving. A
bisect across every merge point came back flat. The test now counts entries the scan examines instead
of timing it, and passes.

What the counting revealed is worth knowing and lives in
[`perf-unordered-dispatch-rescans-the-inflight-prefix.md`](perf-unordered-dispatch-rescans-the-inflight-prefix.md):
UNORDERED genuinely re-walks the whole in-flight prefix on every pass, KEY examines exactly one entry
per record, and the ratio is workload-dependent rather than a constant.

**Kept rather than deleted because of the process failure, which is the durable lesson.** The method
was changed four times to make a failing test pass - fastest-of-three, the ratio, per-thread CPU time,
`@Isolated` - before anyone questioned the instrument. Three of those were tried after a bisect had
already shown the code was unchanged. **Changing a test's method repeatedly to make it agree is tuning
until it agrees**, and it is written into two agent briefs from the same day as an anti-pattern.

## 2. `1001 deliveries for 1000 records` - and no, the tests did not catch it

Seen **once** in a full direct-pull run. **0 out of 11 reproduction attempts.** The signature of a
double delivery, which the direct-pull write-up explicitly claims cannot happen.

**It was caught by someone reading a number in a log, not by a test.** That is worth stating plainly
because the coverage that exists is uneven in a way the headline "18 tests, all proven red" conceals:

- **Selection is now covered** - concurrent claim, the ordered invariant, claim-loss - and it **cannot**
  produce this. That much is genuinely settled.
- **The redelivery paths are not covered at all**: retry after failure, abandonment without a verdict,
  and the stale sweep. **That is where an extra delivery must come from**, and it is exactly the
  territory nothing tests.

**So the honest state is: the paths that were tested are clean, and the extra delivery came from the
paths that were not.** `DirectPullEngineParityTest.pausingStopsDeliveryAndResumingDeliversTheRestExactlyOnce`
now guards the shape with an exact count, but a guard is not a diagnosis.

**What to do:** cover the three redelivery paths under concurrent pull before anything else on this
engine. A record that fails, is retried, and is abandoned mid-flight crosses all three.

## 3. NOT OURS - the close path is owned by a separate PR

`innerDoClose` releasing the worker pool outside any `finally` is real, and a sweep found the same
shape in `VertxParallelEoSStreamProcessor#close`, `BrokerPollSystem#doClose`, `doClose`'s own
`finally` and `ProducerManager#close`. **Antony has PRs addressing this; do not open another.** The
merge order is those first, then into this trunk.

## 4. The busy-shard count - the design that may resolve 1

**Owner's proposal.** `ShardManager` currently enters **every** shard on every pass with no guard - not
even `isEmpty()` - paying an iterator, a `HashSet`, an `ArrayList` and a walk-to-head **to discover that
an ordered shard's head is in flight**, then breaking.

**A per-shard count of in-flight records** makes that an O(1) check: an ordered shard is selectable iff
the count is zero. One field on the object that already owns the fact, so nothing can disagree with
anything - unlike a parallel collection of available shards, or a map of booleans, both of which are two
structures to keep in step. **It also replaces an estimate**: `getUpperBoundOnSelectableWork()` computes
`min(awaitingSelection, shardCount)` only because the per-shard truth is unavailable.

**`UNORDERED` simply ignores it.** Shards there are never blocked, the map is a `ConcurrentSkipListMap`
and the claim is a CAS, so two workers entering the same shard is already safe - they take what they can
and a losing claim skips. The check costs one comparison that is never true.

**What it does not solve:** `UNORDERED`'s waste, where N workers walk the same in-flight prefix. That
remains its own question - see
[`next-direct-pull-unordered-selection.md`](next-direct-pull-unordered-selection.md).

## 5. Work that is queued, and what each is waiting for

**Several of these are blocked on the same thing: a quiet machine.** That constraint has been worked
around all day rather than named, and it is worth naming because parallel agents generate most of the
load themselves - so running them concurrently has been buying less than it appears.

| Item | Blocked on | Why it matters |
|---|---|---|
| **Cover the three redelivery paths** (item 2) | nothing - do this next | Retry, abandonment, stale sweep. A record that fails, retries, then is abandoned mid-flight crosses all three |
| **Implement the busy-shard count** (item 4) | item 2 landing first | O(1) selectability for ordered shards, replacing a walk-to-head that exists only to discover the head is in flight |
| **Measure Reactor / Mutiny / ProxyProcessor** | the arms being wired, then a quiet machine | **Every cross-engine claim currently rests on Vert.x plus an assumption that `ExternalEngine` makes the family behave alike** |
| **Re-take the direct-pull crossover** | a quiet machine | The 3.2x at ten workers and the collapse at five thousand were measured at load 7-860 |
| **Re-measure dispatch cost at 2ms/5000** | a quiet machine | Both attempts at removing the UNORDERED rescan measured 0% and +0.2%, at operating points that predate virtual threads putting the engine at near-zero handler delay |
| **A CI matrix axis over execution modes** | the JDK 21 lane existing | [`test-opt-in-engine-paths-are-unexercised.md`](test-opt-in-engine-paths-are-unexercised.md) - direct pull is exercised by nothing |

## 6. Not performance, and not to be lost behind it

| Item | Note |
|---|---|
| **The key-distribution axis** | Every published number uses all-distinct keys, which is a best case for any key-sharded design - [`next-performance-regression-testing.md`](next-performance-regression-testing.md) |
| **The engine comparison in the docs** | Needs the EoS axis, not just throughput, or it steers users off the only engine that supports it - [`next-docs-publish-the-engine-comparison.md`](next-docs-publish-the-engine-comparison.md) |
| **`ThreadCeiling` on Linux** | Forty lines, no broker, no dependencies. Until it runs, every ceiling figure here is one operating system's - [`next-benchmark-a-model-of-work-not-work.md`](next-benchmark-a-model-of-work-not-work.md) |
| **Read astubbs#260's reasoning** | It corrected one shutdown-commit assertion; a sibling test makes a similar one and nobody checked. Five minutes, may close two flake entries |
| **The `ExternalEngine` regression** | Reclassified: it is a tax on the one engine family that is not thread-bound, and its true size is unknown because every measurement went through the capped stub |

## Operational: take toolchains locally, never globally

A subagent switched sdkman's global `current` symlink from JDK 17 to 21 and **broke the build in an
unrelated worktree** - Lombok 1.18.20 cannot delombok on 21, and it surfaced as
`NoSuchFieldError: JCTree$JCImport ... qualid`, which looks nothing like "somebody changed my JDK".

**Rule: set `JAVA_HOME` per invocation, or use a directory-local `.sdkmanrc` inside your own worktree.
Never `sdk default`.** Several agents and the user's own sessions share this machine.

**And it is not merely etiquette.** Work in progress on a JDK 21 CI lane has to select its toolchain
per job; an agent whose local habit is "switch the global default" will write that assumption into the
workflow.
