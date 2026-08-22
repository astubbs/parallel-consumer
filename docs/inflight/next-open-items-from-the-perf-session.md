# Next: what this performance session left open, ranked

<!-- inflight-type: next -->
<!-- inflight-impact: correctness -->
<!-- inflight-labels: needs-measurement -->

Opened 2026-08-22. **The open items from the session, in the order they deserve attention**, so the
next person does not have to reconstruct them from forty commits.

## THE THREE, promoted to the top by Antony 2026-08-22 - do not let these sink again

Each is queued, none is started, and each now has a measurement behind it that it did not have when
it was first raised. **They are listed first because all three lost every slot today to whatever was
red**, which is how the busy-shard count sat untouched for a day despite being the owner's own design.

| | Item | The number that now backs it |
|---|---|---|
| **A** | **Can the async engines also run on virtual threads?** [`next-async-engines-on-virtual-threads.md`](next-async-engines-on-virtual-threads.md) | `ExternalEngine` returns a worker pool of ONE, so the two features have never met - yet `core-vt` reached 5,000 records in flight where `core` reached 2,824, and that difference is not the user function |
| **B** | **The core futures API** [`next-core-async-user-function.md`](next-core-async-user-function.md) | Every async engine reaches its configured concurrency at 100ms; `core` reaches 2,824 of 5,000 and is **23% slower**. The advantage is reachable from core, and the engine modules are the only way to get it today |
| **C** | **Virtual threads under GraalVM native, and the proxy** [`next-virtual-threads-under-graalvm-native.md`](next-virtual-threads-under-graalvm-native.md) | `proxy` came within **1%** of virtual threads at 2ms, on the path every non-JVM client takes. Java as a foreign language: virtual-thread concurrency without having virtual threads |

**They are related, which is the argument for doing them together.** B would make the engine modules
largely redundant; A asks whether those modules can take the feature B generalises; C is what B and
virtual threads buy the ten non-JVM clients. A decision on B changes what A and C are worth.

**Each has one untested assumption at its centre, and all three are cheap to falsify**: does
`useVirtualThreads` even reach an `ExternalEngine` (A); does a `CompletableFuture` return compile
under Jabel's Java 8 target at every call site (B); does the reflective virtual-thread lookup survive
native-image's closed-world analysis (C).

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

## 2. SETTLED - `1001 deliveries for 1000 records` was selection after all

Diagnosed and fixed 2026-08-22. The claim was check-then-act: `isAvailableToTakeAsWork()` evaluated
three terms and `onQueueingForExecution()`'s compare-and-set re-validated only the in-flight one, so
a puller whose decision predated another puller's completion could claim an already-succeeded record.
Both fields are now one atomic `WorkContainer.ExecutionState` and the claim is a single
compare-and-set from the state it evaluated. Sighting record and the reproduction numbers are in
[`test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md).

**Kept, because this entry was confidently wrong in a way worth remembering.** It read "selection is
now covered ... and it **cannot** produce this. That much is genuinely settled", and directed the
next person at the redelivery paths instead. The selection tests were real and were proven red under
sabotage - they simply tested two workers claiming *simultaneously*, which the CAS did exclude, and
not a claim whose decision predated a completion, which it did not. **Coverage of a mechanism is not
coverage of the property**, and "proven red under sabotage" says only that the sabotage you chose was
detectable.

The redelivery paths it pointed at were genuinely uncovered, and now are: retry, abandonment and the
crossing of both are pinned by `WorkClaimStateMachineTest`.

## 3. NOT OURS - the close path is owned by a separate PR

`innerDoClose` releasing the worker pool outside any `finally` is real, and a sweep found the same
shape in `VertxParallelEoSStreamProcessor#close`, `BrokerPollSystem#doClose`, `doClose`'s own
`finally` and `ProducerManager#close`. **Antony has PRs addressing this; do not open another.** The
merge order is those first, then into this trunk.

## 4. SETTLED - the busy-shard count is in

Owner's design, implemented 2026-08-22 as `ProcessingShard#getCountOfWorkInFlight()`: an ordered
shard is selectable iff the count is zero, and `getUpperBoundOnSelectableWork()` now counts the
shards that can actually yield instead of `min(awaitingSelection, shardCount)`.

**Two things about it that were not obvious in advance and are worth carrying forward.** The charge
is taken and released by the record's own claim/land transition rather than by the sites that add and
remove shard entries - which is what makes the revoked-partition return path, the one path that tells
no shard anything, need no special case. And it did **not** move
`OrderingModeDispatchParityTest`'s KEY count: that workload never returns a record, and the shard
iterator resumes rather than restarts, so no occupied shard is ever revisited for the guard to skip.

**What it does not solve:** `UNORDERED`'s waste, where N workers walk the same in-flight prefix. That
remains its own question - see
[`next-direct-pull-unordered-selection.md`](next-direct-pull-unordered-selection.md).

## 5. Work that is queued, and what each is waiting for

**Several of these are blocked on the same thing: a quiet machine.** That constraint has been worked
around all day rather than named, and it is worth naming because parallel agents generate most of the
load themselves - so running them concurrently has been buying less than it appears.

| Item | Blocked on | Why it matters |
|---|---|---|
| **Cover the stale sweep under concurrent pull** | nothing - do this next | Retry and abandonment landed with item 2's fix; the stale sweep is the one redelivery path still uncovered, and it is the one that crosses partition revocation |
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
