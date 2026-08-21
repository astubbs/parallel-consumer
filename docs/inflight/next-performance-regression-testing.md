# Next: make performance regression testing formal enough to catch a regression

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->

**We already have performance tests.** The gap is that they are not formal enough to *fail* on a
regression, and this is not hypothetical: a **35% throughput regression shipped from 0.4.0.0 and went
unnoticed for five years** ([`perf-throughput-regression-since-0-3.md`](perf-throughput-regression-since-0-3.md)).
It was found by accident, while rescuing a 2021 demo, because the demo's number no longer matched a
recording.

Opened 2026-08-21 at the owner's direction, split out of
[`next-formal-verification-and-correctness-methods.md`](next-formal-verification-and-correctness-methods.md).

## What "formal enough" has to mean

The existing perf lane is a **required PR gate** already (see
[`test-required-perf-lane-scope.md`](test-required-perf-lane-scope.md)), and it did not catch this -
so more of the same is not the answer. The properties it lacks:

- **An asserted number, not a recorded one.** A benchmark that prints a figure nobody compares is
  telemetry. The test must fail. llingr's version of this is a zero-allocation invariant on their
  work-item pool where *a regression fails a test, not a benchmark* - the framing worth copying.
- **A baseline that survives across versions**, so "slower than the last release" is expressible.
  Absolute thresholds rot on shared CI runners; **ratios against a control arm in the same run** do
  not. The bench harness already does this: a plain-`KafkaConsumer` arm that touches no PC code, which
  moved 4.2% while PC moved 31% - the control is what made the finding credible.
- **Both engines.** The regression was invisible in core and 45% in Vert.x. A lane that measures one
  path proves nothing about the other. `bench/run-bisect.sh` now has `MODE=core` and `MODE=pc`.
- **Pinned logging.** Non-negotiable, and the reason is recorded in `bench/conf/logback.xml`: at
  logback's default DEBUG the same build measured 2,595 msg/s versus 16,231 at WARN. **The effect is
  not uniform across versions** - 0.3.2.0 moved 2% - so it manufactures phantom cliffs. An unpinned
  perf test measures its own configuration.
- **A long enough measurement window.** At 20,000 records every version returned ~4,200 msg/s because
  consumer-group join dominated; the arms only separate at 350,000. A too-short benchmark reports a
  confident null.

## What exists to build on

[`bench/run-bisect.sh`](../../bench/run-bisect.sh) and
[`bench/Bench.java.template`](../../bench/Bench.java.template), written for the investigation. They
already do broker-and-dataset-once, arms as Maven coordinates, pinned logging, a control arm, both
engines, and **observed peak in-flight measured at the stub** rather than inferred from config. Raw
results are under `bench/results/`.

What it is not yet: wired into CI, and not a pass/fail gate. Turning it into one needs a decision on
where it runs, because accurate benchmarking on shared runners is exactly what
`pr-highcpu-fast-feedback.yml` already records as belonging in a separate isolated or on-demand run.

## A performance target, and finding where the overhead lives

**Recorded 2026-08-21 at the owner's direction: PC should have an explicit target of matching a
best-in-class engine's per-record overhead**, not merely of not regressing against itself. Measured
today, against the Go engine on identical workloads:

| Simulated per-record work | PC behind by |
|---|---|
| 0ms | 1.78x |
| 2ms | 1.26x |
| 20ms | 1.20x |
| 100ms | 1.04x |

The gap is concentrated entirely in per-record framework overhead, and it is **not urgent** - at any
realistic per-record latency it is a few percent, and configuration choices swing throughput more
than the engine does. But "we are 1.78x behind on the pure-overhead case" is now a number rather than
a worry, and it is a legitimate target.

**The prerequisite is instrumentation, and PC has none of the right kind.** We can measure end-to-end
throughput; we cannot say where a record's time goes inside the engine. Before optimising anything,
build the ability to attribute cost:

- **Per-stage timing on the record path** - poll to work-manager registration, registration to
  dispatch, dispatch to user function entry, completion to commit-queue entry. A sampled histogram
  per stage, not per record.
- **Allocation attribution.** llingr's published account leans heavily on allocation avoidance
  (pooled work items, a zero-allocation invariant asserted in tests). PC has never counted
  allocations per record.
- **Lock and contention profiling** on the shared structures - the owner's original hypothesis about
  thread-safe collections was not supported at the boundary examined, but it has never been tested
  where the overhead actually is.
- **A published per-message overhead figure**, measured the way llingr publishes theirs: fully
  contended, no simulated latency, so it is a worst case rather than a flattering one.

**Sequencing:** instrument first, publish the figure second, optimise third. Optimising before
attribution is how the 35% regression stayed invisible for five years - nobody could see which term
had grown.

**The first lead was measured and refuted, which sharpens the target rather than removing it.** The
0ms observation - llingr at peak in-flight 2-3 beating PC at 100 - looked like PC paying for
unnecessary fan-out. A concurrency sweep says otherwise: PC at concurrency 1 does 8,473 msg/s and
climbs monotonically to 63,915 at 1,000, with no knee. **Fan-out is what hides PC's per-record cost,
not what causes it.**

**Concurrency 1 measures that cost directly, and it is the number to target:**

| | per-record cost at concurrency 1 |
|---|---|
| PC core | **118µs** |
| llingr | 13.2µs |
| franz-go, no engine | 8.9µs |

**But the honest reading is that this bounds PC and the Java client jointly.** A bare franz-go arm
beats llingr at every measured point - so llingr's apparent advantage over PC is mostly the Go client,
and llingr gives ~17% of it back versus using franz-go directly. There is currently **no Java-side
equivalent control**: the `vanilla` arm drives real HTTP rather than sleeping, so it cannot isolate
the Java client the way `franz` isolates franz-go.

**So the next measurement is a prerequisite for the target, not optional:** a `Bench` mode of plain
`KafkaConsumer` plus a thread pool plus a sleep. Until that exists, "PC costs 118µs per record" is
really "PC and the Java client together cost 118µs", and optimising the engine against an unsplit
number risks working on the wrong term - which is precisely the mistake this note exists to prevent.

## Open questions

- **Gate on ratio or on absolute?** Ratio against the in-run control arm is the defensible one.
- **Where does it run?** A required PR lane will be noisy; an on-demand or nightly lane will be
  ignored. The middle option is a required lane that gates only on a large ratio change.
- **What is the baseline?** Last release, or a pinned known-good version resolved from Central - the
  harness can already resolve any published version, which makes "compare against 0.5.3.2" free.

## Related

- [`perf-throughput-regression-since-0-3.md`](perf-throughput-regression-since-0-3.md) - the
  regression that motivated this, and the harness.
- [`test-required-perf-lane-scope.md`](test-required-perf-lane-scope.md) - the existing lane.
- [`ci-mutation-testing.md`](ci-mutation-testing.md) - same epistemology, applied to correctness:
  coverage is not evidence.

## Evidence practices worth copying, ranked by how cheap they are

A competitor three months old publishes better performance evidence than this project does, and
almost none of it is expensive. Listed cheapest first, because the cheap ones are the ones that will
actually get done.

1. **Commit the raw results, not just a chart.** They ship `benchmark_results.csv` - 540 rows of
   per-run data - alongside the harness. It is what makes their own claims checkable, including the
   one that turned out to be a tautology. **We already do this** (`bench/results/*.csv`); the missing
   half is that nothing points at it.
2. **State the hardware and the method beside every number.** CPU model, core count, cache sizes,
   memory, OS and kernel. One paragraph, written once.
3. **Say what the handler does, out loud.** Their methodology document is explicit that the handler is
   a `time.Sleep` and that broker and database costs are excluded. That single sentence is what
   separates an honest benchmark from a misleading one, and it costs nothing.
4. **Publish a worst-case per-message overhead figure**, measured fully contended with no simulated
   latency - the *unflattering* configuration. Theirs is ~1.26µs on x86_64, with per-architecture
   variants. PC has never measured this.
5. **Make the harness runnable by a sceptic.** `./run_benchmarks.sh` with committed configs. Ours is
   close - `bench/run-bisect.sh` - but needs a documented one-command path and a stated expectation of
   what a clean run looks like.
6. **Publish an efficiency measure, not only a throughput number.** Their "93-99% of theoretical
   fan-out" is a better claim than any absolute rate because it is *scale-free* and self-limiting:
   it cannot be inflated by picking a favourable machine. PC's equivalent is straightforward -
   achieved throughput against `concurrency / per-record-latency`.
7. **A methodology document.** Theirs is 520 lines. Ours would be shorter and would still be more than
   exists today.

**And the discipline that outranks all of them: name the baseline.** The failure in their material is
not the harness, which is good - it is a headline ratio whose comparator was never run. Any number PC
publishes must say what it was measured against, on what workload, at what concurrency. See the
teardown in [`next-llingr-questions-and-answers.md`](next-llingr-questions-and-answers.md).

**Where PC's demonstration is already stronger, and should stay that way.** Their benchmark runs
against an in-memory mock broker. PC's comparison demo runs against a real broker, can be pointed at
**the user's own topic and data**, and can generate synthetic data whose shape the user dials - key
cardinality, failure rate, per-record delay, concurrency, partitions, instances. That answers *"will
this help me?"* rather than *"is this fast in the author's harness?"*, and it is the more convincing
artifact by a wide margin. See
[`branch-classic-comparison-demo.md`](branch-classic-comparison-demo.md).

## Where the 118µs actually goes - attribution, 2026-08-21

The gap is **almost entirely above the Kafka client, and mostly above the JVM.** Three independent
lines converge on that, which is what makes it worth acting on.

**The Kafka client is not the problem - roughly 0.3-1µs per record.** Kafka's own JMH
`RecordBatchIterationBenchmark` (attached to `apache/kafka#13135`) measures ~195ns/record for the
decode-and-decompress path at 100B/LZ4 with realistic batch sizes. Add `ConsumerRecord`,
`RecordHeaders`, `Optional` and deserialization and it is under 1µs. **That is under 5% of PC's
19.3µs/record at concurrency 100, and under 1% of the 118µs at concurrency 1.**

**Two pieces of folklore are false, verified against the 3.9.2 bytecode this build actually uses:**

- **Client metrics are not per-record.** `FetchMetricsAggregator.record(...)` takes *counts* and is
  invoked once per `CompletedFetch` drain - per partition, per fetch. Interceptors take whole
  `ConsumerRecords`.
- **`check.crcs` is not a per-record knob on modern brokers.** `DefaultRecord.ensureValid()`
  disassembles to a bare `return`; CRC runs once per *batch*, and only for `magic >= 2`. The config's
  warning about overhead is a v0/v1-era leftover.

**Warmup is refuted by data already collected**, which is the tidiest result here. Window lengths
from the CSV: core at concurrency 1 runs **41.3s**; core at 100 runs **6.75s**; franz at 1 runs
**3.12s**. Java's *relative* position is **worst in the longest run** - 8% of franz over 41s, 49% over
6.75s. Warmup cannot produce that ordering. To erase the 2x it would have to consume ~45% of a 6.75s
window.

**So the 118µs is a blocking round trip, not CPU.** 118µs on an M2 Pro is roughly 400,000 cycles - no
amount of allocation, header overhead or GC produces that. It is PC's control-loop to executor to
mailbox to control-loop handoff, which is why it collapses to 19.3µs at concurrency 100 and 15.7µs at
1,000, while franz-go sits flat at 8.9-9.4µs across the whole range. **118µs and 19.3µs are not in
conflict** - they measure different concurrency levels, and the difference between them *is* the
finding.

### The removable work, source-verified in this checkout

On the `UNORDERED`/`batchSize=1` path the bench exercises. This is ordinary optimisation, not
architecture:

| Site | Per-record cost |
|---|---|
| `ShardKey.ofTopicPartition` | allocates `TopicPartitionKey` + `TopicPartition` for a value that is **constant per partition** |
| `WorkContainer.comparator` | a `Comparator.comparing(...).thenComparing(...)` built **per instance**, whose key extractor concatenates topic and partition |
| `KafkaUtils.toTopicPartition` | `new TopicPartition(...)` on every call - **25 call sites** |
| `ShardManager.onSuccess` | `retryQueue.remove(wc)` for **every successful record** - takes a *fair* `ReentrantReadWriteLock` write lock, allocates a key with boxed `Integer`/`Long`, and removes nothing. Guard on `hasPreviouslyFailed()` |
| `runUserFunction` | `.stream().collect(groupingBy(...))` to partition a **one-element list**, plus a throwaway `ArrayList` in the absent branch |
| `handleStaleWork` | builds `PollContextInternal` **before** the `isEmpty()` check |
| `partition()` | a fresh `ArrayList` per batch - i.e. per record at `batchSize=1` |

Also worth a look, not per-record: `getIncompleteOffsetsBelowHighestSucceeded()` uses
`parallelStream()` on the **common ForkJoinPool** at commit time, competing with the worker pool over
a few hundred elements.

**Justified and not to be touched:** the `incompleteOffsets` `ConcurrentSkipListMap` work. That is the
offset encoding PC exists for.

### The realistic split, and where a target belongs

1. **Allocation and stream machinery** - the table above. Cheap, low-risk, probably single-digit µs.
2. **The serialised control-loop round trip** - the large term, and architectural. It is the same
   finding this note already reached from the other direction: `ExternalEngine`'s missing 2x pipeline
   buffer was worth 35%. **franz-go reaching 112k msg/s with one inline goroutine and no dispatch at
   all is the shape of the thing PC is not doing.**

**Neither requires changing the JVM or the Kafka client.** The runtime is worth maybe 1.15-1.4x of
the 2.05x at the pessimistic end - thread park/unpark against a channel send is the honest part of
that, and it is entangled with the architecture rather than separable from it.

**Do not move to the KIP-848 async consumer for performance.** `KAFKA-18376` reports >50% CPU against
the classic consumer's ~10%; `KAFKA-20904` is open now with *"CPU usage has doubled for the consumer
protocol"*.

### Three harness confounds found, all pushing the same way

Each is real, cheap to fix, and currently invisible in the numbers:

1. **`Thread.sleep(0)` is not free; `time.Sleep(0)` is.** HotSpot's `JVM_Sleep` calls
   `os::naked_yield()` -> `sched_yield()` when millis is 0, plus a JNI and thread-state transition;
   Go returns immediately for `ns <= 0`. So at delay 0 the Java arm makes a syscall per record the Go
   arm does not. **Sized at ~1%** - recorded because it is the tempting answer and it is wrong.
2. **The 2ms rows are dominated by sleep overshoot in *both* runtimes.** Mean handler residency at
   ceiling 100: core **3.93ms**, franz **3.16ms**, llingr **3.25ms** - every arm overshoots a 2ms
   sleep by more than the sleep itself, and macOS on Apple Silicon is documented as the worst platform
   for sleep variance. **The 1.16-1.25x figures at 2ms are therefore not clean engine measurements.**
3. **The Java arm deserializes and the Go arms do not** - `StringDeserializer` on key and value, two
   String allocations and two UTF-8 decodes per record. Small (~150-300ns) but asymmetric, and
   `bench/franz/main.go`'s own comment claims the opposite. `ByteBufferDeserializer` is the genuinely
   zero-copy choice; `ByteArrayDeserializer` still copies.

### Next experiments, by information per unit of effort

1. **Two measured passes in one JVM**, fresh group per pass - about ten lines. **Prediction stated in
   advance: pass 2 within ~15% of pass 1**, which retires warmup entirely and forces the
   investigation onto control-loop latency.
2. **The Java-side floor** - `KafkaConsumer` + fixed thread pool + sleep, no PC. Still the single
   measurement that makes 118µs attributable.
3. **Windowed throughput** in both arms - shape rather than one scalar.
4. **Is ~110k a system ceiling?** franz-go is flat at 106-112k from concurrency 1 to 1,000 with peak
   in-flight never above 4, so the poll loop is the limit - but the data cannot say whether that is
   franz-go's CPU or the Docker-on-macOS broker path. **Run two franz consumers in different groups
   at once:** ~220k aggregate means per-consumer CPU; ~110k means the environment caps every arm and
   the whole comparison has a ceiling nobody measured.
5. `-Xlog:gc` settles GC; `-XX:TieredStopAtLevel=1` as a second arm measures what C2 contributed.

### What must not be claimed

- **Not "the Java Kafka client is slow"** - the evidence points the other way, and no isolated
  per-record measurement of it exists anywhere. Apache's JMH suite has no consumer fetch/decode
  benchmark at all.
- **Not "the JVM costs 2x"** - the 41-second concurrency-1 run is the JVM at its most warmed-up and
  its most embarrassing simultaneously.
- **Not franz-go's own "10-20x" figures** - withdrawn from its README, methodology credibly attacked,
  never reproduced.

## Key distribution must become a first-class benchmark axis

**Added 2026-08-21.** `bench/` currently sweeps `MODES`, `DELAYS` and `CONCURRENCIES`. It does not
sweep **key distribution**, and that is the axis that decides whether a stated concurrency is
reachable at all - a point made sharply by reading a competitor's generator, which assigns a globally
unique key to every record and therefore measures a ceiling no real workload can reach
([`market-analysis-llingr.md`](market-analysis-llingr.md) section 5c).

**The same criticism applies to us.** Our integration tests choose their key distribution too. The
answer is not to stop generating data - it is to sweep the distribution and to state which one
produced any number we publish.

The set worth running, from ceiling to floor:

| Distribution | What it measures |
|---|---|
| **All-unique** (one key per record) | The ceiling. Comparable to a competitor's published conditions, and the max-cold-path case for our own shard bookkeeping |
| **Uniform over N keys**, N above / at / below the in-flight target | Where the shard mechanism starts to bind, and how gracefully |
| **Zipf** (see below) | The realistic case, and the one nobody publishes |
| **Single hot key** | The floor, and the honest limit to state alongside any ordering claim |
| **Clustered runs** (same key repeated consecutively) | Amortisation of per-key setup, and whether traversal stays fair |

**What Zipf means, since it is about to appear in results columns.** A Zipf (or Zipfian) distribution
is the "a few things account for most of the volume" shape: rank the keys by frequency and the *n*-th
most common appears roughly proportional to `1/n` as often as the most common. The single most common
key might be 20% of all records, the next 10%, the next 7%, and a long tail of keys appear once or
twice each. It is named after the linguist who observed it in word frequencies, and the same shape
turns up in city sizes, website traffic and - the reason it matters here - **customer, account and
device identifiers in real event streams.** A handful of large accounts produce most of the events.

**It is the interesting case precisely because it is neither of the two easy ones.** All-unique keys
give perfect parallelism; a single hot key gives none. Zipf gives a workload where a few keys are
genuinely serial bottlenecks while thousands of others are trivially parallel - which is what real
data does, and what a benchmark has to include before its numbers mean anything about production.

**`UNORDERED` mode should be run across all five**, because its result should be flat - concurrency in
that mode does not depend on key distribution at all. Demonstrating that flatness *is* the
differentiator described in the landing-page work (dropped 2026-08-22; see git history for `next-landing-page.md`), and it is a
measurement rather than a claim.

Depends on the harness gaining a key-shape generator; the delay and concurrency sweeps already have the
loop structure to hang it on.

### Zipf specifically is worth its own experiment, later

**Owner's direction, 2026-08-21: record it as interesting to do later.** It is the only distribution
in the table above that nobody in this space publishes, and it is the one closest to production data.

Two questions it would answer that no other distribution can:

- **Where does the shard mechanism start to cost more than it returns?** Under Zipf a handful of keys
  are serial bottlenecks while thousands are trivially parallel. That is the mixture the shard design
  exists for, and it has never been measured - only reasoned about.
- **Does `UNORDERED` stay flat?** It should, by construction, since it ignores keys entirely. A flat
  line across all-unique, uniform, Zipf and single-hot-key would be the clearest possible statement of
  what choosing `UNORDERED` buys, and it is a *measurement* rather than a claim.

Needs a Zipf generator in the harness's produce step - a parameterised exponent, so the skew itself
becomes an axis rather than one arbitrary shape. Not blocking anything; do it when the
key-distribution axis lands.

## Audit: how many concurrent collections can go back to plain ones

**Owner's direction, 2026-08-21, and the reasoning is worth preserving verbatim in substance:** the
concurrent collections were adopted defensively, during a period of bugs, crashes and lockups, and the
response was to make *everything* a concurrent collection just in case. That was the right call under
those conditions. It is not obviously still the right call, and nobody has checked.

**The evidence for concurrent collections costing throughput, honestly stated:**

| For | Against |
|---|---|
| `ProcessingShard.entries` is a `ConcurrentSkipListMap` and its own javadoc says it replaced a `TreeMap` because of "concurrency errors (missing WorkContainers) under high pressure" - a defensive change, explicitly | The 2,750 in-flight plateau is **not** caused by them: a bare `KafkaConsumer` with a thread pool and none of these structures hits the same ceiling ([`bug-in-flight-ceiling-above-2000-concurrency.md`](bug-in-flight-ceiling-above-2000-concurrency.md)) |
| Skip lists are pointer-chasing with poor cache locality, and `size()` on one is O(n) | PC sits within a few percent of that bare-client floor at most measured points, and is *faster* than it at two of them |
| The dispatch scan re-walks each shard's skip list from the head every pass, past every in-flight container | The two points where PC does lag by ~20% are unattributed - they could be collections, or the control loop, or something else |
| The 0.3 -> 0.5 throughput regression is still unattributed, and "more defensive concurrent code" has been the standing hypothesis for it | That hypothesis has never been tested by changing a collection and measuring |

**So the honest ranking: this is worth doing, and it is not the top of the list.** The largest term
by a wide margin is the Kafka client itself - the Java floor reaches 31-67% of the Go floor depending
on the point. Nothing done to PC's collections closes a gap that exists without PC.

**The order that follows from the evidence:**

1. **Virtual threads**, as a measurement first. It is the one term both Java arms share and neither Go
   arm has, and it is a single controlled run.
2. **The two ~20% cells** (2ms at 5,000 concurrent, 100ms at 1,000). Small, well-defined, and PC's own.
   A profiler over these two points would say whether collections are implicated before any are changed.
3. **This audit**, informed by 2 rather than performed on suspicion.

**How to do it when its turn comes**, so it does not become a rewrite:

- **Enumerate every concurrent collection and name the writer set for each.** Several are written only
  by the control loop and read by it - those are candidates outright. `ProcessingShard.entries` is not:
  it is written by worker threads on completion and read by the control loop on dispatch.
- **Prefer a cheaper concurrent structure over a plain one where sharing is real.** Going from
  `ConcurrentSkipListMap` to a plain `TreeMap` behind the existing single-threaded dispatch is a
  correctness argument to be made carefully; going to an ordered structure with better locality is not.
- **Change one collection at a time and measure.** The whole point of the harness is that a
  single-variable change is cheap. A batch change that improves throughput teaches nothing about which
  change did it, and a batch change that breaks something under load teaches even less.
- **Keep the reason the skip list was introduced.** "Missing WorkContainers under high pressure" is a
  correctness bug, not a performance preference. Whatever replaces it must be shown not to reintroduce
  that, under the same pressure - which means the chaos and slam-style tests, not a benchmark.
