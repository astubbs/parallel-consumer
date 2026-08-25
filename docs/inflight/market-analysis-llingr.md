# Market analysis: llingr / llingr-demux - the closest comparable product

<!-- inflight-type: register -->

<https://llingr.io/> · <https://github.com/llingr> · benchmarks at
<https://github.com/llingr/llingr-demux/benchmarks>

Identified 2026-08-20 by the owner while reviewing
[`perf-throughput-regression-since-0-3.md`](perf-throughput-regression-since-0-3.md). Site claims below are **from llingr's published material**, read once and not verified by us - treat
llingr's performance figures as claims until reproduced. Findings marked *source-verified* come from
llingr's public repositories and are checkable.

**Writing convention for these notes (owner, 2026-08-21): refer to the product as `llingr`, never as
"they", "he" or "she".** Focus on the product, not the people behind it. It keeps the analysis about
capabilities rather than personalities, it stays accurate when a one-person project becomes a team,
and it makes the notes safe to quote. The same rule applies to every companion note listed at the
foot of this file.

## Owner's decisions, 2026-08-21 - read these before acting on anything below

   An earlier revision of this note recommended replying to the author's open invitation for
   feedback; **that recommendation is withdrawn** and this supersedes it. Any comparison produced
   here is internal.
2. **Include llingr in private benchmarking where possible**, for research and potentially for
   internal marketing input. llingr's benchmarks are published and re-runnable, which makes this cheap.
3. **Performance is not where to compete.** The two projects already share the processing model, and
   a Go engine will be faster than a JVM one. Competing on throughput is competing on llingr's ground.
   **The differentiators to press are features** - auto-scaling first
   ([`next-auto-scaling.md`](next-auto-scaling.md)), and the offset-encoding and transaction
   capabilities below, which llingr does not appear to have.

## Terms and questions, answered

**TLA+** - "Temporal Logic of Actions", Leslie Lamport's specification language for concurrent and
distributed systems (TLA is the logic; TLA+ the language built on it; PlusCal a pseudocode front-end
that compiles to it). You describe the *design* - states and permitted transitions - and the TLC model
checker then **exhaustively explores every reachable interleaving**, rather than sampling the way
a test does. It checks *safety* properties ("this must never happen") and *liveness* ones ("this must
eventually happen"). AWS is the well-known industrial user (S3, DynamoDB); Azure Cosmos DB and MongoDB
have also published on it. It verifies a **model you write**, not your source code - so it finds
design races, and it cannot tell you the implementation matches the model. That is why llingr asserts the
model's invariants in unit tests as well, and why we should if we do this.

**"Marketplace (coming soon)"** - a commercial distribution channel, appearing beside "Arrange a Demo"
on the JVM and Rust pages. The JVM artifacts are already behind a **licence-key-authenticated Maven
repository** (the llingr example puts `${llingr_license_key}` in a `<server>` block in `settings.xml`), so
"marketplace" most likely means cloud marketplace listings for billing and procurement. **The JVM
build is not freely downloadable.**

**"Patent pending" - what could there be?** A fair question, and the honest answer is that nothing in
the *architecture* looks novel: hash-key-to-worker fan-out with a contiguous-commit frontier is old,
and **Parallel Consumer itself is public prior art from 2020**. What patent applications usually claim
in this situation is not the architecture but a specific *mechanism* - the llingr shard-padding and
cold/hot worker-parking scheme, or the particular pre-commit resolution algorithm. Whether such a
claim survives examination is a different question from whether it is filed, and filings are cheap
relative to deterrent value. **Not legal advice**, and nothing here should be relied on as a
freedom-to-operate opinion - but the practical effect on us is small, because PC's approach is
independently developed, publicly documented and predates the filing.

**AGPL-3.0 vs PC's Apache-2.0, for a company** - this is the biggest practical gap between the two
projects, and it has nothing to do with engineering:

| | Apache-2.0 (PC) | AGPL-3.0 (llingr engine) |
|---|---|---|
| Kind | Permissive | Strong copyleft, **plus the network clause** |
| Using it in a closed-source product | Fine | The commercial licence is the practical route |
| The network clause (§13) | n/a | If users interact with a modified version **over a network**, you must offer them its source. A Kafka consumer is exactly a network service. |
| Linking as a library | Fine | Under the FSF's reading, linking creates a derivative work - AGPL is not LGPL |
| Patent grant | Express grant from contributors, with retaliation clause | §11 provides for patents, but the vendor holds a pending patent and sells a separate licence |
| Typical corporate policy | Approved by default | **Frequently banned outright** - Google's published policy is the well-known example - and otherwise requires legal review |

For a meaningful share of enterprise evaluators, AGPL ends the conversation before any benchmark is
run, and the commercial licence turns an engineering choice into a procurement one. That is not a
criticism of llingr's model - it is deliberate, and it is how the product is funded.

**Control record gaps** - Kafka's transactional **commit and abort markers occupy real offsets in the
log but are never delivered to consumers**. So a consumer reading a transactional topic sees offsets
it will never receive. Any design that waits for a *contiguous* run of processed offsets must know
these gaps are permanent, or it waits forever.

**Transaction boundary gaps** - the same problem from the other side: under `read_committed`, records
belonging to **aborted** transactions are filtered out by the client and never delivered, again
leaving permanent holes.

**Log compaction gaps** - a compacted topic deletes superseded records for a key, so historical
offsets simply disappear.

All three say the same thing: **offsets are not contiguous, and a system that assumes llingr is will
stall.** Note who this bites harder. llingr's entire commit design is a contiguous-commit pointer, so a
permanent gap is an existential case that must be special-cased - which is presumably why llingr calls these
out prominently. PC tracks *incompletes* rather than waiting for contiguity, and has already done the
compaction work (`confluentinc#409`). **Worth confirming PC handles the other two deliberately rather
than incidentally.**

## Where PC differentiates - analysis, 2026-08-21

Written to the owner's framing: **not on throughput**. llingr shares the processing model and a Go
engine will beat a JVM one, so throughput is llingr's ground. What follows is ranked by how defensible
each one looks, and each says what would falsify it.

### 1. Committing ahead of gaps. This is the deep one.

llingr buffers out-of-order completions in an **unbounded in-memory slice** and commits **only the
highest contiguous offset**. llingr's own telemetry exposes `gapBufferDepth` and `highestReadyOffset`
sitting ahead of `committedOffset`, and the README asks operators to alert on that depth growing.

PC encodes the *incomplete offset set* into commit metadata and commits **past** the gaps. Three
consequences that are not marketing:

- **One pathological key throttles its partition.** A key that is slow, retrying, or stuck holds the
  contiguous-commit design back; everything completed behind it stays uncommitted. That is
  head-of-line blocking reappearing at the commit layer - the exact problem the product exists to
  remove, displaced one level down.
- **The buffer is bounded, so the failure mode is a stall.** Wide gaps exceed the cache and
  backpressure must kick in. Their own docs tell users to raise it "for high-jitter workloads where
  widely varying processing times leave wide gaps" - which is a tuning knob PC does not need,
  because the gap set is encoded rather than buffered.
- **Restart reprocesses everything after its commit point.** PC restarts knowing exactly which offsets
  were done.

**Falsifiable how:** if llingr adds offset-metadata encoding, this collapses. It is the hardest part of
the problem - PC has `OffsetSimultaneousEncoder`, run-length and bitset encodings, compaction
handling and a density benchmark behind it - so it is unlikely to be a quick follow.

#### MEASURED 2026-08-21, and the first bullet above is WRONG as written

`bench/run-divergence.sh` now tests this directly; the full write-up, the caveats and the raw data are
in [`perf-throughput-regression-since-0-3.md`](perf-throughput-regression-since-0-3.md) under *"The
other benchmark"*. Three corrections, kept here because this is the note that makes the claim:

- **"One pathological key throttles its partition" is true of PC too.** With one record stuck for 25
  seconds, **both engines' committed offsets froze at exactly the same offset** - PC commits the
  lowest incomplete offset, like any contiguous-commit design. The head-of-line blocking at the commit
  layer is not something PC avoids; the difference is entirely in the *metadata*, where PC wrote nine
  bytes (`RunLengthV2`, runs `[1, 199998]`) and llingr wrote its franz-go client UUID. **The
  committed offset is the wrong metric for this comparison in either direction.**
- **"The buffer is bounded, so the failure mode is a stall" was not observed.**
  `CommitPartitionSliceLen` is documented in llingr's own config README as *pre-allocating* gap
  tracking space (default 400, min 50, max 2,000), and a 199,998-record gap did not stall it - it
  finished the dataset at full speed and committed nothing. **The failure mode is silent redelivery on
  restart, not backpressure.** Weaker than this note claimed, and more accurate.
- **"Restart reprocesses everything after its commit point" is confirmed, at 100.0%.** Crash mid-run and
  restart: llingr redelivered every one of the ~100,000 records it had already completed; PC
  redelivered 6.4% of them, and that 6.4% was purely the commit interval, not the encoding.

**And the advantage is a ratio, not a constant.** Wasted work is about `commitInterval x throughput`
for PC against `timeSinceTheStall x throughput` for a contiguous-commit design - so a crash within one
commit interval of the stall costs both designs the same. Measured at 15.6x (500ms commit interval),
6.4x (5s default, 20ms handler) and 1x (a run shorter than two commit cycles). Any headline number
has to carry that.

**Not measured, and the most likely falsifier:** many small gaps rather than one large one. PC's
metadata is capped at 4KB with backpressure above 75%, so a scattered incomplete set is where the
encoding could actually fail - and it is exactly the case llingr's docs describe as their tuning
scenario. Do not publish the advantage internally-or-otherwise as unconditional until that arm exists.

### 2. Licence and patent posture. The most practically decisive, and the least technical.

PC is **Apache 2.0**, with no patent claim. llingr is **AGPL-3.0 plus a commercial licence for
closed-source use, patent pending**. AGPL is a hard procurement blocker at a large share of
enterprises, and "patent pending" is a second one for anybody whose legal team reads it. For a
material set of evaluators this decides the question before any benchmark is run.

**Falsifiable how:** llingr relicenses. Given the commercial model, unlikely.

### 3. Ordering modes beyond per-key.

PC offers `UNORDERED`, `KEY` and `PARTITION`. llingr's model is per-key by construction - keys route
to workers. Unordered is materially faster when ordering is not required, and partition ordering is
what users migrating from a plain consumer actually want first. **Verify before relying on this**: it
is inferred from llingr's architecture, not from a statement that other modes are absent.

### 4. Transactional exactly-once produce.

PC has `PERIODIC_TRANSACTIONAL_PRODUCER`. Nothing on llingr's site indicates transactional produce; the
only transaction references are `read_committed` fetch isolation (consuming transactional data) and
"transaction latency" used loosely. Caveat this honestly in any comparison: **PC's own external
engines throw on transactional mode**, so this is a core-only differentiator - and the llingr Relay is the
thing it would be compared against.

### 5. Auto-scaling. Confirmed from llingr's documentation, not inferred.

**llingr has exactly the same dial PC has, and says so.** From the llingr configuration reference:

> `ConcurrentKeys` **Default: 250 · Min: 1 · Max: 5,000. The primary performance tuning parameter.**
>
> *"Rate limiting is not normally recommended - tuning ConcurrentKeys is generally more effective for
> backpressure control."*
>
> *"If your database connection pool has 200 connections and each message requires one write, setting
> ConcurrentKeys above 200 creates backpressure at the database layer."*
>
> *"The engine has no opinion on Kubernetes timing, **autoscaling**, or the Kafka client's blocking
> timeouts."*

The site's "Auto-scaling on a dial, not a budget meeting" is a claim about **manual dialling being
cheaper than re-partitioning**, not about adaptive concurrency. They disclaim autoscaling explicitly.

So [`next-auto-scaling.md`](next-auto-scaling.md)'s differentiator claim - *"no known competitor does
runtime-discovered, per-instance adaptive concurrency"* - **survives contact with the closest
competitor there is.** The earlier caveat on this note is discharged.

And llingr's documentation makes our argument for us: the value of `ConcurrentKeys` depends on the
user's database connection pool. That is a runtime property of someone else's system, which is exactly
the case for discovering it rather than configuring it.

### 5a. Why this outranks engine speed

Owner's framing, and the measurements support it: **engine performance matters far less than user
processing delay, and far less again than getting the concurrency right.** At a 2ms simulated delay
this session measured the *same build* spanning 16,300 to 31,400 msg/s purely by changing
`maxConcurrency` - a 1.9x swing from one setting - while the whole five-year engine regression was
35%. Raise the per-record delay to anything realistic (an HTTP call, a database write) and the
engine's own cost disappears into the noise entirely. **The setting dominates the engine.** Competing
on engine microseconds optimises the smallest term.

### 5b. The one thing llingr *does* do dynamically - and why it is still not auto-scaling

**Correction recorded 2026-08-21**, after the owner pushed back on an earlier over-simplification
here. It is not fair to say llingr's concurrency is purely static.

**The shape, in terms a JVM reader will recognise:** it behaves like a bounded executor pool that
creates a worker when work arrives for a key that has none, and retires that worker after an idle
period. `ConcurrentKeys` is the pool's maximum size, not its fixed size. So the number of live workers
does rise and fall with the offered load, and a workload touching ten keys does not pay for 250
workers. That is genuine demand-driven scale-out, and calling it "a fixed pool" was wrong.

**Why it is still categorically different from what [`next-auto-scaling.md`](next-auto-scaling.md)
proposes:**

- **It measures nothing.** The only input is "did a record arrive for a key that has no worker". There
  is no observation of throughput, latency, queue depth, or of anything downstream.
- **It has no feedback term, so it cannot scale *down* under pressure.** If the database is
  saturating, more distinct keys still means more workers - the mechanism pushes in the direction that
  makes it worse. A closed loop would back off.
- **The ceiling is still the guess.** `ConcurrentKeys` is exactly the number llingr's own
  documentation says to derive from your database connection pool. Growing up to a hand-chosen ceiling
  is not discovering the ceiling.

**A first attempt at the one-liner was "llingr reacts to its own input; adaptive concurrency reacts to
the system it is feeding" - and the owner is right that this is wrong**, because it understates what
PC's system would do. Correction recorded 2026-08-21.

Adaptive concurrency is not only about the *downstream* system. Consider an infinitely scalable
external service that never slows down, and a single PC instance: **raising max concurrency from 1,000
to 1,000,000 makes it slower anyway**, because of costs entirely internal to the pipeline - control-loop
bookkeeping, shard traversal, work-queue contention, thread scheduling - and costs *upstream* through
the Kafka client, in fetch sizing, buffer pressure and commit round trips. **A closed loop measuring
delivered throughput observes that turnover and stops at the real optimum, without needing to know
which term caused it.** That is the point of measuring the outcome rather than modelling the causes.

So the distinction is about **whether there is a measurement at all**, not about where the bottleneck
lives:

- **llingr's mechanism has one input** - "did a record arrive for a key with no worker" - and therefore
  one behaviour: grow. It cannot detect that growth stopped helping, wherever the limit came from.
- **Adaptive concurrency observes the achieved result** and finds its own ceiling, whether that ceiling
  is the user's database, PC's own overhead, or the Kafka client's fetch pipeline. **All three are
  discovered by the same loop**, which is exactly why it is worth building rather than documenting a
  formula for the user to apply.

**And it does not stop at the instance boundary.** Once an instance has found its internal maximum and
partitions remain unclaimed, the correct next move is *external*: signal for another instance - a
Kubernetes replica joining the group - then **wait for the metrics to settle before reassessing**,
because the rebalance itself perturbs every measurement the decision depends on. That
settle-then-reassess step is the part a naive autoscaler gets wrong, and PC is in the rare position of
being able to see both sides of it: it knows its own saturation *and* it is a group member that knows
how many partitions are unclaimed.

**Phrasing to use from now on:** *adaptive, feedback-driven* concurrency, with **internal and external
scaling as one loop** - not merely *dynamic* concurrency, which is now contestable. The differentiator
claim in [`next-auto-scaling.md`](next-auto-scaling.md) is unaffected and should be restated in these
terms.

### 5c. What llingr's benchmark actually feeds itself - every key unique

**Read from the source, 2026-08-21**, after the owner pointed out that the *number* of keys is the
uninteresting half of the question and the *distribution* is the half that decides everything. The
generator is published, so this is not inference.

`tests/testkit/scenario/generate_messages.go`, the entire key assignment:

```go
Key: fmt.Sprintf("key-%d-%s", i, randomString(30)),
```

`i` is the global message index. **Every message in the benchmark has a globally unique key.** Not
modulo-cycled over a fixed set, not random from a range, not skewed. Distinct keys equals message
count: 100,000 distinct keys per run, perfectly interleaved, no key ever repeating.

**The generator takes no key-count parameter at all.** `GenerateMessages(messageCount, numPartitions)`
- the `-keys` flag and the `concurrent_keys` JSON field feed `ConcurrentKeys` only and never reach the
data. So key cardinality is not a dimension of the published benchmark; it is a constant, pinned at
its maximum.

**Why this is the best case by construction.** The worker map is keyed by record key. With every key
unique it *never* finds an existing worker, so no record ever queues behind a sibling. **The
mechanism the library exists to provide - serial ordering within a key - is never exercised in a
single published run.** Nothing in the data can bind concurrency, so the concurrency dial is the only
thing that can, which is precisely the result the chart reports.

The efficiency denominator makes the coupling explicit (`tests/testkit/hostapp/host_app.go`):

```go
theoreticalTPS = float64(concurrentKeys) * (1_000_000.0 / float64(h.processorLatency.Microseconds()))
efficiency     = (actualTPS / theoreticalTPS) * 100.0
```

The ideal is *"all `ConcurrentKeys` workers busy 100% of the time"*. With unique keys that ideal is
attainable; with any finite key distribution it is not, and the same formula would print a much lower
number for reasons that have nothing to do with the framework. **The published efficiency figures
(99.1% / 98.0% / 93.7%) are an upper bound that a real key distribution cannot approach, and the
distribution is not stated next to them.**

**The fair half of the reading, which should be stated too.** Unique keys make the *framework overhead*
measurement genuinely harder, not easier: every record pays the cold path - worker borrow, map insert,
map delete, guard acquire and release - with no amortisation across a key's run. llingr's own framing
of the number ("how much throughput the framework loses to coordination overhead") is an accurate
description of what was run. The measurement is not rigged; it is narrow, and the narrowness is on the
axis that carries the headline claim.

**Bounded-cardinality cases do exist in llingr's repo, and none of them are benchmarks.** `tests/`
contains `key-%d` with `i % k` for k in {1, 2, 3, 5, 10, 50, 250}, and a 2M-message run over 500
random keys - including the fully adversarial single-key case. All are **correctness** assertions
(every record processed, offsets monotonic, no duplicates) at `ConcurrentKeys: 4`, and none report
throughput or efficiency. **There is no Zipf, hot-key, or skew model anywhere in the repository.**

**Other things the harness removes from the measurement**, worth knowing before comparing any number
of ours to any number of theirs:

- **`Poll` is a slice index into an in-memory `MockBroker`** - no fetch, no decode, no rebalance, no
  commit round trip, no broker at all.
- **The handler ignores the record entirely and sleeps.** The 508-byte value field is written once at
  generation and never read anywhere in the repo, so deserialisation and payload cost are zero.
- **Startup and shutdown are excluded** from the timing window.

### 5e. Correcting "the measurement is honest" - it is internally valid and externally meaningless

**The owner pushed back on this and is right.** An earlier draft of 5c conceded that llingr's
efficiency figures were an honest measurement of coordination overhead, narrow but not rigged. That
concession was too generous, for a reason that only becomes visible once the key scheme is known.

**With every key unique, per-key ordering is vacuous.** A key with exactly one record cannot be
ordered against anything. So the published benchmark is measuring the engine **in effect running
unordered** - in a product that **has no unordered mode**. Every configuration a user can actually
select does something the benchmark never does.

That reframes what the number is. It is not a narrow measurement of the product; it is a measurement
of a mode the product does not offer, under conditions no deployment can reproduce:

- **No ordering work.** The worker map never hits, so no record ever queues behind a sibling.
- **No broker.** `Poll` is a slice index into memory.
- **No payload.** The handler ignores the record; the 508-byte value is never read anywhere in the
  repository.
- **No startup or shutdown** in the timing window.

**What survives is narrow but real**, and worth keeping straight rather than overcorrecting: the
figures do bound llingr's per-record bookkeeping cost, and unique keys make that particular cost
*higher*, not lower, because every record pays the cold path. **As a microbenchmark of one code path
it is valid. As a statement about what the product does for a user it is not, and it is published as
the latter.**

### 5f. Protocol overhead is most of what a Kafka consumer does, and it is excluded

**Owner's point, 2026-08-21, and it is the sharper half of the criticism.** How an engine talks to a
real broker - fetch sizing, prefetch depth, buffering, decompression, deserialisation, commit round
trips, rebalance handling - is not overhead around the measurement. **For a Kafka consumer it is a
large fraction of the work**, and it is exactly the part that an in-memory mock deletes.

We have measured this, on our own harness, against a real broker with our own dataset
(`bench/results/delay-sweep-llingr.csv`): at 100ms delay and 100 concurrent, llingr reached 974.6
msg/s against a theoretical 1,000 - **97.5% efficiency with a real broker in the path.** That is a
good result and it should be stated as one; the engine is not fragile. But it is a result at
**concurrency 100**, and the published headline is at **5,000**, where the fetch pipeline has to
supply fifty times as many records per unit time and an in-memory slice index no longer stands in for
a broker.

**And it lands on us too, which is the actionable part.** Our own harness must not quietly exclude the
same costs to make our numbers look better:

- **Keep the real broker in the path**, always. It is already how `bench/` works and it should stay a
  stated property of every number we publish, not an implementation detail.
- **Publish what the real broker reveals, rather than hiding it.** Running PC at maxConcurrency 5,000
  and 100ms turned up a ceiling nobody knew about: in-flight plateaued around **2,750** and would not
  move. It is **not** the broker - partition count and `max.poll.records` were both ruled out by
  controlled runs - which makes it a limit inside PC itself, and one that no in-memory harness would
  have surfaced because nobody would have run the configuration. Tracked in
  [`bug-in-flight-ceiling-above-2000-concurrency.md`](bug-in-flight-ceiling-above-2000-concurrency.md).
- **Partition count is therefore an axis too**, now recorded as a column in `bench/`'s results. A
  number taken at one partition and a number taken at ten are two different experiments, and printing
  them in the same table without the column is how a confound gets published.

**The general rule, for the landing page as much as the harness:** show the honest number with the
protocol cost in it. It is smaller, it is defensible, and it is the number the reader's system will
actually produce.

### 5g. The unordered comparison, run - and PC loses this one

**Owner's instruction, 2026-08-21:** *"so they're just running unordered benchmarks. In which case
add PC unordered processing to the comparison table. Set PC to max 5,000 and unordered and see how
they compare."*

**Done, and the result is not the one we would have chosen. It goes in exactly as measured**, per the
honest-comparison charter in
[`parked-testing-as-a-feature-for-the-clients.md`](parked-testing-as-a-feature-for-the-clients.md) -
publishing the case we expect to lose is the whole point of having the charter.

Conditions, identical for both arms: **real broker**, one dataset of 500,000 records with all keys
distinct, 10 partitions, 100ms sleep handler, PC in `UNORDERED` mode, one consumer group per run.
Raw data: [`bench/results/high-concurrency-unordered.csv`](../../bench/results/high-concurrency-unordered.csv).

| Concurrency setting | PC msg/s | PC peak in flight | llingr msg/s | llingr peak in flight | Theoretical |
|---:|---:|---:|---:|---:|---:|
| 250 | 2,241 | **250** (exact) | - | - | 2,500 |
| 1,000 | 7,318 | **1,000** (exact) | - | - | 10,000 |
| 5,000 | **19,577** | **2,751** | **37,744** | **5,000** (exact) | 50,000 |

**llingr is 1.9x faster at 5,000, and the reason is not engine speed** - it is that llingr reaches
its configured concurrency and PC does not. PC plateaus at 2,751 records in flight against a setting
of 5,000. Normalised against the concurrency each actually achieved, the two are much closer: PC
19,577 against an achievable 27,510 (71%), llingr 37,744 against 50,000 (75%).

### The franz control arm settles it: llingr's engine contributes nothing to that gap

**The owner's read on seeing the 1.9x - "that sounds a lot like franz-go" - is correct, and the
control arm was built for exactly this question.** `bench/franz` drives franz-go with **no engine at
all**: a bare client, a fixed worker pool, a sleep and a counter. Whatever it scores is the floor, and
only what llingr scores *above* that floor is attributable to llingr.

| Concurrency | franz-go, no engine | llingr | llingr's margin over the bare client |
|---:|---:|---:|---:|
| 250 | 2,437.7 | 2,434.0 | **-0.15%** |
| 1,000 | 9,326.3 | 9,306.0 | **-0.22%** |
| 5,000 | 37,599.6 | 37,698.9 | **+0.26%** |

**Three settings, three matches inside a quarter of one percent.** llingr's engine adds nothing
measurable over a plain franz-go consumer with a worker pool - and at 5,000 it is very slightly
*behind* the floor at two of the three points, which is what "no measurable difference" looks like
when you are inside the noise.

**This is not a criticism of llingr's engine and should not be written as one.** It is the expected
result, and it is our own argument from 5a coming back around: at a 100ms handler the engine's own
cost is a rounding error against the sleep. Any competent engine scores the client's floor here. What
the table actually establishes is **which component the difference lives in** - and it is the client
library, not the engine.

**The consequence for the comparison is large.** The 1.9x is a **franz-go versus Java Kafka client**
result that a PC-versus-llingr framing would have mis-attributed entirely to the engines. Every
cross-language number in this note has to carry the same control or it means nothing.

**And it exposes the arm we are still missing.** There is a franz-go floor and there is no Java floor:
`vanilla` is single-threaded by construction, so nothing measures *the Java client plus a thread pool*
at a given concurrency. Without it, PC's figure bounds PC and the Java client **jointly**, and any
deficit is silently charged to PC. A `pool` arm - plain `KafkaConsumer`, fixed thread pool, semaphore,
sleep, counter, no engine - has now been added to `bench/Bench.java.template` to close that gap. Until
it has been run, **the honest statement is that the PC column is an upper bound on PC's own cost, not
a measurement of it.**

**That plateau is a defect, and it is now tracked as one** -
[`bug-in-flight-ceiling-above-2000-concurrency.md`](bug-in-flight-ceiling-above-2000-concurrency.md).
It is not the broker, not the fetch pipeline, not the harness and not the load-factor buffer: four
controlled runs ruled each of those out. It is a dispatch-rate limit inside PC, and at 250 and 1,000
concurrency it does not appear at all - the engine hits those settings exactly.

**Three things this run is worth for, beyond the number:**

- **It found a real defect that nothing else would have.** Nobody had run PC at 5,000 concurrency, so
  nobody had seen the ceiling. The competitor comparison paid for itself here regardless of who won.
- **It confirms the protocol-overhead point from 5f, measured on the other side.** llingr publishes
  93.65% efficiency at 5,000 keys on an in-memory mock; against a real broker with a real fetch
  pipeline it reached **75.5%**. A real broker costs it about 18 points, which is precisely the cost
  its published figures exclude - and roughly the size of the gap it claims elsewhere.
- **It is the number to beat, and the target is legible.** Fixing the dispatch ceiling is worth more
  than any micro-optimisation in the engine, because it is the difference between honouring the user's
  setting and quietly not.

**And it is not a reason to change the positioning.** Per 5a, the concurrency setting dominates the
engine and the engine dominates nothing; the differentiators worth pressing remain recovery cost,
ordering modes, licence, ecosystem reach, and adaptive concurrency - the last of which would have
*found* this ceiling by itself.

### 5d. What this means for our own harness

Two direct consequences, both actionable:

- **This is the same thing our integration tests do**, as the owner noted - we choose the key
  distribution too, and we should hold ourselves to the standard we are applying here. The fix is not
  to stop generating data; it is to **state the distribution next to every number we publish**, and to
  publish more than one distribution.
- **Key distribution becomes a first-class axis in `bench/`**, alongside delay and concurrency. The set
  worth running: all-unique (the ceiling), uniform over N keys for N above, at and below the
  concurrency target, Zipf (the realistic case), and single-hot-key (the floor). Recorded as a task in
  [`next-performance-regression-testing.md`](next-performance-regression-testing.md).

**And the landing-page consequence**, per the state-what-we-do rule: never quote an efficiency figure
without the distribution that produced it. A number whose conditions are stated is more persuasive than
a bigger number whose conditions are not - which is the whole argument of
[`parked-testing-as-a-feature-for-the-clients.md`](parked-testing-as-a-feature-for-the-clients.md).


### 6. Ecosystem depth on the JVM.

Kafka Streams integration, and native Vert.x / Reactor / Mutiny engines rather than a sidecar. For a
JVM shop already reactive, PC is a library; the comparison is not sidecar-versus-sidecar.

### Feature comparison: PC master today vs llingr

Deliberately **current master only** - no historical versions - because that is what a user would
evaluate. PC column from this repository; llingr column from llingr's published docs, unverified.

| | PC (master) | llingr |
|---|---|---|
| Key-ordered concurrency past partition count | Yes | Yes |
| Ordering modes | **`UNORDERED`, `KEY`, `PARTITION`** as first-class options | Per-key only as a mode, but **emulable by rewriting the key** via `WithExtractEnvelope` - at the cost of worker-per-message churn for the unordered case |
| Commit strategy | **Encodes the incomplete-offset set into commit metadata; commits past gaps** | Commits the highest contiguous ack only; out-of-order held in an **unbounded** in-memory slice |
| Produce path at all | **Yes - poll-and-produce, produce tied to commit** | **None.** `BrokerPort` is `Poll`/`CommitOffsets` only; the user produces with llingr's own client |
| Transactional EoS produce | Yes, core (`PERIODIC_TRANSACTIONAL_PRODUCER`); external engines throw | **Explicitly declined** - "Possible, but costly" |
| Retry policy | Delay providers, custom backoff, max-retry skip | **None documented.** An error goes straight to dead-letter |
| Topics per consumer | Many (subscription/pattern) | **One.** "For multiple topics, create multiple consumers" |
| Batching | Yes (`batchSize`) | **None.** All three language handler signatures take one message |
| Concurrency setting | Manual (`maxConcurrency`) | **Manual (`ConcurrentKeys`)** - same problem |
| Adaptive concurrency | Roadmap, astubbs#227 | **Explicitly disclaimed** |
| Dead letter queue | **Not built in** (astubbs#149 open) | **Built in and mandatory**, with a `WriteDeadLetter` callback |
| Engine languages | JVM only | **Go (original), native JVM, Rust via FFI** |
| Polyglot | Sidecar proxy, unreleased (astubbs#242) | Relay gRPC container - **announced, not shipped**: no bridge repo exists in llingr's GitHub org, docs say "coming soon" |
| Broker adapters | Kafka | Kafka (2 clients). **NATS and Pulsar are marked "planned"** - no adapter repos exist |
| Formal verification | None | **TLA+, two models** |
| Chaos testing | Yes; rebalance-focused | Yes; broader, incl. network fault injection |
| Mutation testing | Yes | Yes |
| Metrics | Micrometer | Prometheus bundled, pluggable sink |
| Web GUI | In progress (astubbs#215/#268) | Not evident; rich JSON snapshot endpoint |
| Licence | **Apache-2.0** | AGPL-3.0 + commercial, **patent pending** |
| Availability | Free, Maven Central | JVM build behind a **licence-key Maven repo** |
| Production history | Years | **~3 months public.** Company incorporated 2026-05-08, GitHub org 2026-05-16, engine repo 2026-05-18 |

### The roadmap read against llingr

From [`docs/data/roadmap.yaml`](../data/roadmap.yaml) (merged to master, `reviewed_at_release:
0.6.0.0`). This is the more useful comparison than the feature table, because it says whether the
plan closes the gaps or widens the lead.

**Roadmap items where llingr is already there - we are catching up, not leading:**

| Roadmap entry | Horizon | Their position |
|---|---|---|
| `dead-letter-queue` | next-0x | **Shipping**, and first-class in the llingr polyglot protocol |
| `running-instance-visibility` | **1.0** | Snapshot endpoint today; ours is the furthest-out horizon we have |
| `docs-site` | next-0x | Full site today, plus a promotional one |
| `virtual-threads` | next-0x | JVM build is Loom-aware now |
| `current-java-and-kafka` | next-0x | JDK 21+ baseline; PC still targets Java 8 bytecode |
| `health-check-surface` | next-0x | Circuit breaker today |

**Roadmap items with no counterpart there - these widen the lead:**

| Roadmap entry | Horizon | Note |
|---|---|---|
| `streams-parallelism-preview` | **0.6.0.0** | Per-key parallelism for a Kafka Streams topology. No equivalent |
| `connect-integration-preview` | **0.6.0.0** | Kafka Connect sink on PC. No equivalent |
| `survive-producer-fencing` | next-0x | Only meaningful because PC has transactional produce at all |
| `micro-batching` | next-0x | Batch by size and time; not evident there |
| `delivered-value-metrics` | next-0x | Measuring the thing the library exists to improve |

**And the finding that matters most: auto-scaling is not on the roadmap.**

The owner has named adaptive concurrency as *the* way to differentiate, this note confirms the
closest competitor explicitly disclaims it, and [`next-auto-scaling.md`](next-auto-scaling.md) raised
it on 2026-08-18 to candidate killer feature - but `roadmap.yaml` has no entry for it. astubbs#227 is
open on the tracker, which the roadmap's own reader contract says is where individual issues live;
the question is whether this has outgrown that. **If it is the differentiator, it should be something
"a reader can watch finish"**, which is that file's stated bar for an entry.

Two other observations from the same read:

- **The two 0.6.0.0 feature previews are the strongest near-term differentiators in the whole
  comparison**, and both are integrations rather than engine work - consistent with the owner's
  framing that engine speed is the smallest term.
- **`bounded-buffers`** (next-0x) is worth reading against llingr's design: the llingr pre-commit cache is
  explicitly bounded and *documented as a tuning knob* for high-jitter workloads. Bounding our buffers
  should not quietly import the stall behaviour that bound implies for llingr - PC's advantage is that
  the incomplete set is encoded rather than buffered, and a bound applied in the wrong place would
  give that away.

### Two engines, one binding - and no published mechanism keeping them in sync

Answering the owner's challenge that you cannot call Rust a native engine if it wraps Go. **Correct,
and llingr agrees** - the llingr FAQ says so explicitly:

> *"Is the Rust crate a port of the engine? **No, a binding.** Where the JVM edition is a native
> re-implementation of the verified design, **the Rust edition embeds the Go engine itself, compiled
> as a C library.**"*

Independently verified in the llingr repository: `llingr-rs-kafka` contains a `bridge/` directory of Go
source whose `go.mod` requires `github.com/llingr/llingr-demux v0.12.2`, with `src/ffi.rs` and
`src/trampolines.rs` as the FFI layer. Building the crate compiles Go. So the count is **two claimed
engine implementations (Go, JVM) and one FFI binding (Rust)** - plus the relay, which is a network hop
to the Go engine. Four consumption paths, two engines.

**How are the two engines kept behaviourally in sync? No mechanism is published.** Searched every
site page and every public repo for a conformance suite, compatibility vectors, a golden-trace
corpus, a shared machine-readable specification, or a model-derived test generator. **Not found.**
What exists is three prose assertions: a `/jvm/` bullet reading *"Same chaos testing suite"* with no
elaboration and no repository, and an FAQ answer that the verification invariants *"are integral to
the technology, and the codebase faithfully aligns with them."* That is an assertion of care, not a
mechanism.

**This is a real and interesting weakness, and it is the same problem astubbs#242 solved
deliberately.** PC's proxy work built a **shared cross-language conformance suite** precisely because
"ten definitions of correct is no definition at all" - eleven client implementations driven through
the same scenarios, where agreement is evidence rather than coincidence. llingr has two independent
implementations of a formally-verified design and nothing published that checks one against the
other. Their TLA+ work is also described entirely in Go terms - the discovered race is a Go race -
and nothing states the models were re-checked against the JVM code.

**Their feature-parity claim is contradicted in llingr's own FAQ**: burst-capacity overflow is
*"Currently supported in Go only"*, against *"full feature parity"* elsewhere on the same page.

### Corrections after verification, 2026-08-21

An earlier revision of this note overstated llingr's position in four places. Verified against all 15
site pages, llingr's GitHub org listing and four repository READMEs:

- **"A shipping polyglot story" was wrong.** `llingr-relay` is **announced, not shipped** - llingr's docs
  list `llingr-grpc-kafka-bridge (coming soon)` and llingr's GitHub org contains **no bridge or relay
  repository**. astubbs#242 is unreleased; so is theirs. The race is level, not lost.
- **"Non-Kafka brokers" was wrong.** NATS and Pulsar adapters are marked **planned**, and neither
  repository exists. Today llingr ships Kafka only, via two clients.
- **"Retry control: circuit breaker; DLQ routing" was too generous.** No retry policy is documented at
  all - **a handler error goes straight to dead-letter**. Their circuit breaker is a shutdown valve,
  not a retry mechanism.
- **The relay's direction is inverted from PC's proxy.** llingr's relay is the gRPC *client* and the
  user's app is the *server*; PC's sidecar is the server and the client dials in. Worth noting in
  astubbs#242 - llingr chose the shape Beam explicitly rejected, for the reason Beam gives (runners
  often cannot accept inbound connections).

### Things llingr has that we do not - the honest list, revised

1. **A built-in dead letter queue**, mandatory, with a first-class callback and a place in the
   correctness story. PC has astubbs#149 open and unbuilt. **This is the clearest single gap.**
2. **Formal verification**, and the race it found - see
   [`next-formal-verification-and-correctness-methods.md`](next-formal-verification-and-correctness-methods.md).
3. **A heartbeat in the polyglot protocol.** llingr's five methods are `ProcessMessage`,
   `WriteDeadLetter`, `SendMetrics`, `NotifyShutdown`, `Heartbeat`. PC's proxy has `dispatch`
   negotiated today; leases and heartbeats are designed but unimplemented by any client.
4. **A published micro-optimisation account** and per-core efficiency claims (~1.26µs/message,
   ~2% CPU). PC has never measured its own per-message overhead as such.
5. **Richer runtime introspection**: a snapshot endpoint with per-partition committed vs
   highest-ready offsets, gap-buffer depths, guard-channel utilisation, per-shard worker counts, and
   sliding 15-second throughput windows with latency.
6. **A JDK 21 baseline**, against PC's Java 8 bytecode target.
7. **Marketing and documentation**, which is a real asset and not a soft one.

### And the list the other way - verified advantages, several of them large

The earlier draft undersold this badly. Every item verified against llingr's published material:

1. **No first-class unordered or partition-ordered mode** - but the position is softer than an
   earlier revision of this note claimed. There is no mode selector: dispatch is unconditionally
   `FNV(key) -> shard -> worker`, and the llingr FAQ sends unordered workloads to Share Groups, i.e.
   to a different product. **However, key extraction is pluggable** via `WithExtractEnvelope`, so a
   user can rewrite the key to emulate the other modes - return the partition number for
   partition-ordering, or a unique value per message for unordered.

   The costs are real and worth stating rather than dismissing: a unique key per message creates a
   worker per message, each holding a concurrency token, so throughput is then capped by
   `ConcurrentKeys` and worker churn; and an empty key collides on one worker, fully serialised. So
   PC's advantage here is **first-class, tuned modes versus an emulation with a performance cliff** -
   still an advantage, but not the absence of the capability.
2. **No batching, anywhere.** Every handler signature in Go, Rust and the JVM takes one message.
3. **No produce path at all.** `BrokerPort` is `Poll` + `CommitOffsets`; there is no produce
   operation in the contract. PC's poll-and-produce, with the produce tied to the offset commit, has
   no counterpart - and transactional EoS is therefore structurally impossible for them, which they
   acknowledge and decline on throughput grounds.
4. **No retry policy.** A failing handler dead-letters immediately. PC has retry delays, custom
   backoff, and max-retry skipping. For a workload with transient downstream failures this is a
   category difference, not a feature gap.
5. **One topic per consumer.** "For multiple topics, create multiple consumers."
6. **llingr's DLQ is mandatory because llingr's architecture requires it** - see the DLQ section above.

### Their dead letter queue, in detail - a starting shape for astubbs#149

Recorded at the owner's request: when the DLQ brainstorm resumes
(`docs/plans/2026-08-18-001-investigate-dlq-prior-art-report.md` on `docs/310-dlq-brainstorm`, draft
PR astubbs#313, tracking astubbs#149), this market research should be on master and llingr's design is a
reasonable place to start from.

**The shape, from llingr's docs:**

- **A second required callback, not configuration.** `newBuilder(topic, processMessage,
  writeDeadLetter)`. Rust states it plainly: three traits, two required - *"Required so a failed
  message always has somewhere to go."*
- **The library never owns the destination.** The host writes the dead letter wherever it likes - a
  database, another topic, object storage. The engine does not produce.
- **Trigger and reason:** `process` returning an error, or throwing/panicking, routes the message to
  the dead-letter handler **with the error text as the reason**. The handler receives the message and
  the `Throwable`/error.
- **Failures are contained**, explicitly: *"a panicking process dead-letters its message with the
  reason `panic in process callback`, and the consumer keeps running."*
- **The callback must be blocking**, on the worker that owns the message's key - *"must be
  synchronous/blocking - llingr-demux provides the concurrency"*, and do not spawn work outliving the
  call. Same rule as the process handler, so ordering and accounting hold.
- **Dead-lettering makes the message eligible to commit.**
- **A circuit breaker guards the DLQ itself:** *"If dead-letter writes fail - indicating an
  infrastructure problem - the circuit breaker stops polling and signals the application for
  coordinated shutdown."* The DLQ is not allowed to fail silently.
- **First-class in observability**: a `llingr_engine_dead_letter_duration_seconds` histogram, a
  `deadLetters` count since assignment, `deadLetterCount` per sliding window and `totalDeadLettered`
  in the snapshot.
- **First-class in testing**: chaos runs against *dual* persistence stores (primary and dead letter),
  including dead-letter outages and flapping; a validator asserts **every produced message reached
  the primary store or the dead letter store, with no gaps**; and dead-letter routing under error
  injection is one of four core verified scenarios.

**The part to think about rather than copy - and it is the interesting one.** llingr's DLQ is *required*
because llingr's commit design needs it. With a contiguous-commit design, a permanently failing message halts
commits for that partition **forever** - so there must be a way to give up on a record and move the
pointer. The DLQ is that escape hatch, which is why it is load-bearing rather than a convenience.

**PC has no such forcing function.** Committing past gaps means a poison record does not stop the
commit point; it stays in the incomplete set while everything else commits. So PC can afford an
*optional* DLQ, and should not import "required" without deciding it independently. Worth stating in
the requirements, because it is the kind of design constant that gets copied by default.

**Worth copying regardless:** the reason-carrying signature, blocking-on-the-owning-worker,
exception containment, the circuit breaker on DLQ write failure, the metrics, and the
reaches-one-store-or-the-other test invariant.

### Counterfactual: how PC would be positioned with the roadmap fully delivered

Owner's question. Assume every `roadmap.yaml` entry is shipped - DLQ, virtual threads, micro-batching,
health checks, bounded buffers, producer-fencing survival, current Java and Kafka, running-instance
visibility, docs site, delivered-value metrics, Streams and Connect previews matured - **plus**
adaptive concurrency, which is in the backlog rather than the roadmap and is the differentiator.

**What the gap list collapses to.** Of the seven things they currently have and we do not, the
roadmap closes five: DLQ, runtime introspection (running-instance-visibility), JDK baseline
(current-java-and-kafka), health surface, and documentation. **Two survive: formal verification and a
published per-message overhead figure** - and neither is a feature, both are evidence practices. That
is a comfortable place to be, because both are things we can adopt without asking permission
([`next-formal-verification-and-correctness-methods.md`](next-formal-verification-and-correctness-methods.md),
[`next-performance-regression-testing.md`](next-performance-regression-testing.md)).

**What our advantage list looks like in that world.** The six verified advantages above all persist,
because none of them is a roadmap item - llingr is architectural or licensing facts. Added to them:

- **Adaptive concurrency**, which llingr explicitly disclaims, against a product whose primary tuning
  parameter is a number the user must guess against llingr's own database connection pool.
- **Kafka Streams and Connect integrations**, in an ecosystem llingr does not operate in. llingr's engine is
  Go-first; a Streams integration means entering the JVM ecosystem properly, not adding a binding.
- **Micro-batching**, against no batching at all.

**The honest read of that position: strong on capability, weaker on proof and presentation.** The
roadmap is well aimed at the capability gaps and aimed at nothing else. Two risks follow, and both
are worth naming now rather than discovering later:

1. **The roadmap does not fund the differentiator.** Adaptive concurrency is the single biggest
   feature-level separation available and it sits in `backlog` marked architectural, with two 2020
   prototypes that never worked. Everything else on the roadmap narrows a gap; this one opens one.
2. **Delivering the roadmap does not make anyone believe it.** With every entry shipped, our claims
   would still rest on prose while theirs rest on a TLA+ state count, a chaos matrix and published
   benchmarks. **Capability and credibility are different axes, and we are only working one of them.**
   That is what makes the correctness-methods note strategic rather than housekeeping.

**One thing that would not change: llingr would still be faster per message**, and that would still
matter less than either of us can currently prove - which is what the harness experiment below is
for.

### Where llingr is ahead, stated plainly

Not recording this would make the rest untrustworthy. Formal verification with TLA+ and a chaos suite
far broader than ours ([`next-formal-verification-and-correctness-methods.md`](next-formal-verification-and-correctness-methods.md));
per-core efficiency and a published micro-optimisation account; telemetry depth; a polyglot story
already shipping while astubbs#242 is unreleased; and a Go engine that will be faster than the JVM
for the same model.

### The one-line positioning this suggests

Not "faster". And **not the first draft of this line either**, which read *"PC commits past the gaps,
so one slow key never holds up a partition"* - the second half of that is **false**, and the
divergence measurement above is what falsified it. PC commits the *lowest incomplete offset*, so a
stuck record freezes the committed offset on both engines identically. The claim survived several
revisions of this note before anyone measured it.

**The version that survives measurement is about restart cost:**

> **One stuck record, one crash: PC reprocessed 6% of the work it had already done; a
> contiguous-commit design reprocessed 100% of it. Nine bytes of commit metadata is the
> difference. And it is Apache 2.0.**

**The nuance that remains true, and belongs with it.** A *graceful* rebalance costs a
contiguous-commit design nothing - llingr's drain coordinator commits before releasing partitions,
and that path is well built. The exposure is the **ungraceful** one: a hard kill, an OOM, a timeout.
So state it accurately - **not "llingr loses messages", but "llingr redoes more work than PC does when
a shutdown is not graceful"** - and note that it compounds with how often partitions move, which for
a Kubernetes audience running rolling deploys and HPA scaling is constantly.

**And carry the bound with the claim, because it is a ratio and not a constant.** PC's redelivery
window is `commitInterval x throughput`; a contiguous-commit design's is `timeSinceTheStall x throughput`. Measured at
15.6x, 6.4x - and **1x**, in a run kept deliberately as a negative control, where the crash fell
within one commit interval of the stall and both designs lost the same. A claim published without
that bound would be the same kind of statement as the 200x one.

## What this is not

Not a threat assessment, and not a reason to hurry. Two independent projects reaching the same
architecture - key-ordered concurrency past partition count, FFI for the systems languages, a gRPC
sidecar for everything else - is the strongest available evidence that the architecture is right.
The useful posture is to learn from a peer who has already shipped parts of it and has offered to
talk, not to race him.
