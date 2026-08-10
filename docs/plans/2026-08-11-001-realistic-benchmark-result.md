# Result: what the seam does on a workload nobody chose to flatter it

For `parallel-consumer-streams` (astubbs#255). The plan is
[2026-08-11-001-test-ks-streams-realistic-benchmark-plan.md](2026-08-11-001-test-ks-streams-realistic-benchmark-plan.md).

This is the **second** benchmark. The first,
[`HeadOfLineBlockingBenchmarkTest`](../../parallel-consumer-streams/src/test/java/io/confluent/parallelconsumer/streams/integrationTests/HeadOfLineBlockingBenchmarkTest.java),
is unchanged and stays: it isolates the property under laboratory conditions - one 1500ms blocker, twentyfour
25ms records behind it - and its 57x is real. Isolating a property is what makes good science, and it is also
what makes poor persuasion: a reader who wants to dismiss the number can attack the design and never engage
with it.

**This benchmark exists so that attack has nowhere to land.** Its job was never to beat 57x.

---

## 1. The headline, in the units an operator thinks in

A queue of **1200 authorisation-shaped records** had built up on a topic. The processor started cold and
worked through it.

| | Time to clear the queue | Records per second |
|---|---|---|
| Stock Kafka Streams | **47 seconds** | 25.8/s |
| With the seam on | **15 seconds** | 96.0/s |

**31 seconds saved on a 47-second backlog, a 3.72x catch-up rate.**

That is the whole claim, and it needs no explanation of key ordering or head-of-line blocking to act on. The
scenario is the one every operator has met at an uncomfortable hour: the consumer was down, or a replay was
started, or a rebalance handed a partition to an instance that was behind, and the only question is how long
until we are caught up.

The workload was not chosen to flatter it:

- **Keys are Zipf-distributed** (s=1.0, 200 cards), so a few keys are hot and their records still run one at a
  time. Skew is the single largest tax on this mechanism and it is on by default. The exponent sits inside the
  interquartile range Twitter published for 54 production cache clusters.
- **Per-record cost is a distribution, not a constant** - 20ms median, 200ms p99, lognormal. A constant cost
  quietly flatters a worker pool; a tail leaves some workers stuck while others turn over, which is what real
  pools do.
- **The work is a realistic mix**: a real Jackson parse of a real 512-byte JSON payload, the service call, a
  decision, a serialise. Not a bare sleep.
- **One partition and one StreamThread**, which is the configuration a reviewer is right to attack. It is
  attacked directly in §5.

**Independent corroboration that the magnitude is not an artefact:** other implementations of key-level
concurrency for Kafka Streams, built by unrelated parties, report three- to six-fold gains on the same
mechanism. 3.72x sits inside that range rather than above it.

---

## 2. Refuted predictions, first

Four of the nine predictions written before the run were refuted. Two of them changed what the benchmark
measures, and one of them found a defect in the benchmark itself.

### 2.1 REFUTED - "a saturated backlog barely uses wake-on-work"

The plan predicted that a cold-start backlog would leave this module's own most recent optimisation idle,
because with work always available the poll would never have to wait. That mattered: it would have meant the
headline owed nothing to a fix landed days earlier, which is stronger evidence than a result that depends on
it.

**Measured: the split-wait branch fired on 1122 of 1200 records - roughly nine in ten.**

The mechanism is obvious in hindsight and was not predicted. A backlog keeps the *broker* supplied, but
Parallel Consumer's max concurrency still bounds what the StreamThread may take in one pass, so the thread
returns to the poll while its workers are still running and finds itself waiting on *them* rather than on the
broker. **Saturating the topic does not saturate the thread.**

The counter is therefore not evidence of anything, and the test that counted it has been replaced by a control
arm that varies wake-on-work as its single term and measures how much of the advantage survives without it -
which is the honest form of the question.

### 2.2 REFUTED - "CPU-bound work shows little or no gain"

This is the framing the work was commissioned with, and it is too strong.

Stock Kafka Streams processes one record at a time per StreamThread **whether that thread is blocked or
computing**. A worker pool spreads computation across spare cores exactly as it spreads waits across time. On
an idle twelve-core machine with one StreamThread, the seam should therefore gain on CPU-bound work too - and
it does.

The accurate statement is narrower: **what the seam parallelises is work the StreamThread cannot proceed past,
and a busy core is one of those.** The genuine negative control is CPU-bound work on a machine with no spare
cores, which is measured separately in §4.

### 2.3 REFUTED - "a larger payload dilutes the advantage"

Predicted on the reasoning that parse and serialise are CPU work on the same thread, so a bigger payload raises
the non-blocking share of a record.

Measured: **512B gave 3.81x and 8192B gave 4.06x** - no material difference, and if anything the wrong way. The
prediction assumed serialisation was a meaningful share of a record's cost. Against a 20ms median service call,
parsing even eight kilobytes of JSON is a rounding error. Payload size would only matter at a far smaller
per-record cost, which is a different workload rather than a different data shape.

### 2.4 REFUTED, and it indicted the benchmark rather than the seam - the CPU fixture was a sleep in disguise

The first CPU-bound fixture spun `while (System.nanoTime() < deadline)`. That is not CPU-bound work: it is a
busy-wait for a fixed **duration**, and a duration is what a sleep also is. Four workers each waiting out their
own deadline finish in the same wall-clock time whether the machine has twelve spare cores or none, so
contention could not possibly show up.

**The bug was caught by the negative control failing.** CPU-bound work with eleven of twelve cores deliberately
burned still measured 3.42x, which is impossible for genuinely CPU-bound work - so the fixture was at fault,
not the seam. A control arm that goes the wrong way is doing exactly its job, and this is the second time in
this module's history that a control has caught a fixture defect rather than a code defect.

The fixture now fixes the **instruction count** rather than the clock: a calibration pass measures this
machine's hashing throughput once, and the work unit is sized from that. Under contention the same work now
genuinely takes longer.

### 2.5 REFUTED - the steady-state cell showed nothing, and publishing that is the point

Offered a Poisson stream of 9 authorisations a second - roughly 84% of what one stock instance can carry -
the seam measured **0.99x on end-to-end p99**. No advantage at all.

That is a real result and it belongs next to the headline rather than in a footnote. **Below saturation this
buys you nothing.** Stock keeps up, there is no queue, and a mechanism whose only job is to work several
records at once has nothing to work on. The advantage appears when the offered load approaches what a single
instance can carry - and most visibly when it has already fallen behind and has a backlog to clear, which is
why §1 is the headline and this is not.

### 2.6 The metric that could not see the thing it was measuring

The steady-state cell was first measured with **in-chain latency** - from the record entering the processor to
it completing - which is what the existing benchmark uses and which is correct there. It read 0.99x at p50, p99
and max, a flat null across the board.

**That flatness was suspicious rather than informative, and chasing it found a hole in the measurement.**
In-chain latency starts when a record enters the chain. A record waiting in the consumer's buffer for a free
StreamThread has not entered the chain yet, so the wait is invisible - and that wait is precisely what
head-of-line blocking is. Once a record is actually running it costs the same in either arm, so in-chain
latency is guaranteed to look flat whether or not the seam is doing anything.

Every arm now also records **end-to-end latency from the moment the producer was handed the record**, and the
steady-state cell is measured on that. The 0.99x above survived the fix, so the null result stands - but it
now stands on a statistic that could have shown an effect.

---

## 2b. The no-penalty claim, checked on the statistic a sceptic computes first - it holds

The module promises "no penalty when you fall back to traditional Kafka Streams usage", and the evidence
behind it was a **per-record median** that wake-on-work moved from about 0.70x to about 0.99x. **Whole-batch
drain time on a single key had never been re-measured after that fix**, and on the pre-wake-on-work branch it
stood at **0.57x** - far worse than the median implied. A claim that holds for one statistic but not for total
wall clock is the easiest possible way to be caught out, because total wall clock is the first thing anyone
measures for themselves.

Measured on this branch, one key, 300-record backlog:

| Statistic | Stock | Seam on | Ratio |
|---|---|---|---|
| **Whole-batch drain** | 13266ms | 13198ms | **1.01x** |
| Sustained rate | 22.0/s | 22.2/s | 1.01x |
| Per-record median | 28ms | 26ms | 1.08x |

**Parity, on all three.** The claim survives its most natural test, and it does not depend on which statistic
is quoted - which is what the 0.57x figure on the older branch could not have said. Whole-batch drain is now
reported in **every** cell of the matrix alongside the rate, so this cannot silently regress again.

Where the two statistics *do* diverge, they diverge in the unsurprising direction and both are printed: in the
blocking cells the drain ratio runs about half a turn below the rate ratio (for example 3.12x drain against
3.77x rate), because a drain is total wall clock including startup while the rate is trimmed. Neither is wrong;
they answer different questions, and §6 says which one carries the claim and why.

---

## 3. Confirmed predictions

- **The single-key floor held, and it is the falsifier.** With every record on one key, KEY ordering permits at
  most one in flight, so the seam must not win. Measured **0.99x** - very slightly slower, which is what a
  handoff through a pool costs when there is nothing to parallelise. A control that goes marginally the wrong
  way for a principled reason is stronger evidence than one that ties. Had this cell won, every other number
  here would have been measuring a faster harness and would have had to be withdrawn.
- **Skew degrades the advantage monotonically**, as predicted, and substantially.
- **The advantage is a rate, not an artefact of backlog depth** - see §6.
- **The mixed profile falls between the two pure profiles.**

---

## 4. Every cell, including the ones where the seam does nothing

Backlog drain, one partition, one StreamThread, worker pool of 4, sustained catch-up rate. Held constant except
where the row says otherwise: Zipf s=1.0 keys, blocking work, 512B payload, 300-record backlog.

### Key distribution

| Key distribution | Stock rate | Seam rate | Rate ratio | Stock drain | Seam drain | Drain ratio |
|---|---|---|---|---|---|---|
| **Single key** | 22.0/s | 21.9/s | **0.99x** | 13337ms | 13376ms | **1.00x** |
| Zipf s=1.5, 100 keys | 22.1/s | 44.2/s | 2.00x | 13292ms | 6933ms | 1.92x |
| Zipf s=1.0, 100 keys | 21.9/s | 88.2/s | 4.03x | 13371ms | 4141ms | 3.23x |
| Uniform, 100 keys | 22.1/s | 89.2/s | 4.05x | 13269ms | 3493ms | 3.80x |
| One key per record | 22.0/s | 90.1/s | 4.09x | 13259ms | 3421ms | 3.88x |

**Read the skew rows before quoting the others.** Raising the Zipf exponent from 1.0 to 1.5 - both inside the
range Twitter measured across production clusters, and 1.5 below their median of 1.21 plus one quartile -
**halves the advantage, from 4.03x to 2.00x**. At s=1.5 a single key carries roughly a third of the entire
stream, and that third is a serial queue no worker pool can open up. Anyone whose keyspace is hotter than
s=1.0 should expect materially less than the headline.

The single-key row is the falsifier, and it lands at parity on both statistics. See §2b for why the drain
column specifically matters there.

The uniform and one-key-per-record rows are the ceiling, and a sceptic is right that real traffic does not look
like that.

### Data shape

| Payload | Stock rate | Seam rate | Rate ratio | Stock drain | Seam drain | Drain ratio |
|---|---|---|---|---|---|---|
| 512B JSON | 22.0/s | 82.9/s | 3.77x | 13302ms | 4262ms | 3.12x |
| 8192B JSON | 22.1/s | 82.4/s | 3.73x | 13505ms | 4162ms | 3.24x |

No effect from payload size. See §2.3.

This is also the clearest illustration of why both statistics are printed everywhere: the drain ratio sits
about half a turn below the rate ratio in every blocking cell, because a drain is total wall clock including
startup while the rate is trimmed. They are not in conflict - they answer different questions - but a reader
who computes only one of them should be able to see the other next to it.

### Processing profile

| Profile | Stock rate | Seam rate | Rate ratio | Drain ratio |
|---|---|---|---|---|
| Fully blocking (b=1.0) | 22.1/s | 82.6/s | 3.74x | 3.24x |
| Mixed (b=0.5) | 23.0/s | 89.0/s | 3.87x | 3.33x |
| CPU-bound (b=0.0), idle machine, 1 StreamThread | 27.4/s | 105.6/s | 3.85x | 3.16x |
| **CPU-bound (b=0.0), at equal thread count** | 88.0/s | 104.8/s | **1.19x** | - |

**The last row is the boundary of the claim, and it is the one to read.** Stock there gets 4 partitions and 4
StreamThreads; the seam gets one of each plus a pool of 4. Same number of threads computing, and the advantage
essentially disappears - 1.19x, inside the band this suite calls "no material advantage".

The third row is why the received wisdom in §2.2 is too strong: on an idle machine with one StreamThread, the
seam parallelises CPU work just as readily as it parallelises waits, because stock runs one record at a time
per thread either way. **What the seam buys is threads without partitions, not more productive threads.** For
CPU-bound work that distinction is the whole story; for blocking work it is not, because a blocked thread is
not occupying a core and stock's extra threads spend their time waiting rather than computing.

Two earlier attempts at this control failed, and both are recorded in §2.4 rather than discarded.

---

## 5. The objection that matters most: "just add partitions"

Every experiment here runs **one partition**, which is the configuration that makes the seam look best.
Published guidance is six to twelve partitions for a topic of consequence, and large fleets average far more.
A reviewer who stops reading at "one partition" is being reasonable.

So the counter-proposal is run rather than argued with, in
[`PartitionScalingBenchmarkTest`](../../parallel-consumer-streams/src/test/java/io/confluent/parallelconsumer/streams/integrationTests/PartitionScalingBenchmarkTest.java),
with the keyspace deliberately set to one key per record so that partitions spread evenly - the objection's
best case, which is the form worth answering. 600-record backlog, 20ms p50 / 200ms p99 blocking work.

| Configuration | Rate | vs baseline |
|---|---|---|
| Stock, 1 partition, 1 StreamThread | 25.8/s | 1.00x (baseline) |
| **Stock, 4 partitions, 4 StreamThreads** | **100.5/s** | **3.90x** |
| **Seam, 1 partition, 1 StreamThread, pool of 4** | **97.5/s** | **3.78x** |
| Seam + 4 partitions and 4 StreamThreads | 403.4/s | 15.65x |

**The objection is right, and answering it does not cost the claim.** Given four partitions and four
StreamThreads, stock reaches 3.90x - essentially what the seam reaches from a single partition (3.78x). Those
two rows landing together is the honest result, and it is not a defeat: the claim was never "faster than stock
at equal concurrency", it is that the concurrency arrives **without** the partition count. The two routes buy
comparable throughput at different prices. Partitions cost a partition and a consumer per unit of concurrency,
and are only available if the topic was created that way and the keys spread evenly. A worker pool costs
threads.

It also means the single-partition baseline used throughout this suite is **not** what produces the advantage:
stock given four times the resources lands in the same region, so the other cells are measuring the mechanism
rather than an artificially hobbled control.

**An unplanned finding, from the fourth row: multi-partition dispatch works, and the two compose.** Four tasks
each with a pool of four reached 15.65x - close to the sixteenfold the arithmetic allows. This module's
caveats list multi-task dispatch as untested, and it is still untested in the senses that matter (rebalancing,
state, failure), so treat the number as a probe rather than a supported configuration. But "it does not work"
is not the reason to avoid it.

What this benchmark does **not** settle, and should not pretend to: partitions are a global, up-front,
hard-to-change resource with per-broker and per-cluster ceilings, while a pool size is a local setting. That is
an argument about operational cost, not a number.

---

## 6. Why the claim rests on the catch-up rate and not on the time to drain

Three figures are reported and they do not agree, which is the point.

- **Absolute time saved** compounds with backlog depth without limit.
- **Time-to-drain ratio** rises with depth, because a fixed startup cost - assignment, first poll, and Parallel
  Consumer's own load factor which deliberately does not scale for the first two seconds of a run - is
  amortised over more work.
- **Sustained catch-up rate is flat in depth**, because both arms are throughput-limited from the first second:
  stock at roughly one record per mean cost, the seam at roughly pool-size records per mean cost. There is
  nothing left in that ratio to compound.

Only the third is a property of the dispatch mechanism, so only the third carries an assertion. This follows
[`choose-the-statistic-that-states-the-claim.md`](../solutions/best-practices/choose-the-statistic-that-states-the-claim.md),
which was written after the first benchmark asserted on a p99 that at n=24 was simply the maximum.

**A second piece of evidence for the same choice, and it is the one that surprised:** in-chain latency
percentiles are nearly identical in the two arms (p50 26ms against 25ms, p99 204ms against 207ms). That is not
a null result - it is the measurement showing where the queueing lives. Once a record has entered the chain it
costs what it costs in either arm; the waiting a backlog creates happens *before* entry. A latency percentile
cannot see it. The rate can.

---

## 7. How to reproduce, and what it costs

```bash
bin/streams-benchmark.sh --scenario backlog     # the headline experiment      ~12 min
bin/streams-benchmark.sh --scenario matrix      # every cell in section 4       ~5 min
bin/streams-benchmark.sh --scenario payments    # the domain demonstration      ~6 min
bin/streams-benchmark.sh --scenario partitions  # section 5's counter-proposal  ~1 min
bin/streams-benchmark.sh --help                 # every parameter, with its default
```

Measured: the matrix's five experiments took 265.7s, the two payment experiments 315.1s, and the partition
experiment 44.7s, on the machine below. Results print as framed blocks; everything else in the output is
Kafka's own logging.

Every workload parameter is overridable, so a result can be re-derived under a different configuration rather
than taken on trust - `--skew`, `--keys`, `--blocking-fraction`, `--cost-p50`, `--cost-p99`, `--payload-bytes`,
`--records`, `--rate`, `--seed`, `--pool`. `--repeat N` runs the whole scenario N times.

The benchmarks are `@Tag("performance")` and excluded from the default build, because a benchmark that adds
minutes to every pull request is a benchmark that gets deleted.

**Machine:** Apple Silicon, 12 cores, 32 GB, Java 17, Kafka 3.9.2 in Testcontainers, everything else idle.
**Reproduction:** every figure here is from a single run of each arm unless stated. That is an anecdote per
cell, not a distribution - `--repeat` exists precisely because this document cannot claim otherwise.

---

## 8. What could not be made realistic, and other honest limits

- **Record size is chosen, not cited.** No named operator publishes a percentile distribution of Kafka record
  sizes. The nearest published figure puts payment events at 512 bytes to 2 kilobytes, and that document is an
  author's construction rather than a measured trace. The matrix sweeps the parameter instead of defending one
  value.
- **The service call is simulated.** The latency is a sleep drawn from a distribution; the concurrency around
  it is real. No benchmark in this class does otherwise, but it is worth saying rather than implying.
- **Kafka Streams orthodoxy says not to make this call at all.** The recommended design is to materialise the
  lookup into Kafka and do a table join, and where that is possible it is the better answer - the seam is not
  needed. This workload represents the residue where materialisation is unavailable: a third-party API you do
  not own, a PII vault that is the security boundary by design, a versioned model endpoint, data too large or
  too volatile to replicate. That residue is real, and it is exactly where a Kafka Streams user is stuck with
  one record at a time per partition today.
- **One StreamThread, one partition, one task** remains this module's standing limit, and multi-task dispatch
  is untested. The partition experiment probes it and reports whatever happens.
- **A topology this module supports.** Stateless, non-windowed, no joins. A reviewer might reasonably pick a
  windowed velocity rule instead, which would be a fairer test of Kafka Streams and which this module refuses
  outright. Saying so is more honest than quietly choosing a supported topology and not mentioning why.

---

## 9. A defect found in passing, not fixed here

[`HeadOfLineBlockingBenchmarkTest`](../../parallel-consumer-streams/src/test/java/io/confluent/parallelconsumer/streams/integrationTests/HeadOfLineBlockingBenchmarkTest.java)
carries `@see KeyCardinalityScalingBenchmarkTest` - a javadoc reference to a class that was specified as
Experiment B in the spike plan and never written. The reference points at nothing on any branch.

Left alone deliberately: that file is the property-isolating benchmark and is not to be edited. The experiment
it names - a cardinality sweep - is now subsumed by the key-distribution axis in §4, which adds the dimension
Experiment B lacked: skew.

The same decision applies to the percentile helper. `LatencyDistribution` in the new `benchmark` package is a
copy of that file's private `Latencies`, not an extraction, and the duplication is deliberate rather than
overlooked - collapsing them would mean editing the one file this work was told to leave untouched.
