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

Run twice, hours apart, on a machine that had been doing other work in between: 25.8/s against 96.0/s the
first time and 26.0/s against 96.6/s the second, both 3.72x. That is a reproduction, not a spread - see §7 for
which figures in this document have one and which do not.

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

**One caveat travels with this figure and is not optional.** It is the product of concurrent dispatch *and*
wake-on-work together. With the poll optimisation disabled the same experiment measures 1.31x, not 3.76x -
see §2.1. Both ship, both default on, so 3.72x is what a user gets; but the seam alone does not produce it.

---

## 2. Refuted predictions, first

Six of the predictions written before the run were refuted, and a seventh problem was found in the measurement
rather than in the thing measured. Two of them changed what this benchmark measures at all, two were caught by
a control arm failing rather than by review, and two contradict expectations this work was commissioned with.

### 2.1 REFUTED, twice, and the second one is the most important number in this document

The plan predicted that a cold-start backlog would leave this module's own most recent optimisation -
wake-on-work - idle, because with work always available the poll would never have to wait. That mattered a
great deal: it would have meant the headline owed nothing to a fix landed days earlier, and a result that
survives the removal of your own optimisation is far stronger than one that depends on it.

**First refutation: the split-wait branch fired on 1132 of 1200 records - 94%.** A backlog keeps the *broker*
supplied, but Parallel Consumer's max concurrency still bounds what the StreamThread may take in one pass, so
the thread returns to the poll while its workers are mid-flight and finds itself waiting on *them* rather than
on the broker. **Saturating the topic does not saturate the thread.** Obvious in hindsight, and not predicted.

That made the counter worthless as evidence, so it was replaced by a control arm that varies wake-on-work as
its single term. **Second refutation, and it is the one that matters:**

| Arm (1200-record backlog) | Rate | vs stock |
|---|---|---|
| Stock, seam off | 25.9/s | - |
| Seam on, **wake-on-work OFF** | 33.8/s | **1.31x** |
| Seam on, wake-on-work ON | 97.2/s | 3.76x |

**The backlog headline does not survive without wake-on-work. It depends on it overwhelmingly** - concurrent
dispatch alone accounts for 1.31x of the 3.76x, and the poll fix accounts for the remaining 2.45x. The claim
"a backlog result cannot be attributed to that optimisation" was not merely unproven, it was backwards.

The mechanism is the same one the first refutation exposed. Without wake-on-work the StreamThread, having
handed out all the work its concurrency limit allows, blocks in `poll()` for the whole budget - so it is not
there to refill the pool the moment a worker finishes, and the pool starves. Concurrent dispatch can only pay
off if something keeps it fed.

**What this changes about how the result should be quoted.** The seam and wake-on-work are one mechanism for
this purpose, not two, and they ship together and default on - so the 3.76x is what a user actually gets, and
the headline stands. But nobody may write "this result does not depend on our recent poll optimisation", and
anyone tempted to disable wake-on-work should know it costs roughly two thirds of the benefit.

### 2.1b REFUTED - the advantage does not grow with backlog depth. It shrinks.

The expectation this work was commissioned with was that the advantage would compound with depth, since stock
drains a partition one record at a time however deep the queue is. The plan predicted something narrower: that
the *absolute* saving would compound, the *time-to-drain ratio* would rise as startup was amortised, and the
*rate ratio* would be flat. Measured across three depths:

| Backlog depth | Stock | Seam on | Rate ratio | Drain ratio | Seconds saved |
|---|---|---|---|---|---|
| 200 | 21.9/s | 90.0/s | **4.11x** | 3.07x | 6s |
| 1200 | 25.8/s | 97.5/s | **3.78x** | 3.12x | 32s |
| 3000 | 26.2/s | 90.3/s | **3.45x** | 2.88x | 75s |

**Only the absolute saving behaved as expected.** The rate ratio *declines* monotonically with depth - 4.11x
to 3.45x - and the drain ratio is roughly flat rather than rising. Both halves of the plan's prediction were
wrong, and the commissioning expectation was wrong in the other direction: nothing here compounds except the
seconds on the clock.

The decline is small but consistent, and this benchmark does not establish its cause. The candidate worth
testing first is that Parallel Consumer's own bookkeeping grows with the number of records registered but not
yet complete, so a deeper backlog costs slightly more per record on the seam's side than on stock's. That is a
hypothesis, not a finding, and it is recorded as one.

**Why this matters for anyone sizing this:** the mechanism is at its best on moderate backlogs, and a very deep
one erodes the ratio somewhat. It never erodes the absolute benefit, which is what an operator actually feels -
75 seconds saved at depth 3000 against 6 at depth 200.

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
- **Skew degrades the advantage monotonically**, as predicted, and substantially - 4.03x at s=1.0 down to
  2.00x at s=1.5.
- **The result is not JVM warm-up.** Running the arms stock-first gave 3.75x and PC-first gave 3.72x, a
  disagreement of 0.03x. This is the only one of the three warm-up defences that could have falsified the
  others, and it did not. A discarded warm-up pass runs before any measured arm, and the sustained-rate
  statistic trims the first and last decile of each drain.
- **The mixed profile falls between the two pure profiles**, as an Amdahl split predicts.
- **The absolute time saved compounds with backlog depth** - 6s, 32s, 75s at depths 200, 1200 and 3000. This
  was the only part of the depth prediction that held; see §2.1b for the two parts that did not.

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

## 4b. The narrow domain workload

Card-payment authorisation screening: an authorisation arrives, a fraud-scoring service is called, a decision
is emitted. Stateless, non-windowed, no joins - the surface this module supports. Keyed by card, because that
is what an issuer would key on and because velocity rules need one card's authorisations in order.

Chosen as though a hostile reviewer chose it: it is the canonical Kafka Streams enrichment shape, this
repository already models it independently of any benchmark (a screening example on another branch fixes its
scoring call at 200ms, the same order of magnitude), and it is the project's stated second persona. What is
*not* favourable about it: the keyspace is skewed, and real screening has genuine CPU work either side of the
call, both of which cost the seam.

**Catching up after an outage** - 900 queued authorisations, 123 distinct cards, scoring call 60ms p50 /
400ms p99:

| | Time to clear the queue | Authorisations per second |
|---|---|---|
| Stock | 84s | 10.8/s |
| Seam on | 28s | 40.6/s |
| | **3.00x** | **3.76x** |

**Steady state** - the same workload offered as a Poisson stream at 9 authorisations a second, roughly 84% of
what one stock instance can carry:

| Statistic | Stock | Seam on | Ratio |
|---|---|---|---|
| End-to-end p50 | 68ms | 67ms | 0.99x |
| End-to-end p99 | 411ms | 405ms | 0.99x |
| Achieved rate | 9.1/s | 9.0/s | 0.99x |

**Nothing. Below saturation this buys you nothing at all**, and that pair of tables side by side is the most
useful thing in this document for someone deciding whether to adopt it. The same workload, the same code, the
same keys: a large difference when there is a queue, and none when there is not.

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

Three figures are reported, they do not agree, and the disagreement is the point. The depth sweep in §2.1b is
what settled which of them to trust:

- **Absolute time saved** compounds with backlog depth - 6s, 32s, 75s across the sweep. It is the figure an
  operator feels, and it is the only one that behaved as predicted.
- **Time-to-drain ratio** was predicted to rise with depth as the fixed startup cost is amortised. It does not;
  it is roughly flat (3.07x, 3.12x, 2.88x). It still carries no assertion, because it mixes startup - assignment,
  first poll, and Parallel Consumer's own load factor which deliberately does not scale for the first two
  seconds - into a number about dispatch.
- **Sustained catch-up rate** was predicted to be flat in depth, on the reasoning that both arms are
  throughput-limited from the first second. It declines instead (4.11x, 3.78x, 3.45x).

So the statistic chosen for the claim turned out to be *less* depth-invariant than predicted, and saying so
matters more than defending the choice. It is still the right one to assert on - it is the only one of the
three that excludes startup and speaks about dispatch alone - but the honest form of the claim is "3.4x to 4.1x
depending on backlog depth" rather than a single figure that happens to have been measured at 1200.

This follows
[`choose-the-statistic-that-states-the-claim.md`](../solutions/best-practices/choose-the-statistic-that-states-the-claim.md),
which was written after the first benchmark asserted on a p99 that at n=24 was simply the maximum.

**A second piece of evidence for the same choice, and it is the one that surprised:** in-chain latency
percentiles are nearly identical in the two arms (p50 26ms against 25ms, p99 204ms against 207ms). That is not
a null result - it is the measurement showing where the queueing lives. Once a record has entered the chain it
costs what it costs in either arm; the waiting a backlog creates happens *before* entry, and in-chain latency
starts too late to see it.

Every arm now also records **end-to-end latency from the send** (§2.6), which does see that wait. It is the
right statistic for the paced experiments, and it is still the wrong one for a backlog: with everything
produced before the topology starts, a record's end-to-end latency is dominated by how deep in the queue it
happened to sit, which is a restatement of the drain rather than an independent measurement of it. Three
statistics, three different questions - and the discipline is choosing which one an assertion rests on, not
collecting more of them.

**Whole-batch drain is now printed in every cell** beside the rate, because it is the statistic a reader
computes first and the one on which a claim can silently regress while the others look fine. §2b is what that
change was for.

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
**Reproduction, stated per figure rather than as a blanket claim:**

- The **headline** was run twice, hours apart: 3.72x both times (§1). Reproduced.
- The **single-key floor** was measured four times across different runs - 0.99x, 0.99x, 1.00x, 1.01x. The
  falsifier is the most-repeated figure here, which is the right way round.
- **Every other cell is a single run of each arm.** That is an anecdote per cell, not a distribution.
  `--repeat N` exists precisely because this document cannot claim otherwise, and anyone quoting a matrix cell
  should run it themselves first.

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
