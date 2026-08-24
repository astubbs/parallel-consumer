# The tail experiment ran, and the workload it had never modelled is what separates the arms

<!-- inflight-type: next -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-decision -->

**Say the headline first.** On a skewed key distribution - the workload people adopt Parallel
Consumer *for* - `KEY` and `PARTITION` ordering sustain **one record in flight out of a configured
24**, and run at **a third of `UNORDERED`'s throughput on the identical records**. `peak_in_flight`
reads 24 for all three arms and says nothing about it.

That is not the tail's doing. It is the **key distribution's**, and it appears with a completely flat
handler.

## What this replaces

[`next-the-tail-experiment.md`](next-the-tail-experiment.md) was blocked, for a reason that was
arithmetic rather than a fact about ordering: **every run this harness did drained a backlog produced
before the window opened**, so residence was buffered depth over throughput, and two arms with the
same buffer and the same ceiling had equal residence *by construction*. Its measured null -
`PARTITION` and `KEY` at residence p99 **15,568ms against 15,565ms** - came from an experiment that
could not have shown a difference had one existed.

It named two things it needed. Both are now built:

| Needed | Built |
|---|---|
| Records fed **during** the measured window at a controlled rate | `BENCH_ARRIVAL_RATE`, swept as `ARRIVAL_RATES` |
| A skewed key distribution, so `KEY` ordering constrains anything at all | `BENCH_KEY_DISTRIBUTION=zipf` / `hot` |

And it needed three things it had not noticed were missing - see
[the instrument section](#the-instrument-was-broken-in-five-ways-and-four-of-them-were-silent).

## The saturated baseline, and the result that does not need the tail at all

Pre-produced path, 12,000 records, 24 partitions, `maxConcurrency` 24, `messageBufferSize` 20,000,
`max.poll.records` 500, Zipf over 200 keys (top key **16.6%** of traffic, top ten **49.4%**), timer
callee, LOCAL build `3335897970`, Kafka 4.3.1 broker, JDK 17, one repeat each.

| ordering | msg/s | peak in flight | **sustained in flight** | residence p50 | residence p99 | one-minute load |
|---|---:|---:|---:|---:|---:|---:|
| `UNORDERED` | **1,307.2** | 24 | **22** | 2,951 | 5,903 | 5.3 |
| `KEY` | **419.9** | 24 | **1** | 2,952 | 24,696 | 7.0 |
| `PARTITION` | **352.5** | 24 | **1** | 3,757 | 30,064 | 5.7 |

**`UNORDERED` sustains 22 of its 24 permitted records in flight. `KEY` and `PARTITION` sustain one.**
Same records, same bytes, same broker, same minute, a constant 10ms handler and no failures anywhere.

**`peak_in_flight` is 24 for every row**, which is why this was never seen: the harness only ever
recorded the maximum, every arm touches its configured concurrency briefly at the start, and a
maximum cannot distinguish an engine that holds full width from one that reaches it once. The
sustained figure is new, and it exists because it refuted a prediction written a few hours earlier -
see [Predictions](#predictions-and-what-happened-to-them).

### The control that makes it a measurement rather than an observation

The same arms, the same handler, the same everything - **one term changed, the key distribution**:

| ordering | keys | msg/s | sustained in flight | residence p99 |
|---|---|---:|---:|---:|
| `UNORDERED` | distinct | 1,310.2 | 22 | 5,903 |
| `KEY` | **distinct** | **1,311.9** | 22 | 5,903 |
| `PARTITION` | distinct | 1,172.2 | 19 | 6,442 |
| `UNORDERED` | zipf | 1,307.2 | 22 | 5,903 |
| `KEY` | **zipf** | **419.9** | **1** | **24,696** |
| `PARTITION` | zipf | 352.5 | 1 | 30,064 |

**With distinct keys, `KEY` ordering costs 0.13% against `UNORDERED` - 1,311.9 against 1,310.2, and
an identical residence p99 to the millisecond. With skewed keys it costs 3.1x.**

This is the project's own critique, measured:
[`next-benchmark-a-model-of-work-not-work.md`](next-benchmark-a-model-of-work-not-work.md) wrote
*"we have never actually tested key ordering, we have tested `UNORDERED` wearing its name"*. The
distinct-key row is not merely similar to `UNORDERED`, it is **indistinguishable from it**, which is
the strongest possible form of that claim. Every `KEY` figure this project has published was taken
under that condition.

### Failures cost the ordered arms far more than the tail does

Same arms, saturated, 12,000 records. `tail` is a 101x handler tail at an unchanged 10ms mean;
`tailf` adds a 1% failure rate, which PC retries after its default one-second delay.

| ordering | keys | flat | tail | **tailf** | tailf vs flat |
|---|---|---:|---:|---:|---:|
| `UNORDERED` | zipf | 1,307.2 | 1,224.6 | 1,076.1 | **-18%** |
| `KEY` | zipf | 419.9 | 404.4 | 251.6 | **-40%** |
| `PARTITION` | zipf | 352.5 | 417.5 | 198.9 | **-44%** |
| `share-explicit` | zipf | 959.7 | 981.5 | 945.8 | **-1%** |

**A 101x handler tail moves throughput by a few percent for every arm - prediction 1 of the tail note
holds.** A 1% failure rate moves it by 40-44% for the ordered arms and 18% for the unordered one. The
`PARTITION` `tail` row at 417.5 is *above* its own flat row at 352.5, which is a single-repeat spread
rather than a finding and is why the arrival matrix below runs two repeats.

**The failure axis, not the tail axis, is what interacts with ordering** - and it is the axis this
project has never once turned on. A failed record on a hot key holds that key's whole queue for the
retry delay, and the hot key is a sixth of the stream.

`share-explicit` is untouched by either, at 945-981 msg/s throughout. It has no ordering to hold up,
and a released record is immediately re-acquirable where PC waits out a second - so this row is not
like-for-like on retry cost and must not be quoted as one.

### Why - and it is arithmetic, not a mystery

A shard under `KEY` ordering is one key, and a shard runs **one record at a time**. Under Zipf the top
key carries 16.6% of the stream, so its records must run serially however wide the engine is: 16.6% of
the dataset at 10ms each is a floor on the run's duration that no amount of concurrency touches.
Under `PARTITION` the same argument runs on the busiest partition, which here holds **21.5%**.

The broker's own lag table, read mid-run on an earlier configuration, is the picture: **every one of
24 partitions at lag 0 except partition 1**, which held 4,291 of 20,000 records and was still
draining. The engine was not slow. It was idle, with 23 partitions finished and one key left.

## Controlled arrival: the same arms, unsaturated

Records fed **during** the measured window on an absolute schedule, into a topic created for that
run, at 50 / 70 / 90 percent of **each arm's own** saturated capacity. A common absolute rate would
have put one arm at 30% utilisation and another at 120%, and every queueing figure is a function of
utilisation before it is a function of anything else.

**The producer is not the bottleneck, and every row carries the proof.** Requested against achieved
arrival rate, feed-lag p99, and the fed-minus-completed backlog are columns; a run whose achieved
rate diverges from its request by more than 5%, or whose feed-lag p99 exceeds 100ms, is recorded as
`ARRIVAL_VOID` rather than reported. Across the matrix, achieved matched requested **exactly** at
every rate, with feed-lag p99 of 1-3ms.

### What controlled arrival did to the measurement, before any comparison

| `core` / `KEY`, zipf keys, flat handler | residence p50 | residence p99 |
|---|---:|---:|
| saturated, draining a pre-produced backlog | 2,952ms | 24,696ms |
| **arrival at 50% of capacity** | **12ms** | **26ms** |

**Three orders of magnitude.** The saturated figure was buffer depth over throughput and essentially
nothing else, exactly as [`next-the-tail-experiment.md`](next-the-tail-experiment.md) diagnosed. Any
comparison drawn from it was comparing buffers.

### The comparison the whole run exists for

**End-to-end p99, in milliseconds** - completion minus the record's *intended* send instant, median
of two repeats. The handler's own p99 is **505ms** in every tailed cell, so that is the floor an arm
adds nothing to. Full data in
[`bench/results/arrival-tail-skew-matrix.csv`](../../bench/results/arrival-tail-skew-matrix.csv);
84 rows, none voided, one-minute load 3.6-14.2 throughout.

| arm | keys | 50% | 70% | 90% | flat control, 90% |
|---|---|---:|---:|---:|---:|
| `core` `UNORDERED` | zipf | **508** | **507** | **507** | 17 |
| `core` `KEY` | **distinct** | **510** | **509** | **508** | - |
| `core` `PARTITION` | **distinct** | **512** | **513** | **512** | - |
| `core` `KEY` | zipf | 512 | **750** | **1,200** | 69 |
| `core` `PARTITION` | zipf | 516 | **900** | **1,454** | 166 |
| `share-explicit` | zipf | 661 | 746 | **5,291** | 34 |

**Read the two `distinct` rows first, because they are the reason every previous run of this
experiment produced nothing.** On all-distinct keys, `KEY` and `PARTITION` sit at 508-513ms *at every
utilisation* - within 8ms of the handler's own p99, and flat as the load rises. **On that workload
this experiment has no result to find.** Change one term - the key distribution - and the same arms
at the same rates over the same record counts reach 1,200ms and 1,454ms.

**`UNORDERED` is the control that shows it is not the tail's doing**: 508 / 507 / 507, unmoved by
utilisation, because nothing is ever waiting on anything else.

### The flat control, which is what makes the tailed numbers mean anything

| arm | keys | flat 50% | flat 70% | flat 90% | tail 90% | tail / flat |
|---|---|---:|---:|---:|---:|---:|
| `core` `UNORDERED` | zipf | 40 | 17 | 17 | 507 | 30x |
| `core` `KEY` | zipf | 37 | 44 | 69 | 1,200 | **17x** |
| `core` `PARTITION` | zipf | 34 | 53 | 166 | 1,454 | 9x |
| `share-explicit` | zipf | 34 | 35 | 34 | 5,291 | **156x** |

With a flat handler every arm sits between 17ms and 166ms and the spread is uninteresting. **The
difference between the two tables is the entire finding**, which is why the flat control was run.

### The measure that changes the answer: `share`'s residence is not its latency

| `share-explicit`, tail, 90% | value |
|---|---:|
| residence p99 - poll-return to completion | **510ms** |
| **end-to-end p99 - intended send to completion** | **5,291ms** |
| backlog p99 - fed minus completed | **1,982 records** |

**Residence says `share-explicit` is indistinguishable from `UNORDERED`. End-to-end says it is 3.6x
worse than `PARTITION`, the worst arm in the matrix.** Both are correctly measured; they answer
different questions, and only one of them is the latency a caller experiences.

The mechanism is the one [`next-the-tail-experiment.md`](next-the-tail-experiment.md) predicted for
this arm without being able to measure it: **a share consumer cannot poll while a batch is
outstanding**, so records it has not fetched wait in the broker - and residence, which starts at
poll-return, is not charged for that wait at all. It is textbook coordinated omission, it is
invisible to every measure this harness had before today, and **a results table carrying only
residence would have declared `share` the winner of this experiment.**

### Utilisation is honest for `flat` and `tail`, and not for `tailf`

Rates are 50/70/90% of each arm's **flat** capacity. The tailed workload holds the same mean service
time, so its capacity is within a few percent of flat and the labels are right. **The 1% failure rate
lowers capacity a lot**, so the `tailf` columns are at higher true utilisation than they say:

| arm | saturated `tailf` capacity | true utilisation at the 50/70/90 labels |
|---|---:|---|
| `core` `KEY` | 251.6/s | 83% / **117%** / **150%** |
| `core` `PARTITION` | 198.9/s | 88% / **124%** / **159%** |
| `core` `UNORDERED` | 1,076.1/s | 61% / 85% / **109%** |

**The bolded cells are overloaded**, and their e2e figures (KEY 10,168 and 12,863; PARTITION 7,640
and 18,641) measure an unbounded growing queue rather than an engine. Their backlog columns say so -
708 and 784 records. They are reported because overload is a real operating condition, but they must
not be quoted as tail latencies.

## Predictions, and what happened to them

**Two of the six were refuted, and the refutations are the more useful half.**

### From [`next-the-tail-experiment.md`](next-the-tail-experiment.md), written before any of this

**1. "Throughput barely moves for any arm." HELD.** Saturated, skewed keys: `KEY` 419.9 -> 404.4
(-3.7%), `UNORDERED` 1,307.2 -> 1,224.6 (-6.3%), `share-explicit` 959.7 -> 981.5 (+2.3%). A 101x
handler tail is 1% of records and a mean absorbs it, exactly as the note said. **Reporting msg/s here
would have produced the null result the note existed to prevent.**

**2. "`PARTITION` p99 degrades worst." HELD among the PC orderings, REFUTED overall.** On the tailed
skewed workload at 90%: `PARTITION` 1,454 > `KEY` 1,200 > `UNORDERED` 507, which is the blocking-unit
argument intact. But `share-explicit` reaches **5,291**, so the worst arm in the matrix is not
`PARTITION`.

**3. "`share` degrades second, and by roughly the batch size relative to `PARTITION`'s partition
depth." REFUTED.** It degrades **first** - 3.6x worse than `PARTITION` - and it does so as a cliff
rather than a slope: 661 at 50%, 746 at 70%, 5,291 at 90%. Its blocking unit is not merely larger
than one record, it is *the whole fetch pipeline*, because a batch cannot be polled while the previous
one is outstanding.

**4. "`KEY` and `UNORDERED` stay closest to the handler's own distribution." REFUTED for `KEY` on the
realistic workload - and the note said this would matter.**

The note wrote: *"If prediction 4 fails, the argument this project has been building all day is
wrong."* It fails, and the honest reading is narrower than that sentence:

- `UNORDERED`: **508 / 507 / 507** against a handler p99 of 505. It holds exactly.
- `KEY` **on distinct keys**: **510 / 509 / 508.** It holds exactly - and this is the condition every
  previously published `KEY` figure was taken under.
- `KEY` **on skewed keys**: **512 / 750 / 1,200.** It holds at 50% and fails from 70% up.

**So `KEY` ordering is not free, and the belief that it was is an artefact of the key distribution
nobody had a knob for.** What survives is the *ranking*: at 90% utilisation `KEY` is still 1.2x
better than `PARTITION` and 4.4x better than `share-explicit`. What does not survive is the claim
that `KEY` costs nothing - it costs 2.4x the handler's own p99 at high load.

**5. "The ordering between 2 and 3 could invert. That would be the interesting result." HELD, and it
is.** `share` and `PARTITION` swap: `share` is better at 50-70% (661/746 against 516/900) and far
worse at 90% (5,291 against 1,454). **Which of them you should fear depends entirely on how loaded
you run**, and no single-operating-point comparison could have said so.

### The prediction written for this run, in [`next-skewed-keys-should-starve-key-ordering.md`](next-skewed-keys-should-starve-key-ordering.md)

**(a) "`KEY` peak in flight falls below `maxConcurrency`." REFUTED AS WRITTEN, and the refutation
built a column.** `peak_in_flight` is **24 of 24** in every saturated row, skewed or not. The
prediction named the wrong statistic: every arm touches full width briefly at the start, and a
maximum cannot see what happens afterwards. The *mechanism* held completely - **sustained** in flight
is 22 for `UNORDERED` and **1** for `KEY` and `PARTITION` on the same records - which is why
`inflight_p50` now exists. Under controlled arrival the sustained figure tracks Little's law to the
record (`KEY` at 210/294/378 per second and a 10ms handler holds 2/3/4), which is the internal check
that the whole arrival apparatus is sound.

**(b) "`KEY`'s advantage over `PARTITION` shrinks; the rank holds." HELD.** `KEY` beats `PARTITION`
in every single cell of the matrix. The magnitude is the finding: on distinct keys `KEY` is
*indistinguishable from `UNORDERED`*, so the distinct-key data implied `KEY` ordering was free, and
it is not.

**(c) "`KEY`'s e2e p99 rises relative to `UNORDERED` under skew - and if they do not separate,
`BENCH_KEY_DISTRIBUTION` did nothing." HELD.** Distinct keys: 508 against `UNORDERED`'s 507. Skewed:
1,200 against 507. The knob did something.

**(d) "A tail and a skew compound rather than add." HELD.** At 90%: skew alone (flat, zipf) costs
69ms; tail alone (tailed, distinct) costs 508ms. Additively that predicts about 560ms. Measured:
**1,200ms.**

## 2026-08-23: re-taken across every engine and against the last public release

**The 3.1x reproduces, no engine escapes it, and PC's default buffer makes it 2.3x worse again.**
Same broker, same two topics, same operating point - 12,000 records, 24 partitions, `maxConcurrency`
24, 10ms flat handler, two repeats, `messageBufferSize` 20,000 - with the engine family and the
released version added as arms. Full data:
[`bench/results/realistic-ordering-matrix.csv`](../../bench/results/realistic-ordering-matrix.csv).

| arm | `UNORDERED`, zipf | `KEY`, zipf | cost of `KEY` | `KEY` sustained in flight |
|---|---:|---:|---:|---:|
| `core` | 1,232.3 | **370.9** | **3.32x** | **2** of 24 |
| `core` **@ 0.5.3.3** | 1,227.8 | **369.7** | **3.32x** | **2** of 24 |
| `core-vt` | 1,160.7 | 362.6 | 3.20x | 2 |
| `core-dpvt` | 1,155.3 | 361.6 | 3.19x | 2 |
| `vertx` | 1,267.8 | 407.9 | 3.11x | 2 |
| `reactor` | 1,272.7 | 409.0 | 3.11x | 2 |
| `mutiny` | 1,291.6 | 410.0 | 3.15x | 2 |
| **`proxy`** | 796.9 | **78.0** | **10.2x** | **0** |

**Every PC-engine arm pays between 3.1x and 3.3x, and every one sustains 2 records in flight of a
configured 24.** `proxy` is the exception and it is much worse - see
[`perf-engine-comparison-2026-08-22.md`](perf-engine-comparison-2026-08-22.md), where it matters
because that path is the ceiling for every non-JVM client. Virtual threads do not help, direct pull does not help, and neither does moving to an
`ExternalEngine`. That settles the question
[`bug-partition-ordering-starves-on-a-narrow-buffer.md`](bug-partition-ordering-starves-on-a-narrow-buffer.md)
left open - *"the direct-pull engine takes work from the shards itself and may not share this;
unmeasured"* - **it shares it exactly**, because the constraint is not how work is fetched or
selected. The busiest shard may run one record at a time, and nothing above it can change that.

**On distinct keys the same arms are indistinguishable from `UNORDERED`**, which is the control that
ties this table to every published figure: `core` 1,217.2 `KEY` against 1,224.3 `UNORDERED`, 0.5.3.3
1,210.8 against 1,223.4, and every engine within 1.5% of itself across the two modes.

### The buffer makes it materially worse, and the default is what users get

This note says above that the hot-key floor "is not the buffer", on the grounds that
`messageBufferSize` was already 20,000. That is true and it is only half the picture. **One term
changed, `core`, Zipf keys, flat handler:**

| `messageBufferSize` | failures | `KEY` msg/s | `UNORDERED` msg/s | cost of `KEY` |
|---|---|---:|---:|---:|
| 20,000 | none | 370.9 | 1,232.3 | 3.3x |
| **PC's default** | none | **161.8** | 1,218.9 | **7.5x** |
| **PC's default** | **1%** | **95.7** | 1,093.6 | **11.4x** |

`UNORDERED` does not move - 1% between the first two rows - which is what makes the ordered figure
attributable, and **0.5.3.3 behaves identically** (168.3 and 95.0), so this is not something the fork
introduced. **A user who configures nothing pays 7.5x for `KEY` ordering, and 11.4x once 1% of
records fail** - the hot-key floor, the narrow-buffer starvation and the retry delay all compound.

**Against the workload every published figure was taken on** - all-distinct keys, `UNORDERED`, no
failures, buffer 20,000, 1,224.3 msg/s - **the realistic default configuration runs at 95.7, which is
12.8x slower**, at a drain p99 of 123 seconds over 12,000 records. Data:
[`bench/results/realistic-default-buffer-control.csv`](../../bench/results/realistic-default-buffer-control.csv).

### Three instrument defects, all found by reading a column against its own configuration

Continuing the count this note started at eight. All three produced plausible numbers.

| Defect | What it looked like | Fixed |
|---|---|---|
| **`inflight_p50` sampled from JVM start** | four to six seconds of zeros prepended to every run, so the column read **0** at 2,978 msg/s on a four-second run and 22 on a nine-second one | sampler armed by the first record into the user function |
| **The work model's RNG was per-thread** | virtual threads are one thread per task, so every record got a fresh `Random` and one draw from it. At a configured 1%, `core-vt` took **1.85%** and platform `core` took 1.07% - the virtual-thread arms were handed 85% more failures than the arms they were compared against | SplitMix64 over one atomic counter |
| **The proxy arm had a sixth work-model call site** | `ProxyArm.Worker` has its own pool and timer, so it ran a flat never-failing handler however the model was configured: **zero** injected failures where every other arm reported 121-222 | `workDelayMs`/`workShouldFail` per record, failure reported as `Report.Failure` |

**The virtual-thread one was confirmed off-broker in forty lines**: `new Random(42 + i).nextDouble()
< 0.01` fires 222 times over 12,000 consecutive seeds and one shared `Random` fires 128 - the two
numbers the two arms had just reported, exactly.

## What this changes

- **`KEY` ordering has a price and it is now measured.** 3.1x throughput and 2.4x tail latency at
  high load, on a Zipf key distribution, against `UNORDERED` on identical records. Every figure this
  project has published for `KEY` was taken on all-distinct keys, where the price is **zero** - not
  small, zero, to within 0.13% on throughput and 1ms on residence p99.
- **The `share` comparison needs re-taking with an end-to-end measure.**
  [`perf-share-groups-versus-pc-2026-08-22.md`](perf-share-groups-versus-pc-2026-08-22.md)'s 2.5x is
  a saturated throughput figure. Under controlled arrival at 90% utilisation `share`'s end-to-end p99
  is 4.4x PC's `KEY` arm and 10x PC's `UNORDERED` arm, and its own residence meter does not show it.
- **The prefetch fix in
  [`bug-partition-ordering-starves-on-a-narrow-buffer.md`](bug-partition-ordering-starves-on-a-narrow-buffer.md)
  will not fix this.** That note prescribes expressing the prefetch target in shard coverage rather
  than records. The buffer here was already 20,000 - deeper than the whole dataset - and `KEY` still
  sustained one record in flight. **A hot key is a serial queue whatever the buffer holds.** What
  would help is a different thing entirely: something that stops a single shard bounding the run, and
  nothing in PC currently can.

## The instrument was broken in five ways, and four of them were silent

Every one of these was live before this work and would have corrupted the run it was found preparing.

| Defect | What it looked like | Why it was silent |
|---|---|---|
| **The work model reached three call sites out of five** | `share`, `share-explicit`, `reactor`, `mutiny` and `proxy` ran a FLAT handler however `BENCH_DELAY_P99` and `BENCH_FAILURE_RATE` were set | The row recorded the configured tail in its own column. A tail experiment naming `share` as an arm would have compared a tailed PC arm against a flat share arm and called the difference an engine property |
| **The share arm recorded no latency at all** | every latency column on a share row was `-` | A dash reads as "this arm cannot report that", the ordinary case for an old release or a non-PC arm |
| **`run_with_deadline` killed nothing** | the row said `RUN_TIMEOUT` and the JVM carried on running, holding its consumer group and its CPU, while the next arm was measured beside it | `kill -TERM` hit the backgrounded subshell first, so the JVM was reparented to init and the `pkill -P` written to fix exactly this looked for the children of a dead process |
| **The produce retried two billion times** | the produce never returned; the topic held 17,806 of the 20,000 records its name claimed | The producer is idempotent by default, so a recreated topic answers `OUT_OF_ORDER_SEQUENCE_NUMBER`, and the default retry count is `Integer.MAX_VALUE`. Two partitions of 24 sat in that loop while 22 finished |
| **A short topic hung every arm, forever, at no CPU** | "the benchmark hangs" | Every arm waits for a fixed count. `runCore`'s `while (responses.get() < expected)` never exits, and logs nothing |
| **`run_one` returned 0 however the JVM died** | the produce logged "produced 20000 records" for a run that wrote nothing at all | A shell function returns its LAST command's status, and two log checks sit below the java pipeline |
| **Every measure but residence was rendered after teardown** | drain p99 of 51,945ms on a 9.3-second run | `residence` was snapshotted at the window close from the day it landed, with a comment explaining why. The reasoning was correct and was applied to one measure out of five |
| **The pre-produce appended instead of topping up** | running a sweep twice doubled the dataset; the arm then stopped early with a full buffer it was never billed for | `msg_per_sec` is then measured over whichever records finished FIRST - which on a skewed distribution flatters the ordered arms specifically, because the records they are slowest at are the ones left behind. A `KEY` row came back at 1,285 msg/s, *faster than `UNORDERED`* |

**Six of these eight produce a plausible number rather than an error.** That is the property they
share, and it is the reason this note leads with them rather than burying them: the four previous
null results this project has recorded were all read off instruments in this condition.

## Is this null result real, or is the instrument still broken?

**It is not a null result** - the arms separate by 2.4x on `KEY` and 10x on `share`. But the same
question has to be answerable, so here is what makes these numbers checkable rather than merely
plausible:

- **Every arrival rate was achieved.** Requested against achieved agrees to within **1% on all 84
  rows**, with feed-lag p99 between **1ms and 3ms** against a 100ms gate - checkable from the
  committed file, not taken on trust. The producer was not the bottleneck, and the harness voids the
  run rather than reporting it if that ever stops being true.
- **Sustained in-flight matches Little's law at every operating point.** `KEY` at 210/294/378 records
  a second with a 10ms mean handler holds 2/3/4 in flight. An instrument that was measuring the wrong
  thing would not land on `L = lambda W` by accident.
- **The skew is verified at the broker, not just in the config.** The producing run prints a
  `KEYDIST` receipt (200 distinct keys, top key 16.6%, top ten 49.4%), and the live arrival topic's
  partitions were read mid-run independently: 859 records on the busiest against ~200 on the median.
- **The distinct-key control reproduces the old null exactly** - 510 / 509 / 508 against a 505ms
  handler, flat across utilisation. If the harness were inventing separation, that row would show it.
- **The saturated and unsaturated measurements disagree by three orders of magnitude in the direction
  the theory predicts** (residence p50 2,952ms saturated against 12ms at 50% utilisation), and the
  drain figures now equal the run duration to the millisecond, which they did not before the snapshot
  fix.
- **Load stayed between 3.6 and 14.2** for all 84 rows, under the ~20 discard threshold, and is a
  column on every row.

**The one thing that is NOT established** is anything about a real handler. Every figure here is a
`Thread.sleep` or a timer - see
[`next-benchmark-a-model-of-work-not-work.md`](next-benchmark-a-model-of-work-not-work.md), whose
first half is untouched by this work.
