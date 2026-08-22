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

## The controlled-arrival matrix

*(filled in below once the sweep lands)*

## Predictions, and what happened to them

*(filled in below)*

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
