<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Version-over-version throughput bench

Answers one question: **has Parallel Consumer's throughput changed between released versions, and
where?** It exists because the rescued 2021 demo ran roughly 30% slower than the asciinema cast the
README links, and that needed measuring rather than guessing.

The finding, the numbers and the caveats live in
[`docs/inflight/perf-throughput-regression-since-0-3.md`](../docs/inflight/perf-throughput-regression-since-0-3.md).
This file is only how to run it.

It since grew a second question it was not built for - **how does Parallel Consumer compare to an
engine that is not Parallel Consumer?** - because every arm above measures PC against itself and so
produces numbers with no scale. That is [the `llingr` arm](#the-llingr-arm---private-research-only),
and it is **private research only**.

And a third question, which needs a different harness because throughput cannot see it: **what does
PC's offset encoding actually buy?** That is
[the divergence harness](#the-divergence-harness---what-committing-past-gaps-actually-buys),
`bench/run-divergence.sh`, and it measures committed offsets and redelivery rather than messages per
second. Asking that question honestly then needed a third thing: a
measurement of the *client* each engine sits on, which is
[the `franz` arm](#the-franz-arm---the-client-control) and carries no such restriction.

## Run it

```sh
bench/run-bisect.sh [records] [delayMs] [concurrency] [repeats]
bench/run-bisect.sh 350000 2 100 2          # the defaults used for the recorded result
```

Needs Docker and network access to Central. It starts a broker named `pc-bench-broker`, **reuses it
if already running**, produces the dataset once, then runs each version against it. Nothing is torn
down at the end - a sweep is usually run more than once, and re-producing 350,000 records per
attempt is the cost this design exists to avoid.

Override the sweep:

```sh
PC_VERSIONS="0.3.0.2 0.5.3.2 LOCAL" CLIENT_PINS="NATIVE 3.9.2" bench/run-bisect.sh
MODES="core llingr" DELAYS="0 2 20 100" bench/run-bisect.sh   # engine comparison across delays
MODES="core llingr franz" CONCURRENCIES="1 10 100 1000" DELAYS=0 bench/run-bisect.sh
MODES="pc reactor mutiny proxy" BENCH_ASYNC_STUB=1 bench/run-bisect.sh  # the four engines, async callee
BENCH_WORK=/some/dir bench/run-bisect.sh     # keep resolved classpaths between sweeps
BENCH_SKIP_PRODUCE=1 bench/run-bisect.sh     # topic already holds the dataset; don't re-produce
```

`MODES`, `DELAYS`, `CONCURRENCIES`, `ORDERINGS` and `ARRIVAL_RATES` are lists, so one invocation
produces a whole comparison table - the full cross product of the five. `MODE`, `BENCH_ORDERING` and
the positional `delayMs` and `concurrency` still work and now just mean a list of one.

### Controlled arrival - `ARRIVAL_RATES`, and why every latency figure before it was unusable

```sh
ARRIVAL_RATES="400 560 720" ORDERINGS="KEY PARTITION" MODES=core \
  BENCH_KEY_DISTRIBUTION=zipf BENCH_KEY_COUNT=200 BENCH_DELAY_P99=1020 \
  bench/run-bisect.sh 15000 20 24 2
```

**Every run this harness does by default drains a backlog produced before the window opened.**
Residence time is then buffered depth over throughput, so two arms with the same buffer and the same
throughput ceiling have equal residence *by construction*. That is not a subtle bias: `PARTITION` and
`KEY` at 24 in flight over 24 partitions, with the handler's p99 at 50x its median, came back at
residence p99 **15,568ms against 15,565ms**. The experiment could not have shown a difference had one
existed.

`ARRIVAL_RATES` feeds records **during** the measured window at a fixed rate, into a **topic created
for that run**, so the consumer starts with nothing queued and residence measures the engine rather
than the queue. It is a list because at 100% utilisation every queueing system measures its backlog -
sweep it as a fraction of the arm's own measured throughput (50%, 70%, 90%) and the percentiles turn
up somewhere in between.

- **The schedule is absolute** - record *i* is due at `t0 + i / rate`, not "the last send plus an
  interval" - so producer jitter cannot silently lower the arrival rate.
- **`t0` is when the consumer is demonstrably live.** `BENCH_ARRIVAL_WARMUP` records go first and the
  clock starts when the arm reports finishing one, so a slow group join is not fed into the arm as a
  backlog.
- **A run whose feed could not hold its schedule is VOIDED**, as `ARRIVAL_VOID`, not recorded. If the
  producer is the bottleneck the whole run measures the producer, and its percentiles answer a
  different question than the one in the column heading. `arrival_achieved`, `feed_lag_p99_ms` and
  `backlog_p99` are the evidence, on every row, that it was not.
- **`msg_per_sec` stops being a throughput measure** under controlled arrival. It is bounded by the
  arrival rate by construction, and the run's wall clock includes the group join. Read it only as a
  check that the arm kept up.

**A fresh topic, rather than starting an existing one at `latest`.** The reset is applied at
*assignment*, so a record produced between subscribe and assignment is silently skipped, the arm
never reaches its expected count, and the row comes back as a timeout - which reads as a slow arm. A
fresh topic with the harness's usual `earliest` turns that race into a harmless ordering.

### Key distribution - `BENCH_KEY_DISTRIBUTION`, the axis that makes `KEY` ordering mean something

| Value | What it produces |
|---|---|
| `distinct` (default) | `key-0`, `key-1`, ... - one key per record. Today's behaviour, byte for byte |
| `zipf` | Zipf over `BENCH_KEY_COUNT` keys (default 1,000) with exponent `BENCH_ZIPF_EXPONENT` (default 1.0) |
| `hot` | uniform over `BENCH_KEY_COUNT` keys - a bounded key set with **no** skew, the control that separates "few keys" from "unevenly used keys" |

**All-distinct keys is the best possible case for any key-sharded design**, and it was the only one
this harness could produce. A shard under `KEY` ordering is one key; with every key distinct, every
record is its own shard of exactly one entry, so the ordering constraint binds nothing and `KEY`
behaves precisely like `UNORDERED`. That is why the two have matched in every table here - **the
project had never tested key ordering, it had tested `UNORDERED` wearing its name.**

Seeded from `BENCH_WORK_SEED` by a single-threaded loop, so the key for record *i* depends only on
*i* and every arm, repeat and sweep sees the identical key sequence. The producing run prints a
`KEYDIST` receipt line - distinct keys, top-key share, top-ten share - because a skew that was
requested and silently not applied looks exactly like a skew that was applied and did nothing.

The distribution is part of the **topic name**, for the same reason the partition count is: two
datasets differing only in their keys must never share a topic.

Concurrency became an axis for the same reason delay did, and the finding that forced it is worth
knowing before you use it: at delay 0 llingr beat PC while holding **two or three** records in
flight against PC's hundred, which raised the question of whether PC was paying to fan out work
that had none in it. That question cannot be asked while concurrency is a fixed positional
argument. It was asked, and [the answer is in the note](#results-in-this-directory).

`LOCAL` is this checkout, resolved as an ordinary Maven coordinate, so install it first:

```sh
./mvnw install -DskipTests -Dcopyright.skip=true
```

It used to read the local build out of `target/` directories instead. That special case bought
nothing and cost a confusing failure - a worktree that had never been built failed as `package
bz.stub.parallelconsumer does not exist` - and, far worse, it resolved a **different classpath** from
every other arm. See the logging trap below.

Results land in `$BENCH_WORK/results.csv` as
`pc_version,client_pin,mode,delay_ms,concurrency,repeat,msg_per_sec,peak_in_flight` plus the eight
latency columns below. `delay_ms` and
`concurrency` were each added when that setting became a swept axis - without them a file sweeping
it cannot be read back. **Older files under `results/` are left alone rather than backfilled**: they
are records of what was run, and rewriting a measurement file to a newer schema invites reading a
value into it that nobody measured. `curve.csv`, `core-curve.csv` and `matched-concurrency.csv`
predate `delay_ms` and were all taken at 2ms; `delay-sweep-llingr.csv` predates `concurrency` and
was taken at 100.

### The three latency columns, which are not three views of one thing

Until 2026-08-22 this harness measured throughput and nothing else, which is exactly the number a
serial engine can hide behind: a run finishing in the same wall clock can have put one record in a
hundred through a queue ten times as long, and no column here would have moved. There are now two
latency measures, and reading either as corroboration of the other is the mistake to avoid.

**`residence_p50_ms` / `p99` / `p999` / `max` - poll-return to completion.** What a record spends
INSIDE the engine. For any Parallel Consumer arm this is read off PC's own
`pc.record.residence.time` meter, which starts when the `WorkContainer` is built from a poll batch
and stops when the control loop finishes with the delivery - failures, abandonments and every retry
included. Produce time and broker wait are deliberately excluded: they are the environment, not the
engine.

**This is the only column that is not a restatement of throughput.** PC decides for itself how much
to fetch and how deep to buffer, so under a backlog residence is Little's law applied to those
buffers - buffered depth over throughput. It is therefore what distinguishes an arm holding 543
records of a configured 5,000 because nothing more has been fetched, from one holding 543 because
they are fetched and queued. Nothing else here can tell those apart.

**`drain_p50_ms` / `p99` / `p999` / `max` - engine start to completion.** The dataset is produced
once, before any arm runs, so engine start is a genuine common arrival instant for every record.
**For a fixed record count this is roughly position/throughput, so it largely restates `msg_per_sec`
in latency units** - say so wherever it is quoted, and never present the two as agreeing evidence.
It earns its column because it is the honest measure of a backlog drain: a consumer restarting, or
catching up on lag, which is a real operational situation rather than a benchmark artefact, and the
number an operator actually feels.

**Which arms report which.** `residence` is blank (`-`) for `llingr` and `franz`, which are not PC
and do not print it. `vanilla` and `pool` measure it themselves, from the same two instants -
poll-return, and the completion of the work - so their numbers sit in the same column legitimately.
An arm that could only approximate it reports blank instead: **a blank column is recoverable and a
number measured a different way is not.** Residence is never taken by timing the user function; the
handler's duration is the harness's INPUT (see the work model), so measuring it would be measuring
the question.

**Precision differs between the two, deliberately.** `drain` is computed by sorting every recorded
sample, so its percentiles are exact. `residence` for a PC arm comes through Micrometer's percentile
histogram and is accurate to roughly 3% at these magnitudes. The bench also installs a MeterFilter
widening that histogram's expiry before PC registers anything, and that is load-bearing rather than
tidy: a probe that recorded 1,000 samples at 1,000ms, waited 70 seconds and asked again reported a
p50 of **4.98ms**, having silently forgotten all of them, while the meter's count still read 1,010.
Without the filter, any run longer than Micrometer's rotation would publish percentiles for its tail
alone, with nothing anywhere saying so. With it, the same probe reported 1,006ms.

**`e2e_p50_ms` / `p99` / `p999` / `max` - the record's INTENDED send instant to completion.** Only
populated under `ARRIVAL_RATES`, because only then does a record have an arrival instant at all.

**It is the one measure coordinated omission cannot fool, and that is why it exists.** Residence
starts at poll-return, so a record that sat in the broker because the consumer had fallen behind is
charged **nothing** for the wait - under an arrival sweep that is precisely the failure mode that
would make a saturated arm look fast, which is the same shape of false negative the backlog drain
produced before controlled arrival existed. Each record carries its own scheduled send time in its
value, so the measurement starts from the instant the workload *intended* to hand it over, not from
whenever the producer managed to. It therefore also charges the feeder's own lateness, which is why
`feed_lag_p99_ms` is gated and a late feed voids the run.

**Read `p999` and `max`, not the mean.** Head-of-line blocking shows up in the upper percentiles long
before it shows up anywhere else, which is the entire reason these columns exist.

### `inflight_p50` - because `peak_in_flight` is a maximum, and a maximum answers the wrong question

`peak_in_flight` is the highest the engine ever reached. `inflight_p50` is the median of a 20ms
sample taken across the whole run - what it **sustained**.

They come apart exactly where it matters. An arm that touches its configured concurrency once, in
its first second, and then runs two records at a time for two minutes reports the same
`peak_in_flight` as one that holds full concurrency throughout, and those are not the same engine to
anybody choosing between them. Measured 2026-08-22: `KEY` ordering on a skewed key distribution
reported **peak 24 of a configured 24** while taking **4.1x** longer than `UNORDERED` over the
identical records - because the run's whole tail is one hot key draining one record at a time. The
peak was true and told you nothing.

Read the two together: `peak == concurrency` with `inflight_p50` far below it is an arm that is
*starved*, not one that is *slow*.

**It samples from the first record into the user function, not from JVM start, and it did not until
2026-08-22.** The sampler is started from `main` - it has to be, because each arm takes its own clock
and there is no earlier common hook - so before the gate it recorded a zero every 20ms through
consumer construction, subscribe and the consumer-group join. That is four to six seconds of zeros
prepended to every run's samples: **invisible on a thirty-second run and decisive on a four-second
one.** Measured while calibrating the realistic-workload campaign: `core`, `UNORDERED`,
maxConcurrency 200, 12,000 records - **2,978 msg/s at a reported sustained in flight of ZERO**, where
Little's law puts it near 30 and the fixed instrument reads 196. The same arm at maxConcurrency 24,
whose run lasts 9.8s rather than 4.0s, reported 22 and looked perfectly healthy.

**So a short run's `inflight_p50` taken before that fix is not merely noisy, it is wrong by the whole
quantity being measured** - and it was wrong quietly, in a column beside rows where it was roughly
right. Once armed it stays armed, so a genuine idle stretch mid-run - which is exactly what a hot key
produces, and exactly what this column exists to show - is still recorded as the zero it is.

### `pc_build` - `LOCAL` names a coordinate, not a build

`PC_VERSIONS=LOCAL` resolves to `bz.stub.parallelconsumer:*:0.6.0.0-SNAPSHOT` out of a `~/.m2` that
every worktree and every concurrent session on the machine shares. Whoever ran `mvn install` most
recently owns it, and a sweep already in progress picks the change up at its next JVM start. On
2026-08-22 that happened twice in one evening and put four rows in a results file measured against
code their author had never seen; `pc_version` read `LOCAL` for every row and identified nothing.

`pc_build` carries the core jar's checksum, taken **before and after every run**, so a swap that
happens mid-cell voids that cell as `BUILD_CHANGED_<before>_TO_<after>` rather than being averaged
into it. Two rows that disagree are visibly two experiments instead of two repeats.

**`pc_build` detects the swap; `LOCAL_VERSION` prevents it, and a sweep longer than a few minutes
should use it.** `LOCAL_VERSION=<version>` changes the coordinate `LOCAL` resolves to, so a sweep can
measure a version nobody else on the machine has heard of:

```bash
./mvnw -B versions:set -DnewVersion=0.6.0.0-myrun-SNAPSHOT -DgenerateBackupPoms=false
JAVA_HOME=~/.sdkman/candidates/java/17.0.18-tem ./mvnw -B install -DskipTests -Dcopyright.skip=true
git checkout -- '*/pom.xml' pom.xml          # put the poms back BEFORE committing anything
LOCAL_VERSION=0.6.0.0-myrun-SNAPSHOT MODES=core bench/run-bisect.sh
```

It is additive - the ordinary `0.6.0.0-SNAPSHOT` coordinate is left exactly as another session left
it, so there is nothing to restore afterwards and no window in which somebody else's sweep is
measuring your build. **This is not hypothetical**: `0.6.0.0-SNAPSHOT` was overwritten by another
session partway through the realistic-workload campaign's calibration, between one run and the next,
and the only outward sign was the residence column going blank.

**Two cautions.** `cksum` on a jar is not a code identity - two installs of the *same* source produce
different checksums, because the archive carries timestamps - so `pc_build` answers "did this change
under me", never "is this the code I think it is". And `versions:set` rewrites every pom in the tree:
put them back before committing, or the version bump ships with your measurement.

### Results in this directory

| File | What it holds |
|---|---|
| `curve.csv`, `core-curve.csv` | The version bisect, Vert.x and core, at 2ms / concurrency 100 |
| `matched-concurrency.csv` | 0.3.2.0 against LOCAL at three ceilings, the matched-concurrency check |
| `delay-sweep-llingr.csv` | core against llingr across four delays, at concurrency 100 |
| `concurrency-sweep-0ms.csv` | core, llingr and franz across seven concurrencies at delay 0 |
| `concurrency-sweep-2ms.csv` | the same three arms at delay 2ms, concurrency 25 / 100 / 1000 |
| `direct-pull-delay-sweep.csv` | the shipped engine against the direct-pull engine, 3 delays x 2 concurrencies |
| `direct-pull-concurrency-sweep-0ms.csv` | the same two engines at delay 0 across six concurrencies - where the result is |
| `ordering-head-of-line-latency.csv` | `PARTITION` against `KEY` at three buffer depths, flat and tailed handler - the first latency comparison |
| `saturated-skew-baseline.csv` | The saturated baseline on a Zipf key distribution: `KEY` / `PARTITION` / `UNORDERED` / `share-explicit` across three workloads, plus the distinct-key control. **`KEY` costs 0.13% on distinct keys and 3.1x on skewed ones** |
| `arrival-tail-skew-matrix.csv` | The tail experiment: the same arms under **controlled arrival** at 50/70/90% of each arm's own capacity, skewed and distinct keys, flat / tailed / tailed-with-failures. The first file here with an end-to-end latency column |
| `realistic-ordering-matrix.csv` | The realistic-workload re-take, ordering half: seven engines plus **0.5.3.3 from Maven Central**, `KEY` against `UNORDERED`, distinct keys against Zipf, 0% against 1% failures. 12,000 records, 24 partitions, 10ms, `maxConcurrency` 24, `messageBufferSize` 20,000 |
| `realistic-throughput-matrix.csv` | The same re-take at the operating point the engine and share-group tables were published at - 100,000 records, 2ms, `maxConcurrency` 5,000, `UNORDERED` - with the key distribution and the failure rate added. Includes the **one-partition rows that reproduce the published tables**, so a figure that moved can be attributed |
| `realistic-default-buffer-control.csv` | `realistic-ordering-matrix.csv`'s workload with one term changed: PC's **default** `messageBufferSize` instead of 20,000. The ordered arm loses another 2.3x; `UNORDERED` does not move |

**The two skew files are the only ones taken on anything but all-distinct keys**, and that is the
single most important caveat on every other row in this table: with distinct keys `KEY` ordering
constrains nothing, so every `KEY` figure above is `UNORDERED` under a different name. See
[`docs/inflight/perf-the-tail-experiment-ran-2026-08-22.md`](../docs/inflight/perf-the-tail-experiment-ran-2026-08-22.md).

`ordering-head-of-line-latency.csv` carries three extra LEADING columns the others do not -
`message_buffer_size` and `handler_p99_ms`, because both were swept and neither is a column the
harness emits, and `load_1min`, because the machine was shared throughout. It was taken from four
separate invocations, hence the leading form; and it predates the harness's own `load_1m` column, so
that column is absent from its body. **Read it with three caveats.** The
`message_buffer_size` 240 cell has KEY rows only: the two PARTITION rows were voided when a
concurrent session replaced the shared `LOCAL` build mid-cell
([`docs/inflight/perf-local-is-a-coordinate-not-a-build.md`](../docs/inflight/perf-local-is-a-coordinate-not-a-build.md)).
The second repeat of `20000,1000,PARTITION` came back at 13 in flight rather than 24 under a load of
12 and disagrees with its own first repeat by a factor of two; it is left in rather than dropped,
which is what the load column is for. And every row is a saturated backlog drain, so its residence
figures are bounded by buffer depth over throughput - what they do and do not support is in
[`docs/inflight/next-the-tail-experiment.md`](../docs/inflight/next-the-tail-experiment.md).

What the two `concurrency-sweep-*` files mean is in
[`docs/inflight/perf-throughput-regression-since-0-3.md`](../docs/inflight/perf-throughput-regression-since-0-3.md),
under "Concurrency as an axis, and the client control the llingr comparison was missing"; the two
`direct-pull-*` files are [their own arm](#the-direct-pull-arm---a-second-engine-inside-this-one).

## The direct-pull arm - a second engine inside this one

`bench/run-direct-pull.sh` compares Parallel Consumer's shipped engine against an experimental
**direct-pull** engine, in which workers take their own work straight from the shards and no
intermediate executor queue exists. It is selected by the mode name `core-dp`, which `run-bisect.sh`
translates into `-Dpc.directPull=true`: a system property rather than a new `Bench` argument, because
this template compiles against every released version in the sweep and none of them has the option.

```sh
BENCH_SKIP_PRODUCE=1 BENCH_TOPIC=bench-500000-p10 BENCH_PARTITIONS=10 \
  bench/run-direct-pull.sh 500000 3
DELAYS=0 CONCURRENCIES="10 100 250 500" \
BENCH_SKIP_PRODUCE=1 BENCH_TOPIC=bench-500000-p10 BENCH_PARTITIONS=10 \
  bench/run-direct-pull.sh 500000 3
```

**Why it has its own driver rather than being `MODES="core core-dp"`.** `run-bisect.sh` loops mode on
the OUTSIDE, so two arms compared in one invocation are also compared across whatever happened to the
machine between the two blocks - the fourth trap below, which nothing in the harness catches. This
driver runs **one invocation per point with both arms in it**, so the two land within a minute of
each other, and it **alternates which arm goes first** between repeats, because whichever runs first
pays for a colder cache and always giving that to the same arm is a bias rather than noise. It also
records `uptime` before and after every point, into `$BENCH_WORK/direct-pull-load.txt`.

The finding is in
[`docs/inflight/perf-direct-pull-measured.md`](../docs/inflight/perf-direct-pull-measured.md), and it
is not a single number: the direct-pull engine is **three times faster at concurrency 10 and twenty
times slower at 5,000**, so any measurement of it at one concurrency is meaningless.

## What it controls for, and why each one is there

- **One broker, one dataset, produced once.** Every arm reads the same bytes from the same broker
  with a fresh consumer group. The first version of this started a Testcontainers broker and produced
  inside each measured run, which made a sweep unaffordable and meant no two arms read the same data.
- **One harness source.** `Bench.java.template` is compiled once per arm with only `__PKG__`
  substituted, because the fork renamed `io.confluent.parallelconsumer` to `bz.stub.parallelconsumer`.
  Nothing else differs between arms. Each engine's own ~30 lines live in `arms/<Engine>Arm.java.template`
  and are resolved by name at runtime - [why, below](#the-engine-arms).
- **Nothing compiled is ever cached.** Only the Maven classpath resolution is, keyed on the generated
  pom's checksum. `prepare()` used to return early when `cp.txt` existed, and on 2026-08-21 that ran a
  whole sweep against a harness build from hours earlier: `BENCH_ASYNC_STUB` was silently ignored and
  three new engine modes all fell through the old template's `else` branch into the Vert.x arm. Four
  "engines" that were one engine. Caught only because a control disagreed with a committed number.
- **`kafka-clients` as its own dimension.** PC's transitive client moved 2.5.1 -> 3.9.2 across the
  bisect range, so a throughput change could be either. `CLIENT_PINS` separates them.
- **A vanilla `KafkaConsumer` arm** (`Bench vanilla`) that touches no PC code, as the control for
  everything environmental. Note what it does *not* control for: it drives real HTTP to the WireMock
  stub rather than sleeping, so it is not a floor for the in-handler-sleep arms and must not be read
  as one. **There is still no Java-side sleep-handler control** - see the franz arm below for why
  that gap now matters.
- **A `franz` arm**, franz-go with no engine at all, as the client-side control for the llingr
  comparison. [Its own section is below.](#the-franz-arm---the-client-control)

- **Logging is pinned for every arm** (`bench/conf/logback.xml`, first on the classpath). See below.

## The engine arms

The project ships four engines. Until 2026-08-21 it compared one.

| Mode | Engine | Arm file | Extra artifact |
|---|---|---|---|
| `core` | `ParallelEoSStreamProcessor` - **not** an `ExternalEngine` | in `Bench` itself | - |
| `pc` (alias `vertx`) | `VertxParallelEoSStreamProcessor` | `arms/VertxArm.java.template` | - |
| `reactor` | `ReactorProcessor` | `arms/ReactorArm.java.template` | `parallel-consumer-reactor` |
| `mutiny` | `MutinyProcessor` | `arms/MutinyArm.java.template` | `parallel-consumer-mutiny` |
| `proxy` | `ProxyProcessor` - what every non-JVM client runs on (astubbs#242) | `arms/ProxyArm.java.template` | `parallel-consumer-proxy` |

**Why separate files.** `Bench.java.template` must still compile against every release in the version
bisect, back to 0.3.0.2. Mutiny and the proxy exist in **no** published release, so importing them in
the shared template would not add an arm - it would delete the bisect. Instead `Bench` names no engine
class and resolves mode `foo` to class `FooArm` reflectively; `run-bisect.sh`'s arm table decides which
arm sources to compile and which artifacts to put in the generated pom. Adding an engine is one new
file plus one line in that table.

**Sweeping `mutiny` or `proxy` over published `PC_VERSIONS` is expected to fail** - the artifact does
not exist, so resolution fails and the row is recorded `COMPILE_FAILED`. That is the honest answer,
and it fails only the mode that asked for it: the arm artifact is per mode, not per sweep.

**These four arms have been smoke-run, not measured.** Each starts, consumes, and prints a parsing
`RESULT` line. There are no rows for them in `results/`, deliberately: every run available so far was
taken while another session held ~1,000% CPU against the same broker, where the same operating point
returned 9,050 msg/s and then 1,883 four minutes later. Numbers taken there are not evidence, and
this directory has already shipped one phantom finding from a comparison that looked sound.

### The callee is the axis that matters, and it has three settings

Every one of these engines is asynchronous, so the question that decides their numbers is **whether
the thing they call holds a thread while it works**. The harness makes that a setting:

| | What the callee is | Reaches concurrency? |
|---|---|---|
| *(default)* | WireMock, thread per request, listener sleeps on the serving thread | No - capped near `r x delay` (~2,650 at 100ms), server-side |
| `BENCH_ASYNC_STUB=1` | a Vert.x HTTP server answering on `setTimer`, holding no thread | Yes |
| `BENCH_TIMER_CALLEE=1` | no server at all - a bare timer. **A control, not a comparable row** | Yes, and with no HTTP client in the way |

**A high-concurrency, long-delay number taken through the default stub is capped by this harness, not
by Parallel Consumer** - see
[`docs/inflight/perf-vertx-already-beats-the-thread-ceiling.md`](../docs/inflight/perf-vertx-already-beats-the-thread-ceiling.md).
The third setting exists because the async-stub result left ~36% of theoretical throughput
unattributed and named the HTTP client's connection pool as the suspect: with a timer there is no
socket and no pool, so whatever ceiling remains is the engine's.

**The proxy arm has no HTTP callee at all.** Its callee is a connected worker on the far side of the
dispatch protocol, so `arms/ProxyArm.java.template` builds one, in the same two forms, selected by the
same `BENCH_ASYNC_STUB`. It drives the engine directly across its `DispatchSink`/`report` seam: **no
gRPC**, so its numbers are an upper bound on the deployed proxy and say nothing about the wire.

## The share-groups arms - Kafka's own answer, and the only non-library arm here

`share` and `share-explicit` are a bare `KafkaShareConsumer` loop with **no Parallel Consumer at
all** - the same category as `vanilla` and `franz`. That is the point: every other arm in this
harness is a library, and KIP-932 share groups are the **broker** changing the rules underneath the
whole category, with per-record acknowledgement and concurrency that is not bounded by partition
count.

**It is not blocked on PC supporting Kafka 4.** The only Kafka-4 dependency is the broker;
`CLIENT_PINS` already pins `kafka-clients` per sweep, independently of the PC version.

```bash
BENCH_BROKER=share BENCH_TIMER_CALLEE=1 MODES="core core-vt share share-explicit" \
  PC_VERSIONS=LOCAL CLIENT_PINS=4.3.1 bench/run-bisect.sh 100000 2 5000 2
```

**`BENCH_BROKER=share` starts a SECOND broker** - Kafka 4.3.1, its own container name and port -
rather than replacing the 3.9.0 one other sessions on this machine are using. Three settings make
share groups work on a single node and all three are required; `lib/broker.sh#use_share_broker`
carries them and says why each one fails if omitted.

### Both acknowledgement modes, and why neither can poll ahead

| Mode | `share.acknowledgement.mode` | What acknowledges |
|---|---|---|
| `share` | `implicit` (default) | the next `poll()` acknowledges everything the previous one delivered |
| `share-explicit` | `explicit` | every record individually, before the next `poll()` |

**Neither mode lets an honest processor poll while a batch is outstanding.** Explicit forbids it -
the client throws `IllegalStateException`. Implicit permits it and *acknowledges records that have
not been processed*, which is at-most-once delivery wearing an at-least-once label. So the arm is
batch-synchronous by construction: poll, run the batch, finish it, poll again. **That is the
structural difference from Parallel Consumer**, which keeps records from many polls outstanding at
once - and it is what PC's offset encoding buys.

### Three things about this arm that are not obvious

- **`max.poll.records` does nothing.** Setting it to 100 left the batch at 2,606 records and the poll
  count unchanged at 9 for a 20,000-record dataset. The batch is what the share session acquired,
  bounded by the `share.partition.max.record.locks` **group** config (default 2,000) per
  share-partition, not by the consumer's poll size. The `max_poll_records` column is therefore
  meaningless for share rows.
- **`auto.offset.reset` and `enable.auto.commit` are REFUSED**, not ignored - `ConfigException` at
  construction. Neither concept survives the design: there is no consumer-side offset to commit, and
  where to start is a property of the group. The arm removes both.
- **Where to start is a group config the arm must set itself.** `share.auto.offset.reset` defaults to
  `latest`, and every run joins a fresh group, so without an `Admin` call before subscribing the arm
  reads nothing and hangs until the run deadline.

### Reading its numbers

`peak_in_flight` is the column to trust, as always here - and for these arms it is also the one that
shows the ceiling, because the batch IS the concurrency. The `concurrency` argument is applied as a
semaphore so the axis means something below the batch size; above it, it is inert.

## The `llingr` arm - private research only

> **Read [`llingr/NOTICE.md`](llingr/NOTICE.md) before running this.** `llingr-demux` is AGPL-3.0
> and patent pending, and the owner's standing decision - recorded in
> [`docs/inflight/next-competitor-llingr.md`](../docs/inflight/next-competitor-llingr.md) - is **no
> public comment on llingr, anywhere**. Numbers from this arm are internal research input. Do not
> publish them, quote them externally, or put them in marketing.

Every other arm compares Parallel Consumer against itself, so nothing it prints has a scale. Sharing
a JVM, a client and a control loop means every arm carries the same floor and ceiling, and
"26,000 msg/s" has nothing to be read against. `llingr-demux` is the nearest external implementation
of the same processing model - key-ordered concurrency past partition count, offsets committed after
out-of-order completion - which makes it the outside reference point the sweep never had.

```sh
MODES="core llingr" DELAYS="0 2 20 100" PC_VERSIONS=LOCAL CLIENT_PINS=NATIVE \
  bench/run-bisect.sh 350000 2 100 2
```

That invocation produced [`results/delay-sweep-llingr.csv`](results/delay-sweep-llingr.csv); what it
means is written up in
[`docs/inflight/next-competitor-llingr.md`](../docs/inflight/next-competitor-llingr.md), not here.

**Requires Go**; the modules need a newer toolchain than most machines have, so the build runs with
`GOTOOLCHAIN=auto` and Go fetches its own. With no `go` on `PATH` the arm logs why and is skipped -
a sweep of five other arms should not die because one machine lacks a compiler.

### The delay sweep is the measurement

Pinning delay was right for a *version* bisect and is wrong for an *engine* comparison. At 0ms the
number is almost entirely per-record framework overhead; at 100ms the sleep dominates and any
correct engine converges on `records * delay / concurrency`. A single delay therefore cannot
distinguish the two engines from the two runtimes - only the shape across delays can, and whether
the gap closes as work time grows is the actual result. Hence `DELAYS`, and hence `delay_ms` is a
results column.

### Why it is compared against `core`, not `pc`

`core` and `llingr` both simulate work by sleeping inside the handler, so their peak columns and
their rates mean the same thing. The `pc` arm drives real HTTP through Vert.x to a WireMock stub;
comparing it to an in-handler sleep would measure Vert.x's HTTP client, not the engine. `pc` is
still the right arm for a *version* question - it is where the ExternalEngine cliff lives.

### What it shares with the Java arms, and what it cannot

Same broker, same topic, same bytes, a fresh consumer group per run, the same
`RESULT <mode> <count> <ms> <msgPerSec> peak=<n>` line parsed by the same function, and the same
in-flight accounting - incremented on entry to the handler, decremented on exit, maximum kept. PC's
`maxConcurrency` maps to llingr's `ConcurrentKeys`, which the library caps at 5,000; the harness
clamps and says so rather than letting the library panic mid-sweep.

**Name the unfairness rather than hiding it.** Neither engine is being given its best configuration,
so this is a like-for-like harness result, not a verdict:

- **The Kafka clients are different implementations.** PC uses the Java client; the llingr arm uses
  `franz-go` via `llingr-adapter-franz`. Fetch tuning does not correspond field-for-field, so both
  run their own defaults. Some of any gap is client, not engine - the same confound `CLIENT_PINS`
  exists to separate on the Java side. **That is what [the `franz` arm](#the-franz-arm---the-client-control)
  is now for**, and it changed the reading of this comparison rather than annotating it: measure
  franz-go with no engine, and llingr's advantage is bounded by what its client was already doing.
  What is still missing is the mirror image - a Java-side sleep-handler control - so PC's own
  deficit against that floor cannot yet be split into client and engine.
- **The dataset gives llingr its best case on key routing.** Every record has a distinct key, so
  llingr's per-key workers never contend. A keyed workload with few hot keys would look different.
- **The topic has one partition.** That is the case both projects exist to serve - concurrency past
  partition count - but it is not either engine's throughput ceiling, which more partitions would
  raise.
- **Commit behaviour differs by design**, and that difference is the subject of
  `next-competitor-llingr.md` rather than of this benchmark: llingr commits the highest contiguous
  offset, PC encodes the incomplete set and commits past gaps. Throughput on a clean run does not
  exercise it.
- **Go's logging is pinned the same way the JVM's is**, for the reason the logging trap below
  records: a `nexus.Logger` that drops info and debug, keeping warnings and errors on stderr.

## The `franz` arm - the client control

Ordinary BSD-3-Clause code, no llingr dependency, **no publication restriction**: `bench/franz` is
franz-go with nothing on top. It exists because the llingr comparison had a confound the Java arms
would never have been allowed to keep.

PC reaches Kafka through the Java client; llingr reaches it through franz-go. A gap between them is
therefore some mixture of engine and client, and the sweep had no way to separate the two. That is
the exact confound `CLIENT_PINS` exists to remove on the Java side, and across languages it has no
counterpart - you cannot pin a Go program to the Java client. So instead of separating the client,
this arm **measures it**: poll, sleep, count. Whatever franz-go scores with no engine is the floor,
and only what llingr scores *above that floor* can be credited to llingr.

- **Concurrency model, since "a Go arm" does not say it.** A fixed worker pool of `-concurrency`
  goroutines fed by an **unbuffered** channel. Unbuffered so the poll loop cannot run ahead of the
  workers - with a buffer, the peak-in-flight column would be measuring the buffer instead of the
  pool. `-concurrency 1` is not a pool of one: it runs the handler inline in the poll loop, no
  goroutine and no channel, so the serial number is not carrying a handoff that costs more than the
  work does.
- **It is a floor, not a competitor, and it is weaker than both engines on purpose.** It does not
  order by key, so two records sharing a key run concurrently and in either order - the whole
  problem both engines exist to solve, free here. And it commits nothing it has processed:
  franz-go's default autocommit commits what has been **polled**, so a crash loses records that were
  fetched and never ran. Left at the client default deliberately; changing it would make this a
  third engine rather than a floor.
- **Its own Go module**, like `bench/llingr`, so no shipped artifact picks a benchmark's
  dependencies up transitively. Unlike `bench/llingr` there is no licence reason for it - benchmark
  code simply is not product.

The measurement it was built for is written up in
[`docs/inflight/perf-throughput-regression-since-0-3.md`](../docs/inflight/perf-throughput-regression-since-0-3.md).

## The machine is shared, and a concurrent benchmark is not background noise

Several agent sessions run against this checkout at once, and more than one of them benchmarks. On
2026-08-21 a second session's sweep held ~1,000% CPU against the *same* broker and the *same* topic;
a run of this harness alongside it was scheduled at **0.2% CPU** and returned **1,883 msg/s at an
operating point that had produced 9,050 four minutes earlier**. That is not noise to be averaged out.

- **Check first**: `ps -Ao pcpu,etime,args -r | grep '[B]ench '` - any hit that is not yours means
  wait, or your numbers are somebody else's scheduling artefact.
- **Round-robin the repeats.** Run the sweep once per repeat rather than repeating each cell in
  place, so an arm's repeats are spread across whatever else the machine is doing instead of all
  landing inside one disturbance.
- **Record `uptime` either side of every batch, and say so in the results file.** Load has ranged
  from 8 to 667 on this machine in a single day.
- **`peak_in_flight` is the load-robust column; `msg_per_sec` is not.** The in-flight plateau held at
  2,438-2,840 across an 80x load range while throughput over the same range moved 4,648 to 22,844.
  When the machine cannot be quietened, prefer the question that peak in-flight answers.
- **Namespace your scratch directory.** Sessions share one scratchpad path, and two of them writing
  `results.csv` or `assemble.sh` is silent.

## The Vert.x arm has NO timer-callee form, and scores well without one

`vertx`/`pc` issue their HTTP request through the engine's own `vertxHttpReqInfo`, so the arm cannot
be handed a callee that is not an HTTP server. Under `BENCH_TIMER_CALLEE` there is no server:
`Bench#calleePort` returns 0, every request fails, and **the arm still prints a plausible figure** -
17,221 msg/s, mid-table - because the engine's `onResponse` callback fires on failures too. The only
tell is `peak_in_flight` = 0.

It is expensive as well as wrong: the failing runs spun at 190% CPU and took the machine's load from
12 to 44, contaminating every other arm measured in the same round. **`run-bisect.sh` now refuses the
combination**; use `BENCH_ASYNC_STUB=1` for a non-blocking callee this arm can actually reach.

## Four traps this harness has paid for - it encodes the first three, not the fourth

- **Jabel must be stripped from the compile classpath.** It ships as a transitive of older PC
  releases and javac auto-loads it as a compiler plugin via `ServiceLoader`; its 2021 ASM then dies
  on modern class files with `Unsupported class file major version 61`.
- **Logging configuration is a 6x confound, and it is invisible.** With no logback config on the
  classpath, logback defaults to DEBUG and Parallel Consumer logs per record. The same build measured
  **2,595 msg/s at default DEBUG and 16,231 msg/s with logging pinned to WARN**. Older versions barely
  notice the same setting - 0.3.2.0 moved 2% - so the effect is *not* uniform across arms and does not
  cancel out: it manufactures a cliff wherever per-record logging was added. This went unnoticed at
  first because the local arm resolved `parallel-consumer-core`'s tests jar, which ships a
  `logback-test.xml`, while every arm from Central did not. A benchmark that does not pin logging is
  measuring its own configuration.
- **The machine is a confound this harness does not measure, and agent sessions make it worse.**
  Every arm here is a throughput number off one laptop, so anything else running is in the result. On
  2026-08-21 a second agent session was running `bench/run-divergence.sh` against the same broker in
  the same worktree while a sweep was in progress, and it was only noticed at commit time. Check
  `uptime` and `ps` before a sweep, say in the write-up what the machine was doing, and prefer to
  re-take any cross-arm ratio you intend to rely on. The harness loops mode on the OUTSIDE - right
  for compiling each classpath once, wrong for isolating a slow drift, because two arms compared
  across separate invocations are also compared across whatever happened in between. **Nothing in
  the harness checks any of this** - unlike the three above, this one is entirely on the operator.
- **Too small a dataset reads as "no difference".** At 20,000 records every version returned about
  4,200 msg/s, because consumer-group join dominated the window. The arms only separate once the
  measured period is long enough to reach steady state; 350,000 is where the recorded result was
  taken.

## Do not try to rebuild the 2021 source tree

It needs JDK 13, which has no Apple Silicon build; an x86 JDK under Rosetta would skew a throughput
measurement worse than the toolchain difference it fixes; and Jabel's 2021 ASM cannot read JDK 17
class files however many module exports are opened. Consuming the **published artifact** from Central
avoids all of it, and is more faithful anyway - it is the jar users actually got.

## The divergence harness - what committing PAST gaps actually buys

`bench/run-divergence.sh` with [`bench/Divergence.java.template`](Divergence.java.template) and the
scenario runners in [`bench/llingr/scenarios.go`](llingr/scenarios.go). It answers a different
question from everything above, and it exists because **throughput cannot see this project's central
design decision**.

Parallel Consumer encodes the *incomplete offset set* into the commit metadata and commits past the
gaps. `llingr-demux` holds out-of-order completions in memory and commits the highest **contiguous**
offset. On a clean run where every record succeeds - which is every measurement above - those two
are indistinguishable. With one stuck record, and across a crash, they are not.

**Say it plainly: these scenarios are chosen because they favour Parallel Consumer**, in exactly the
way the pure-throughput benchmark is chosen (by whoever quotes it) because it favours a leaner
engine. The unfairness list below is part of the result, not a disclaimer attached to it.

```sh
bench/run-divergence.sh                                     # all three scenarios, both engines
SCENARIOS=stuck ENGINES=core bench/run-divergence.sh        # one scenario, PC only
COMMIT_INTERVAL_MS=5000 DELAY_MS=20 bench/run-divergence.sh # both engines' shipped defaults
```

Same broker (`bench/lib/broker.sh`, shared with `run-bisect.sh`), same topic, same bytes, one
partition, a fresh group per run. Needs the local build installed:
`./mvnw install -DskipTests -Dcopyright.skip=true`.

### The three scenarios

| Scenario | What it does | What it outputs |
|---|---|---|
| `stuck` | one record in `stallEvery` takes `STALL_MS` while the rest take `DELAY_MS` | committed offset and completed count over time; the **divergence** between them; the committed metadata verbatim |
| `restart` | the same workload killed mid-flight (`Runtime.halt` / `os.Exit` - no drain, no final commit), then restarted on the same group | **how many already-completed records are redelivered** - wasted work, which is the number a user pays |
| `retry` | `FAIL_PERCENT` of records fail on first delivery and succeed on retry | completion rate, and **how much work reaches a dead-letter path a retry would have saved** |

### The output format

Two machine-readable lines plus one evidence line, identical from both engines so one parser reads
both:

```
RESULT  <scenario> <count> <ms> <msgPerSec> peak=<maxInFlight>
RESULT2 scenario=.. engine=.. ms=.. completed=.. delivered=.. redelivered=.. retries=..
        committedOffset=.. highestCompletedOffset=.. divergence=.. maxDivergence=..
        maxCommitFreezeMs=.. metadataBytes=.. peakInFlight=.. commitIntervalMs=.. [scenario fields]
METADATA <the committed offset metadata, verbatim>
```

`RESULT` is byte-compatible with what `run-bisect.sh` already parses. `RESULT2` is `key=value` so an
arm can add a field without invalidating a parser or a stored results file. `METADATA` is a separate
line because base64 padding contains `=`, which truncates the value in any `key=value` parser -
including this harness's own.

Per-run time series land in `$BENCH_WORK/series-*.csv` as
`t_ms,completed,delivered,committed_offset,divergence,in_flight,metadata_bytes`; the summary in
`$BENCH_WORK/divergence.csv`; the verbatim commit metadata in `$BENCH_WORK/metadata.csv`.

### `divergence` is deliberately an over-estimate for PC, and that is why `restart` exists

`divergence = completedThisRun - (committedOffset - baseOffset)`: records finished that sit at or
above the committed position. For a contiguous-frontier design that *is* the wasted work on restart.
For PC it is an over-estimate, because PC's metadata records which of those records are already
done - so the redelivery count is **measured** by crashing and restarting, never inferred from the
committed offset. Reading the offset alone would flatter PC's competitor and then flatter PC; it is
the wrong metric in both directions.

### Four things it controls for, each of which was a wrong answer first

- **Commit cadence is a control, not a setting.** Both engines default to 5s (PC's
  `DEFAULT_COMMIT_INTERVAL`, llingr's `AutoCommitInterval`) and `COMMIT_INTERVAL_MS` sets both. The
  first restart run left it at the default and crashed 2.6s after the *only* commit that had
  happened, so it measured five seconds of commit **lag** and said nothing about commit **strategy**.
  Every results row records the interval that produced it.
- **The counters are snapshotted at the stop point.** `closeDrainFirst()` processes everything still
  outstanding, so a summary read after the close reports the whole topic however few records the
  scenario asked for - the retry arm claimed 199,004 completions against a target of 50,000, and
  nothing about that number looked wrong.
- **Sampling stops before the summary is taken.** Waiting for one more reading after the run let a
  commit that happened *after* the run land in the summary: one stuck run reported a fully-advanced
  committed offset, contradicting its own time series and the two repeats beside it.
- **The session timeout is shortened to 6s on both arms.** A crash test kills the process outright,
  so the dead member holds its assignment until the session expires; at Kafka's 45s default the
  restart measurement is 45 seconds of waiting with no records in it. The resume clock starts at the
  first redelivered record, not at subscribe, for the same reason.

### Where this comparison is unfair to llingr - read before quoting any of it

Everything in [the llingr arm's list above](#what-it-shares-with-the-java-arms-and-what-it-cannot)
still applies (different Kafka clients, all-distinct keys, one partition). These are additional:

- **The workload is chosen to expose one design difference and nothing else.** A stuck record that
  outlives the entire dataset is the worst case for a contiguous frontier and close to the best case
  for offset encoding. Real workloads sit somewhere between this and the clean run, and the clean run
  is the one where llingr is ahead on throughput.
- **PC's advantage is bounded below by the commit interval**, and the measurement says so: wasted
  work is roughly `commitInterval x throughput + inFlight` for PC against
  `timeSinceTheStall x throughput` for a contiguous frontier. **Crash within one commit interval of
  the stall starting and the two are equal.** The advantage is the ratio between those two times,
  not a constant.
- **Retry is a feature comparison, not a defect.** llingr dead-letters on first failure *by design*
  and commits the record anyway - `nexus.WriteDeadLetter`'s own doc says so. A user who wants retries
  writes them into their dead-letter handler. What is measured is the work that reaches that path,
  not a claim that records are lost.
- **PC's retry delay is set to 200ms, not its 1s default.** At the default the arm would be fifty
  seconds of pure waiting, and would measure the delay rather than the mechanism. llingr has no
  equivalent knob because it has no retry, so this one is not matched, and the arm records it.
- **Neither engine is tuned.** llingr's `CommitPartitionSliceLen` (pre-allocated gap tracking, min
  50, max 2,000) is left at its default 400, as is PC's `maxConcurrency` relationship to its encoder
  thresholds. Both would move under tuning.
