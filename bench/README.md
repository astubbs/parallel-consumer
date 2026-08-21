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
BENCH_WORK=/some/dir bench/run-bisect.sh     # keep resolved classpaths between sweeps
BENCH_SKIP_PRODUCE=1 bench/run-bisect.sh     # topic already holds the dataset; don't re-produce
```

`MODES`, `DELAYS` and `CONCURRENCIES` are lists, so one invocation produces a whole comparison
table - the full cross product of the three. `MODE` and the positional `delayMs` and `concurrency`
still work and now just mean a list of one.

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
`pc_version,client_pin,mode,delay_ms,concurrency,repeat,msg_per_sec,peak_in_flight`. `delay_ms` and
`concurrency` were each added when that setting became a swept axis - without them a file sweeping
it cannot be read back. **Older files under `results/` are left alone rather than backfilled**: they
are records of what was run, and rewriting a measurement file to a newer schema invites reading a
value into it that nobody measured. `curve.csv`, `core-curve.csv` and `matched-concurrency.csv`
predate `delay_ms` and were all taken at 2ms; `delay-sweep-llingr.csv` predates `concurrency` and
was taken at 100.

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
  Nothing else differs between arms.
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
