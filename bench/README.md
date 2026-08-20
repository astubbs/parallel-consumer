<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Version-over-version throughput bench

Answers one question: **has Parallel Consumer's throughput changed between released versions, and
where?** It exists because the rescued 2021 demo ran roughly 30% slower than the asciinema cast the
README links, and that needed measuring rather than guessing.

The finding, the numbers and the caveats live in
[`docs/inflight/perf-throughput-regression-since-0-3.md`](../docs/inflight/perf-throughput-regression-since-0-3.md).
This file is only how to run it.

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
BENCH_WORK=/some/dir bench/run-bisect.sh     # keep resolved classpaths between sweeps
```

`LOCAL` is this checkout, resolved as an ordinary Maven coordinate, so install it first:

```sh
./mvnw install -DskipTests -Dcopyright.skip=true
```

It used to read the local build out of `target/` directories instead. That special case bought
nothing and cost a confusing failure - a worktree that had never been built failed as `package
bz.stub.parallelconsumer does not exist` - and, far worse, it resolved a **different classpath** from
every other arm. See the logging trap below.

Results land in `$BENCH_WORK/results.csv` as `pc_version,client_pin,mode,repeat,msg_per_sec`.

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
  everything environmental.

- **Logging is pinned for every arm** (`bench/conf/logback.xml`, first on the classpath). See below.

## Three traps it already encodes

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
- **Too small a dataset reads as "no difference".** At 20,000 records every version returned about
  4,200 msg/s, because consumer-group join dominated the window. The arms only separate once the
  measured period is long enough to reach steady state; 350,000 is where the recorded result was
  taken.

## Do not try to rebuild the 2021 source tree

It needs JDK 13, which has no Apple Silicon build; an x86 JDK under Rosetta would skew a throughput
measurement worse than the toolchain difference it fixes; and Jabel's 2021 ASM cannot read JDK 17
class files however many module exports are opened. Consuming the **published artifact** from Central
avoids all of it, and is more faithful anyway - it is the jar users actually got.
