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

`LOCAL` means this checkout's built classes, so run a build first:

```sh
./mvnw -q -pl parallel-consumer-vertx -am -DskipTests -Dcopyright.skip=true test-compile
```

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

## Two traps it already encodes

- **Jabel must be stripped from the compile classpath.** It ships as a transitive of older PC
  releases and javac auto-loads it as a compiler plugin via `ServiceLoader`; its 2021 ASM then dies
  on modern class files with `Unsupported class file major version 61`.
- **Too small a dataset reads as "no difference".** At 20,000 records every version returned about
  4,200 msg/s, because consumer-group join dominated the window. The arms only separate once the
  measured period is long enough to reach steady state; 350,000 is where the recorded result was
  taken.

## Do not try to rebuild the 2021 source tree

It needs JDK 13, which has no Apple Silicon build; an x86 JDK under Rosetta would skew a throughput
measurement worse than the toolchain difference it fixes; and Jabel's 2021 ASM cannot read JDK 17
class files however many module exports are opened. Consuming the **published artifact** from Central
avoids all of it, and is more faithful anyway - it is the jar users actually got.
