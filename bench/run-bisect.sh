#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Bisects Parallel Consumer throughput across published versions, on one broker, over one dataset.
#
# WHY IT IS SHAPED THIS WAY
#
# The first version of this measurement started a Testcontainers broker and produced the dataset
# inside every measured run. That put container startup and a few hundred thousand produces into
# each data point, made a sweep unaffordable, and - worse - meant no two arms were reading the same
# bytes. Here the broker is started once and the dataset produced once; each run is a fresh consumer
# group re-reading the same topic, so the only thing that varies between data points is the
# classpath.
#
# TWO DIMENSIONS, because the first result had a confound. PC's own transitive kafka-clients moved
# from 2.5.1 to 3.9.2 across the range being bisected, so a throughput change could be either.
# Pinning kafka-clients separates them: sweep PC versions at a fixed client, and sweep clients at a
# fixed PC.
#
# DELAY IS AN AXIS, NOT A SETTING. The version bisect pinned it at 2ms because it was asking a
# question about versions. Comparing engines is a different question: at delay 0 the number is
# almost entirely per-record framework overhead, and at 100ms the sleep dominates and every engine
# converges on records*delay/concurrency. One delay therefore says nothing about an engine's shape -
# only a sweep does. Hence DELAYS, and hence delay_ms is a column in the results.
#
# CONCURRENCY IS AN AXIS TOO, for the same reason and a sharper one. The first engine comparison
# found llingr beating PC at delay 0 while holding a peak of two or three records in flight against
# PC's hundred - so PC was paying to fan out work that had no work in it. Whether a LOW concurrency
# setting beats a high one at delay 0 is therefore a question about PC, not about the other engine,
# and it cannot be asked while concurrency is a fixed positional argument. Hence CONCURRENCIES, and
# hence concurrency is a column in the results: a file swept across it cannot be read back without.
#
# THE franz ARM IS A CONTROL, and it exists because the llingr comparison had an uncontrolled
# variable in it. llingr reaches Kafka through franz-go; PC reaches it through the Java client. A
# gap between them is some mixture of engine and client and nothing in the first sweep could
# separate the two. bench/franz drives franz-go with NO engine at all - the Go-side counterpart of
# the vanilla arm - so whatever it scores is the franz-go floor, and only what llingr scores ABOVE
# that floor can be attributed to llingr.
#
# Usage:  bench/run-bisect.sh [records] [delayMs] [concurrency] [repeats]
set -uo pipefail

RECORDS=${1:-100000}
DELAY_MS=${2:-2}
CONCURRENCY=${3:-100}
REPEATS=${4:-2}
# "pc" = the Vert.x engine (an ExternalEngine); "core" = ParallelEoSStreamProcessor; "vanilla" = a
# plain KafkaConsumer. Different code paths through the control loop, so a result from one says
# nothing about the other. "llingr" is not PC at all - see the llingr arm below.
MODE=${MODE:-pc}
# Modes to sweep. Listing more than one is how a comparison table is produced in a single run, e.g.
#   MODES="core llingr" DELAYS="0 2 20 100" PC_VERSIONS=LOCAL bench/run-bisect.sh
MODES=${MODES:-$MODE}
# Delays to sweep, in milliseconds. Defaults to the single value given positionally, so the old
# invocation still means exactly what it used to.
DELAYS=${DELAYS:-$DELAY_MS}
# Concurrencies to sweep. Same contract as DELAYS: defaults to the positional value, so a caller who
# never heard of this variable gets the behaviour it always had. Meaningless for the vanilla arm,
# which is single-threaded by construction - it will simply repeat the same measurement once per
# value, and each row still records what was asked for.
CONCURRENCIES=${CONCURRENCIES:-$CONCURRENCY}
BUFFER=${BUFFER:-0}

BROKER_NAME=pc-bench-broker
BOOTSTRAP=localhost:19092
WORK=${BENCH_WORK:-$(mktemp -d)}
# mktemp -d creates its directory; a caller-supplied BENCH_WORK is just a path, and every write
# below assumed otherwise - results.csv and the Go build log both failed on a first-run directory.
mkdir -p "$WORK"
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
RESULTS=$WORK/results.csv

# Versions to sweep. All published ones use the io.confluent package; the fork's local build is the
# only bz.stub one, and it is named LOCAL.
PC_VERSIONS=${PC_VERSIONS:-"0.3.0.2 0.3.1.0 0.3.2.0 0.4.0.0 0.4.0.1 0.5.0.0 0.5.1.0 0.5.2.0 0.5.2.8 0.5.3.2 LOCAL"}
# Client pins for the second dimension. EMPTY means "whatever PC drags in", which is what the
# original confound was.
CLIENT_PINS=${CLIENT_PINS:-"NATIVE 3.9.2"}

log() { echo "[bisect] $*" >&2; }

# The broker lives in bench/lib/broker.sh, shared with run-divergence.sh: two harnesses quietly
# agreeing on a DIFFERENT partition count would produce numbers that look comparable and are not.
. "$HERE/lib/broker.sh"

# Resolves a classpath for one (pcVersion, clientPin) pair and compiles the harness against it.
# Echoes "<classesDir>:<classpath>" on success, nothing on failure.
prepare() {
  local pcv=$1 pin=$2 tag="$1-$2"
  local dir=$WORK/$tag
  [ -f "$dir/cp.txt" ] && { echo "$dir/classes:$(cat "$dir/cp.txt")"; return 0; }
  mkdir -p "$dir/classes" "$dir/src"

  # Every arm is a Maven coordinate, including this checkout's own build - install it with
  #   ./mvnw install -DskipTests -Dcopyright.skip=true
  # and it resolves like any other version. Reading the local build out of target/ directories
  # instead was a special case that bought nothing and cost a confusing failure: a worktree that
  # had never been built failed as "package bz.stub.parallelconsumer does not exist".
  local pkg cp gid aid ver
  if [ "$pcv" = "LOCAL" ]; then
    pkg=bz.stub.parallelconsumer; gid=bz.stub.parallelconsumer; ver=${LOCAL_VERSION:-0.6.0.0-SNAPSHOT}
  else
    pkg=io.confluent.parallelconsumer; gid=io.confluent.parallelconsumer; ver=$pcv
  fi
  aid=parallel-consumer-vertx
  local pinblock=""
  [ "$pin" != "NATIVE" ] && pinblock="<dependency><groupId>org.apache.kafka</groupId><artifactId>kafka-clients</artifactId><version>$pin</version></dependency>"
  cat > "$dir/pom.xml" <<POM
<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>
<groupId>bench</groupId><artifactId>arm-$pcv</artifactId><version>1</version>
<dependencies>
  $pinblock
  <dependency><groupId>$gid</groupId><artifactId>$aid</artifactId><version>$ver</version></dependency>
  <dependency><groupId>com.github.tomakehurst</groupId><artifactId>wiremock-jre8</artifactId><version>2.35.2</version></dependency>
  <dependency><groupId>ch.qos.logback</groupId><artifactId>logback-classic</artifactId><version>1.3.14</version></dependency>
</dependencies></project>
POM
  (cd "$dir" && mvn -q -B dependency:build-classpath -Dmdep.outputFile="$dir/cp.raw" >/dev/null 2>&1) || return 1
  cp=$(cat "$dir/cp.raw")

  # Jabel ships as a transitive of older PC releases and javac auto-loads it as a compiler plugin
  # via ServiceLoader, where its 2021 ASM cannot read modern class files. It is compile-time-only
  # for PC's own build and nothing here needs it.
  cp=$(python3 -c "import sys;print(':'.join(p for p in sys.argv[1].split(':') if 'jabel' not in p.lower()))" "$cp")

  # bench/conf goes FIRST on every runtime classpath, so logback.xml is found before anything an
  # arm might drag in. See bench/conf/logback.xml for what this is protecting against.
  cp="$HERE/conf:$cp"
  sed "s/__PKG__/$pkg/" "$HERE/Bench.java.template" > "$dir/src/Bench.java"
  javac -nowarn -cp "$cp" -d "$dir/classes" "$dir/src/Bench.java" >"$dir/javac.log" 2>&1 || return 1
  echo "$cp" > "$dir/cp.txt"
  echo "$dir/classes:$cp"
}

# RESULT <mode> <count> <ms> <msgPerSec> peak=<n>  ->  "<msgPerSec> <n>"
# One parser for every arm, which is the point of making the Go arm print the identical line.
parse_result() { grep '^RESULT' | awk '{p=$6; sub("peak=","",p); print $5, p}'; }
# BENCH_JFR=<dir> records a Java Flight Recorder profile of each measured run into that directory,
# one .jfr per arm. Off by default because recording is not free and every result in bench/results/
# was taken without it - a profiled run and an unprofiled one are not comparable and must not be put
# in the same table.
#
# JFR rather than the profiler the README credits (YourKit): it ships with the JDK, needs no install
# and no agent path, and answers "which methods and which locks" well enough to point at a suspect.
# Reach for YourKit when the suspect needs confirming - allocation attribution and lock-contention
# detail are where it is materially better.
run_one() {
  local cp=$1; shift
  local jfr=()
  if [ -n "${BENCH_JFR:-}" ]; then
    mkdir -p "$BENCH_JFR"
    # stackdepth well above the default 64: the default truncates the lock-acquisition stacks with
    # "..." exactly where the interesting frame is, which makes contention unattributable.
    jfr=(-XX:FlightRecorderOptions=stackdepth=256
         -XX:StartFlightRecording=settings=profile,filename="$BENCH_JFR/$(echo "$*" | tr " /" "__").jfr,dumponexit=true")
  fi
  # ${jfr[@]+"${jfr[@]}"} rather than "${jfr[@]}": macOS ships bash 3.2, where expanding an EMPTY
  # array under `set -u` is an unbound-variable error. Written the obvious way, this made every
  # unprofiled run fail - silently, as RUN_FAILED rows - while the profiled path worked fine.
  java ${jfr[@]+"${jfr[@]}"} -cp "$cp" Bench "$@" 2>/dev/null | parse_result
}

# --- the llingr arm --------------------------------------------------------------------------
#
# PRIVATE RESEARCH ONLY. llingr-demux is AGPL-3.0 and patent pending; read bench/llingr/NOTICE.md
# before running this, and publish nothing it produces.
#
# It is an external reference point, not another PC version, so the PC_VERSIONS and CLIENT_PINS
# dimensions do not apply to it - a Go engine has neither. It reuses everything that makes the
# comparison honest: the same broker, the same topic, the same bytes, a fresh consumer group per
# run, and the same result line.
LLINGR_DIR=$HERE/llingr

# Builds the Go arm to $WORK, deliberately outside the repo: the binary links AGPL code and must
# not be distributable from a checkout. Returns non-zero, quietly, if Go is missing - a machine
# without Go should skip this arm, not fail the sweep it was in the middle of.
prepare_llingr() {
  command -v go >/dev/null 2>&1 || return 1
  local bin=$WORK/llingr-bench
  [ -x "$bin" ] && { echo "$bin"; return 0; }
  # GOTOOLCHAIN=auto because llingr's modules require a newer Go than most machines have installed,
  # and Go can fetch its own toolchain; without it the build fails with a bare version complaint.
  (cd "$LLINGR_DIR" && GOTOOLCHAIN=auto go build -o "$bin" .) >"$WORK/llingr-build.log" 2>&1 || return 1
  echo "$bin"
}

# The engine version goes in the pc_version column, so a results file records WHICH llingr was
# measured. Read from go.mod rather than hardcoded, so a dependency bump cannot silently mislabel.
llingr_version() { version_field "llingr-demux v" "llingr-demux-" "$LLINGR_DIR/go.mod"; }


# --- the franz control arm -------------------------------------------------------------------
#
# NOT private research and NOT AGPL: franz-go is BSD-3-Clause, this directory links no llingr code,
# and nothing it measures is subject to the llingr publication decision. It is still its own Go
# module, kept out of parallel-consumer-proxy-client-go, because bench code is not product and a
# shipped artifact should not acquire a benchmark's dependencies.
#
# It is the Go-side vanilla arm. The llingr comparison confounds engine with client - PC on the Java
# client versus llingr on franz-go - and CLIENT_PINS, which exists to separate exactly that on the
# Java side, has no counterpart across languages. This arm supplies one: franz-go, a fixed worker
# pool, a sleep, a counter, no engine.
FRANZ_DIR=$HERE/franz

prepare_franz() {
  command -v go >/dev/null 2>&1 || return 1
  local bin=$WORK/franz-bench
  [ -x "$bin" ] && { echo "$bin"; return 0; }
  # GOTOOLCHAIN=auto for the same reason the llingr arm needs it - the module's Go directive can
  # outrun the installed toolchain, and Go will fetch its own rather than failing the sweep.
  (cd "$FRANZ_DIR" && GOTOOLCHAIN=auto go build -o "$bin" .) >"$WORK/franz-build.log" 2>&1 || return 1
  echo "$bin"
}

# Read from go.mod, never hardcoded, so a client bump cannot silently mislabel a results file - the
# same rule llingr_version follows, and for the same reason. It picks the field that LOOKS like a
# version rather than a fixed column, because go.mod writes "require <path> <ver>" on one line and
# "<path> <ver>" inside a require block, and the first version of this printed the module path into
# every row of a results file - wrong, and wrong in a way nothing would have caught later.
version_field() { awk -v pat="$1" -v prefix="$2" '$0 ~ pat { for (i = 1; i <= NF; i++) if ($i ~ /^v[0-9]/) { print prefix $i; exit } }' "$3"; }
franz_version() { version_field "twmb/franz-go v" "franz-go-" "$FRANZ_DIR/go.mod"; }

# Both Go arms take the same flags and print the same RESULT line, so there is one runner and one
# dispatch pair rather than two near-identical copies of each.
run_go_arm() {
  "$1" -bootstrap "$2" -topic "$3" -count "$4" -delay "${5}ms" -concurrency "$6" 2>/dev/null | parse_result
}
prepare_go_arm() { case $1 in llingr) prepare_llingr ;; franz) prepare_franz ;; esac; }
go_arm_version() { case $1 in llingr) llingr_version ;; franz) franz_version ;; esac; }

start_broker

# Partition count for the dataset. An axis, not a constant: at maxConcurrency 5,000 a single
# partition bounded the measurement before concurrency did - see Bench.java.template#partitions().
# It is in the default topic name so a 1-partition and a 10-partition dataset can never be mistaken
# for each other, which is exactly the confound this file exists to avoid.
# Ordering mode. An axis for the same reason partition count is: it changes the shape of the shard
# map, which is what the dispatch scan walks. Recorded as a column so a swept file is readable back.
ORDERING=${BENCH_ORDERING:-UNORDERED}
export BENCH_ORDERING=$ORDERING
PARTITIONS=${BENCH_PARTITIONS:-1}
export BENCH_PARTITIONS=$PARTITIONS
TOPIC=${BENCH_TOPIC:-bench-$RECORDS-p$PARTITIONS}

# The dataset: produced once, by the local build, and reused by every arm - including the Go one,
# which is the only way an engine comparison means anything.
if [ "${BENCH_SKIP_PRODUCE:-0}" = 1 ]; then
  log "BENCH_SKIP_PRODUCE=1: assuming $TOPIC already holds >= $RECORDS records"
else
  LOCAL_CP=$(prepare LOCAL NATIVE) || { log "FATAL: cannot build local arm"; exit 1; }
  log "producing $RECORDS records into $TOPIC (once)"
  run_one "$LOCAL_CP" produce "$BOOTSTRAP" "$TOPIC" "$RECORDS" >/dev/null
fi

# delay_ms and concurrency are columns because both are now swept axes; without them a multi-delay
# or multi-concurrency results file cannot be read back. Everything else is unchanged, so llingr,
# franz and PC rows all sit in one table.
echo "pc_version,client_pin,mode,ordering,partitions,delay_ms,concurrency,repeat,msg_per_sec,peak_in_flight" > "$RESULTS"
for mode in $MODES; do
  # The two Go arms differ only in which binary they build and what they call themselves, so they
  # share one branch rather than two near-identical copies of the sweep loop.
  if [ "$mode" = llingr ] || [ "$mode" = franz ]; then
    BIN=$(prepare_go_arm "$mode") || {
      log "SKIP $mode: $(command -v go >/dev/null 2>&1 && echo "build failed, see $WORK/$mode-build.log" || echo "no 'go' on PATH - install Go to measure this arm")"
      continue
    }
    ver=$(go_arm_version "$mode")
    for c in $CONCURRENCIES; do
      for d in $DELAYS; do
        for r in $(seq 1 "$REPEATS"); do
          read -r rate peak <<< "$(run_go_arm "$BIN" "$BOOTSTRAP" "$TOPIC" "$RECORDS" "$d" "$c")"
          [ -z "$rate" ] && { rate=RUN_FAILED; peak=; }
          log "$ver $mode delay=${d}ms conc=$c run$r = $rate msg/s, peak in flight $peak"
          echo "$ver,franz,$mode,$ORDERING,$PARTITIONS,$d,$c,$r,$rate,$peak" >> "$RESULTS"
        done
      done
    done
    continue
  fi

  for pin in $CLIENT_PINS; do
    for pcv in $PC_VERSIONS; do
      CP=$(prepare "$pcv" "$pin") || { log "SKIP $pcv/$pin (resolve or compile failed)"; echo "$pcv,$pin,$mode,$ORDERING,$PARTITIONS,,,,COMPILE_FAILED" >> "$RESULTS"; continue; }
      for c in $CONCURRENCIES; do
        for d in $DELAYS; do
          for r in $(seq 1 "$REPEATS"); do
            read -r rate peak <<< "$(run_one "$CP" "$mode" "$BOOTSTRAP" "$TOPIC" "$RECORDS" "$d" "$c" "$BUFFER")"
            [ -z "$rate" ] && { rate=RUN_FAILED; peak=; }
            log "$pcv/$pin $mode delay=${d}ms conc=$c run$r = $rate msg/s, peak in flight $peak"
            echo "$pcv,$pin,$mode,$ORDERING,$PARTITIONS,$d,$c,$r,$rate,$peak" >> "$RESULTS"
          done
        done
      done
    done
  done
done

log "results: $RESULTS"
cat "$RESULTS"
