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

start_broker() {
  if docker ps --filter "name=$BROKER_NAME" --format '{{.Names}}' | grep -q "$BROKER_NAME"; then
    log "reusing running broker $BROKER_NAME"
    return
  fi
  docker rm -f "$BROKER_NAME" >/dev/null 2>&1
  log "starting broker $BROKER_NAME"
  docker run -d --name "$BROKER_NAME" -p 19092:9092 \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:19092 \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_NUM_PARTITIONS=1 \
    apache/kafka:3.9.0 >/dev/null
  sleep 20
}

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
run_one() { java -cp "$1" Bench "${@:2}" 2>/dev/null | parse_result; }

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
llingr_version() { awk '/llingr-demux v/ {print "llingr-demux-" $2; exit}' "$LLINGR_DIR/go.mod"; }

run_llingr() {
  "$1" -bootstrap "$2" -topic "$3" -count "$4" -delay "${5}ms" -concurrency "$6" 2>/dev/null | parse_result
}

start_broker

TOPIC=${BENCH_TOPIC:-bench-$RECORDS}

# The dataset: produced once, by the local build, and reused by every arm - including the Go one,
# which is the only way an engine comparison means anything.
if [ "${BENCH_SKIP_PRODUCE:-0}" = 1 ]; then
  log "BENCH_SKIP_PRODUCE=1: assuming $TOPIC already holds >= $RECORDS records"
else
  LOCAL_CP=$(prepare LOCAL NATIVE) || { log "FATAL: cannot build local arm"; exit 1; }
  log "producing $RECORDS records into $TOPIC (once)"
  run_one "$LOCAL_CP" produce "$BOOTSTRAP" "$TOPIC" "$RECORDS" >/dev/null
fi

# delay_ms is a column because it is now a swept axis; without it a multi-delay results file cannot
# be read back. Everything else is unchanged, so llingr rows and PC rows sit in one table.
echo "pc_version,client_pin,mode,delay_ms,repeat,msg_per_sec,peak_in_flight" > "$RESULTS"
for mode in $MODES; do
  if [ "$mode" = llingr ]; then
    BIN=$(prepare_llingr) || {
      log "SKIP llingr: $(command -v go >/dev/null 2>&1 && echo "build failed, see $WORK/llingr-build.log" || echo "no 'go' on PATH - install Go to measure this arm")"
      continue
    }
    ver=$(llingr_version)
    for d in $DELAYS; do
      for r in $(seq 1 "$REPEATS"); do
        read -r rate peak <<< "$(run_llingr "$BIN" "$BOOTSTRAP" "$TOPIC" "$RECORDS" "$d" "$CONCURRENCY")"
        [ -z "$rate" ] && { rate=RUN_FAILED; peak=; }
        log "$ver llingr delay=${d}ms run$r = $rate msg/s, peak in flight $peak"
        echo "$ver,franz,llingr,$d,$r,$rate,$peak" >> "$RESULTS"
      done
    done
    continue
  fi

  for pin in $CLIENT_PINS; do
    for pcv in $PC_VERSIONS; do
      CP=$(prepare "$pcv" "$pin") || { log "SKIP $pcv/$pin (resolve or compile failed)"; echo "$pcv,$pin,$mode,,,COMPILE_FAILED" >> "$RESULTS"; continue; }
      for d in $DELAYS; do
        for r in $(seq 1 "$REPEATS"); do
          read -r rate peak <<< "$(run_one "$CP" "$mode" "$BOOTSTRAP" "$TOPIC" "$RECORDS" "$d" "$CONCURRENCY" "$BUFFER")"
          [ -z "$rate" ] && { rate=RUN_FAILED; peak=; }
          log "$pcv/$pin $mode delay=${d}ms run$r = $rate msg/s, peak in flight $peak"
          echo "$pcv,$pin,$mode,$d,$r,$rate,$peak" >> "$RESULTS"
        done
      done
    done
  done
done

log "results: $RESULTS"
cat "$RESULTS"
