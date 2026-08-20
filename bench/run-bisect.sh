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
# Usage:  bench/run-bisect.sh [records] [delayMs] [concurrency] [repeats]
set -uo pipefail

RECORDS=${1:-100000}
DELAY_MS=${2:-2}
CONCURRENCY=${3:-100}
REPEATS=${4:-2}

BROKER_NAME=pc-bench-broker
BOOTSTRAP=localhost:19092
WORK=${BENCH_WORK:-$(mktemp -d)}
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

  local pkg cp
  if [ "$pcv" = "LOCAL" ]; then
    pkg=bz.stub.parallelconsumer
    cp="$REPO/parallel-consumer-vertx/target/classes:$REPO/parallel-consumer-core/target/classes:$(cat "$WORK/local-deps.txt")"
  else
    pkg=io.confluent.parallelconsumer
    local pinblock=""
    [ "$pin" != "NATIVE" ] && pinblock="<dependency><groupId>org.apache.kafka</groupId><artifactId>kafka-clients</artifactId><version>$pin</version></dependency>"
    cat > "$dir/pom.xml" <<POM
<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>
<groupId>bench</groupId><artifactId>arm-$pcv</artifactId><version>1</version>
<dependencies>
  $pinblock
  <dependency><groupId>io.confluent.parallelconsumer</groupId><artifactId>parallel-consumer-vertx</artifactId><version>$pcv</version></dependency>
  <dependency><groupId>com.github.tomakehurst</groupId><artifactId>wiremock-jre8</artifactId><version>2.35.2</version></dependency>
  <dependency><groupId>ch.qos.logback</groupId><artifactId>logback-classic</artifactId><version>1.3.14</version></dependency>
</dependencies></project>
POM
    (cd "$dir" && mvn -q -B dependency:build-classpath -Dmdep.outputFile="$dir/cp.raw" >/dev/null 2>&1) || return 1
    cp=$(cat "$dir/cp.raw")
  fi

  # Jabel ships as a transitive of older PC releases and javac auto-loads it as a compiler plugin
  # via ServiceLoader, where its 2021 ASM cannot read modern class files. It is compile-time-only
  # for PC's own build and nothing here needs it.
  cp=$(python3 -c "import sys;print(':'.join(p for p in sys.argv[1].split(':') if 'jabel' not in p.lower()))" "$cp")

  sed "s/__PKG__/$pkg/" "$HERE/Bench.java.template" > "$dir/src/Bench.java"
  javac -nowarn -cp "$cp" -d "$dir/classes" "$dir/src/Bench.java" >"$dir/javac.log" 2>&1 || return 1
  echo "$cp" > "$dir/cp.txt"
  echo "$dir/classes:$cp"
}

# RESULT <mode> <count> <ms> <msgPerSec> peak=<n>  ->  "<msgPerSec> <n>"
run_one() { java -cp "$1" Bench "${@:2}" 2>/dev/null | grep '^RESULT' | awk '{p=$6; sub("peak=","",p); print $5, p}'; }

start_broker

# The dataset: produced once, by the local build, and reused by every arm.
log "resolving local dependency classpath"
(cd "$REPO" && ./mvnw -q -o -pl parallel-consumer-vertx dependency:build-classpath \
   -Dmdep.outputFile="$WORK/local-deps.txt" -Dmdep.includeScope=test -Dcopyright.skip=true >/dev/null 2>&1)
LOCAL_CP=$(prepare LOCAL NATIVE) || { log "FATAL: cannot build local arm"; exit 1; }
TOPIC=${BENCH_TOPIC:-bench-$RECORDS}
log "producing $RECORDS records into $TOPIC (once)"
run_one "$LOCAL_CP" produce "$BOOTSTRAP" "$TOPIC" "$RECORDS" >/dev/null

echo "pc_version,client_pin,mode,repeat,msg_per_sec,peak_in_flight" > "$RESULTS"
for pin in $CLIENT_PINS; do
  for pcv in $PC_VERSIONS; do
    [ "$pcv" = "LOCAL" ] && [ "$pin" != "NATIVE" ] && continue   # local arm is not re-pinnable here
    CP=$(prepare "$pcv" "$pin") || { log "SKIP $pcv/$pin (resolve or compile failed)"; echo "$pcv,$pin,pc,,COMPILE_FAILED" >> "$RESULTS"; continue; }
    for r in $(seq 1 "$REPEATS"); do
      read -r rate peak <<< "$(run_one "$CP" pc "$BOOTSTRAP" "$TOPIC" "$RECORDS" "$DELAY_MS" "$CONCURRENCY")"
      [ -z "$rate" ] && { rate=RUN_FAILED; peak=; }
      log "$pcv/$pin pc run$r = $rate msg/s, peak in flight $peak"
      echo "$pcv,$pin,pc,$r,$rate,$peak" >> "$RESULTS"
    done
  done
done

log "results: $RESULTS"
cat "$RESULTS"
