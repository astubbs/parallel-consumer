#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# KPipe's ParallelProcessingBenchmark with Parallel Consumer 0.5.3.3 at maxConcurrency 2000 (the ask)
# and at 100 (the control, KPipe's published setting), same box, same JVM, same arms and cells.
set -uo pipefail
S=/tmp/claude-1000/-home-astubbs-git-parallel-consumer/bb3a3e69-8cd3-470b-b47c-d853c068d7a2/scratchpad
export JAVA_HOME=/home/astubbs/.local/share/mise/installs/java/graalvm-community-25.0.2
export PATH="$JAVA_HOME/bin:$PATH"
cd "$S/kpipe" || exit 1
SRC=benchmarks/src/jmh/java/io/github/eschizoid/kpipe/benchmarks/ParallelProcessingBenchmarkInfrastructure.java
STATUS="$S/bench-status.txt"
echo "$(date -Is) start java=$(java -version 2>&1 | head -1)" >> "$STATUS"
for C in 2000 100; do
  sed -i -E "s/(static final int CONFLUENT_MAX_CONCURRENCY = )[0-9]+;/\1$C;/" "$SRC"
  grep -n 'CONFLUENT_MAX_CONCURRENCY =' "$SRC" >> "$STATUS"
  rm -f benchmarks/build/tmp/jmh/jmh.lock
  if ! ./gradlew -q :benchmarks:jmhJar > "$S/bench-build-$C.log" 2>&1; then
    echo "$(date -Is) BUILD FAILED for C=$C - see bench-build-$C.log" >> "$STATUS"; continue
  fi
  JAR=$(find benchmarks/build/libs -name '*-jmh.jar' | head -1)
  cp "$JAR" "$S/kpipe-jmh-pc$C.jar"
  echo "$(date -Is) built C=$C -> $S/kpipe-jmh-pc$C.jar" >> "$STATUS"
  java -jar "$S/kpipe-jmh-pc$C.jar" \
    'ParallelProcessingBenchmark\.(kpipe|confluent|kpipeKeyOrdered|confluentKey)$' \
    -p workMicros=10000,100000 -f 2 -wi 2 -i 3 -foe true \
    -rf json -rff "$S/bench-results-pc$C.json" > "$S/bench-run-$C.log" 2>&1
  echo "$(date -Is) run C=$C exit=$? -> bench-results-pc$C.json" >> "$STATUS"
done
sed -i -E "s/(static final int CONFLUENT_MAX_CONCURRENCY = )[0-9]+;/\1100;/" "$SRC"
echo "$(date -Is) DONE" >> "$STATUS"
