#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Measures what committing PAST gaps buys, against committing only the highest CONTIGUOUS offset.
#
# WHY THIS EXISTS ALONGSIDE run-bisect.sh
#
# run-bisect.sh measures steady-state throughput with every record succeeding. That workload cannot
# see the decision this library is built around, and it is also the workload that flatters a leaner
# engine - so a comparison made only there answers a question nobody is asking. PC encodes the
# INCOMPLETE OFFSET SET into commit metadata and commits past the gaps; llingr-demux holds
# out-of-order completions in memory and commits the contiguous frontier. On a clean run those are
# indistinguishable. With one stuck record, and across a crash, they are not.
#
# SAY IT PLAINLY: THIS SCENARIO IS CHOSEN BECAUSE IT FAVOURS PC. It is the mirror image of the pure
# throughput benchmark, which is chosen (by whoever quotes it) because it favours a leaner engine.
# bench/README.md lists the workloads where this comparison is unfair to the competitor, and that
# list is part of the result, not a disclaimer attached to it.
#
# THREE SCENARIOS
#
#   stuck    one record in N takes far longer than the rest. Samples the COMMITTED OFFSET from the
#            broker throughout, against records actually completed. The output is the divergence.
#   restart  the same workload, killed mid-flight with Runtime.halt (no drain, no final commit),
#            then restarted on the same group. The output is how many records are REDELIVERED -
#            wasted work, which is the number a user actually pays.
#   retry    a percentage of records fail transiently and succeed on retry. PC retries; llingr
#            dead-letters on first failure and commits anyway. The output is how much work reaches a
#            dead-letter path that did not need to.
#
# Usage:  bench/run-divergence.sh [scenarios]
#   bench/run-divergence.sh                       # all three, both engines
#   SCENARIOS="stuck" ENGINES="core" bench/run-divergence.sh
#   RECORDS=200000 STALL_MS=25000 bench/run-divergence.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/lib/broker.sh"

BOOTSTRAP=${BOOTSTRAP:-localhost:19092}
WORK=${BENCH_WORK:-$(mktemp -d)}
mkdir -p "$WORK"

RECORDS=${RECORDS:-200000}
DELAY_MS=${DELAY_MS:-2}
CONCURRENCY=${CONCURRENCY:-100}
REPEATS=${REPEATS:-3}
# Long enough to outlive the whole dataset at this delay and concurrency, which is the point: with a
# contiguous frontier the committed offset then never moves at all, and the divergence is the entire
# run. A stall shorter than the run measures a mixture of the two regimes and reads as noise.
STALL_MS=${STALL_MS:-25000}
# Halt point for the restart scenario. Deliberately mid-run, so both engines are killed with real
# work outstanding rather than at a natural boundary.
HALT_AFTER=${HALT_AFTER:-100000}
# Records for the retry scenario. Smaller, because every failure costs a retry delay.
RETRY_RECORDS=${RETRY_RECORDS:-50000}
FAIL_PERCENT=${FAIL_PERCENT:-10}
# PC's default retry delay is 1s, which would put 50 seconds of pure waiting into the arm and
# measure the delay rather than the mechanism. 200ms is stated in the results rather than hidden.
RETRY_DELAY_MS=${RETRY_DELAY_MS:-200}
# Matched on both engines, and a control rather than a setting - see the header comment in
# Divergence.java.template's engine(). Both libraries default to 5000; at that default the crash test
# measures commit LAG and not commit STRATEGY. 500 makes the lag small enough that what remains is
# the strategy. Run it at 5000 too: that is the real-world number, and it is the same on both sides.
COMMIT_INTERVAL_MS=${COMMIT_INTERVAL_MS:-500}

SCENARIOS=${SCENARIOS:-${1:-"stuck restart retry"}}
ENGINES=${ENGINES:-"core llingr"}

TOPIC=${BENCH_TOPIC:-divergence-$RECORDS}
RESULTS=$WORK/divergence.csv
LLINGR_DIR=$HERE/llingr

log() { echo "[divergence] $*" >&2; }

# --- the Java arm ---------------------------------------------------------------------------------

# Compiles Bench AND Divergence against the local build into one classes dir. Bench comes along
# because it owns the dataset producer: producing from a second copy would risk the two harnesses
# writing subtly different datasets, and every claim here depends on both engines reading the same
# bytes.
prepare_java() {
  local dir=$WORK/java
  [ -f "$dir/cp.txt" ] && { echo "$dir/classes:$(cat "$dir/cp.txt")"; return 0; }
  mkdir -p "$dir/classes" "$dir/src"
  local ver=${LOCAL_VERSION:-0.6.0.0-SNAPSHOT}
  cat > "$dir/pom.xml" <<POM
<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>
<groupId>bench</groupId><artifactId>divergence</artifactId><version>1</version>
<dependencies>
  <dependency><groupId>bz.stub.parallelconsumer</groupId><artifactId>parallel-consumer-vertx</artifactId><version>$ver</version></dependency>
  <dependency><groupId>com.github.tomakehurst</groupId><artifactId>wiremock-jre8</artifactId><version>2.35.2</version></dependency>
  <dependency><groupId>ch.qos.logback</groupId><artifactId>logback-classic</artifactId><version>1.3.14</version></dependency>
</dependencies></project>
POM
  (cd "$dir" && mvn -q -B dependency:build-classpath -Dmdep.outputFile="$dir/cp.raw" >"$dir/mvn.log" 2>&1) || {
    log "FATAL: cannot resolve the local build. Install it first:"
    log "  ./mvnw install -DskipTests -Dcopyright.skip=true"
    return 1
  }
  local cp
  cp=$(cat "$dir/cp.raw")
  # bench/conf FIRST, so logback.xml is found before anything an arm drags in. Default DEBUG logging
  # was once worth 6x on a PC arm; see bench/README.md's logging trap.
  cp="$HERE/conf:$cp"
  sed "s/__PKG__/bz.stub.parallelconsumer/" "$HERE/Bench.java.template" > "$dir/src/Bench.java"
  sed "s/__PKG__/bz.stub.parallelconsumer/" "$HERE/Divergence.java.template" > "$dir/src/Divergence.java"
  javac -nowarn -cp "$cp" -d "$dir/classes" "$dir/src/Bench.java" "$dir/src/Divergence.java" >"$dir/javac.log" 2>&1 || {
    log "FATAL: compile failed, see $dir/javac.log"; return 1
  }
  echo "$cp" > "$dir/cp.txt"
  echo "$dir/classes:$cp"
}

# --- the llingr arm -------------------------------------------------------------------------------
#
# PRIVATE RESEARCH ONLY. llingr-demux is AGPL-3.0 and patent pending; read bench/llingr/NOTICE.md
# before running this, and publish nothing it produces.
prepare_llingr() {
  command -v go >/dev/null 2>&1 || return 1
  local bin=$WORK/llingr-bench
  # GOTOOLCHAIN=auto: llingr's modules need a newer Go than most machines have, and Go fetches its own.
  (cd "$LLINGR_DIR" && GOTOOLCHAIN=auto go build -o "$bin" .) >"$WORK/llingr-build.log" 2>&1 || return 1
  echo "$bin"
}

llingr_version() { awk '/llingr-demux v/ {print "llingr-demux-" $2; exit}' "$LLINGR_DIR/go.mod"; }

# RESULT2 is key=value, so a field may be added without invalidating a parser or a stored file.
# Pulls one field out of a captured run's output.
field() { echo "$1" | tr ' ' '\n' | awk -F= -v k="$2" '$1==k {print $2; exit}'; }

# --- runners --------------------------------------------------------------------------------------

# Runs one arm, capturing stdout to a file rather than a pipe, and sets $out and $META.
# A file because there are now two machine-readable lines to pull out of one run - and because a
# pipeline hides the arm's exit status behind grep's, which is how a failed run once looked like an
# empty result rather than a failure.
out=""; META=""
capture() {
  local tag=$1; shift
  "$@" >"$WORK/$tag.out" 2>"$WORK/$tag.err"
  out=$(grep '^RESULT2' "$WORK/$tag.out")
  META=$(awk '/^METADATA/ {print $2}' "$WORK/$tag.out")
  [ -n "$out" ] || { log "  FAILED: $tag, see $WORK/$tag.err"; return 1; }
}

# The committed metadata is the evidence for the whole comparison - what each engine actually wrote
# to the broker - so it is recorded verbatim beside the numbers, not summarised as a byte count.
record_metadata() { echo "$1,$2,$3,$(field "$out" committedOffset),$META" >> "$WORK/metadata.csv"; }

row() { echo "$*" >> "$RESULTS"; }

run_stuck() {
  local engine=$1 rep=$2 group series
  group=divergence-stuck-$engine-$rep-$(date +%s)
  series=$WORK/series-stuck-$engine-$rep.csv
  if [ "$engine" = core ]; then
    capture "stuck-$engine-$rep" java -cp "$JAVA_CP" Divergence stuck \
      bootstrap="$BOOTSTRAP" topic="$TOPIC" group="$group" count="$RECORDS" delay="$DELAY_MS" \
      concurrency="$CONCURRENCY" commitIntervalMs="$COMMIT_INTERVAL_MS" \
      stall="$STALL_MS" stallEvery="$RECORDS" series="$series" || return 1
  else
    capture "stuck-$engine-$rep" "$LLINGR_BIN" -scenario stuck \
      -bootstrap "$BOOTSTRAP" -topic "$TOPIC" -group "$group" -count "$RECORDS" -delay "${DELAY_MS}ms" \
      -concurrency "$CONCURRENCY" -commit-interval "${COMMIT_INTERVAL_MS}ms" \
      -stall "${STALL_MS}ms" -stall-every "$RECORDS" -series "$series" || return 1
  fi
  log "  stuck/$engine/$rep: $out"
  record_metadata stuck "$engine" "$rep"
  row "stuck,$engine,$rep,$COMMIT_INTERVAL_MS,$(field "$out" completed),$(field "$out" committedOffset),$(field "$out" divergence),$(field "$out" maxDivergence),$(field "$out" maxCommitFreezeMs),$(field "$out" metadataBytes),,,$(field "$out" ms)"
}

# Two processes, one group: crash writes the offsets it finished and then kills the process outright;
# resume rejoins and counts how many of those come back. The redelivered count IS the measurement -
# nothing here infers it from the committed offset, precisely because for PC that inference would be
# wrong in PC's favour: the incomplete set lives in the metadata, not in the offset.
run_restart() {
  local engine=$1 rep=$2 group processed crash_out crash_meta
  group=divergence-restart-$engine-$rep-$(date +%s)
  processed=$WORK/processed-$engine-$rep.txt
  if [ "$engine" = core ]; then
    capture "crash-$engine-$rep" java -cp "$JAVA_CP" Divergence crash \
      bootstrap="$BOOTSTRAP" topic="$TOPIC" group="$group" haltAfter="$HALT_AFTER" delay="$DELAY_MS" \
      concurrency="$CONCURRENCY" commitIntervalMs="$COMMIT_INTERVAL_MS" \
      stall=600000 stallEvery="$RECORDS" logEnd="$RECORDS" processedOut="$processed" || return 1
  else
    capture "crash-$engine-$rep" "$LLINGR_BIN" -scenario crash \
      -bootstrap "$BOOTSTRAP" -topic "$TOPIC" -group "$group" -count "$RECORDS" -halt-after "$HALT_AFTER" \
      -delay "${DELAY_MS}ms" -concurrency "$CONCURRENCY" -commit-interval "${COMMIT_INTERVAL_MS}ms" \
      -stall 600000ms -stall-every "$RECORDS" -processed-out "$processed" || return 1
  fi
  log "  restart/$engine/$rep crash:  $out"
  record_metadata restart-crash "$engine" "$rep"
  crash_out=$out
  row "restart-crash,$engine,$rep,$COMMIT_INTERVAL_MS,$(field "$crash_out" completed),$(field "$crash_out" committedOffset),$(field "$crash_out" divergence),$(field "$crash_out" maxDivergence),$(field "$crash_out" maxCommitFreezeMs),$(field "$crash_out" metadataBytes),,,$(field "$crash_out" ms)"

  if [ "$engine" = core ]; then
    capture "resume-$engine-$rep" java -cp "$JAVA_CP" Divergence resume \
      bootstrap="$BOOTSTRAP" topic="$TOPIC" group="$group" delay="$DELAY_MS" \
      concurrency="$CONCURRENCY" commitIntervalMs="$COMMIT_INTERVAL_MS" processedIn="$processed" || return 1
  else
    capture "resume-$engine-$rep" "$LLINGR_BIN" -scenario resume \
      -bootstrap "$BOOTSTRAP" -topic "$TOPIC" -group "$group" -count "$RECORDS" -delay "${DELAY_MS}ms" \
      -concurrency "$CONCURRENCY" -commit-interval "${COMMIT_INTERVAL_MS}ms" -processed-in "$processed" || return 1
  fi
  log "  restart/$engine/$rep resume: $out"
  row "restart-resume,$engine,$rep,$COMMIT_INTERVAL_MS,$(field "$out" completed),$(field "$out" committedOffset),,,,,$(field "$out" redelivered),$(field "$out" delivered),$(field "$out" ms)"
  # alreadyDoneBeforeCrash is what the redelivered count has to be read against: redelivering 8,000
  # of 100,000 and redelivering 8,000 of 8,000 are opposite results.
  row "restart-wasted,$engine,$rep,$COMMIT_INTERVAL_MS,$(field "$out" alreadyDoneBeforeCrash),,,,,,$(field "$out" redelivered),,"
}

run_retry() {
  local engine=$1 rep=$2 group
  group=divergence-retry-$engine-$rep-$(date +%s)
  if [ "$engine" = core ]; then
    capture "retry-$engine-$rep" java -cp "$JAVA_CP" Divergence retry \
      bootstrap="$BOOTSTRAP" topic="$TOPIC" group="$group" count="$RETRY_RECORDS" delay="$DELAY_MS" \
      concurrency="$CONCURRENCY" commitIntervalMs="$COMMIT_INTERVAL_MS" \
      failPercent="$FAIL_PERCENT" retryDelay="$RETRY_DELAY_MS" || return 1
  else
    capture "retry-$engine-$rep" "$LLINGR_BIN" -scenario retry \
      -bootstrap "$BOOTSTRAP" -topic "$TOPIC" -group "$group" -count "$RETRY_RECORDS" -delay "${DELAY_MS}ms" \
      -concurrency "$CONCURRENCY" -commit-interval "${COMMIT_INTERVAL_MS}ms" -fail-percent "$FAIL_PERCENT" || return 1
  fi
  log "  retry/$engine/$rep: $out"
  row "retry,$engine,$rep,$COMMIT_INTERVAL_MS,$(field "$out" completed),$(field "$out" committedOffset),,,,,$(field "$out" deadLettered),$(field "$out" delivered),$(field "$out" ms)"
}

# --- go ---------------------------------------------------------------------------------------

start_broker

JAVA_CP=$(prepare_java) || exit 1

# The dataset must hold EXACTLY $RECORDS records: a resume run stops on quiescence rather than on a
# target count, so a longer log inflates every redelivery number with nothing going red. The producer
# appends, so this is checked rather than assumed - running the harness twice would otherwise double
# the log, silently.
END=$(java -cp "$JAVA_CP" Divergence logend bootstrap="$BOOTSTRAP" topic="$TOPIC" 2>/dev/null | awk '/^LOGEND/ {print $2}')
END=${END:-0}
if [ "$END" = "$RECORDS" ]; then
  log "$TOPIC already holds exactly $RECORDS records; not producing"
elif [ "$END" = 0 ]; then
  log "producing $RECORDS records into $TOPIC (once)"
  java -cp "$JAVA_CP" Bench produce "$BOOTSTRAP" "$TOPIC" "$RECORDS" >/dev/null 2>&1
else
  log "FATAL: $TOPIC holds $END records, expected $RECORDS or none."
  log "  Set BENCH_TOPIC to an unused name, or delete this topic and re-run."
  log "  (kafka-topics.sh inside $BROKER_NAME cannot reach it - the broker advertises localhost:19092,"
  log "   which is a host address, so the delete has to come from an admin client on the host.)"
  exit 1
fi

LLINGR_BIN=""
if echo "$ENGINES" | grep -q llingr; then
  LLINGR_BIN=$(prepare_llingr) || {
    log "SKIP llingr: $(command -v go >/dev/null 2>&1 && echo "build failed, see $WORK/llingr-build.log" || echo "no 'go' on PATH")"
    ENGINES=$(echo "$ENGINES" | sed 's/llingr//')
  }
  [ -n "$LLINGR_BIN" ] && log "llingr arm: $(llingr_version)"
fi

echo "scenario,engine,repeat,committed_offset,committed_metadata" > "$WORK/metadata.csv"
echo "scenario,engine,repeat,commit_interval_ms,completed,committed_offset,divergence,max_divergence,max_commit_freeze_ms,metadata_bytes,redelivered_or_dead_lettered,delivered,ms" > "$RESULTS"

for scenario in $SCENARIOS; do
  for engine in $ENGINES; do
    for rep in $(seq 1 "$REPEATS"); do
      log "$scenario / $engine / repeat $rep"
      case $scenario in
        stuck)   run_stuck   "$engine" "$rep" ;;
        restart) run_restart "$engine" "$rep" ;;
        retry)   run_retry   "$engine" "$rep" ;;
        *) log "unknown scenario $scenario"; exit 2 ;;
      esac
    done
  done
done

log "results:      $RESULTS"
log "commit metadata: $WORK/metadata.csv"
log "time series:  $WORK/series-*.csv"
cat "$RESULTS"
