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
#
# THE OTHER ExternalEngines ARE ARMS TOO, as of 2026-08-21: "reactor", "mutiny" and "proxy". Until
# then the project shipped four engines and compared one, and every cross-engine claim rested on
# Vert.x plus the ASSUMPTION that a shared ExternalEngine superclass makes the family behave alike.
# The proxy arm is the one that had to exist: astubbs#242's language proxies reach PC through
# ProxyProcessor and through nothing else, so its ceiling is the ceiling of every non-JVM client.
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
# Seconds the one-off produce stage may take before the sweep gives up. It talks to a broker that
# may be wedged, and it prints nothing on success, so an unbounded produce is indistinguishable from
# a finished one. Raise it for a dataset much larger than a few hundred thousand records.
PRODUCE_TIMEOUT=${BENCH_PRODUCE_TIMEOUT:-30}
# Seconds any single measured run may take. One minute is ample for every arm at every operating
# point this harness sweeps - at 100,000 records the slowest legitimate combination is a couple of
# seconds of work plus JVM start and a consumer-group join - so anything that reaches this limit is
# not slow, it is not going to finish. A run that times out is recorded as RUN_TIMEOUT rather than
# RUN_FAILED, because the two mean different things: a timeout at this cap is an arm that cannot
# complete the workload, a failure is a broken arm.
RUN_TIMEOUT=${BENCH_RUN_TIMEOUT:-60}

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

# --- the engine arm table -------------------------------------------------------------------------
#
# ONE TABLE, read by two things: which arm source to compile, and which Maven artifact that arm needs
# on top of the base. Bench itself names no engine class - it resolves "<mode>" to "<Mode>Arm" by
# reflection - because Bench.java.template must still compile against 0.3.0.2, and Mutiny and the
# proxy exist in NO published release. An import of them in the shared template would not add an arm,
# it would delete the version bisect.
#
# A mode with no entry here needs no arm: vanilla, pool and core are implemented in Bench itself
# because they use no engine module. A mode whose artifact does not exist at the version being swept
# fails to resolve and is recorded as COMPILE_FAILED, which is the honest outcome - reactor exists in
# published releases, mutiny and proxy do not.
arm_class() {
  case $1 in
    pc|vertx) echo VertxArm ;;
    reactor)  echo ReactorArm ;;
    mutiny)   echo MutinyArm ;;
    proxy)    echo ProxyArm ;;
  esac
}
# The base pom already carries parallel-consumer-vertx (it supplies both the Vert.x engine and the
# vertx-core the async stub is built on), so the Vert.x arm needs nothing extra.
arm_artifact() {
  case $1 in
    reactor) echo parallel-consumer-reactor ;;
    mutiny)  echo parallel-consumer-mutiny ;;
    proxy)   echo parallel-consumer-proxy ;;
  esac
}

# THE ARM SET IS PER MODE, NOT PER SWEEP, and that is a correctness property rather than tidiness.
# Deriving it once from MODES put every swept mode's artifact into ONE generated pom, so adding
# "mutiny" to a version bisect made that pom demand parallel-consumer-mutiny at 0.5.3.2 - which does
# not exist - and resolution then failed for the WHOLE arm directory. Every mode in the sweep, core
# included, reported COMPILE_FAILED at every published version: one unavailable engine deleting ten
# other arms' rows. Keyed per mode, an absent artifact fails only the mode that asked for it.

# The broker lives in bench/lib/broker.sh, shared with run-divergence.sh: two harnesses quietly
# agreeing on a DIFFERENT partition count would produce numbers that look comparable and are not.
. "$HERE/lib/broker.sh"

# Resolves a classpath for one (pcVersion, clientPin) pair and compiles the harness against it.
# Echoes "<classesDir>:<classpath>" on success, nothing on failure.
#
# ONLY THE MAVEN RESOLUTION IS CACHED. The sources are regenerated and recompiled every time, and
# that is not belt-and-braces: caching the COMPILED CLASSES silently published wrong numbers on
# 2026-08-21. A BENCH_WORK directory left over from an earlier session still had cp.txt in it, so
# this function returned before generating anything, and an entire sweep ran a build of the harness
# from hours earlier. The old build had no async stub and no arm dispatch, so BENCH_ASYNC_STUB was
# ignored and three brand-new engine modes all fell through that template's `else` branch into the
# VERT.X arm - four "different engines" that were one engine, at a third of the throughput the same
# operating point had just produced, with nothing anywhere saying so. It was caught only because the
# Vert.x control disagreed with a committed figure.
#
# javac on four small files is a second; `dependency:build-classpath` across eleven versions is
# minutes. Cache the slow half, never the half that decides what actually runs.
prepare() {
  local pcv=$1 pin=$2 armmode=${3:-core}
  local arm; arm=$(arm_class "$armmode")
  # The arm is part of the cache key because it is part of what gets compiled and resolved.
  local dir=$WORK/$pcv-$pin-${arm:-noarm}
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
  # The engine modules the arms in this sweep need, at the same version as the base artifact - so a
  # bisect that reaches a release without one of them fails to RESOLVE and is recorded, rather than
  # silently measuring today's engine against yesterday's core.
  local armblock="" extra
  extra=$(arm_artifact "$armmode")
  [ -n "$extra" ] && armblock="<dependency><groupId>$gid</groupId><artifactId>$extra</artifactId><version>$ver</version></dependency>"
  cat > "$dir/pom.xml" <<POM
<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>
<groupId>bench</groupId><artifactId>arm-$pcv</artifactId><version>1</version>
<dependencies>
  $pinblock
  $armblock
  <dependency><groupId>$gid</groupId><artifactId>$aid</artifactId><version>$ver</version></dependency>
  <dependency><groupId>com.github.tomakehurst</groupId><artifactId>wiremock-jre8</artifactId><version>2.35.2</version></dependency>
  <dependency><groupId>ch.qos.logback</groupId><artifactId>logback-classic</artifactId><version>1.3.14</version></dependency>
</dependencies></project>
POM
  # The cached half: resolution is minutes across a full sweep and depends only on the generated pom,
  # which is regenerated above - so a changed arm set invalidates it by changing the pom's checksum.
  if [ -f "$dir/cp.raw" ] && [ -f "$dir/pom.sum" ] && [ "$(cksum < "$dir/pom.xml")" = "$(cat "$dir/pom.sum")" ]; then
    cp=$(cat "$dir/cp.raw")
  else
    (cd "$dir" && mvn -q -B dependency:build-classpath -Dmdep.outputFile="$dir/cp.raw" >/dev/null 2>&1) || return 1
    cksum < "$dir/pom.xml" > "$dir/pom.sum"
    cp=$(cat "$dir/cp.raw")
  fi

  # Jabel ships as a transitive of older PC releases and javac auto-loads it as a compiler plugin
  # via ServiceLoader, where its 2021 ASM cannot read modern class files. It is compile-time-only
  # for PC's own build and nothing here needs it.
  cp=$(python3 -c "import sys;print(':'.join(p for p in sys.argv[1].split(':') if 'jabel' not in p.lower()))" "$cp")

  # bench/conf goes FIRST on every runtime classpath, so logback.xml is found before anything an
  # arm might drag in. See bench/conf/logback.xml for what this is protecting against.
  cp="$HERE/conf:$cp"
  # The shared harness, its arm interface, and ONLY the arms this sweep needs. Compiling every arm
  # unconditionally would put the Mutiny and proxy types on the compile path of a bisect that is not
  # measuring them, and neither exists in any published release - so the sweep would fail at the arm
  # rather than at the version, which is a confusing way to say "not published".
  #
  # Wipe first. A .class left behind by a previous sweep's arm set is still resolvable by name, and
  # Bench finds its arms BY NAME - so a stale ProxyArm.class would run happily against a template
  # that no longer exists. That is the same failure the caching comment above describes, one level
  # down, and it costs nothing to make impossible.
  rm -f "$dir/classes"/*.class "$dir/src"/*.java
  sed "s/__PKG__/$pkg/" "$HERE/Bench.java.template" > "$dir/src/Bench.java"
  sed "s/__PKG__/$pkg/" "$HERE/BenchArm.java.template" > "$dir/src/BenchArm.java"
  [ -n "$arm" ] && sed "s/__PKG__/$pkg/" "$HERE/arms/$arm.java.template" > "$dir/src/$arm.java"
  javac -nowarn -cp "$cp" -d "$dir/classes" "$dir/src"/*.java >"$dir/javac.log" 2>&1 || return 1
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
#
# THE `-dp` SUFFIX SELECTS THE DIRECT-PULL ENGINE, and it is a mode suffix rather than a new Bench
# argument for one reason: Bench.java.template is compiled against EVERY released version in the
# sweep, and none of them has the option. Anything added to the template has to compile against
# 0.3.0.2 as well as this checkout. So `core-dp` runs the ordinary `core` harness path and passes the
# engine selection as a JVM system property, which old versions simply ignore.
#
# It stays a distinct MODE, rather than an environment variable set around the whole sweep, because
# the results file has to be able to tell the two arms apart - and because it lets one invocation
# alternate them, which is the only defence this harness has against machine drift between arms.
# THE SERIAL ARMS, and why the sweep has to know which they are.
#
# `vanilla` (a plain KafkaConsumer) and `franz` (franz-go with no engine) process ONE record at a
# time. They are floors, not engines, and that is the whole point of them - but it means their
# runtime is `records x delay` and CONCURRENCY DOES NOTHING. At 100,000 records and a 100ms delay
# that is 10,000 seconds, nearly three hours, for a single repeat.
#
# Left to itself the sweep simply sits there. Nothing is wrong, nothing is logged, and the last line
# on screen is whatever the previous stage printed - which is exactly how two sweeps were abandoned
# on the belief that the produce step had wedged, when the process actually running was `Bench
# vanilla` doing precisely what it was asked to.
#
# So the arm is SKIPPED, with its projection recorded in the results file rather than merely logged,
# because a reader comparing engines needs to see that the floor was not measured at this operating
# point and why. Their result there is arithmetic anyway - a serial arm at delay d converges on
# 1000/d msg/s and holds one record in flight.
is_serial_arm() { case $1 in vanilla|franz) return 0 ;; *) return 1 ;; esac; }

# Seconds a serial arm may be PROJECTED to take before it is skipped rather than run.
#
# Defaults to RUN_TIMEOUT so the two cannot disagree. They answer different questions - this one is a
# decision not to spend the time, RUN_TIMEOUT is a backstop against a run that will never finish -
# but a projection cap ABOVE the run cap would be incoherent: an arm projected at 100s would pass
# this check and then be killed at 60s anyway, recorded as a timeout rather than as the deliberate
# skip it should have been.
SERIAL_ARM_MAX_SECONDS=${BENCH_SERIAL_ARM_MAX_SECONDS:-$RUN_TIMEOUT}

serial_projection_seconds() { echo $(( RECORDS * $1 / 1000 )); }

# BOUNDED EXECUTION, because a stock macOS has neither `timeout` nor `gtimeout` and this script has
# to run on both platforms. Backgrounds the command, polls, and escalates TERM then KILL.
#
# WHY IT EXISTS. Two stages of this harness can run effectively forever and neither announces that it
# is doing so:
#
#   * A SERIAL ARM AT A HIGH DELAY. `vanilla` and `franz` have no engine and process one record at a
#     time, so their runtime is records x delay REGARDLESS of concurrency. 100,000 records at 100ms is
#     10,000 seconds - nearly three hours, for one repeat, and the sweep just sits there. That is not
#     a hang to be diagnosed, it is arithmetic, and it killed two sweep attempts before anyone worked
#     out the process everyone was watching was a measurement rather than the produce step above it.
#   * THE PRODUCE STAGE, which talks to a broker that may be wedged.
#
# Returns 124 on expiry, the same code GNU `timeout` uses, so a caller can tell a timeout from a
# failure.
run_with_deadline() {
  local secs=$1; shift
  "$@" &
  local pid=$! waited=0
  while kill -0 "$pid" 2>/dev/null; do
    if [ "$waited" -ge "$secs" ]; then
      kill -TERM "$pid" 2>/dev/null
      sleep 2
      kill -KILL "$pid" 2>/dev/null
      # the JVM is a grandchild of this shell, so killing the subshell is not enough
      pkill -KILL -P "$pid" 2>/dev/null
      return 124
    fi
    sleep 1
    waited=$((waited + 1))
  done
  wait "$pid"
}

run_one() {
  local cp=$1; shift
  local engine=()
  if [ "${1:-}" = "core-dp" ]; then
    engine=(-Dpc.directPull=true)
    shift
    set -- core "$@"
  fi
  # core-vt: same mode-suffix trick as core-dp, and it exists for the same reason - the template is
  # compiled against every released version in the sweep and none of them has the option, so the
  # selection has to be a system property old versions ignore.
  #
  # RUN BOTH ARMS ON THE SAME JVM. A JDK 21+ java must be on PATH for this mode to do anything, and
  # running the platform arm on 17 while the virtual arm ran on 21 would confound JDK version with
  # thread type - which is the one variable this comparison is about. Set JAVA_HOME/PATH once, for
  # the whole sweep, and let the platform arm run there too.
  if [ "${1:-}" = "core-vt" ]; then
    engine=(-Dpc.virtualThreads=true)
    shift
    set -- core "$@"
  fi
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
  # STDERR GOES TO A FILE AND IS THEN CHECKED, rather than to /dev/null.
  #
  # bench/conf/logback.xml pins every arm at WARN and says why in its own header: "an arm that is
  # failing and retrying should still say so, and silence would hide it." This line threw that away,
  # so the one signal that separates a slow arm from a FAILING one never reached anybody. PC retries
  # a failed user function after a second by default: an arm with a real failure rate is measuring
  # retry scheduling, and prints a throughput figure while doing it.
  #
  # ONLY ERRORS BEFORE BENCH_WINDOW_CLOSED COUNT. Teardown closes the callee while the engine still
  # has records out, so every run ends with roughly `concurrency` failures - see Bench#windowClosed,
  # which also records the control that established they are teardown and not a storm. Checking the
  # whole file would fire on every run, and a warning that always fires is not a warning.
  local err=$WORK/last-run.err
  java ${engine[@]+"${engine[@]}"} ${jfr[@]+"${jfr[@]}"} -cp "$cp" Bench "$@" 2>"$err" | parse_result
  # `sed '/PAT/q'`, not `sed -n '1,/PAT/p'`: a `1,/PAT/` range does not test the end pattern on line
  # 1, so when the marker IS line 1 - which happens whenever an arm logs nothing during its run, the
  # healthy case - the range never closes and the whole teardown log is scanned. That fired the
  # warning on a clean run the first time it was written.
  if sed '/BENCH_WINDOW_CLOSED/q' "$err" 2>/dev/null | grep -qE '^ERROR|fail signal'; then
    log "WARNING: $* logged errors INSIDE the measured window - that result includes retries. See $err"
    cp "$err" "$err.$(echo "$*" | tr ' /' '__')"
  fi
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
  # Bounded: a wedged broker here silently costs the whole sweep, and there is no output to watch -
  # nothing is logged when the produce SUCCEEDS either, so a stale "producing..." line is the last
  # thing on screen whether it finished a second ago or never will.
  if ! run_with_deadline "$PRODUCE_TIMEOUT" run_one "$LOCAL_CP" produce "$BOOTSTRAP" "$TOPIC" "$RECORDS" >/dev/null; then
    rc=$?
    if [ "$rc" = 124 ]; then
      log "FATAL: producing $RECORDS records exceeded ${PRODUCE_TIMEOUT}s. Is the broker healthy? Raise BENCH_PRODUCE_TIMEOUT for a larger dataset."
    else
      log "FATAL: producing $RECORDS records failed (exit $rc)"
    fi
    exit 1
  fi
  log "produced $RECORDS records into $TOPIC"
fi

# delay_ms and concurrency are columns because both are now swept axes; without them a multi-delay
# or multi-concurrency results file cannot be read back. Everything else is unchanged, so llingr,
# franz and PC rows all sit in one table.
#
# records IS A COLUMN even though one invocation cannot vary it, and callee likewise. Both are
# settings a sweep fixes and a RESULTS FILE accumulates across invocations, which is exactly the
# shape that has burned this repository: a 100,000-record row was compared against a 500,000-record
# one and published as a 21% ordering-mode deficit that did not exist. The callee has now done the
# same thing to Vert.x - the WireMock stub caps high-concurrency runs server-side, so a capped row
# and an uncapped one sit side by side looking comparable. Emitting them is cheap; recovering them
# later is impossible.
# CALLEE_LABEL doubles as the record of which stub the run used, resolved once so no row can
# disagree with another about a setting that is process-wide.
if [ -n "${BENCH_TIMER_CALLEE:-}" ]; then CALLEE_LABEL=timer
elif [ -n "${BENCH_ASYNC_STUB:-}" ]; then CALLEE_LABEL=async
else CALLEE_LABEL=blocking; fi
MAX_POLL=${BENCH_MAX_POLL_RECORDS:-500}
echo "pc_version,client_pin,mode,callee,ordering,records,partitions,max_poll_records,delay_ms,concurrency,repeat,msg_per_sec,peak_in_flight" > "$RESULTS"
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
        proj=$(serial_projection_seconds "$d")
        if is_serial_arm "$mode" && [ "$proj" -gt "$SERIAL_ARM_MAX_SECONDS" ]; then
          log "SKIP $mode delay=${d}ms: serial arm, projected ${proj}s for $RECORDS records (cap ${SERIAL_ARM_MAX_SECONDS}s). Concurrency does not apply to it."
          echo "$ver,franz,$mode,n/a,$ORDERING,$RECORDS,$PARTITIONS,default,$d,$c,,SKIPPED_SERIAL_${proj}s," >> "$RESULTS"
          continue
        fi
        for r in $(seq 1 "$REPEATS"); do
          out=$(run_with_deadline "$RUN_TIMEOUT" run_go_arm "$BIN" "$BOOTSTRAP" "$TOPIC" "$RECORDS" "$d" "$c"); rc=$?
          read -r rate peak <<< "$out"
          if [ "$rc" = 124 ]; then rate=RUN_TIMEOUT_${RUN_TIMEOUT}s; peak=
          elif [ -z "$rate" ]; then rate=RUN_FAILED; peak=; fi
          log "$ver $mode delay=${d}ms conc=$c run$r = $rate msg/s, peak in flight $peak"
          echo "$ver,franz,$mode,n/a,$ORDERING,$RECORDS,$PARTITIONS,default,$d,$c,$r,$rate,$peak" >> "$RESULTS"
        done
      done
    done
    continue
  fi

  for pin in $CLIENT_PINS; do
    for pcv in $PC_VERSIONS; do
      CP=$(prepare "$pcv" "$pin" "$mode") || { log "SKIP $pcv/$pin $mode (resolve or compile failed)"; echo "$pcv,$pin,$mode,$CALLEE_LABEL,$ORDERING,$RECORDS,$PARTITIONS,$MAX_POLL,,,,COMPILE_FAILED," >> "$RESULTS"; continue; }
      for c in $CONCURRENCIES; do
        for d in $DELAYS; do
          proj=$(serial_projection_seconds "$d")
          if is_serial_arm "$mode" && [ "$proj" -gt "$SERIAL_ARM_MAX_SECONDS" ]; then
            log "SKIP $mode delay=${d}ms: serial arm, projected ${proj}s for $RECORDS records (cap ${SERIAL_ARM_MAX_SECONDS}s). Concurrency does not apply to it."
            echo "$pcv,$pin,$mode,$CALLEE_LABEL,$ORDERING,$RECORDS,$PARTITIONS,$MAX_POLL,$d,$c,,SKIPPED_SERIAL_${proj}s," >> "$RESULTS"
            continue
          fi
          for r in $(seq 1 "$REPEATS"); do
            out=$(run_with_deadline "$RUN_TIMEOUT" run_one "$CP" "$mode" "$BOOTSTRAP" "$TOPIC" "$RECORDS" "$d" "$c" "$BUFFER"); rc=$?
            read -r rate peak <<< "$out"
            if [ "$rc" = 124 ]; then rate=RUN_TIMEOUT_${RUN_TIMEOUT}s; peak=
            elif [ -z "$rate" ]; then rate=RUN_FAILED; peak=; fi
            log "$pcv/$pin $mode delay=${d}ms conc=$c run$r = $rate msg/s, peak in flight $peak"
            echo "$pcv,$pin,$mode,$CALLEE_LABEL,$ORDERING,$RECORDS,$PARTITIONS,$MAX_POLL,$d,$c,$r,$rate,$peak" >> "$RESULTS"
          done
        done
      done
    done
  done
done

log "results: $RESULTS"
cat "$RESULTS"
