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
    # KIP-932 share groups. NOT an engine and NOT Parallel Consumer - a bare KafkaShareConsumer, the
    # same category as vanilla and franz - which is precisely why it needs no PC support for Kafka 4
    # and no arm artifact. Both acknowledgement modes are ONE class selected by a system property;
    # see run_one's share-explicit branch.
    share|share-explicit) echo ShareArm ;;
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

# WHICH BROKER, and why it is a variable now.
#
# BENCH_BROKER=share selects a Kafka 4.3.1 broker with KIP-932 enabled, on its own name and port, so
# the share arm can be measured WITHOUT disturbing the 3.9.0 container other sessions on this machine
# are using. Anything else - including unset - is the 3.9.0 broker on 19092 that every committed
# results file was taken against, byte for byte the configuration it always had.
#
# BROKER_NAME/BROKER_PORT/BROKER_IMAGE may also be set directly, for a broker this table does not
# know about. BOOTSTRAP is DERIVED rather than set, so it cannot disagree with the port - the two
# were independent hardcoded constants before, which is one edit away from a sweep that starts one
# broker and measures another.
[ "${BENCH_BROKER:-}" = share ] && use_share_broker
BROKER_NAME=${BROKER_NAME:-pc-bench-broker}
BROKER_PORT=${BROKER_PORT:-19092}
BOOTSTRAP=localhost:$BROKER_PORT

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

# RESULT <mode> <count> <ms> <msgPerSec> peak=<n> res=<p50/p99/p999/max> drain=<p50/p99/p999/max>
#   ->  "<msgPerSec> <peak> <res p50> <res p99> <res p999> <res max> <drain p50> ... <drain max>"
# One parser for every arm, which is the point of making the Go arm print the identical line.
#
# THE LATENCY FIELDS ARE OPTIONAL, and the parser is written so that an arm which does not print them
# still yields the two fields it always did, followed by eight dashes. The Go arms print the old line
# and are not being rewritten for this; a parser that demanded the new fields would turn their rows
# into RUN_FAILED, which is the shape of failure this harness has already published once.
parse_result() {
  grep '^RESULT' | awk '{
    p = $6; sub("peak=", "", p);
    res = "-/-/-/-"; drain = "-/-/-/-"; e2e = "-/-/-/-";
    arr = "-/-"; feed = "-/-/-"; backlog = "-/-/-"; fails = "-";
    for (i = 7; i <= NF; i++) {
      if ($i ~ /^res=/)     { res = substr($i, 5) }
      if ($i ~ /^drain=/)   { drain = substr($i, 7) }
      if ($i ~ /^e2e=/)     { e2e = substr($i, 5) }
      if ($i ~ /^arr=/)     { arr = substr($i, 5) }
      if ($i ~ /^feed=/)    { feed = substr($i, 6) }
      if ($i ~ /^backlog=/) { backlog = substr($i, 9) }
      if ($i ~ /^fails=/)   { fails = substr($i, 7) }
    }
    # EVERY FIELD MUST BE NON-EMPTY, and this is not cosmetic. The row is read back with a bare
    # `read -r a b c ...`, whose default IFS collapses runs of whitespace - so an empty field does not
    # produce an empty variable, it DISAPPEARS and shifts every value after it one column left. A
    # results file written that way is not corrupt-looking; it is plausible, and wrong from the first
    # empty cell rightwards. An arm that reported no backlog gauge put "0" under injected_failures the
    # first time this was written.
    if (res == "-")     { res = "-/-/-/-" }
    if (drain == "-")   { drain = "-/-/-/-" }
    if (e2e == "-")     { e2e = "-/-/-/-" }
    if (arr == "-")     { arr = "-/-" }
    if (feed == "-")    { feed = "-/-/-" }
    if (backlog == "-") { backlog = "-/-/-" }
    if (fails == "")    { fails = "-" }
    split(res, r, "/"); split(drain, d, "/"); split(e2e, e, "/");
    split(arr, a, "/"); split(feed, f, "/"); split(backlog, b, "/");
    print $5, p, r[1], r[2], r[3], r[4], d[1], d[2], d[3], d[4],
          e[1], e[2], e[3], e[4], a[1], a[2], f[2], b[2], b[3], fails;
  }'
}
# A KILLED RUN'S LATENCY FIELDS ARE NOT A MEASUREMENT, and they are not absent either - the arm was
# killed after printing whatever it had managed, so the parser reads real-looking numbers taken over a
# truncated window against a record count that was never reached. Those must not sit in the same
# column as a completed run's. The CSV was already blanked for a timeout; the LOG LINE was not, so the
# two disagreed - the log quoted percentiles the results file did not contain, which is precisely the
# shape of thing this harness has published wrongly before.
clear_latency_fields() {
  rp50=; rp99=; rp999=; rmax=; dp50=; dp99=; dp999=; dmax=
  ep50=; ep99=; ep999=; emax=; arrreq=; arrach=; feedp99=; backp99=; backmax=; fails=
}

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
  # core-dpvt: BOTH options at once, and they genuinely compose rather than merely coexisting.
  # DirectPullWorkerPool#start takes an Executor and runs its pullers on it -
  # `pool.start(workerThreadPool.get(), maxConcurrency)` - so whatever #setupWorkerPool returned is
  # what the pullers run on. Turn on virtual threads and direct pull's workers ARE virtual threads.
  #
  # That makes this the combination most likely to be good, and it went unmeasured for a day: direct
  # pull is the only arm that reaches its configured concurrency (5,000 of 5,000 in flight, against
  # the shipped engine's 382-668 at 2ms), and virtual threads are what stop N pullers costing N
  # platform threads - which is what made direct pull ruinous at 5,000 before the scan fix.
  if [ "${1:-}" = "core-dpvt" ]; then
    engine=(-Dpc.directPull=true -Dpc.virtualThreads=true)
    shift
    set -- core "$@"
  fi
  # share-explicit: the same mode-suffix trick, and here it is not about old versions - it is about
  # keeping the two acknowledgement modes in ONE arm class so they cannot drift apart, while leaving
  # them two distinct rows in the results file. Implicit acknowledges a whole poll at once; explicit
  # acknowledges every record individually and refuses to poll while any is outstanding. That is a
  # real difference in broker load and it may be the whole result, so it has to be a column value.
  if [ "${1:-}" = "share-explicit" ]; then
    engine=(-Dbench.shareAckMode=explicit)
    shift
    set -- share "$@"
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
  # A `NOTE:` FROM THE ARM MEANS IT COULD NOT MEASURE SOMETHING IT WAS ASKED FOR, and until this was
  # surfaced the only symptom was a dash in a column - which reads as "this arm does not report that",
  # the entirely normal case for an old release or a non-PC arm.
  #
  # What that hid: a concurrent session ran `mvn install` and replaced the shared ~/.m2 LOCAL
  # 0.6.0.0-SNAPSHOT with a build from another branch, partway through a sweep. Half the rows lost
  # their residence column and every one of them had its throughput measured against somebody else's
  # code, with nothing anywhere saying so. `LOCAL` names a coordinate, not a build, and any session on
  # this machine can change what it points at while a sweep is running.
  #
  # A warning cannot prevent that. What it can do is make the sweep say out loud that a column it was
  # asked for is missing, which is the point at which somebody checks the jar.
  if sed '/BENCH_WINDOW_CLOSED/q' "$err" 2>/dev/null | grep -qE '^NOTE:'; then
    log "WARNING: $* could not measure something it was asked for:"
    sed -n '/^NOTE:/p' "$err" | while IFS= read -r note; do log "         $note"; done
    log "         If this is a LOCAL row, check the core jar has not been replaced by another session."
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
# Orderings to sweep. Same contract as DELAYS and CONCURRENCIES: defaults to the single value, so
# every existing invocation means what it always did.
#
# IT IS A SWEEP AXIS NOW BECAUSE THE ALTERNATIVE PRODUCED THE COUNT NOBODY WANTED TO SEE. Ordering
# was a per-invocation environment variable, so comparing two orderings meant two sweeps - and across
# every results file taken on 2026-08-22 that came to 369 UNORDERED rows, 7 KEY and 4 PARTITION.
# UNORDERED is the mode Parallel Consumer has no differentiator in. Swept from one invocation the
# arms also ALTERNATE, which is the only defence this harness has against machine drift between them.
ORDERINGS=${ORDERINGS:-$ORDERING}
PARTITIONS=${BENCH_PARTITIONS:-1}
export BENCH_PARTITIONS=$PARTITIONS

# THE KEY DISTRIBUTION IS PART OF THE DATASET, so it is part of the topic name and a column.
#
# `distinct` - one key per record - was the only distribution this harness could produce, and it is
# the best possible case for KEY ordering: every record is its own shard, so the ordering constraint
# binds nothing and KEY behaves exactly like UNORDERED. See Bench.java.template's key-distribution
# section. Two datasets that differ only in their key distribution must never share a topic name -
# that is the same confound the partition count is already in the topic name to prevent.
KEY_DIST=${BENCH_KEY_DISTRIBUTION:-distinct}
export BENCH_KEY_DISTRIBUTION=$KEY_DIST
KEY_SUFFIX=""
[ "$KEY_DIST" != distinct ] && KEY_SUFFIX="-$KEY_DIST${BENCH_KEY_COUNT:+x$BENCH_KEY_COUNT}"
TOPIC=${BENCH_TOPIC:-bench-$RECORDS-p$PARTITIONS$KEY_SUFFIX}

# CONTROLLED ARRIVAL. Rates to sweep, in records per second; empty (the default) is the pre-produced
# path this harness has always used, so every committed figure stays reproducible.
#
# WHY IT IS A SWEEP AND NOT A SETTING. At 100% utilisation every queueing system measures its own
# backlog, so a single arrival rate near saturation answers the same question the pre-produced path
# already answers. The interesting behaviour is where the percentiles turn up, and finding it needs
# the rate swept as a fraction of the arm's own measured throughput - 50%, 70%, 90%.
ARRIVAL_RATES=${ARRIVAL_RATES:-}
arrival_mode() { [ -n "$ARRIVAL_RATES" ]; }

# A FRESH TOPIC PER RUN, and only under controlled arrival.
#
# THE CONSUMER MUST NOT START WITH A BACKLOG - that is the whole point of the arrival axis, and there
# were two ways to get it. Starting an existing topic's consumer at `latest` looked cheaper and is
# wrong: the reset is applied at ASSIGNMENT, so any record produced between subscribe and assignment
# is silently skipped, the run never reaches its expected count, and it fails as a timeout - which
# reads as a slow arm. A fresh topic with the harness's usual `earliest` has no such race at all: a
# record fed before the group joins is still consumed, from offset 0, it merely arrives early. The
# race becomes a harmless ordering rather than a lost record, and the feeder's warmup barrier removes
# even that.
#
# Deleted immediately after the run rather than at the end of the sweep: a sweep that is killed
# half-way should not leave a hundred topics behind, and the delete costs a second outside any
# measured window.
fresh_topic_name() { echo "bench-arr-$$-$1"; }
delete_topic() {
  docker exec "$BROKER_NAME" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "localhost:$BROKER_PORT" --delete --topic "$1" >/dev/null 2>&1
}

# THE LOCAL BUILD'S IDENTITY, as a column.
#
# `PC_VERSIONS=LOCAL` resolves to a Maven COORDINATE out of a ~/.m2 shared by every worktree and
# every concurrent session on this machine. Whoever ran `mvn install` most recently owns it, and a
# sweep in progress picks the change up at its next JVM start with nothing anywhere saying so - which
# happened twice on 2026-08-22 and put four rows in a results file against code their author never
# saw. `pc_version` said LOCAL for every row and identified nothing.
# See docs/inflight/perf-local-is-a-coordinate-not-a-build.md, which names exactly this column as the
# fix. Checked before AND after every run, so a swap that happens mid-cell voids that cell instead of
# being averaged into it.
pc_build_id() {
  local jars count
  jars=$(printf '%s' "$1" | tr ':' '\n' | grep -E 'parallel-consumer-core[^/]*\.jar$')
  count=$(printf '%s' "$jars" | grep -c . )
  [ "$count" = 1 ] || { echo ""; return; }
  [ -f "$jars" ] || { echo ""; return; }
  cksum < "$jars" | awk '{print $1}'
}

# The dataset: produced once, by the local build, and reused by every arm - including the Go one,
# which is the only way an engine comparison means anything.
LOCAL_CP=""
if arrival_mode; then
  # NOTHING IS PRE-PRODUCED under controlled arrival - the records are fed during each run, into a
  # topic created for that run. The local classpath is still resolved here because every fresh topic
  # is created through it.
  LOCAL_CP=$(prepare LOCAL NATIVE) || { log "FATAL: cannot build local arm"; exit 1; }
  log "controlled arrival: rates [$ARRIVAL_RATES]/s, fresh topic per run, nothing pre-produced"
elif [ "${BENCH_SKIP_PRODUCE:-0}" = 1 ]; then
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

# LOAD IS A COLUMN, because msg_per_sec is not load-robust and every reader of a results file has so
# far had to take the load figure on trust from a sentence in a document.
#
# It is not a footnote on this machine. During the first share-arm sweep the 1-minute load moved
# between 4 and 41 - several other sessions were running their own bench sweeps at the same time -
# and two repeats of the IDENTICAL share row came back at 15,382 and 66,489 msg/s. Without this
# column those two rows are indistinguishable from a real bimodality in the arm, and the committed
# convention ("discard anything above ~20") cannot be applied to a file after the fact at all.
#
# Sampled immediately BEFORE each run rather than after: the 1-minute average trails, so the value
# that best describes the machine a run is about to meet is the one measured going in.
load_1m() { uptime | sed 's/.*load averages*: //' | awk '{print $1}'; }

# THE LATENCY COLUMNS, and why there are eight of them rather than one.
#
# residence_* is poll-return to completion - what a record spends INSIDE the engine, read off Parallel
# Consumer's own pc.record.residence.time meter for a PC arm and measured the same way in Bench for
# vanilla and pool. It is the only measure here that is not a restatement of throughput: PC chooses how
# much to fetch and how deep to buffer, so residence is Little's law applied to those buffers.
#
# drain_* is engine start to completion. The dataset is produced once, before any arm runs, so engine
# start is a valid common arrival instant for every record - but for a fixed record count it is roughly
# position/throughput, so IT LARGELY RESTATES msg_per_sec IN LATENCY UNITS. It is not independent
# corroboration of a residence figure and must never be read as such. It earns its place as the honest
# measure of a backlog drain, which is what an operator feels when a consumer restarts or catches up.
#
# p999 and max are here because a mean is what a serial engine hides behind: head-of-line blocking
# shows up in the upper percentiles long before it shows up anywhere else.
#
# e2e_* is the THIRD measure and it only exists under controlled arrival: completion minus the
# record's INTENDED send instant. It is the one measure coordinated omission cannot fool. Residence
# starts at poll-return, so a record that sat in the broker because the consumer was behind is
# charged nothing for the wait - and under an arrival sweep that is precisely the failure mode that
# would make a saturated arm look fast. Blank on the pre-produced path, where there is no arrival
# instant to measure from because every record arrived before the run.
#
# arrival_requested/arrival_achieved, feed_lag_p99_ms and backlog_* are the EVIDENCE THAT THE
# PRODUCER WAS NOT THE BOTTLENECK. If the feed cannot hold its schedule the whole experiment measures
# the producer; the run is voided rather than recorded, and these columns are what a reader checks
# when they want to know how close it came.
echo "pc_version,pc_build,client_pin,mode,callee,ordering,key_dist,records,partitions,max_poll_records,buffer,delay_ms,concurrency,repeat,msg_per_sec,peak_in_flight,load_1m,residence_p50_ms,residence_p99_ms,residence_p999_ms,residence_max_ms,drain_p50_ms,drain_p99_ms,drain_p999_ms,drain_max_ms,e2e_p50_ms,e2e_p99_ms,e2e_p999_ms,e2e_max_ms,arrival_requested,arrival_achieved,feed_lag_p99_ms,backlog_p99,backlog_max,injected_failures" > "$RESULTS"

# The eighteen latency-and-arrival fields on a row that HAS a measurement but could not report them.
NO_LATENCY=",,,,,,,,,,,,,,,,,"
# EVERYTHING AFTER msg_per_sec on a row that has no measurement at all - peak, load, and the eight
# latency fields, as ten empty cells. One variable rather than a hand-counted run of commas at each
# of the four such sites: counting them by hand is what put the skip rows one column short the first
# time this file grew a column, and a short row is silent - nothing reads a results file strictly
# enough to notice. verify_row_widths below is the backstop.
NO_MEASUREMENT=",,,,,,,,,,,,,,,,,,,,"
# THE NON-BLOCKING ENGINES MUST NOT BE MEASURED WITH A BLOCKING CALLEE.
#
# vertx, reactor, mutiny and proxy exist to run a user function that does NOT hold a thread - that is
# the entire proposition. Handed a callee that blocks, they hold a thread per record like everything
# else, and the number produced says nothing about the engine: it measures the blocking stub, through
# a more expensive path. Measured on 2026-08-22 at 5,000 concurrency, mutiny with a blocking callee
# came back at 5,745 msg/s holding 180 records in flight - a figure with no meaning, sitting in a
# table next to arms for which the same callee is entirely appropriate.
#
# So the harness refuses, rather than trusting whoever runs it to remember. Set BENCH_ASYNC_STUB=1
# (the callee completes on a timer and holds no thread) or BENCH_TIMER_CALLEE=1 (no server at all).
# BENCH_ALLOW_BLOCKING_ENGINE=1 overrides, for the one legitimate case: deliberately measuring what
# an engine costs when a user gives it blocking work, which is a real question about a real mistake -
# but it has to be asked on purpose.
#
# THE SHARE ARMS ARE IN THIS SET TOO, though they are not engines. Their user function is a
# CompletionStage that never blocks - Bench#callCallee, the same one every arm here uses - so handed
# the thread-per-request WireMock callee they would measure its container-thread pool rather than
# share groups, which is exactly the failure this guard exists for.
is_nonblocking_engine() { case $1 in vertx|pc|reactor|mutiny|proxy|share|share-explicit) return 0 ;; *) return 1 ;; esac; }

# A SHARE ARM AGAINST A BROKER WITHOUT SHARE GROUPS FAILS AS "RUN_FAILED", which is the same row a
# broken arm produces - so the harness checks instead of leaving it to be diagnosed. This is not
# hypothetical: the first share sweep run here said "reusing running broker pc-bench-broker" and
# produced exactly that row, because BENCH_BROKER=share had been silently ignored (see lib/broker.sh).
#
# The check is on the FEATURE, not on the image tag, because the feature is what actually decides:
# share.version must be finalized at level 1 AND the share rebalance protocol must be enabled, and a
# 4.3 broker started without group.coordinator.rebalance.protocols=...,share has the first and not
# the second.
for m in $MODES; do
  case $m in share|share-explicit)
    # THE OUTPUT IS CAPTURED FIRST, then matched - it is not piped into `grep -q`. This script runs
    # under `set -o pipefail`, and `grep -q` exits the instant it matches, which SIGPIPEs the
    # producer: `docker exec ... | grep -q` therefore returns 141 ON SUCCESS. Written the obvious way
    # this guard fired against a broker that had share groups perfectly well, and its error message
    # told the reader to do the thing they had already done.
    features=$(docker exec "$BROKER_NAME" /opt/kafka/bin/kafka-features.sh --bootstrap-server "localhost:$BROKER_PORT" describe 2>/dev/null)
    if ! printf '%s\n' "$features" | grep -Eq 'share\.version.*FinalizedVersionLevel: [1-9]'; then
      log "FATAL: mode '$m' needs KIP-932 share groups, and broker '$BROKER_NAME' does not have them finalized."
      log "       Share groups went GA in Kafka 4.2.0. Run with BENCH_BROKER=share to start a 4.3.1"
      log "       broker on its own name and port, leaving the 3.9.0 one other sessions use untouched."
      exit 1
    fi
    break ;;
  esac
done

# THE VERT.X ARM HAS NO TIMER FORM, AND SILENTLY SCORES WELL WITHOUT ONE.
#
# bench/README.md has said this in prose since the timer callee was added - the Vert.x engine issues
# the HTTP call itself, through `vertxHttpReqInfo`, so it cannot be handed a callee that is not an
# HTTP server. Nothing enforced it, and on 2026-08-22 a sweep did exactly that.
#
# What it produces is not an error. `VertxArm` points the engine at `Bench#calleePort`, which returns
# 0 when no stub is running; every request fails; the engine's `onResponse` callback still fires, so
# the arm reaches its expected count and prints a NUMBER - 17,221 msg/s, comfortably mid-table,
# sitting in a results file next to arms that did the work. The one visible tell is `peak_in_flight`
# = 0, because nothing ever arrived at a callee, and no reader is required to notice that.
#
# It is also expensive: the failing runs spun at 190% CPU and drove the machine's load average from
# 12 to 44, which then contaminated every OTHER arm measured in the same round.
for m in $MODES; do
  case $m in
    vertx|pc)
      if [ -n "${BENCH_TIMER_CALLEE:-}" ]; then
        log "FATAL: mode '$m' issues its own HTTP request through the engine (vertxHttpReqInfo), so it"
        log "       has no BENCH_TIMER_CALLEE form - there is no server to call, every request fails,"
        log "       and the arm still prints a plausible throughput figure with peak_in_flight 0."
        log "       Use BENCH_ASYNC_STUB=1 for a non-blocking callee this arm can actually reach."
        exit 1
      fi ;;
  esac
done

if [ "$CALLEE_LABEL" = blocking ] && [ -z "${BENCH_ALLOW_BLOCKING_ENGINE:-}" ]; then
  for m in $MODES; do
    if is_nonblocking_engine "$m"; then
      log "FATAL: mode '$m' is a non-blocking engine and the callee is blocking, which measures the stub rather than the engine."
      log "       Set BENCH_ASYNC_STUB=1 (timer-completed callee, holds no thread) or BENCH_TIMER_CALLEE=1 (no server)."
      log "       BENCH_ALLOW_BLOCKING_ENGINE=1 overrides, if measuring blocking work through an async engine IS the question."
      exit 1
    fi
  done
fi

for mode in $MODES; do
  # The two Go arms differ only in which binary they build and what they call themselves, so they
  # share one branch rather than two near-identical copies of the sweep loop.
  if [ "$mode" = llingr ] || [ "$mode" = franz ]; then
    BIN=$(prepare_go_arm "$mode") || {
      log "SKIP $mode: $(command -v go >/dev/null 2>&1 && echo "build failed, see $WORK/$mode-build.log" || echo "no 'go' on PATH - install Go to measure this arm")"
      continue
    }
    ver=$(go_arm_version "$mode")
    if arrival_mode; then
      log "SKIP $mode: the Go arms take no arrival-rate flag and produce nothing, so a controlled-arrival sweep cannot include them"
      echo "$ver,,franz,$mode,n/a,$ORDERING,$KEY_DIST,$RECORDS,$PARTITIONS,default,$BUFFER,,,,NO_ARRIVAL_SUPPORT$NO_MEASUREMENT" >> "$RESULTS"
      continue
    fi
    for c in $CONCURRENCIES; do
      for d in $DELAYS; do
        proj=$(serial_projection_seconds "$d")
        if is_serial_arm "$mode" && [ "$proj" -gt "$SERIAL_ARM_MAX_SECONDS" ]; then
          log "SKIP $mode delay=${d}ms: serial arm, projected ${proj}s for $RECORDS records (cap ${SERIAL_ARM_MAX_SECONDS}s). Concurrency does not apply to it."
          echo "$ver,,franz,$mode,n/a,$ORDERING,$KEY_DIST,$RECORDS,$PARTITIONS,default,$BUFFER,$d,$c,,SKIPPED_SERIAL_${proj}s$NO_MEASUREMENT" >> "$RESULTS"
          continue
        fi
        for r in $(seq 1 "$REPEATS"); do
          load=$(load_1m)
          out=$(run_with_deadline "$RUN_TIMEOUT" run_go_arm "$BIN" "$BOOTSTRAP" "$TOPIC" "$RECORDS" "$d" "$c"); rc=$?
          read -r rate peak rp50 rp99 rp999 rmax dp50 dp99 dp999 dmax ep50 ep99 ep999 emax arrreq arrach feedp99 backp99 backmax fails <<< "$out"
          if [ "$rc" = 124 ] || [ -z "$rate" ]; then
            [ "$rc" = 124 ] && rate=RUN_TIMEOUT_${RUN_TIMEOUT}s || rate=RUN_FAILED
            peak=; latency=$NO_LATENCY; clear_latency_fields
          else latency="$rp50,$rp99,$rp999,$rmax,$dp50,$dp99,$dp999,$dmax,$ep50,$ep99,$ep999,$emax,$arrreq,$arrach,$feedp99,$backp99,$backmax,$fails"; fi
          log "$ver $mode delay=${d}ms conc=$c run$r = $rate msg/s, peak in flight $peak, load $load, residence p50/p99/p99.9/max ${rp50:--}/${rp99:--}/${rp999:--}/${rmax:--}ms, drain ${dp50:--}/${dp99:--}/${dp999:--}/${dmax:--}ms"
          echo "$ver,,franz,$mode,n/a,$ORDERING,$KEY_DIST,$RECORDS,$PARTITIONS,default,$BUFFER,$d,$c,$r,$rate,$peak,$load,$latency" >> "$RESULTS"
        done
      done
    done
    continue
  fi

  for pin in $CLIENT_PINS; do
    for pcv in $PC_VERSIONS; do
      CP=$(prepare "$pcv" "$pin" "$mode") || { log "SKIP $pcv/$pin $mode (resolve or compile failed)"; echo "$pcv,,$pin,$mode,$CALLEE_LABEL,$ORDERING,$KEY_DIST,$RECORDS,$PARTITIONS,$MAX_POLL,$BUFFER,,,,COMPILE_FAILED$NO_MEASUREMENT" >> "$RESULTS"; continue; }
      for ord in $ORDERINGS; do
        export BENCH_ORDERING=$ord
        for c in $CONCURRENCIES; do
          for d in $DELAYS; do
            proj=$(serial_projection_seconds "$d")
            if is_serial_arm "$mode" && [ "$proj" -gt "$SERIAL_ARM_MAX_SECONDS" ]; then
              log "SKIP $mode delay=${d}ms: serial arm, projected ${proj}s for $RECORDS records (cap ${SERIAL_ARM_MAX_SECONDS}s). Concurrency does not apply to it."
              echo "$pcv,,$pin,$mode,$CALLEE_LABEL,$ord,$KEY_DIST,$RECORDS,$PARTITIONS,$MAX_POLL,$BUFFER,$d,$c,,SKIPPED_SERIAL_${proj}s$NO_MEASUREMENT" >> "$RESULTS"
              continue
            fi
            # 0 is the pre-produced path - one iteration, no arrival control, exactly as before.
            # Written as a literal 0 rather than an empty string because `for x in ${VAR:-""}` is
            # unquoted word splitting: an empty default produces ZERO iterations, not one, and the
            # whole sweep silently does nothing.
            for arrival in ${ARRIVAL_RATES:-0}; do
              export BENCH_ARRIVAL_RATE=$arrival
              for r in $(seq 1 "$REPEATS"); do
                topic=$TOPIC
                if [ "$arrival" != 0 ]; then
                  CELL=$((${CELL:-0} + 1))
                  topic=$(fresh_topic_name "$CELL")
                  run_with_deadline "$PRODUCE_TIMEOUT" run_one "$LOCAL_CP" produce "$BOOTSTRAP" "$topic" 0 >/dev/null \
                    || { log "SKIP $mode $ord arrival=$arrival: could not create $topic"; continue; }
                fi
                build_before=$(pc_build_id "$CP")
                load=$(load_1m)
                out=$(run_with_deadline "$RUN_TIMEOUT" run_one "$CP" "$mode" "$BOOTSTRAP" "$topic" "$RECORDS" "$d" "$c" "$BUFFER"); rc=$?
                build_after=$(pc_build_id "$CP")
                read -r rate peak rp50 rp99 rp999 rmax dp50 dp99 dp999 dmax ep50 ep99 ep999 emax arrreq arrach feedp99 backp99 backmax fails <<< "$out"
                if [ "$rc" = 124 ] || [ -z "$rate" ]; then
                  if [ "$rc" = 124 ]; then rate=RUN_TIMEOUT_${RUN_TIMEOUT}s
                  # Exit 3 is Bench's arrival verdict: the feed could not hold the schedule it was
                  # asked for, so the run measured the producer. A DISTINCT label from RUN_FAILED,
                  # because the two mean opposite things - this arm worked and the harness did not.
                  elif [ "$rc" = 3 ]; then rate=ARRIVAL_VOID
                  else rate=RUN_FAILED; fi
                  peak=; latency=$NO_LATENCY; clear_latency_fields
                else latency="$rp50,$rp99,$rp999,$rmax,$dp50,$dp99,$dp999,$dmax,$ep50,$ep99,$ep999,$emax,$arrreq,$arrach,$feedp99,$backp99,$backmax,$fails"; fi
                # THE JAR CHANGED UNDER THE RUN. Not a slow row, not a noisy row - a row measured
                # against two different builds, which is not a measurement of either.
                if [ -n "$build_before" ] && [ "$build_before" != "$build_after" ]; then
                  log "WARNING: the parallel-consumer-core jar changed during this run ($build_before -> $build_after)."
                  log "         Another session installed over the LOCAL coordinate. Row voided."
                  rate=BUILD_CHANGED_${build_before}_TO_${build_after}
                  peak=; latency=$NO_LATENCY; clear_latency_fields
                fi
                arrival_note=""
                [ "$arrival" != 0 ] && arrival_note=" arrival=${arrival}/s (achieved ${arrach:--}, feed lag p99 ${feedp99:--}ms, backlog p99 ${backp99:--}), e2e p50/p99/p99.9/max ${ep50:--}/${ep99:--}/${ep999:--}/${emax:--}ms,"
                log "$pcv/$pin $mode $ord delay=${d}ms conc=$c run$r = $rate msg/s, peak in flight $peak, load $load,$arrival_note residence p50/p99/p99.9/max ${rp50:--}/${rp99:--}/${rp999:--}/${rmax:--}ms, drain ${dp50:--}/${dp99:--}/${dp999:--}/${dmax:--}ms"
                echo "$pcv,$build_before,$pin,$mode,$CALLEE_LABEL,$ord,$KEY_DIST,$RECORDS,$PARTITIONS,$MAX_POLL,$BUFFER,$d,$c,$r,$rate,$peak,$load,$latency" >> "$RESULTS"
                [ "$arrival" != 0 ] && delete_topic "$topic"
              done
            done
          done
        done
      done
    done
  done
done

# A SHORT ROW IS SILENT, so check rather than trust. Every time this file has grown a column, at
# least one of the four no-measurement sites has been left a comma short - the sweep still finishes,
# the file still opens, and every value to the right of the gap is read against the wrong heading.
verify_row_widths() {
  local want got bad=0
  want=$(head -1 "$RESULTS" | awk -F, '{print NF}')
  while IFS= read -r row; do
    got=$(printf '%s' "$row" | awk -F, '{print NF}')
    if [ "$got" != "$want" ]; then
      log "WARNING: results row has $got fields, header has $want: $row"
      bad=$((bad + 1))
    fi
  done < <(tail -n +2 "$RESULTS")
  [ "$bad" = 0 ] || log "WARNING: $bad malformed row(s) - the columns to the right of the gap are misaligned."
}
verify_row_widths

log "results: $RESULTS"
cat "$RESULTS"
