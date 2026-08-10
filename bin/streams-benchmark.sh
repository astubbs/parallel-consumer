#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Runs the parallel-consumer-streams benchmarks: the card-payment demonstration, the cold-start
# backlog catch-up experiment, and the synthetic workload matrix.
#
# These are @Tag("performance") and are excluded from the default build, because a benchmark that
# adds minutes to every PR is a benchmark that gets deleted. This script is how you run them, and
# it is the same route bin/performance-test.sh takes.
#
# Every workload parameter can be overridden, so a result can be re-derived under a different
# configuration rather than taken on trust. Run with --help for the list.
#
# Requires Docker: the benchmarks run against a real broker via Testcontainers.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

SCENARIO="payments"
REPEAT=1
MAVEN_ARGS=()
BENCH_PROPS=()

usage() {
  cat <<'EOF'
Usage: bin/streams-benchmark.sh [--scenario NAME] [workload options] [-- extra-maven-args...]

SCENARIOS  (--scenario, default: payments)
  payments   Card-payment authorisation screening. The demonstration - one plausible
             workload, readable output.                             ~6 minutes
  backlog    Cold-start backlog catch-up, swept over three backlog depths, plus the
             warm-up and arm-order controls. The headline experiment. ~12 minutes
  matrix     The synthetic matrix: key distribution, processing profile, data shape.
             This is where the cells that DON'T favour PC live.      ~10 minutes
  all        Everything above.                                       ~25 minutes

WORKLOAD OPTIONS  (each overrides the default the scenario would otherwise pick)
  --records N            Records per arm
  --keys N               Distinct keys
  --key-distribution D   SINGLE | UNIFORM | ZIPF | HIGH_CARDINALITY
  --skew S               Zipf exponent. 0 is flat, 1.0 is Zipf's law, higher is hotter
  --cost-p50 MS          Median per-record service cost
  --cost-p99 MS          99th-percentile per-record service cost (the tail)
  --blocking-fraction F  1.0 = all blocking IO, 0.0 = all CPU-bound, 0.5 = mixed
  --payload-bytes N      JSON payload size per record
  --rate R               Records/second offered. 0 means pre-load a backlog instead
  --seed N               Change it to draw a different sample from the same distributions
  --pool N               Worker threads per task (also PC's max concurrency)
  --no-wake-on-work      Run with the wake-on-work poll optimisation disabled

OTHER
  --repeat N             Run the whole scenario N times. A benchmark run once is an anecdote
  --help                 This message

EXAMPLES
  bin/streams-benchmark.sh
  bin/streams-benchmark.sh --scenario backlog
  bin/streams-benchmark.sh --scenario payments --skew 2.0        # a much hotter key
  bin/streams-benchmark.sh --scenario matrix --blocking-fraction 0
EOF
}

fail() {
  echo "streams-benchmark: $1" >&2
  echo "Run --help for the list of options." >&2
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scenario)           SCENARIO="${2:?--scenario needs a value}"; shift 2 ;;
    --records)            BENCH_PROPS+=("-Dpc.bench.records=${2:?}"); shift 2 ;;
    --keys)               BENCH_PROPS+=("-Dpc.bench.keys=${2:?}"); shift 2 ;;
    --key-distribution)   BENCH_PROPS+=("-Dpc.bench.keyDistribution=${2:?}"); shift 2 ;;
    --skew)               BENCH_PROPS+=("-Dpc.bench.skew=${2:?}"); shift 2 ;;
    --cost-p50)           BENCH_PROPS+=("-Dpc.bench.costP50Ms=${2:?}"); shift 2 ;;
    --cost-p99)           BENCH_PROPS+=("-Dpc.bench.costP99Ms=${2:?}"); shift 2 ;;
    --blocking-fraction)  BENCH_PROPS+=("-Dpc.bench.blockingFraction=${2:?}"); shift 2 ;;
    --payload-bytes)      BENCH_PROPS+=("-Dpc.bench.payloadBytes=${2:?}"); shift 2 ;;
    --rate)               BENCH_PROPS+=("-Dpc.bench.rate=${2:?}"); shift 2 ;;
    --seed)               BENCH_PROPS+=("-Dpc.bench.seed=${2:?}"); shift 2 ;;
    --pool)               BENCH_PROPS+=("-Dpc.streams.dispatch.poolSize=${2:?}"); shift 2 ;;
    --no-wake-on-work)    BENCH_PROPS+=("-Dpc.streams.wakeOnWork.enabled=false"); shift ;;
    --repeat)             REPEAT="${2:?--repeat needs a value}"; shift 2 ;;
    --help|-h)            usage; exit 0 ;;
    --)                   shift; MAVEN_ARGS=("$@"); break ;;
    # An unknown flag is an error, never a silent default. A typo'd parameter that ran anyway
    # would report a measurement of a configuration nobody asked for, which is worse than not
    # running at all.
    *)                    fail "unknown option '$1'" ;;
  esac
done

case "$SCENARIO" in
  payments) TESTS="PaymentAuthorisationBenchmarkTest" ;;
  backlog)  TESTS="BacklogCatchUpBenchmarkTest" ;;
  matrix)   TESTS="WorkloadMatrixBenchmarkTest" ;;
  all)      TESTS="PaymentAuthorisationBenchmarkTest,BacklogCatchUpBenchmarkTest,WorkloadMatrixBenchmarkTest" ;;
  *)        fail "unknown scenario '$SCENARIO' (expected: payments, backlog, matrix, all)" ;;
esac

echo "streams-benchmark: scenario=$SCENARIO repeat=$REPEAT"
if [[ ${#BENCH_PROPS[@]} -gt 0 ]]; then
  echo "streams-benchmark: overrides: ${BENCH_PROPS[*]}"
fi
echo "streams-benchmark: results print as framed blocks headed '===' - search the output for them."
echo

STARTED_AT=$SECONDS

for ((run = 1; run <= REPEAT; run++)); do
  if [[ "$REPEAT" -gt 1 ]]; then
    echo "streams-benchmark: === run $run of $REPEAT ==="
  fi
  ./mvnw --batch-mode \
    -pl .,parallel-consumer-streams \
    -Dcopyright.skip=true \
    -DskipUTs=true \
    -Dit.test="$TESTS" \
    -Dfailsafe.failIfNoSpecifiedTests=false \
    -Dincluded.groups=performance \
    -Dexcluded.groups= \
    ${BENCH_PROPS[@]+"${BENCH_PROPS[@]}"} \
    verify \
    ${MAVEN_ARGS[@]+"${MAVEN_ARGS[@]}"}
done

ELAPSED=$((SECONDS - STARTED_AT))
echo
echo "streams-benchmark: done. scenario=$SCENARIO runs=$REPEAT elapsed=$((ELAPSED / 60))m$((ELAPSED % 60))s"
echo "streams-benchmark: the framed report blocks above carry the findings; everything else is Kafka's own logging."
