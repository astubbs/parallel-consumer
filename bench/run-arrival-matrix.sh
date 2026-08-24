#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# The tail-experiment protocol, automated - because the 2026-08-22 matrix (84 rows in
# arrival-tail-skew-matrix.csv) was driven by hand and the method existed only in the note that
# described it. This script IS the method:
#
#   for each (mode, ordering, key distribution):
#     1. measure the arm's own FLAT saturated capacity (pre-produced backlog, 12,000 records)
#     2. run controlled arrival at 50 / 70 / 90 percent of THAT capacity, 5,250 records, 2 repeats,
#        for each requested workload (flat / tail / tailf / tail-distinct)
#
# Rates are per-arm because every queueing figure is a function of utilisation before it is a
# function of anything else; a common absolute rate would put one arm at 30% and another at 120%.
# Rates for ALL workloads come from the FLAT capacity, exactly as the original matrix did: the
# tail's mean service time is within a few percent of flat so the labels stay honest, and tailf's
# true utilisation being higher than its label is a documented property of the original data that
# re-deriving rates per-workload would silently change.
#
# COMBOS is lines of  mode|ordering|keydist|workload,workload,...
# Callee: BENCH_TIMER_CALLEE for every arm except pc/vertx, which has no timer form (the harness
# refuses it) and gets BENCH_ASYNC_STUB - its callee column says so.
#
# Output schema matches arrival-tail-skew-matrix.csv: workload,utilisation prefix + run-bisect's
# own columns. Capacity rows go to a separate file with run-bisect's plain schema.
set -uo pipefail
cd "$(dirname "$0")/.."

OUT=${OUT:-bench/results/arrival-tail-skew-matrix-2.csv}
CAP_OUT=${CAP_OUT:-bench/results/arrival-matrix-2-capacity.csv}
WORK_ROOT=${WORK_ROOT:?set WORK_ROOT to a scratch dir for classpath caches}

DELAY=10; CONC=24; CAP_RECORDS=${CAP_RECORDS:-12000}; ARR_RECORDS=${ARR_RECORDS:-5250}; TAIL_P99=505; FAIL_RATE=0.01
FRACTIONS="50 70 90"

export BENCH_BROKER=share
export BENCH_PARTITIONS=24 BENCH_MAX_POLL_RECORDS=500 BUFFER=20000
export BENCH_KEY_COUNT=200
export PC_VERSIONS=LOCAL CLIENT_PINS=NATIVE

COMBOS=${COMBOS:-"
core|KEY|zipf|flat,tail
core-vt|KEY|zipf|flat,tail,tailf
core-vt|UNORDERED|zipf|flat,tail
core-vt|KEY|distinct|tail
core-dpvt|KEY|zipf|flat,tail,tailf
core-dpvt|UNORDERED|zipf|flat,tail
core-dpvt|KEY|distinct|tail
pc|KEY|zipf|flat,tail
pc|UNORDERED|zipf|flat,tail
reactor|KEY|zipf|flat,tail
reactor|UNORDERED|zipf|flat,tail
mutiny|KEY|zipf|flat,tail
mutiny|UNORDERED|zipf|flat,tail
proxy|KEY|zipf|flat,tail
proxy|UNORDERED|zipf|flat,tail
"}

log() { printf '%s %s\n' "$(date '+%H:%M:%S')" "$*"; }

results_header() { head -1 "$1"; }

# Column 15 is msg_per_sec, column 31 arrival_requested, in run-bisect's results schema.
capacity_from() {
  awk -F, 'NR>1 && $15+0>0 {v=$15} END {print v+0}' "$1"
}

ensure_headers() {
  local sample=$1
  [ -f "$CAP_OUT" ] || results_header "$sample" > "$CAP_OUT"
  [ -f "$OUT" ] || echo "workload,utilisation_pct_of_flat_capacity,$(results_header "$sample")" > "$OUT"
}

total=0; done_count=0; started=$(date +%s)
total=$(echo "$COMBOS" | grep -c '|') || true

echo "$COMBOS" | grep '|' | while IFS='|' read -r mode ord kd workloads; do
  done_count=$((done_count + 1))
  work="$WORK_ROOT/$mode"; mkdir -p "$work"
  callee_env="BENCH_TIMER_CALLEE=1"; [ "$mode" = pc ] && callee_env="BENCH_ASYNC_STUB=1"

  log "[$done_count/$total] $mode/$ord/$kd - capacity run"
  env $callee_env BENCH_WORK="$work" MODES="$mode" ORDERINGS="$ord" BENCH_KEY_DISTRIBUTION="$kd" \
    bench/run-bisect.sh "$CAP_RECORDS" "$DELAY" "$CONC" 1 >> "$WORK_ROOT/driver.log" 2>&1
  rc=$?
  ensure_headers "$work/results.csv"
  if [ $rc -ne 0 ]; then log "  capacity run FAILED rc=$rc - skipping combo"; continue; fi
  cap=$(capacity_from "$work/results.csv")
  if [ -z "$cap" ] || [ "${cap%.*}" -le 0 ] 2>/dev/null; then
    log "  no numeric capacity ($(tail -1 "$work/results.csv" | cut -d, -f15)) - skipping combo"
    continue
  fi
  tail -n +2 "$work/results.csv" >> "$CAP_OUT"

  # macOS ships bash 3.2, so no associative arrays: three plain vars and a lookup function.
  rates=""; rate_50=""; rate_70=""; rate_90=""
  for f in $FRACTIONS; do
    r=$(awk -v c="$cap" -v f="$f" 'BEGIN {printf "%d", c*f/100}')
    [ "$r" -lt 1 ] && r=1
    rates="$rates $r"; eval "rate_$f=$r"
  done
  log "  flat capacity $cap msg/s -> rates:$rates"

  for wl in $(echo "$workloads" | tr , ' '); do
    wl_env=""
    case $wl in
      flat) ;;
      tail) wl_env="BENCH_DELAY_P99=$TAIL_P99" ;;
      tailf) wl_env="BENCH_DELAY_P99=$TAIL_P99 BENCH_FAILURE_RATE=$FAIL_RATE" ;;
      tail-distinct) wl_env="BENCH_DELAY_P99=$TAIL_P99" ;;
      *) log "  unknown workload '$wl' - skipped"; continue ;;
    esac
    kd_run=$kd; [ "$wl" = tail-distinct ] && kd_run=distinct
    log "  [$mode/$ord/$kd] workload=$wl arrival"
    env $callee_env $wl_env BENCH_WORK="$work" MODES="$mode" ORDERINGS="$ord" \
      BENCH_KEY_DISTRIBUTION="$kd_run" ARRIVAL_RATES="${rates# }" \
      bench/run-bisect.sh "$ARR_RECORDS" "$DELAY" "$CONC" 2 >> "$WORK_ROOT/driver.log" 2>&1
    rc=$?
    [ $rc -ne 0 ] && log "  workload $wl run rc=$rc (rows may still be usable)"
    tail -n +2 "$work/results.csv" | while IFS= read -r row; do
      req=$(echo "$row" | awk -F, '{print $31+0}')
      util=""
      [ "$req" = "$rate_50" ] && util=50
      [ "$req" = "$rate_70" ] && util=70
      [ "$req" = "$rate_90" ] && util=90
      echo "$wl,$util,$row" >> "$OUT"
    done
  done
done

elapsed=$(( $(date +%s) - started ))
rows=$(( $(wc -l < "$OUT" 2>/dev/null || echo 1) - 1 ))
log "DONE: $rows arrival rows in $OUT, capacity rows in $CAP_OUT, $((elapsed/60))m$((elapsed%60))s total"
