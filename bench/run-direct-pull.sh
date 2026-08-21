#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Measures the direct-pull engine against the shipped one, INTERLEAVED AT EACH POINT.
#
# WHY THIS EXISTS RATHER THAN A SINGLE run-bisect.sh INVOCATION
#
# run-bisect.sh loops mode on the OUTSIDE, which its own README names as a trap: two arms compared
# across separate blocks are also compared across whatever else happened to the machine in between,
# and this machine is shared with other agent sessions whose load moves by an order of magnitude
# within minutes. So instead of one invocation sweeping every point per mode, this drives one
# invocation PER POINT with both modes in it - baseline and direct pull for the same delay and the
# same concurrency land within a minute of each other, which is the only defence available against
# drift.
#
# It also ALTERNATES WHICH ARM GOES FIRST between repeats. Whichever runs first at a point pays for
# a colder page cache and a broker that has just been idle, and always giving that to the same arm
# is a systematic bias, not noise that repeats average away.
#
# Everything else - the broker, the topic, the classpath, the result line - is run-bisect.sh's, so
# the two harnesses cannot disagree about what was measured.
#
#   bench/run-direct-pull.sh [records] [repeats]
set -uo pipefail

RECORDS=${1:-500000}
REPEATS=${2:-3}
DELAYS=${DELAYS:-"0 2 100"}
CONCURRENCIES=${CONCURRENCIES:-"1000 5000"}

HERE=$(cd "$(dirname "$0")" && pwd)
WORK=${BENCH_WORK:-$(mktemp -d)}
mkdir -p "$WORK"
OUT=$WORK/direct-pull.csv
LOADLOG=$WORK/direct-pull-load.txt

export BENCH_WORK=$WORK
export BENCH_SKIP_PRODUCE=${BENCH_SKIP_PRODUCE:-1}
export BENCH_TOPIC=${BENCH_TOPIC:-bench-$RECORDS-p10}
export BENCH_PARTITIONS=${BENCH_PARTITIONS:-10}
export BENCH_ORDERING=${BENCH_ORDERING:-UNORDERED}
export PC_VERSIONS=${PC_VERSIONS:-LOCAL}
export CLIENT_PINS=${CLIENT_PINS:-NATIVE}

echo "pc_version,client_pin,mode,ordering,partitions,delay_ms,concurrency,repeat,msg_per_sec,peak_in_flight" > "$OUT"
: > "$LOADLOG"

log() { echo "[direct-pull] $*" >&2; }

# The machine is a confound this harness cannot remove, so it records it instead: load before and
# after every point, in the same file as the results, so a suspicious cell can be checked against
# what else was running when it was taken.
note_load() { echo "$1 $(uptime)" >> "$LOADLOG"; }

for r in $(seq 1 "$REPEATS"); do
  # Alternate the order, see the header.
  if [ $((r % 2)) -eq 1 ]; then modes="core core-dp"; else modes="core-dp core"; fi
  for c in $CONCURRENCIES; do
    for d in $DELAYS; do
      note_load "before r$r c$c d$d:"
      log "repeat $r, delay ${d}ms, concurrency $c, order [$modes]"
      MODES="$modes" bash "$HERE/run-bisect.sh" "$RECORDS" "$d" "$c" 1 >/dev/null 2>>"$WORK/direct-pull.log"
      # Stamp the real repeat number over run-bisect's own, which is always 1 here.
      awk -F, -v r="$r" 'NR>1 {$8=r; OFS=","; print}' "$WORK/results.csv" >> "$OUT"
      note_load "after  r$r c$c d$d:"
      tail -2 "$OUT" >&2
    done
  done
done

log "results: $OUT"
log "machine load through the sweep: $LOADLOG"
cat "$OUT"
