#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# WHAT THE BROKER IS DOING, which no msg/s figure in this harness has ever recorded.
#
# WHY IT EXISTS. Share groups acknowledge PER RECORD, and the acknowledgement is durable: the share
# coordinator writes delivery state into the internal __share_group_state topic. Parallel Consumer
# batches the same information into an encoded offset commit. That is a real difference in BROKER
# load, and it appears NOWHERE in a consumer-side throughput number - so an arm could win the msg/s
# column while costing the broker several times as much, and every existing results file would call
# that a win.
#
# WHAT IT MEASURES, and why these two.
#
#   cpu_usec   Cumulative CPU time of the broker container, from the cgroup. CUMULATIVE, not a
#              percentage: `docker stats` samples an instant, so it cannot be differenced across a
#              run and it cannot be compared between two runs of different lengths. A delta in
#              CPU-microseconds divided by the records processed is CPU cost PER RECORD, which is
#              the number that actually settles "is this win bought with broker load".
#   log_kb     On-disk size of the broker's log directory, per topic. The share coordinator's state
#              topic shows up here and an offset commit shows up in __consumer_offsets, so the two
#              acknowledgement designs can be compared by what they durably write.
#
# Usage:
#   bench/broker-load.sh snapshot [container]        # key=value lines, to a file
#   bench/broker-load.sh diff <before> <after> [n]   # the delta, per record if n given
set -uo pipefail

CONTAINER=${BENCH_BROKER_CONTAINER:-pc-bench-broker-4}

snapshot() {
  local c=${1:-$CONTAINER}
  echo "epoch_ms=$(date +%s)000"
  # usage_usec is the whole container - broker and controller are the same JVM here, which is what a
  # single-node KRaft broker IS, so there is nothing to separate and nothing being hidden.
  docker exec "$c" awk '/^usage_usec/ {print "cpu_usec=" $2}' /sys/fs/cgroup/cpu.stat
  # Per-topic totals rather than per-partition: a sweep creates a fresh group per run, so the
  # partition list of __share_group_state is stable but the interesting quantity is the total.
  # `sh -c`, because the glob has to be expanded INSIDE the container. Written as a bare argument the
  # HOST shell expands it, finds no /tmp/kafka-logs locally, and passes the literal `*` through - du
  # then fails and the whole log_kb half of the snapshot is silently empty, which looks exactly like
  # a broker that wrote nothing.
  docker exec "$c" sh -c 'du -sk /tmp/kafka-logs/*' 2>/dev/null |
    awk '{ n = $2; sub(".*/", "", n); sub("-[0-9]+$", "", n); kb[n] += $1 }
         END { for (t in kb) print "log_kb_" t "=" kb[t] }' | sort
}

# Prints "<key> <before> <after> <delta>" for every key present in both, and - when a record count is
# given - the per-record cost of the CPU delta in microseconds. Keys absent from `before` are treated
# as zero, which is what a topic created DURING the window looks like.
diff_snapshots() {
  local before=$1 after=$2 records=${3:-0}
  awk -v records="$records" '
    FNR == NR { b[$1] = $2; next }
    { key = $1; a[key] = $2 }
    END {
      for (key in a) {
        if (key == "epoch_ms") continue
        d = a[key] - (key in b ? b[key] : 0)
        if (d == 0) continue
        line = sprintf("%-28s %14d %14d %+14d", key, (key in b ? b[key] : 0), a[key], d)
        if (key == "cpu_usec" && records > 0)
          line = line sprintf("   %.2f usec/record", d / records)
        print line
      }
    }' FS='=' OFS=' ' "$before" "$after" | sort
}

case ${1:-} in
  snapshot) shift; snapshot "$@" ;;
  diff)     shift; diff_snapshots "$@" ;;
  *) echo "usage: $0 snapshot [container] | $0 diff <before> <after> [records]" >&2; exit 2 ;;
esac
