#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Fails if an O(n) accessor is passed as a plain argument to log.trace/log.debug, or to
# addArgument() without a lambda, anywhere in main source.
#
# WHY THIS EXISTS
#
# SLF4J defers FORMATTING, not ARGUMENT EVALUATION. `log.trace("... {}", expensive())` calls
# expensive() on every pass at every log level, including the levels production runs at - the
# disabled level saves the string building and nothing else. That is widely known and still
# reliably missed, because the line reads as if the level guards it.
#
# It reached this repo's control loop - the hottest path in the library - on
# astubbs/parallel-consumer#29, as an argument calling
# getNumberOfWorkQueuedInShardsAwaitingSelection(), which sums a counter across EVERY processing
# shard. Under KEY ordering the shard map is keyed per record key, so the scan grows with in-flight
# key cardinality exactly when the loop spins fastest. The two neighbouring log statements in the
# same file were both correctly guarded, which is the tell that this is a slip rather than a habit -
# and precisely the kind of slip review does not catch twice in a row.
#
# WHY NOT ARCHUNIT, WHICH IS WHERE THIS REPO PUTS ITS OTHER STRUCTURAL INVARIANTS
#
# ArchUnit reads the bytecode CALL GRAPH. It can see "controlLoop calls
# getNumberOfWorkQueuedInShardsAwaitingSelection"; it cannot see that one such call sits inside
# `if (log.isTraceEnabled())` and another does not, because a guard is CONTROL FLOW and the call
# graph has no notion of it. An ArchUnit rule would flag the correctly-guarded sites identically,
# and a rule with permanent exceptions is a rule nobody trusts. Source text can see the guard, so
# the check lives here. SpotBugs cannot do it either: findbugs-slf4j is wired in, but its detectors
# are about format strings and placeholders, not argument cost.
#
# WHY A DENYLIST RATHER THAN COST ANALYSIS
#
# "Expensive" is not decidable from source, so EXPENSIVE_ACCESSORS below is a hand-kept list of the
# calls known to walk a collection. It is deliberately short: the point is to protect the few
# accessors that scan per-shard or per-partition state, not to police every method call in a log
# statement. Add a name when you write an accessor that scans - and if you are adding one, say in
# its javadoc that it scans, because the next person reads that before they read this file.
#
# THE FIX, in preference order:
#   1. log.atTrace().addArgument(() -> expensive()).log("...")  - atTrace() returns the NOP builder
#      when the level is off, and NOP's addArgument(Supplier) never calls get(). Costs nothing.
#      parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/internal/
#      HotPathLogArgumentsAreDeferredTest.java asserts that SLF4J behaviour, because this script
#      cannot see it and an upgrade that changed it would break the fix silently.
#   2. if (log.isTraceEnabled()) { ... }  - the older idiom, still used in this codebase.
#
# SCOPE: main source only. Test code logs freely and is not on anybody's hot path.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Accessors known to walk a collection. See "WHY A DENYLIST" above before adding one.
EXPENSIVE_ACCESSORS='getNumberOfWorkQueuedInShardsAwaitingSelection|getNumberOfIncompleteOffsets|sumOfShardAvailableCounters|getCountOfWorkTracked|getNumberOfRecordsParkedForRetry'

# How many lines above a log statement may carry its isXEnabled() guard. Six covers a guard plus a
# comment block between it and the call; more than that and the guard is too far away to read as one.
GUARD_LOOKBACK=6

files=$(find . -path '*/src/main/java/*' -name '*.java' -not -path './target/*' | sort)

# One awk invocation PER FILE, using END rather than gawk's ENDFILE. mawk is the default awk on
# Debian/Ubuntu and has no ENDFILE, so the multi-file form parsed cleanly, ran nothing, and printed
# a pass over a file containing the exact defect this script exists to catch - found here only
# because the self-test is a negative control. Do not "optimise" this back into one invocation
# without checking which awk the runner has.
violations=""
for f in $files; do
  found=$(
    awk -v accessors="$EXPENSIVE_ACCESSORS" -v lookback="$GUARD_LOOKBACK" -v path="$f" '
      function trim(s) { gsub(/^[ \t]+|[ \t]+$/, "", s); return s }
      function guarded(start,    j, lo) {
        lo = start - lookback; if (lo < 1) lo = 1
        for (j = start; j >= lo; j--) {
          if (line[j] ~ /is(Trace|Debug)Enabled[ ]*\(\)/) return 1
        }
        return 0
      }
      { n++; line[n] = $0 }
      END {
        for (i = 1; i <= n; i++) {
          # A plain log.trace(/log.debug( statement: gather it until the closing );
          if (line[i] ~ /log\.(trace|debug)\(/) {
            stmt = line[i]; j = i
            while (stmt !~ /\);[ ]*$/ && j < n) { j++; stmt = stmt " " line[j] }
            if (stmt ~ accessors && !guarded(i)) {
              printf "%s:%d: unguarded O(n) accessor as a log argument\n    %s\n", path, i, trim(line[i])
            }
            i = j
            continue
          }
          # The fluent form is only free when the argument is a lambda. addArgument(expensive()) is
          # eager, and reads as if it were deferred - the worst of both.
          if (line[i] ~ /addArgument\(/ && line[i] ~ accessors && line[i] !~ /->/) {
            printf "%s:%d: addArgument() evaluates eagerly without a lambda\n    %s\n", path, i, trim(line[i])
          }
        }
      }
    ' "$f"
  )
  if [ -n "$found" ]; then
    violations="${violations}${found}
"
  fi
done

violations=$(printf '%b' "$violations")

if [ -n "$violations" ]; then
  printf 'check-hot-log-args: O(n) accessor evaluated regardless of log level\n\n%s\n\n' "$violations"
  printf 'SLF4J defers formatting, not argument evaluation - these run at every log level.\n'
  printf 'Fix with log.atTrace().addArgument(() -> ...).log("..."), or an isTraceEnabled() guard.\n'
  printf 'See this script header for why ArchUnit cannot express this rule.\n'
  exit 1
fi

printf 'check-hot-log-args: no eagerly-evaluated O(n) accessors in log arguments\n'
