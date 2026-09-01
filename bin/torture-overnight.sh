#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# shell-justified: process orchestration is the core of this harness, not incidental to it - watchdog
# timers, jstack against a live PID, killing a wedged JVM, traps and wall-clock budgets across an
# eight-hour run. That is bash's domain and Node would wrap it rather than replace it. The part the
# Node-default rule is actually aimed at - the verdict logic that decides what a run MEANT - is the
# part of this file that HAS been wrong three times, and it is covered by bin/test-torture-overnight.sh
# for that exact reason. Stating the weakness rather than claiming there isn't one: if the verdict
# logic grows further, it should move out of here into .mjs, not be defended by this comment.
# OVERNIGHT TORTURE HARNESS - MVP spike. Runs for hours in short cycles, hunting the lock-ups and
# data-skips confluentinc#857's family has not yet accounted for, and packages everything an agent
# needs to review it in the morning without asking anyone a question.
#
# WHY CYCLES. A live-locked run tells you nothing while it hangs and everything the moment you look
# at it. Rather than teach the harness to detect every way PC can wedge, each cycle gets a hard
# wall-clock budget; when it expires the WATCHDOG TAKES A THREAD DUMP AND THEN KILLS IT. Restarting
# every 30 minutes costs almost nothing against an 8-hour run, and the dump is the artefact that
# turns a hang into a diagnosis - the six captures that identified the revoke deadlock exist only
# because something dumped the stack while it was still stuck. A cycle that finishes early does not
# burn its slot; the watchdog returns the moment the build exits and the next cycle starts.
#
# WHAT IT HUNTS, and why these first. The AB-BA revoke deadlock is fixed and verified. What remains
# unaccounted for in that family is:
#   * COMMIT-RESPONSE TIMEOUTS reported in the field twice and never reproduced (astubbs#175, astubbs#177).
#   * DATA SKIP - confluentinc#875 describes an offset silently never delivered, lag growing, and a
#     restart making it reappear. That is not a liveness failure and no liveness detector will see
#     it. The end-of-run LEDGER_LOSS check is what would catch it (see COMPLETENESS below).
#   * the UNBOUNDED REVOKE WAIT in transactional mode - astubbs#44 / confluentinc#803, which
#     carries upstream's `verified bug` label (one of a couple of dozen that do; this file
#     previously called it the only one, which was false). THIS RUN DOES NOT HUNT IT. See below.
#
# ############################################################################################
# THE TRANSACTIONAL HUNT IS NOT HAPPENING, AND THIS HARNESS WILL NOT PRETEND OTHERWISE.
#
# An earlier revision of this script passed `-Dchaos.commitMode=PERIODIC_TRANSACTIONAL_PRODUCER`
# on three of its six rotation entries. NO SUCH PROPERTY EXISTS. Nothing in the tree reads it;
# every chaos scenario hardcodes its commit mode in its own source:
#
#     ChaosChurnStormIT ................... PERIODIC_CONSUMER_ASYNCHRONOUS
#     ChaosKeyOrderIT ..................... PERIODIC_CONSUMER_ASYNCHRONOUS
#     AbstractRevokeUnderWorkScenario ..... PERIODIC_CONSUMER_SYNC
#         (and so ChaosRevokeUnderWorkIT, ...CooperativeIT, ...DrainIT)
#
# The flag was accepted and silently ignored, so every cycle ran the hardcoded mode while the
# cycle directory name, the tally and the summary all said TRANSACTIONAL. That is worse than not
# running the hunt: it manufactures an eight-hour artefact asserting the transactional mode was
# tortured clean, which is the exact class of measurement error this whole investigation keeps
# tripping over. The mode labels are gone; each cycle now RECORDS THE MODE IT OBSERVED in the log
# rather than the mode somebody hoped for.
#
# Plumbing a real property is NOT a one-line change, which is why it is not done here.
# AbstractRevokeUnderWorkScenario's SYNC value is load-bearing and says so in a comment - sync
# commits sharpen the revoke-vs-commit lock contention that IS the confluentinc#857 recipe. Making
# that scenario transactional changes what the experiment is, per scenario, and that decision is
# adjacent to the one docs/inflight/bug-857-transactional-revoke-wait.md says not to settle alone.
#
# WHY THE ROTATION STILL DOES NOT HUNT IT - and this reason CHANGED on 2026-09-01, so read it
# rather than the version you may remember. It used to be that no chaos scenario existed for that
# mode at all: `grep -rl PERIODIC_TRANSACTIONAL_PRODUCER` over the chaostests package returned
# NOTHING, so no flag or rotation entry could have reached it, and the 214-cycle run of 2026-08-29
# confirmed it from the other side - 129 SYNC, 85 ASYNC, zero transactional.
#
# A VEHICLE NOW EXISTS: ChaosRevokeUnderWorkTransactionalIT overrides commitMode() to return
# PERIODIC_TRANSACTIONAL_PRODUCER, and it is reachable by name:
#
#     bin/torture-overnight.sh --scenario ChaosRevokeUnderWorkTransactionalIT
#
# What is still true is the sentence at the top of this block: the DEFAULT ROTATION does not run
# it, so an ordinary overnight run is not a transactional hunt and must not be reported as one.
# What is also still true is the harder half - that scenario's first green run established only
# that it RUNS. Whether repeating it hunts the unbounded revoke wait is an OPEN QUESTION, not
# something this script asserts by being able to select it.
#
# The vehicles that DO exist are outside the chaos group, which is why --groups exists:
#   RebalanceEoSDeadlockTest   untagged; its javadoc keeps the mode deliberately, because the
#                              producer transaction lock exists ONLY in that mode. This is the
#                              closest thing to a transactional revoke-wait reproducer in the tree.
#   TransactionTimeoutsTest    @Tag("transactions")
# So:  bin/torture-overnight.sh --groups '' --scenario RebalanceEoSDeadlockTest --cycles 20
# Whether repeating either of those actually hunts the unbounded revoke wait is an open question,
# NOT something this script asserts by being able to run them.
# ############################################################################################
#
# COMPLETENESS, and what it does and does not catch. ChaosScenarioBase.assertScenarioSlos calls
# ProgressProbe.ledger, which compares the producer-side expectedKeys against what the user
# function actually saw and emits `LEDGER_LOSS: N produced records never consumed`. That IS
# independent of PC's own offset accounting - it is the application's observation - so a record
# never delivered fails the run. What it does NOT catch is confluentinc#875's exact shape: the
# ledger asks "was it ever delivered" once, at the end, so a skip that self-heals inside the cycle
# is invisible to it. Catching that needs a TIME-BOUNDED claim (delivered within N seconds of
# production), which does not exist yet and is real design work, not a grep.
#
# WHY EVERY CHECK BELOW IS A COMPARISON, NOT A JUDGEMENT. Five times in one week a measurement
# error, not the system, was the answer: an inverted reproducer, a probe whose window never opened,
# a grep narrower than the question. So this script refuses to infer. It fails at startup if jstack
# is missing rather than writing empty dumps; it reads only failsafe reports NEWER than the cycle
# that produced them rather than trusting whatever is on disk; it reports the commit mode it saw in
# the log rather than the one it asked for; and it flags an empty siloed-log directory rather than
# packaging a tarball of nothing.
#
# NOT DOCKER YET. Deliberately: containers are the right long-term shape
# (docs/inflight/test-pc-soak-harness-architecture.md) but a script runs tonight.
#
# Usage:
#   bin/torture-overnight.sh                        # 8 hours, 30-minute cycles (the overnight run)
#   bin/torture-overnight.sh 8 30                   # same, positional (hours, cycle-minutes)
#   bin/torture-overnight.sh --cycles 1             # ONE cycle, then stop - the smoke test
#   bin/torture-overnight.sh --hours 0 --minutes 30 # a SHORT run - --minutes ADDS to --hours,
#                                                   # which defaults to 8, so bare --minutes 30 is 8h30m
#   bin/torture-overnight.sh --cycles 1 --scenario ChaosChurnStormIT --cycle-minutes 20
#
# Options:
#   --hours N           total wall-clock budget in hours (default 8)
#   --minutes N         total wall-clock budget in minutes, ADDED to --hours (default 0)
#   --cycle-minutes N   per-cycle budget before the watchdog dumps and kills (default 30)
#   --cycles N          stop after N cycles, whichever limit is reached first (0 = no limit,
#                       which is the default)
#   --scenario CLASS    run only this scenario instead of rotating (repeatable)
#   --groups G          JUnit tag filter (default "chaos"); "" selects untagged tests, which is how
#                       the transactional vehicles above are reached
#   --out DIR           output directory (default /tmp/torture-<stamp>, or $TORTURE_OUT)
#   --list              print what WOULD run - each scenario and the commit mode its source
#                       hardcodes - then exit. Honours --scenario, in either order.
#   -h, --help          this text
#
# Exit codes:  0 ran to completion   1 a cycle FAILED or HUNG   2 could not run (preflight)
set -u

D="$(git rev-parse --show-toplevel 2>/dev/null)" || { echo "not in a git repo" >&2; exit 2; }
CHAOS_SRC="$D/parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/chaostests"
# Search the whole integration tree, not just the chaos package: --groups lets a non-chaos vehicle
# be driven (the transactional ones are not chaos scenarios - see the header). Defined here rather
# than in preflight because --list resolves scenarios too, and runs earlier.
IT_SRC="$D/parallel-consumer-core/src/test-integration/java"

# The rotation. Scenario classes only - a commit mode here would be a wish, not an instruction.
# The trailing comment is the mode the class HARDCODES, kept for the reader; the run reports the
# mode it actually observed, so this comment going stale cannot corrupt a result.
ROTATION=(
  "ChaosChurnStormIT"                 # ASYNC
  "ChaosRevokeUnderWorkIT"            # SYNC
  "ChaosRevokeUnderWorkCooperativeIT" # SYNC
  "ChaosRevokeUnderWorkDrainIT"       # SYNC
  "ChaosKeyOrderIT"                   # ASYNC
)

usage() { sed -n '/^# Usage:/,/^# Exit codes:/p' "$0" | sed 's/^# \{0,1\}//'; }

# Collapse a stream of matches to one deduplicated comma-joined line. Used wherever a cycle reports
# a SET of observed values, so "two modes were seen" and "one mode was seen" read differently.
join_unique() { sort -u | tr '\n' ',' | sed 's/,$//'; }

list_rotation() {
    echo "rotation (scenario -> the commit mode its source hardcodes):"
    for s in "${ROTATION[@]}"; do
        # A scenario that extends the shared base inherits the base's hardcoded mode; one that does
        # not must not be credited with it. Reading both files unconditionally reported every class
        # as running BOTH modes, which is the kind of tidy-looking wrong answer this script exists
        # to stop producing.
        # --list runs BEFORE preflight, so a class that does not exist must say so rather than
        # share the `unknown` word with a class whose mode simply could not be parsed. Those are
        # different answers and only one of them is a typo.
        src="$(find "$IT_SRC" -name "$s.java" -print -quit 2>/dev/null)"
        if [ -z "$src" ]; then printf '  %-36s %s\n' "$s" "NO SUCH CLASS"; continue; fi
        srcs=("$src")
        if grep -q 'extends AbstractRevokeUnderWorkScenario' "$src" 2>/dev/null; then
            srcs+=("$CHAOS_SRC/AbstractRevokeUnderWorkScenario.java")
        fi
        m=$(grep -ohE '\.commitMode\(CommitMode\.[A-Z_]+' "${srcs[@]}" 2>/dev/null \
              | sed 's/.*CommitMode\.//' | join_unique)
        # `unknown` means the mode could not be read from source - which is the honest answer for a
        # non-chaos vehicle, and the same reason those cycles report modes=UNOBSERVED unless a PC
        # actually starts and logs one.
        printf '  %-36s %s\n' "$s" "${m:-unknown}"
    done
}

# EVERY missing-value case exits 2, not 1. `${2:?...}` exits 1, and 1 is documented as "a cycle
# FAILED or HUNG" - so a typo in an unattended `nohup ... &` invocation was indistinguishable from a
# night that found a real defect. 2 is this script's cannot-run code and the only honest answer.
need_value() { echo "$1 needs a value${2:+ - $2}" >&2; exit 2; }

ONLY=()
HOURS=8; MINUTES=0; CYCLE_MIN=30; MAX_CYCLES=0; OUT_OVERRIDE=""; TEST_GROUPS=chaos
DO_LIST=no
positional=0
while [ $# -gt 0 ]; do
    case "$1" in
        --hours)         [ $# -ge 2 ] || need_value --hours; HOURS="$2"; shift 2 ;;
        --minutes)       [ $# -ge 2 ] || need_value --minutes; MINUTES="$2"; shift 2 ;;
        --cycle-minutes) [ $# -ge 2 ] || need_value --cycle-minutes; CYCLE_MIN="$2"; shift 2 ;;
        --cycles)        [ $# -ge 2 ] || need_value --cycles; MAX_CYCLES="$2"; shift 2 ;;
        --scenario)      [ $# -ge 2 ] || need_value --scenario; ONLY+=("$2"); shift 2 ;;
        # `${2:?}` cannot guard this one: `--groups ''` is a REAL value - it selects untagged tests,
        # which is how the transactional vehicles are reached - so the missing-value case has to be
        # caught by counting arguments instead. Without this, a trailing bare `--groups` left
        # `shift 2` failing with $1 unchanged, and the parser span forever instead of erroring.
        --groups)        [ $# -ge 2 ] || need_value --groups "'' selects untagged tests, and is a real value"
                         TEST_GROUPS="$2"; shift 2 ;;
        --out)           [ $# -ge 2 ] || need_value --out; OUT_OVERRIDE="$2"; shift 2 ;;
        # Deferred, not printed here: --scenario replaces the rotation AFTER this loop, so listing
        # inline answered "what will run" with the built-in rotation even when --scenario was given.
        --list)          DO_LIST=yes; shift ;;
        -h|--help)       usage; exit 0 ;;
        -*)              echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
        *)
            positional=$((positional+1))
            case "$positional" in
                1) HOURS="$1" ;;
                2) CYCLE_MIN="$1" ;;
                *) echo "unexpected argument: $1" >&2; usage >&2; exit 2 ;;
            esac
            shift ;;
    esac
done

for n in "$HOURS" "$MINUTES" "$CYCLE_MIN" "$MAX_CYCLES"; do
    case "$n" in ''|*[!0-9]*) echo "expected a whole number, got: $n" >&2; exit 2 ;; esac
done
[ "$CYCLE_MIN" -gt 0 ] || { echo "--cycle-minutes must be at least 1" >&2; exit 2; }

if [ "${#ONLY[@]}" -gt 0 ]; then ROTATION=("${ONLY[@]}"); fi
[ "$DO_LIST" = no ] || { list_rotation; exit 0; }

# MAX_CYCLES is the string "0" when unset, never empty, so `${MAX_CYCLES:-unlimited}` never fired -
# every unlimited run announced itself and headed its SUMMARY.md with `cycles=0`, which reads as the
# opposite of what it means. The label is derived once and printed from one place.
cycles_label=unlimited
[ "$MAX_CYCLES" -eq 0 ] || cycles_label="$MAX_CYCLES"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${OUT_OVERRIDE:-${TORTURE_OUT:-/tmp/torture-$STAMP}}"
J="${JAVA_HOME:-/Users/astubbs/.sdkman/candidates/java/17.0.18-tem}"
BUDGET=$(( CYCLE_MIN*60 ))
TOTAL=$(( HOURS*3600 + MINUTES*60 ))
[ "$TOTAL" -gt 0 ] || { echo "total budget is zero - set --hours or --minutes" >&2; exit 2; }

# ---- Preflight. Every one of these is a silent failure if left to be discovered mid-run. --------
fail=0
note() { echo "PREFLIGHT: $*" >&2; }
[ -x "$J/bin/jstack" ] || { note "no jstack at $J/bin/jstack - the dump-before-kill IS the design, so this is fatal. Set JAVA_HOME to a full JDK."; fail=1; }
[ -x "$J/bin/java" ]   || { note "no java at $J/bin/java"; fail=1; }
[ -x "$D/mvnw" ]       || { note "no maven wrapper at $D/mvnw"; fail=1; }
for s in "${ROTATION[@]}"; do
    [ -n "$(find "$IT_SRC" -name "$s.java" -print -quit 2>/dev/null)" ] \
        || { note "no such scenario: $s (searched $IT_SRC)"; fail=1; }
done
[ "$fail" -eq 0 ] || { echo "preflight failed - nothing ran" >&2; exit 2; }
# Only after everything else passes, so a rejected run leaves no empty output directory behind.
mkdir -p "$OUT/cycles" "$OUT/dumps" 2>/dev/null || { note "cannot create $OUT"; exit 2; }
[ -w "$OUT" ] || { note "$OUT is not writable"; exit 2; }

TALLY="$OUT/tally.tsv"
: > "$TALLY"
START_TS=$(date +%s)
END=$(( START_TS + TOTAL ))
RPT="$D/parallel-consumer-core/target/failsafe-reports"
worst=0

# say() goes to BOTH the tally and the terminal. A harness that prints nothing for eight hours is
# indistinguishable from one that died in the first minute.
say() { printf '%s\t%s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$TALLY" >&2; }

# Every "how many cycles said X" question is this one line. It is a function because the verdict
# histogram is needed twice - in SUMMARY.md and on the terminal at the end - and two copies of the
# regex would let the two disagree about the same run.
tally_field() { grep -oE "$1" "$TALLY" | sort | uniq -c; }

# grep -c prints "0" AND exits 1 when there is no match, so the obvious `|| echo 0` appends a
# SECOND zero and the tally line splits in two. Ask once, default only when the file is absent.
count() { local n; n="$(grep -cE "$1" "$2" 2>/dev/null)"; printf '%s' "${n:-0}"; }

# ABSENT IS NOT ZERO. The correctness ledger runs inside assertScenarioSlos, which a probe violation
# throws before reaching - so on exactly the runs where a skipped offset would matter, the ledger
# never ran and `loss=0` meant "not measured". It read as clean. Anything that cannot tell absent
# from zero eventually reports the wrong one, so a counter whose source did not run says so.
counted() {   # counted <regex> <logfile> <ran-marker-regex>
    if grep -qE "$3" "$2" 2>/dev/null; then count "$1" "$2"; else printf 'unmeasured'; fi
}

# RECORDS, not lines. `LEDGER_LOSS: 37 produced records never consumed` is ONE line, so counting
# lines reported loss=1 for a 37-record loss - the number a reader would act on, off by the entire
# magnitude of the finding. Sums the captured figure instead, and keeps counted()'s absent-is-not-
# zero contract.
#
# The ran-marker is `[chaos-ledger] expected=`, not a bare `[chaos-ledger]`: TWO classes emit that
# prefix, and this is sound only because ProgressProbe.ledger happens to be called before
# KeyOrderLedger.checkIfRecording. Swap those two calls and `loss=0` silently means unmeasured
# again - sound by call order is not sound by construction, so the marker names the emitter.
sum_of() {   # sum_of <regex-with-one-capture-group> <logfile> <ran-marker-regex>
    grep -qE "$3" "$2" 2>/dev/null || { printf 'unmeasured'; return; }
    sed -nE "s/.*$1.*/\1/p" "$2" 2>/dev/null | awk '{ s += $1 } END { printf "%d", s+0 }'
}

# Did the backlog drain, or stay flat? THE one judgement this investigation keeps getting wrong, so
# the script makes it as a comparison and prints both numbers. -Dchaos.diagnoseStallRecovery keeps
# the run watching past a violation and logs `[diagnose] consumed=N/TARGET` samples; climbing means
# it drained (a timing proxy), flat means a genuine wedge. n/a means the diagnostic produced no
# samples, which is a third answer and never to be read as either of the first two.
# MEASURE ONLY THE SAMPLES AFTER THE VIOLATION FIRED. The diagnostic samples the ENTIRE wait, not
# just the tail, so comparing the first sample to the last says "CLIMBED" for every run that got
# anywhere - including one that stalled dead at 97386 and never moved again, because it climbed to
# 97386 first. That would report the one signature worth finding as the harmless one. The sample
# line carries `violations=N`, so the post-violation window is selectable without guessing.
# inFlight is reported alongside because a flat consumed line with work in flight is a fleet dwelling
# in the heavy tail, not a wedged one - that pair is what makes a flat line interpretable at all.
# MAGNITUDE, NOT JUST DIRECTION. `last > first` made ANY forward movement CLIMBED, so a wedge that
# completed ONE record in half an hour - 97386 to 97387 - filed as "the backlog recovered". Same
# inversion as the window bug, reached the other way: the first fix asked the right question of the
# wrong samples, this one asked a question too weak to separate the answers. Recovery is judged
# against the backlog that was actually outstanding when the detector fired, because "+400 records"
# means recovery with 500 left and means nothing with 50,000 left.
#
# DRAIN_MIN_RECOVERY_PCT is a CALIBRATION, not a truth, which is why the numbers behind it are
# printed on every verdict: `+2752of2614` is the recovery and the outstanding backlog it is measured
# against, so a reader can overrule the word without rerunning anything. A run that moves but not
# enough is CREEPING - its own word, because calling it FLAT would claim evidence of a wedge and
# calling it CLIMBED would claim evidence of recovery, and it is neither.
DRAIN_MIN_RECOVERY_PCT="${DRAIN_MIN_RECOVERY_PCT:-10}"
drain_verdict() {
    awk -v minpct="$DRAIN_MIN_RECOVERY_PCT" '
      /\[diagnose\]/ {
        c=""; v=""; f=""; t="";
        # consumed=N/TARGET - the target is what makes "outstanding" computable, so it is parsed
        # rather than assumed. Without it there is no denominator and no magnitude test.
        if (match($0, /consumed=[0-9]+\/[0-9]+/)) {
          split(substr($0, RSTART+9, RLENGTH-9), pair, "/"); c = pair[1]; t = pair[2];
        }
        if (match($0, /violations=[0-9]+/)) v = substr($0, RSTART+11, RLENGTH-11);
        if (match($0, /inFlight=[0-9]+/))   f = substr($0, RSTART+9,  RLENGTH-9);
        seen++;
        # A line the parse could not read is its own answer. Folding it into the pre-violation case
        # would let a log truncated by interleaved writers read as "nothing fired".
        if (c == "" || v == "") { malformed++; next }
        if (v+0 == 0) next;
        if (n == 0) { first = c; firstF = f; target = t }
        last = c; lastF = f; n++;
      }
      END {
        if (seen == 0)              { printf "n/a"; exit }               # diagnostic never sampled
        if (n == 0 && malformed)    { printf "unparseable(%d)", malformed; exit }
        if (n == 0)                 { printf "no-violation"; exit }      # nothing fired to judge
        if (n == 1)                 { printf "insufficient(n=1)"; exit } # one sample shows no trend
        delta = last - first;
        outstanding = target - first;
        detail = sprintf("%+d-of-%d,%s-to-%s,inFlight-%s-to-%s,n=%d", \
                         delta, outstanding, first, last, firstF, lastF, n);
        if (delta < 0)  { printf "RECEDED(%s)", detail; exit }   # backwards: the word must not say FLAT
        if (delta == 0) { printf "FLAT(%s)", detail; exit }
        if (outstanding > 0 && delta * 100 < outstanding * minpct) { printf "CREEPING(%s)", detail; exit }
        printf "CLIMBED(%s)", detail;
      }' "$1" 2>/dev/null
}

# Dump only OUR OWN process tree. `pgrep -f surefire|failsafe` is machine-wide, and this repo runs
# dozens of worktrees at once - it would file a stranger's stacks under this cycle.
descendants() {
    local kid
    for kid in $(pgrep -P "$1" 2>/dev/null); do printf '%s\n' "$kid"; descendants "$kid"; done
}

# SIGKILL, and children before the parent. This only ever runs on a build already judged wedged - the
# jstack dump has been taken by the time we get here, so there is nothing left to lose by killing
# hard. `pkill -P` sent SIGTERM, which is the signal LEAST likely to work on the process we suspect:
# a JVM deadlocked inside a shutdown hook never returns from it, and killing the parent first would
# reparent it onto init to survive the rest of the night. Re-derived rather than snapshotted, so a
# child spawned during the dump is still caught.
kill_tree() {
    local p
    for p in $(descendants "$1"); do kill -9 "$p" 2>/dev/null; done
    kill -9 "$1" 2>/dev/null
}

# jstack ON A DEADLINE. Attaching to a JVM that cannot reach a safepoint does not fail - it BLOCKS,
# and the call sits synchronously inside the watchdog. So the one input most likely to hang it is
# the exact input this harness exists to collect: a wedged JVM. Unbounded, one such process at hour
# one consumes the remaining seven hours and yields no dump, no verdict, no SUMMARY.md, no tarball
# and no DONE marker - a night's run destroyed by the thing it was hunting, silently.
#
# `timeout(1)` is not on macOS, so the deadline is background-poll-kill, the same shape as the
# cycle watchdog above. 0 = captured, 1 = failed, 2 = timed out (its own outcome: it means the JVM
# was too wedged even to be inspected, which is a finding, not an absence).
JSTACK_TIMEOUT_SECS="${JSTACK_TIMEOUT_SECS:-60}"
jstack_with_deadline() {   # jstack_with_deadline <pid> <outfile>
    local jp="$1" out="$2" jsp waited=0
    "$J/bin/jstack" -l "$jp" > "$out" 2>"$out.err" &
    jsp=$!
    while kill -0 "$jsp" 2>/dev/null; do
        if [ "$waited" -ge "$JSTACK_TIMEOUT_SECS" ]; then
            kill -9 "$jsp" 2>/dev/null; wait "$jsp" 2>/dev/null
            return 2
        fi
        sleep 1; waited=$((waited+1))
    done
    wait "$jsp" 2>/dev/null || return 1
    [ -s "$out" ] || return 1
    return 0
}

# Reports THIS cycle wrote. `find -newer <file>` is POSIX; -newermt is not portable to macOS.
# Lifted out of the cycle loop so it can be fixtured: the bug it exists for - a stale XML from an
# earlier cycle scoring a cycle that never ran as `passed` - is on the calibration list with no
# case, and bin/AGENTS.md asks a fixed bug for a test that goes red against the old code.
fresh_reports() {   # fresh_reports <reports-dir> <scenario> <marker-file>
    find "$1" -name "TEST-*$2*.xml" -newer "$3" 2>/dev/null | sort
}

# SUM ACROSS EVERY FRESH REPORT, never `tail -1`. Taking the last match let directory order decide
# the verdict: with two fresh reports, one carrying failures="1", the cycle scored `passed` and the
# failing report was copied into the cycle directory to be read by nobody. A cycle is FAILED if
# ANY of its reports failed, which is the only aggregation that cannot hide one.
aggregate_stats() {   # aggregate_stats <report-file>...
    [ "$#" -gt 0 ] || return 0
    grep -ohE 'tests="[0-9]+" errors="[0-9]+" skipped="[0-9]+" failures="[0-9]+"' "$@" 2>/dev/null | awk '
      { for (i = 1; i <= NF; i++) if (match($i, /"[0-9]+"/)) {
            k = substr($i, 1, index($i, "=") - 1);
            sum[k] += substr($i, RSTART+1, RLENGTH-2);
        } }
      END { if (NR) printf "tests=%d errors=%d skipped=%d failures=%d",
                          sum["tests"], sum["errors"], sum["skipped"], sum["failures"] }'
}

# The verdict ladder, lifted for the same reason as the two above: it publishes the run's headline
# number and had no test at all. Order matters - a hung cycle is hung whatever its reports say,
# because they may be from before the wedge.
verdict_for() {   # verdict_for <hung:yes|no> <dumps> <stats-string>
    local hung="$1" dumps="$2" stats="$3" t e s f
    if [ "$hung" = yes ] && [ "$dumps" -gt 0 ]; then printf 'HUNG-DUMP-CAPTURED'; return; fi
    if [ "$hung" = yes ]; then printf 'HUNG-NO-DUMP'; return; fi
    t="$(field_of tests "$stats")"; e="$(field_of errors "$stats")"
    s="$(field_of skipped "$stats")"; f="$(field_of failures "$stats")"
    if [ "$t" -eq 0 ]; then printf 'DID-NOT-RUN'; return; fi
    if [ "$e" -gt 0 ] || [ "$f" -gt 0 ]; then printf 'FAILED'; return; fi
    # An all-skipped cycle executed nothing and must not read as a pass. Not reachable today, but
    # this harness passes BOTH -Dfailsafe.failIfNoSpecifiedTests=false and -Dexcluded.groups= - the
    # two settings whose job is to make non-execution non-fatal - so the gap is one flag away.
    if [ "$s" -ge "$t" ]; then printf 'ALL-SKIPPED'; return; fi
    printf 'passed'
}

field_of() {   # field_of <name> <"tests=N errors=N ..."> - 0 when absent, so callers can do arithmetic
    local v; v="$(printf '%s' "$2" | sed -n "s/.*$1=\([0-9]\{1,\}\).*/\1/p")"
    printf '%s' "${v:-0}"
}

summarise() {
    local dumped
    dumped="$(find "$OUT/dumps" -type f -name "*.txt" 2>/dev/null | sort)"
    {   echo "# Torture run $STAMP"
        echo
        echo "Budget: ${HOURS}h${MINUTES}m total, ${CYCLE_MIN}m per cycle, cycles limit $cycles_label"
        echo "Rotation: ${ROTATION[*]}"
        echo
        echo "## Verdicts"
        tally_field 'END [A-Z-]+|END passed'
        echo
        echo "## Commit modes actually observed (NOT requested - see the header)"
        tally_field 'modes=[A-Z_,-]+'
        echo
        echo "## Did the stall-recovery diagnostic engage? (NOT-ENGAGED invalidates every drain= below)"
        tally_field 'diag=[A-Z-]+'
        echo
        echo "## Drain verdicts - did the backlog recover AFTER the detector fired?"
        echo "   CLIMBED recovered materially (a timing proxy) · CREEPING moved but negligibly ·"
        echo "   FLAT did not move · RECEDED went backwards - the last three are all wedge-shaped."
        echo "   n/a, no-violation, insufficient and unparseable are NOT verdicts: they each mean"
        echo "   the question could not be asked, and must never be read as a quiet CLIMBED."
        tally_field 'drain=[A-Za-z/-]+'
        echo
        echo "## The hunted signals"
        echo "   unmeasured is NOT zero - it means the source of that count never ran in that cycle."
        for f in timeouts loss dupes blocked silo; do
            printf '  %s:\n' "$f"
            tally_field "$f=[A-Za-z0-9-]+" | sed 's/^/   /'
        done
        echo
        echo "## Failing cycles, in full - the seeds here are the asset, copy them somewhere durable"
        # Every non-passing verdict, derived from the ladder rather than a second list of names that
        # could fall out of step with it - ALL-SKIPPED was added to verdict_for and would have been
        # missing from a hand-maintained alternation here.
        grep -E 'END ' "$TALLY" | grep -vE 'END (passed|ABANDONED)' || echo "   none"
        echo
        echo "## Cycles with a thread dump (look here first)"
        # One question, asked once. This used to `find` the dumps and then ask `ls -A` whether the
        # directory was empty - two different questions, so a cycle whose jstack FAILED left only a
        # .err file behind, printed no dump, and had its explanatory line suppressed by the .err.
        # The HUNG verdicts are what say whether a cycle overran; this section only says what was
        # captured, and must not answer the other question by implication.
        if [ -n "$dumped" ]; then printf '%s\n' "$dumped"
        else echo "   none captured - check the Verdicts section above for HUNG cycles"; fi
        echo
        echo "## Full tally"
        cat "$TALLY"
    } > "$OUT/SUMMARY.md"
    # Announced by the caller as `packaged:`, so a silent failure here would advertise a tarball that
    # is not there - and the artefacts are the whole point of an unattended run.
    tar -czf "$OUT.tar.gz" -C "$(dirname "$OUT")" "$(basename "$OUT")" \
        || say "PACKAGING FAILED - $OUT.tar.gz was not written; the cycle directories are still in $OUT"
}

# Ctrl-C still gets you a summary and a tarball - an interrupted eight-hour run has results in it.
interrupted=no
# shellcheck disable=SC2329  # invoked by the trap below, not by name
on_signal() { interrupted=yes; say "INTERRUPTED - summarising what ran"; }
trap on_signal INT TERM

say "TORTURE START budget=${HOURS}h${MINUTES}m cycle=${CYCLE_MIN}m cycles=$cycles_label out=$OUT"
say "hunting: commit-response timeouts, silent data skip. NOT transactional - see the script header."

cycle=0
while [ "$interrupted" = no ] && [ "$(date +%s)" -lt "$END" ]; do
    [ "$MAX_CYCLES" -eq 0 ] || [ "$cycle" -lt "$MAX_CYCLES" ] || break
    cycle=$((cycle+1))
    scenario="${ROTATION[$(( (cycle-1) % ${#ROTATION[@]} ))]}"
    seed=$(( (RANDOM<<15 | RANDOM) * 100003 + cycle ))
    cdir="$OUT/cycles/$cycle-$scenario"; mkdir -p "$cdir"
    log="$cdir/run.log"
    remaining=$(( END - $(date +%s) ))
    say "cycle=$cycle START scenario=$scenario seed=$seed budget=${CYCLE_MIN}m remaining=$((remaining/60))m"

    # The freshness marker. Failsafe writes into a shared target/ directory that is never cleaned
    # between cycles, and each scenario recurs every few cycles - so a cycle that never ran would
    # otherwise read the PREVIOUS cycle's XML for the same class and score itself `passed`. Reports
    # older than this marker are, by construction, not this cycle's.
    marker="$cdir/.cycle-start"; : > "$marker"

    JAVA_HOME="$J" "$D/mvnw" -f "$D/pom.xml" -Pci -pl parallel-consumer-core -am verify \
        -DskipUTs=true -Dincluded.groups="$TEST_GROUPS" -Dexcluded.groups= \
        -Dchaos.seed="$seed" -Dit.test="$scenario" \
        -Dpc.log.dir="$cdir/pc-logs" \
        -Dchaos.diagnoseStallRecovery=true \
        -Dfailsafe.failIfNoSpecifiedTests=false -Dcopyright.skip=true -Djacoco.skip=true \
        > "$log" 2>&1 &
    mvn_pid=$!

    # WATCHDOG. Dump before killing - a hang with no stack is a rumour, a hang with a stack is a bug.
    # Test liveness IMMEDIATELY BEFORE judging the budget, never before the sleep that precedes it.
    # Checking first and sleeping second means a build that exits during that 15s window is still
    # declared hung: a clean 1m48s pass was reported HUNG-NO-DUMP because it finished 9 seconds
    # before a 2-minute budget expired. The watchdog was manufacturing the failure it watches for.
    waited=0; hung=no; dumps=0; abandoned=no
    while :; do
        kill -0 "$mvn_pid" 2>/dev/null || break   # finished on its own - not hung, whatever the clock says
        if [ "$waited" -ge "$BUDGET" ]; then
            hung=yes
            say "cycle=$cycle BUDGET EXCEEDED after ${waited}s - dumping our JVMs before kill"
            # The maven launcher itself is included, not just its descendants: it is a JVM, it is
            # the parent of the forked test JVM, and a wedge in the fork's parent is exactly the
            # kind of thing a dump is for. Excluding it left the root of the tree uninspected.
            for jp in "$mvn_pid" $(descendants "$mvn_pid"); do
                dumpfile="$OUT/dumps/cycle-$cycle-pid-$jp.txt"
                jstack_with_deadline "$jp" "$dumpfile"
                case $? in
                    0) dumps=$((dumps+1)); rm -f "$dumpfile.err" ;;
                    2) say "cycle=$cycle jstack TIMED OUT after ${JSTACK_TIMEOUT_SECS}s on pid $jp - that JVM is too wedged to inspect, which is itself a finding"
                       rm -f "$dumpfile" "$dumpfile.err" ;;
                    *) say "cycle=$cycle jstack FAILED on pid $jp - see $dumpfile.err"
                       rm -f "$dumpfile" ;;
                esac
            done
            kill_tree "$mvn_pid"
            break
        fi
        # The interrupt check goes AFTER the sleep, deliberately: the liveness test above must stay
        # immediately before the budget judgement, and anything inserted between them reopens the
        # HUNG-NO-DUMP bug. Killing the build before breaking is what makes the trap's "summarising
        # what ran" true - without it the `wait` below blocks until this cycle ends on its own.
        sleep 15; waited=$((waited+15))
        if [ "$interrupted" != no ]; then
            abandoned=yes
            kill_tree "$mvn_pid"
            break
        fi
    done
    wait "$mvn_pid" 2>/dev/null; rc=$?

    fresh=()
    while IFS= read -r f; do [ -n "$f" ] && fresh+=("$f"); done \
        < <(fresh_reports "$RPT" "$scenario" "$marker")
    stats=""
    if [ "${#fresh[@]}" -gt 0 ]; then
        stats="$(aggregate_stats "${fresh[@]}")"
        mkdir -p "$cdir/failsafe-reports"
        cp "${fresh[@]}" "$cdir/failsafe-reports/" 2>/dev/null
    fi

    # An abandoned cycle gets no verdict - the interrupt killed the build, so its reports describe
    # a run that was stopped, not one that failed. Scoring it manufactured `DID-NOT-RUN rc=137`,
    # set the exit code to 1, and listed a fictitious failing cycle with a seed under "the seeds
    # here are the asset" - so Ctrl-C at hour seven of a clean run invented work for the morning.
    if [ "$abandoned" = yes ]; then
        say "cycle=$cycle END ABANDONED rc=$rc seed=$seed - interrupted mid-cycle, no verdict"
        break
    fi

    verdict="$(verdict_for "$hung" "$dumps" "$stats")"
    [ "$verdict" = passed ] || worst=1

    # What commit mode did PC actually boot with? Read it out of the run rather than asserting it.
    # ManagedPCInstance logs it on every instance start, at info on the integrationTests logger -
    # PC's own boot line is on bz.stub.parallelconsumer, pinned to warn, and never appears.
    # Both the console capture and the siloed harness stream are searched, so losing one still
    # answers the question. UNOBSERVED is a real verdict: it means the run died before any PC
    # started, and it must never be quietly read as "the mode I asked for".
    modeSrc=("$log")
    [ -f "$cdir/pc-logs/harness.log" ] && modeSrc+=("$cdir/pc-logs/harness.log")
    modes="$(grep -ohE 'commitMode=[A-Z_]+' "${modeSrc[@]}" 2>/dev/null | sed 's/commitMode=//' | join_unique)"
    [ -n "$modes" ] || modes=UNOBSERVED

    # The siloed streams are half the point of the tarball. An empty directory here means
    # -Dpc.log.dir did nothing, and a morning review would open a tarball of nothing.
    silo=ok
    if [ ! -d "$cdir/pc-logs" ] || [ -z "$(ls -A "$cdir/pc-logs" 2>/dev/null)" ]; then silo=EMPTY; fi

    # Did the diagnostic actually engage? A flag that is accepted and ignored is this project's
    # signature failure - `-Dchaos.commitMode` was one, and `-Dchaos.diagnoseStallRecovery` itself
    # was another until it was lifted to ChaosScenarioBase. So the run is asked, not assumed.
    if grep -q 'chaos.diagnoseStallRecovery ACTIVE' "$log" 2>/dev/null; then diag=ACTIVE; else diag=NOT-ENGAGED; fi

    # `blocked=` reads THE DUMPS, not the build log. It greps jstack's own header line, and jstack
    # writes to $OUT/dumps/*.txt - that string has never appeared in run.log and never will, so a
    # HUNG cycle whose dump contained a Java-level deadlock reported blocked=0. The harness was not
    # reading the one artefact it exists to produce.
    blocked=unmeasured
    if [ "$dumps" -gt 0 ]; then
        blocked="$(grep -lE 'Found one Java-level deadlock' "$OUT"/dumps/cycle-"$cycle"-pid-*.txt 2>/dev/null | wc -l | tr -d ' ')"
    fi

    say "cycle=$cycle END $verdict rc=$rc seed=$seed modes=$modes silo=$silo dumps=$dumps diag=$diag $(
        printf 'drain=%s timeouts=%s loss=%s dupes=%s blocked=%s' \
          "$(drain_verdict "$log")" \
          "$(counted 'Timeout waiting for commit response' "$log" 'commitMode=')" \
          "$(sum_of 'LEDGER_LOSS: ([0-9]+)' "$log" '\[chaos-ledger\] expected=')" \
          "$(sum_of 'LEDGER_DUPLICATES: ([0-9]+)' "$log" '\[chaos-ledger\] expected=')" \
          "$blocked")"
done

say "TORTURE COMPLETE cycles=$cycle"
summarise
[ -f "$OUT.tar.gz" ] && say "packaged: $OUT.tar.gz"
: > "$OUT/DONE"   # completion marker, so anything polling this run can tell finished from wedged

echo >&2
echo "=========================================================================" >&2
echo " TORTURE RUN COMPLETE - $cycle cycle(s), $(( ( $(date +%s) - START_TS ) / 60 )) minutes" >&2
tally_field 'END [A-Z-]+|END passed' >&2
echo " summary:  $OUT/SUMMARY.md" >&2
if [ -f "$OUT.tar.gz" ]; then echo " tarball:  $OUT.tar.gz" >&2
else echo " tarball:  NOT WRITTEN - packaging failed; the cycle directories are still in $OUT" >&2; fi
echo " dumps:    $OUT/dumps" >&2
echo "=========================================================================" >&2
exit "$worst"
