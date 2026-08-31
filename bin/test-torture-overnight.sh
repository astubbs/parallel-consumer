#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Self-test for the parts of bin/torture-overnight.sh that decide what a run MEANT, plus the
# argument parsing that decides whether it runs at all - which is where this harness has been wrong
# three times. Nothing here needs Docker, a JDK or a broker: the reporting cases run against
# synthetic logs, and the parsing cases exit before any build starts.
#
# WHY THIS EXISTS, and why these cases. The drain verdict decides whether a detector firing is a
# harmless timing proxy (backlog CLIMBED after the violation) or the wedge the whole confluentinc#857
# investigation is hunting (FLAT). Getting it backwards reports the most interesting result in the
# family as the boring one, and nothing downstream would ever question it.
#
# The first implementation compared the FIRST diagnostic sample to the LAST. That reads "CLIMBED" for
# a run that raced to 97386, tripped the detector, and then never moved again - because it climbed to
# 97386 first. The `wedge` case below is exactly that shape and it is the regression this file
# guards: it FAILS against the first-to-last implementation and passes against the current one, which
# is the only thing that makes it evidence rather than decoration.
#
# Usage:  bin/test-torture-overnight.sh
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
TARGET="$HERE/torture-overnight.sh"
[ -f "$TARGET" ] || { echo "cannot find $TARGET" >&2; exit 2; }
# The harness losing its executable bit is a silent CI failure - check-all.sh invokes gates through
# `bash`, so nothing else would notice until an unattended run did not start.
[ -x "$TARGET" ] || { echo "$TARGET is not executable - chmod +x it" >&2; exit 2; }

# Pull each function under test out of the script rather than copying it, so the two cannot drift.
# Sourcing the script itself would run it - it launches maven.
#
# THE EXTRACTION MUST BE CHECKED, not just non-empty. `sed` runs to the next line that is exactly
# `}`, so indenting a closing brace by two spaces - an ordinary reformat - silently captures the
# NEXT function too, `eval` defines both, and the suite reports all green having tested something
# else. A target with no `^}$` after the marker captures hundreds of lines and starts executing the
# harness body. So: the capture must start with the function, end with a bare `}`, and contain
# exactly one top-level opener.
extract_fn() {   # extract_fn <name>
    local name="$1" body
    body="$(sed -n "/^$name() {/,/^}\$/p" "$TARGET")"
    [ -n "$body" ] || { echo "extract: $name() not found in $TARGET - renamed?" >&2; exit 2; }
    case "$(printf '%s\n' "$body" | head -1)" in "$name() {"*) ;; *) false ;; esac \
        || { echo "extract: $name() capture does not start at its definition" >&2; exit 2; }
    [ "$(printf '%s\n' "$body" | tail -1)" = "}" ] \
        || { echo "extract: $name() capture is not brace-terminated - is its closing } indented?" >&2; exit 2; }
    local openers; openers="$(printf '%s\n' "$body" | grep -c '^[a-z_][a-z_]*() {')"
    [ "$openers" -eq 1 ] \
        || { echo "extract: $name() capture spans $openers functions - a reformat has broken the range" >&2; exit 2; }
    printf '%s\n' "$body"
}

eval "$(extract_fn drain_verdict)"
eval "$(extract_fn verdict_for)"
eval "$(extract_fn field_of)"
eval "$(extract_fn aggregate_stats)"
eval "$(extract_fn fresh_reports)"
# drain_verdict reads this; the script defaults it at its own definition site, which the
# extraction above does not carry. Pinned here so the expectations are against a KNOWN
# calibration rather than whatever the environment happens to hold.
# shellcheck disable=SC2034  # consumed by the extracted drain_verdict, not by this file
DRAIN_MIN_RECOVERY_PCT=10

TMP="$(mktemp -d)" || { echo "cannot create a temp dir" >&2; exit 2; }
trap 'rm -rf "$TMP"' EXIT
pass=0; fail=0

# Build a log of [diagnose] samples: each argument is consumed:violations:inFlight
mklog() {
    local f="$TMP/$1.log"; shift
    : > "$f"
    local row c v i
    for row in "$@"; do
        c="${row%%:*}"; v="${row#*:}"; v="${v%%:*}"; i="${row##*:}"
        printf '[diagnose] run: consumed=%s/100000 started=1 inFlight=%s violations=%s observations=0 done=false\n' \
            "$c" "$i" "$v" >> "$f"
    done
    printf '%s' "$f"
}

# WHOLE-STRING, not a prefix. The numbers in the parentheses are not decoration: they are what a
# morning reviewer reads to decide whether a FLAT line is a wedge or a fleet in the heavy tail, and
# summarise()'s `drain=[A-Za-z/-]+` grep depends on the verdict word ending where it does. A prefix
# match would pass a verdict that had kept the right word and lost the evidence behind it.
expect() {   # expect <name> <expected-verdict> <logfile>
    local got; got="$(drain_verdict "$3")"
    if [ "$got" = "$2" ]; then pass=$((pass+1)); printf '  ok    %-14s -> %s\n' "$1" "$got"
    else fail=$((fail+1)); printf '  FAIL  %-14s -> got %s, wanted %s\n' "$1" "$got" "$2"; fi
}

echo "test-torture-overnight: drain_verdict"

# THE REGRESSION CASE. Climbs, trips the detector, then dead flat with work still in flight.
# First-to-last comparison calls this CLIMBED. It is the wedge.
expect wedge 'FLAT(+0-of-2614,97386-to-97386,inFlight-80-to-80,n=3)' \
    "$(mklog wedge 12000:0:50 97386:0:80 97386:1:80 97386:1:80 97386:1:80)"

# The drain shape from the 2026-08-28 answer: flat across the violation, then recovers past target.
expect drain 'CLIMBED(+5260-of-5122,94878-to-100138,inFlight-114-to-67,n=4)' \
    "$(mklog drain 12000:0:50 94878:1:114 94878:1:100 99665:1:80 100138:1:67)"

# A clean run samples throughout and never violates - there is nothing to judge, and saying
# CLIMBED here would launder a passing run into evidence about stalls.
expect clean no-violation "$(mklog clean 12000:0:50 100000:0:0)"

# One post-violation sample cannot show a trend. Distinct from FLAT, which claims evidence.
expect single 'insufficient(n=1)' "$(mklog single 12000:0:50 97386:1:80)"

# The diagnostic never engaged. Must never be reported as either outcome.
printf 'no diagnostic samples here\n' > "$TMP/none.log"
expect no-samples n/a "$TMP/none.log"

# THE MAGNITUDE REGRESSION. One record recovered across the whole post-violation window. Direction
# alone calls this CLIMBED - "the backlog recovered" - for a fleet that moved a single record in half
# an hour. It is a wedge, and CREEPING is the word that claims neither recovery nor a dead stop.
expect creeping 'CREEPING(+1-of-2614,97386-to-97387,inFlight-80-to-79,n=4)' \
    "$(mklog creeping 12000:0:50 97386:1:80 97386:1:80 97386:1:80 97387:1:79)"

# Backwards. FLAT would have printed a verdict contradicted by its own numbers.
expect receded 'RECEDED(-1386-of-2614,97386-to-96000,inFlight-80-to-80,n=2)' \
    "$(mklog receded 97386:1:80 96000:1:80)"

# A line the parser cannot read is its own answer - folding it into the pre-violation case would let
# a log truncated by interleaved writers report as "nothing fired, nothing to judge".
printf '[diagnose] run: consumed=97386/100000 started=1 inFli\n' > "$TMP/torn.log"
expect torn 'unparseable(1)' "$TMP/torn.log"

# ---- The verdict ladder -----------------------------------------------------------------------
#
# It publishes the run's headline number and had no test at all until now. `aggregate_stats` exists
# because `tail -1` let directory order decide: with two fresh reports, one carrying failures="1",
# the cycle scored `passed` and the failing report was copied into the cycle directory unread.
echo "test-torture-overnight: verdict ladder"

vexpect() {   # vexpect <name> <expected> <hung> <dumps> <stats>
    local got; got="$(verdict_for "$3" "$4" "$5")"
    if [ "$got" = "$2" ]; then pass=$((pass+1)); printf '  ok    %-14s -> %s\n' "$1" "$got"
    else fail=$((fail+1)); printf '  FAIL  %-14s -> got %s, wanted %s\n' "$1" "$got" "$2"; fi
}

vexpect hung-dumped   HUNG-DUMP-CAPTURED yes 2 'tests=1 errors=0 skipped=0 failures=0'
vexpect hung-nodump   HUNG-NO-DUMP       yes 0 'tests=1 errors=0 skipped=0 failures=0'
vexpect stale-report  DID-NOT-RUN        no  0 ''
vexpect zero-tests    DID-NOT-RUN        no  0 'tests=0 errors=0 skipped=0 failures=0'
vexpect failure       FAILED             no  0 'tests=2 errors=0 skipped=0 failures=1'
vexpect error         FAILED             no  0 'tests=2 errors=1 skipped=0 failures=0'
vexpect all-skipped   ALL-SKIPPED        no  0 'tests=2 errors=0 skipped=2 failures=0'
vexpect green         passed             no  0 'tests=2 errors=0 skipped=1 failures=0'

# aggregate_stats must SUM, so a failure in any report survives. Directory order decided this before.
mkrpt() { printf '<testsuite tests="%s" errors="%s" skipped="%s" failures="%s">\n' "$2" "$3" "$4" "$5" > "$TMP/$1"; }
# The FAILING report goes FIRST, deliberately. With it last, `tail -1` happens to pick it and the
# case passes against the very bug it guards - which is what the first draft of this case did.
mkrpt a.xml 1 0 0 1
mkrpt b.xml 1 0 0 0
agg="$(aggregate_stats "$TMP/a.xml" "$TMP/b.xml")"
if [ "$(verdict_for no 0 "$agg")" = FAILED ]; then
    pass=$((pass+1)); printf '  ok    %-14s -> %s -> FAILED\n' aggregate "$agg"
else
    fail=$((fail+1)); printf '  FAIL  %-14s -> %s did not reach FAILED\n' aggregate "$agg"
fi

# ---- Freshness selection ----------------------------------------------------------------------
#
# The calibration list carries "a stale failsafe XML scored a cycle that never ran as passed" with no
# case. This is it: an old report for the same class must not be selected by a newer marker.
echo "test-torture-overnight: freshness"
mkdir -p "$TMP/rpt"
: > "$TMP/rpt/TEST-x.ChaosChurnStormIT.xml"
sleep 1
: > "$TMP/marker"
stale="$(fresh_reports "$TMP/rpt" ChaosChurnStormIT "$TMP/marker")"
if [ -z "$stale" ]; then pass=$((pass+1)); printf '  ok    %-14s -> stale report not selected\n' stale-excluded
else fail=$((fail+1)); printf '  FAIL  %-14s -> selected %s\n' stale-excluded "$stale"; fi

sleep 1
: > "$TMP/rpt/TEST-x.ChaosChurnStormIT.xml"
if [ -n "$(fresh_reports "$TMP/rpt" ChaosChurnStormIT "$TMP/marker")" ]; then
    pass=$((pass+1)); printf '  ok    %-14s -> fresh report selected\n' fresh-included
else fail=$((fail+1)); printf '  FAIL  %-14s -> fresh report missed\n' fresh-included; fi

# ---- The fixture must match the Java that produces the real thing ------------------------------
#
# Every [diagnose] case above is a HAND COPY of a format string in ChaosScenarioBase. They agree
# today, and nothing pins them. Rename a field there and the harness reports `no-violation` -
# rendered in SUMMARY.md as "nothing fired, nothing to judge" - while this suite stays green and
# diag=ACTIVE still says the diagnostic ran. A dead-wedged run laundered into a passing one, with
# every instrument agreeing. So assert the real format string still contains the fields parsed here.
echo "test-torture-overnight: fixture pinning"
JAVA_SRC="$HERE/../parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/chaostests/ChaosScenarioBase.java"
if [ ! -f "$JAVA_SRC" ]; then
    fail=$((fail+1)); printf '  FAIL  %-14s -> cannot find ChaosScenarioBase.java\n' diagnose-format
else
    missing=""
    for token in '\[diagnose\]' 'consumed=\{\}/\{\}' 'inFlight=\{\}' 'violations=\{\}'; do
        grep -qE "$token" "$JAVA_SRC" || missing="$missing $token"
    done
    if [ -z "$missing" ]; then pass=$((pass+1)); printf '  ok    %-14s -> Java format string still carries every parsed field\n' diagnose-format
    else fail=$((fail+1)); printf '  FAIL  %-14s -> ChaosScenarioBase no longer emits:%s (drain_verdict parses fields that are gone)\n' diagnose-format "$missing"; fi
fi

# ---- Argument parsing -----------------------------------------------------------------------
#
# A DEADLINE, because the bug this guards is a HANG. A trailing `--groups` with no value left
# `shift 2` failing with $1 unchanged, and the parser span forever - so a plain invocation here
# would hang the self-test rather than fail it, which is the one outcome CI cannot act on.
# `--groups` cannot use the `${2:?...}` guard the other options use, because `--groups ''` is a
# real value: the empty tag selects untagged tests, which is how the transactional vehicles run.
echo "test-torture-overnight: argument parsing"

exits_within() {   # exits_within <name> <seconds> <expected-rc> <arg...>
    local name="$1" limit="$2" want="$3"; shift 3
    bash "$TARGET" "$@" >/dev/null 2>&1 &
    local p=$! waited=0 rc
    while [ "$waited" -lt "$limit" ]; do
        kill -0 "$p" 2>/dev/null || break
        sleep 1; waited=$((waited+1))
    done
    if kill -0 "$p" 2>/dev/null; then
        kill -9 "$p" 2>/dev/null; wait "$p" 2>/dev/null
        fail=$((fail+1)); printf '  FAIL  %-14s -> still running after %ss (it should have exited %s)\n' \
            "$name" "$limit" "$want"
        return
    fi
    wait "$p" 2>/dev/null; rc=$?
    if [ "$rc" -eq "$want" ]; then pass=$((pass+1)); printf '  ok    %-14s -> exit %s\n' "$name" "$rc"
    else fail=$((fail+1)); printf '  FAIL  %-14s -> exit %s, wanted %s\n' "$name" "$rc" "$want"; fi
}

# Assert the MESSAGE, not just the code. Exit 2 is this harness's universal cannot-run code, so
# `ok exit 2` is also what you get when the script never reached the option loop at all - running
# from outside a git repo, for instance. Only this path emits this sentence.
exits_within groups-novalue 10 2 --groups
msg="$(bash "$TARGET" --groups 2>&1 >/dev/null | head -1)"
case "$msg" in
    *"--groups needs a value"*) pass=$((pass+1)); printf '  ok    %-14s -> %s\n' groups-message "$msg" ;;
    *) fail=$((fail+1)); printf '  FAIL  %-14s -> got %s\n' groups-message "$msg" ;;
esac

# --list must describe what would ACTUALLY run. It used to print and exit from inside the option
# loop, before --scenario replaced the rotation, so it answered with the built-in five either way.
# The absent name must be one the DEFAULT rotation contains, or the assertion passes for the wrong
# reason: with ChaosChurnStormIT dropped from ROTATION, the old inline --list printed four scenarios
# and this case still said ok. So the contaminant is verified present in the target first.
if grep -qE '^ *"ChaosChurnStormIT"' "$TARGET"; then
    pass=$((pass+1)); printf '  ok    %-14s -> contaminant is in the default rotation\n' list-premise
else
    fail=$((fail+1)); printf '  FAIL  %-14s -> ChaosChurnStormIT is not in ROTATION, so list-scenario proves nothing\n' list-premise
fi

# Bounded, because this drives the same parser the deadline above exists for; and the exit code is
# checked rather than discarded, so printing the right text and then failing is not a pass.
listed=""; lrc=0
bash "$TARGET" --scenario ChaosKeyOrderIT --list > "$TMP/list.out" 2>"$TMP/list.err" &
lp=$!; n=0
while [ "$n" -lt 10 ] && kill -0 "$lp" 2>/dev/null; do sleep 1; n=$((n+1)); done
if kill -0 "$lp" 2>/dev/null; then
    kill -9 "$lp" 2>/dev/null; wait "$lp" 2>/dev/null; lrc=124
else
    wait "$lp" 2>/dev/null; lrc=$?; listed="$(cat "$TMP/list.out")"
fi
case "$lrc:$listed" in
    124:*) fail=$((fail+1)); printf '  FAIL  %-14s -> --list did not exit within 10s\n' list-scenario ;;
    0:*ChaosChurnStormIT*) fail=$((fail+1)); printf '  FAIL  %-14s -> --list ignored --scenario\n' list-scenario ;;
    0:*ChaosKeyOrderIT*)   pass=$((pass+1)); printf '  ok    %-14s -> --list honours --scenario\n' list-scenario ;;
    *) fail=$((fail+1)); printf '  FAIL  %-14s -> exit %s, output: %s\n' list-scenario "$lrc" "$listed" ;;
esac

# --list exists to print the scenario AND the commit mode its source hardcodes. The mode half was
# unasserted, so reintroducing the exact bug its own comment names - reading the base class's mode
# for every scenario - printed both modes for ChaosKeyOrderIT with this suite green. That is the
# manufactured-label class the whole script is about, one screen from where it is described.
case "$listed" in
    *"PERIODIC_CONSUMER_ASYNCHRONOUS,PERIODIC_CONSUMER_SYNC"*|*"PERIODIC_CONSUMER_SYNC,PERIODIC_CONSUMER_ASYNCHRONOUS"*)
        fail=$((fail+1)); printf '  FAIL  %-14s -> ChaosKeyOrderIT credited with BOTH modes\n' list-mode ;;
    *"ChaosKeyOrderIT"*"PERIODIC_CONSUMER_ASYNCHRONOUS"*)
        pass=$((pass+1)); printf '  ok    %-14s -> mode column names one mode, and the right one\n' list-mode ;;
    *)  fail=$((fail+1)); printf '  FAIL  %-14s -> unexpected mode column: %s\n' list-mode "$listed" ;;
esac

echo "test-torture-overnight: $pass passed, $fail failed"
[ "$fail" -eq 0 ] || exit 1
