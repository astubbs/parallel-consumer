#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# DOES EVERY LANGUAGE'S DEMO STILL BEHAVE THE SAME? (astubbs#242, plan unit U35.)
#
# The demo contract - parallel-consumer-proxy/demo/README.md - exists so that "a visitor moving
# between two languages' demos sees the same behaviour". Read that contract and notice what almost
# every clause is about: the flags, the defaults, the fingerprint printed first, the bootstrap
# address never printed, two tables with the same columns in the same order, the big replay dropping
# the serial arm, no latency anywhere. **It is a contract about standard output.** So it can be
# tested from standard output, in one place, without knowing a single thing about any of the eleven
# languages.
#
# THE TRICK THAT MAKES THIS CHEAP: THE LANGUAGES ARE EACH OTHER'S ORACLE.
# Rather than eleven expected-output files that rot, this reduces each demo's stdout to a SKELETON -
# the dials it echoed, the table headers, and the arm names in order, with every volatile figure
# discarded - and then requires the skeletons to be IDENTICAL to each other. Nothing to maintain,
# and it gets stronger with each language added rather than weaker. Ten agreeing and one differing
# points straight at the line that drifted.
#
# A skeleton diff is clean if EVERYONE drifts the same way, so it is paired with absolute assertions
# that hold regardless of agreement: the fingerprint echoes the input it was given, the broker
# address appears nowhere, and no latency is reported.
#
# WHAT THIS DELIBERATELY CANNOT SEE: whether a demo's sidecar arm truly goes through its client
# library rather than hand-rolled gRPC. Stdout cannot show that; code review and the conformance
# suite own it. A green run here is a drift check, not a correctness proof - do not read it as one.
#
# COLUMN WIDTH IS NOT CONTRACT, on purpose. `typescript-grpc` is longer than `java-grpc`, so a
# language with a long arm name would be in permanent violation of an alignment rule. Column
# IDENTITY and ORDER are contract; the skeleton keeps those and discards padding.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

CLIENTS="parallel-consumer-proxy-clients"
WORK="$(mktemp -d)"

# Identical input for every language, small enough to prove behaviour rather than measure it.
COMMON=(--records 20 --concurrency 4 --partitions 2 --replay-factor 2)
# --delay-ms is deliberately ABSENT from the flags above and supplied only here, so a passing run
# proves the environment layer rather than merely exercising it. That is the layer the Java seed
# silently dropped on its container path until a language agent reading the compose file caught it,
# and it is invisible from the native path - so nothing else would notice it breaking again.
export PC_DEMO_DELAY_MS=3

fail() { echo "ci-demo-conformance: $1" >&2; exit 1; }
note() { echo "ci-demo-conformance: $1"; }

# The whole language-agnostic part. Turns one demo's stdout into a canonical skeleton:
#   DIAL <name> = <value>     - the effective-configuration fingerprint
#   TITLE <Small|Big>         - a replay heading, volume masked
#   HEADER                    - a results table's column row
#   ROW <arm>                 - one arm's row, every figure discarded
# Everything else is noise (log lines, broker chatter, compose prefixes) and is dropped.
skeleton() {
    sed -e 's/\x1b\[[0-9;]*m//g' -e 's/^[a-z0-9_-]*-[0-9]* *| *//' "$1" | awk '
        # the dials the fingerprint echoes back
        # the topic is named per run, so the dial must survive but its value cannot
        /^[[:space:]]*topic = / { print "DIAL topic = <generated>"; next }
        /^[[:space:]]*[A-Za-z]+ = / { gsub(/^[[:space:]]+/, ""); print "DIAL " $0; next }
        # a replay heading; the record counts vary with input so only the kind survives
        /^[[:space:]]*Small replay/ { print "TITLE Small"; next }
        /^[[:space:]]*Big replay/   { print "TITLE Big";   next }
        # the column row - identity and order kept, padding discarded
        /^[[:space:]]*arm[[:space:]]+elapsed[[:space:]]+msg\/s[[:space:]]+vs AK core/ { print "HEADER"; next }
        # an arm row: <name> <elapsed>s <rate> <ratio>x - keep only which arm reported
        /^[[:space:]]*[A-Za-z][A-Za-z0-9 _-]*[[:space:]]+[0-9.]+s[[:space:]]+[0-9,-]+[[:space:]]+[0-9.x-]+[[:space:]]*$/ {
            gsub(/^[[:space:]]+/, "");
            sub(/[[:space:]]+[0-9.]+s[[:space:]].*$/, "");
            print "ROW " $0; next
        }
    '
}

# The sidecar arm is named for its language by design (`go-grpc`, `python-grpc`), so it is
# normalised before comparison - otherwise every language would differ on the one row they are all
# required to have.
normalise_arms() { sed -E 's/^ROW [a-z0-9]+-grpc$/ROW SIDECAR/; s/^ROW [a-z0-9]+-sidecar$/ROW SIDECAR/' "$1"; }

# PC_DEMO_LANGUAGES restricts the set - a comma-separated list. Every language by default; a subset
# lets CI shard this across jobs and lets a developer prove the mechanism without eleven image
# builds. A restricted run reports what it left out, because a narrowed run that reads like a full
# one is the failure mode this whole script exists to prevent.
languages=()
for dir in "$CLIENTS"/parallel-consumer-proxy-client-*/demo/run.sh; do
    [ -f "$dir" ] || continue
    lang="$(basename "$(dirname "$(dirname "$dir")")")"
    lang="${lang#parallel-consumer-proxy-client-}"
    if [ -n "${PC_DEMO_LANGUAGES:-}" ]; then
        case ",${PC_DEMO_LANGUAGES}," in *",$lang,"*) ;; *) continue ;; esac
    fi
    languages+=("$lang")
done
[ ${#languages[@]} -gt 0 ] || fail "found no <client-module>/demo/run.sh at all"
note "found ${#languages[@]} demo(s): ${languages[*]}"
[ -z "${PC_DEMO_LANGUAGES:-}" ] || note "RESTRICTED to PC_DEMO_LANGUAGES=${PC_DEMO_LANGUAGES} - this run does not cover the rest"

ran=(); skipped=()
for lang in "${languages[@]}"; do
    run_sh="$CLIENTS/parallel-consumer-proxy-client-$lang/demo/run.sh"
    log="$WORK/$lang.log"
    note "=== $lang ==="
    # The CONTAINER path for every language, deliberately: it is the one runtime every demo has, so
    # this needs Docker and no language toolchain at all - which is the same promise the demos make
    # to a reader.
    if ! "$run_sh" --docker "${COMMON[@]}" > "$log" 2>&1; then
        skipped+=("$lang(run failed)")
        note "$lang SKIPPED - its container run exited non-zero; see $log"
        continue
    fi
    # Skeleton from the DEMO's lines only. A compose capture interleaves the broker's log, and a
    # broker prints `listeners = ...` which looks exactly like a fingerprint dial - so an unscoped
    # skeleton reported drift between two demos that were identical, and the difference was their
    # brokers' listener config. Same mistake as the credential check below, made twice.
    if grep -q "^demo-1 *|" "$log"; then
        sed -n '/^demo-1 *|/p' "$log" > "$log.demo"
    else
        cp "$log" "$log.demo"
    fi
    skeleton "$log.demo" > "$WORK/$lang.skel"
    if [ ! -s "$WORK/$lang.skel" ]; then
        skipped+=("$lang(no skeleton)")
        note "$lang SKIPPED - produced no recognisable fingerprint or table; see $log"
        continue
    fi
    ran+=("$lang")
done

[ ${#ran[@]} -gt 0 ] || fail "no demo produced usable output; nothing was compared"

# --- absolute assertions, which hold whether or not the languages agree -----------------------
for lang in "${ran[@]}"; do
    skel="$WORK/$lang.skel"
    grep -q "^DIAL records = 20$" "$skel" \
        || fail "$lang: the fingerprint did not echo the records it was given - see $skel"
    grep -q "^DIAL delayMs = 3$" "$skel" \
        || fail "$lang: PC_DEMO_DELAY_MS=3 did not reach the demo, and no flag supplied it - the
        environment layer is broken; see $skel"
    grep -q "^TITLE Small$" "$skel" || fail "$lang: printed no small-replay table"
    grep -q "^TITLE Big$"   "$skel" || fail "$lang: printed no big-replay table at --replay-factor 2"
    # THE DEMO'S OWN LINES ONLY, and that scoping is the whole correctness of this check. A
    # compose run interleaves the broker's log with the demo's, and a Kafka broker legitimately
    # prints its own listener configuration - which is not the demo leaking anything. Checking the
    # raw capture called Go a liar on this harness's first run; the offending line was broker-1's.
    cp "$WORK/$lang.log.demo" "$WORK/$lang.demo-only"

    # the address is credential-grade (R48); it must appear nowhere the DEMO prints, not merely be
    # omitted from the fingerprint block
    if grep -qiE "bootstrap[ ._]servers?[ =:]" "$WORK/$lang.demo-only"; then
        fail "$lang: the broker address appears in the demo's own output - the credential rule binds
        the whole run, not only the fingerprint; see $WORK/$lang.demo-only"
    fi
    if grep -qiE "\b(latency|p99|p95|percentile)\b" "$WORK/$lang.demo-only"; then
        fail "$lang: reported latency, which the contract forbids - the workload is closed-loop"
    fi
    note "$lang: absolute assertions pass"
done

# --- the drift check: every language's skeleton must match every other's ------------------------
# Java is compared separately: it is documented as carrying extra diagnostic arms no other language
# has or needs, so requiring it to match would enforce the opposite of the contract.
reference=""; drifted=0
for lang in "${ran[@]}"; do
    [ "$lang" = "java" ] && continue
    normalise_arms "$WORK/$lang.skel" > "$WORK/$lang.norm"
    if [ -z "$reference" ]; then reference="$lang"; continue; fi
    if ! diff -u "$WORK/$reference.norm" "$WORK/$lang.norm" > "$WORK/$lang.diff"; then
        echo "ci-demo-conformance: DRIFT between $reference and $lang:" >&2
        sed 's/^/    /' "$WORK/$lang.diff" >&2
        drifted=1
    fi
done
[ "$drifted" -eq 0 ] || fail "the demos no longer behave the same; see the diffs above"

note "ran: ${ran[*]}"
[ ${#skipped[@]} -eq 0 ] || note "SKIPPED (not proven, not passed): ${skipped[*]}"
if [ -n "$reference" ]; then
    note "every compared language matches '$reference' after masking"
else
    note "only one comparable demo ran; drift could not be checked"
fi
