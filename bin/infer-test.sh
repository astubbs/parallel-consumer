#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# RacerD (Meta's Infer) over parallel-consumer-core's main code. Non-gating, and run BOTH locally and
# by the `static: racerd` job in .github/workflows/maven.yml.
#
# WHY THIS EXISTS. Every other detector here needs to be told where to look: Error Prone's
# @GuardedBy only fires where somebody wrote the annotation, and the racing-double seam tests can
# only re-prove seams already found by hand. RacerD infers which locks protect which state from how
# the program actually uses them, so it reports races nobody named. It is the only tool in this repo
# that can.
#
# HOW IT RUNS IN CI, and what that costs. The `static: racerd` job on ubuntu-latest is the production
# caller - so treat runtime, cache and toolchain changes here as changes to a hosted lane, not to a
# local convenience script. It needs a ~220 MB toolchain Infer publishes only for linux-x86_64 and
# osx-arm64; the job caches the archive keyed on the pinned version and verifies it against a pinned
# SHA-256 on every run, cache hits included, because the cache is the mutable half.
#
# This header previously said the opposite - "WHY IT IS NOT A CI LANE ... no workflow calls it",
# following bin/lincheck-test.sh's precedent - and stayed that way in the very commit that added the
# job. An independent review caught it. The self-hosted highcpu runner is still the better long-term
# home, since the toolchain would persist between runs instead of being cached per job; that is a
# move, not a reason to describe the lane as absent.
#
# WHY THE JAVAC ROUTE AND NOT `infer run -- ./mvnw`. Infer's Maven integration runs the build under a
# JDK of its own choosing, and this project requires 17. Established with a two-arm control: the exact
# command Infer runs succeeds standalone at JDK 17, and Infer's captured Maven output carries JDK 24+
# warnings the JDK 17 run does not emit. Capturing javac directly sidesteps the wrapper that picks the
# JDK. That diagnosis cost a false "RacerD cannot run here" that stood for several hours.
#
# Exit codes: 0 clean, 1 findings, 2 cannot run. The 2 matters - "Infer is not installed" must never
# read as "no races", the same fail-closed contract bin/lib/node-gate.sh gives the node gates.

set -euo pipefail

INFER_BIN="${INFER_BIN:-infer}"
# RACERD_JDK is a deprecated alias, kept as a fallback for whoever already has it exported - the
# checker set widened past RacerD alone (see the header), so the knob got the name that describes
# what it actually configures.
JDK="${INFER_JDK:-${RACERD_JDK:-$HOME/.sdkman/candidates/java/17.0.18-tem}}"

# DRY RUN: read a canned report instead of running Infer, so the ratchet below can be tested without
# a 220 MB toolchain and a real analysis. Mirrors PIT_DRY_RUN_LOG in bin/ci-mutation-test.sh, which
# exists for the same reason - the interesting logic is the comparison, not the tool that feeds it.
#
# This is not a convenience. Before it existed the ratchet arms in bin/test-check-infer.sh could not
# run anywhere without an Infer run, so they did not run at all: the self-test printed a `skip` in CI
# and, when a report WAS supplied, an `ok:` that incremented the pass counter while asserting
# nothing, under a comment claiming four arms were exercised. The decisive arm - the same-count swap
# that a count ceiling waves through - has never been machine-checked until now.
#
# RACERD_DRY_RUN_REPORT is a deprecated alias, kept as a fallback - same reasoning as RACERD_JDK above.
INFER_DRY_RUN_REPORT="${INFER_DRY_RUN_REPORT:-${RACERD_DRY_RUN_REPORT:-}}"

if [ -z "$INFER_DRY_RUN_REPORT" ] && ! command -v "$INFER_BIN" > /dev/null 2>&1 && [ ! -x "$INFER_BIN" ]; then
    cat >&2 <<'MISSING'
infer-test: Infer is not installed - CANNOT RUN (this is not a pass).

  Download v1.3.0 from https://github.com/facebook/infer/releases, unpack it, and either put its
  bin/ on PATH or set INFER_BIN to the infer executable.

  CHECK THE DIGEST BEFORE YOU UNPACK IT. These are the two builds this project uses, each taken
  from two independent fetches of the pinned release URL; CI verifies the linux one on every run.
  A mismatch is not a download to retry - it is a different artefact than the one this project
  was tested against.

    linux-x86_64  02be62ba931cb43e2d42bbf7abf4bd9a410cf8464cce5453934e25c90a825850
    osx-arm64     60eccd231e27f2a3d65947ef75b9adcd1983528296bd1da6f67a6da02e22a96e

  sha256sum on Linux, shasum -a 256 on macOS.
MISSING
    exit 2
fi

if [ -z "$INFER_DRY_RUN_REPORT" ] && [ ! -x "$JDK/bin/javac" ]; then
    echo "infer-test: no JDK 17 javac at $JDK - CANNOT RUN. Set INFER_JDK." >&2
    exit 2
fi

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

if [ -n "$INFER_DRY_RUN_REPORT" ]; then
    if [ ! -f "$INFER_DRY_RUN_REPORT" ]; then
        echo "infer-test: INFER_DRY_RUN_REPORT=$INFER_DRY_RUN_REPORT does not exist - CANNOT RUN." >&2
        exit 2
    fi
    echo "infer-test: DRY RUN - reading a canned report from $INFER_DRY_RUN_REPORT instead of running Infer."
    mkdir -p "$work/infer-out"
    cp "$INFER_DRY_RUN_REPORT" "$work/infer-out/report.json"
    rc=0
else

echo "infer-test: resolving the compile classpath"
# SCOPED TO CORE, and `-am` is load-bearing twice over. Without `-pl` the goal runs for every module
# and each one OVERWRITES the single -Dmdep.outputFile, so the file left behind is the LAST module's
# classpath - which is how this lane first shipped: it analysed core's sources against the example
# module's dependencies plus a stale installed core jar, and reported a finding count from it. And
# `-pl` WITHOUT `-am` trips the enforcer's ReactorModuleConvergence rule, whose failure reads as
# "your change broke the build" rather than "you scoped the reactor wrong".
JAVA_HOME="$JDK" ./mvnw --batch-mode -Pci dependency:build-classpath \
    -pl parallel-consumer-core -am \
    -Dmdep.outputFile="$work/cp.txt" -q > "$work/cp.log" 2>&1 || {
    echo "infer-test: could not resolve the classpath - see $work/cp.log" >&2
    exit 2
}

find parallel-consumer-core/src/main/java -name '*.java' > "$work/srcs.txt"
count="$(wc -l < "$work/srcs.txt" | tr -d ' ')"
if [ "$count" -eq 0 ]; then
    echo "infer-test: NO SOURCES MATCHED - refusing to report success over an empty set" >&2
    exit 2
fi
echo "infer-test: analysing $count source file(s)"

mkdir -p "$work/classes"
set +e
# EVERY Java checker, not just RacerD. `--racerd-only` disabled two that are ON BY DEFAULT and
# support Java - pulse (null derefs, resource leaks) and starvation (deadlock via lock-order
# inversion) - plus every off-by-default one. The identity ratchet already keys on bug type, so
# widening the checker set needs no schema change: new bug types simply arrive as new identities,
# and anything already known stays known. What this buys is that a null deref introduced tomorrow
# fails the lane, where before it was invisible.
"$INFER_BIN" run --results-dir "$work/infer-out" \
    --annotation-reachability --bufferoverrun --cost --loop-hoisting --scope-leakage \
    -- \
    "$JDK/bin/javac" -cp "$(cat "$work/cp.txt")" -d "$work/classes" -proc:full "@$work/srcs.txt" \
    > "$work/infer.log" 2>&1
rc=$?
set -e

# Infer's own status, checked rather than merely printed. A javac failure part-way through the batch
# can still leave a syntactically valid but PARTIAL report.json, and a partial report undercounts -
# which against a ceiling reads as "fewer races than before" rather than "the analysis did not
# finish". Healthy runs exit 0 here; `infer run` does not use a nonzero code to signal findings.
if [ "$rc" -ne 0 ]; then
    echo "infer-test: infer exited $rc - CANNOT RUN, the analysis did not complete." >&2
    echo "  Any report it left is partial, and a partial report undercounts against the ceiling." >&2
    echo "  Log: $work/infer.log" >&2
    cp "$work/infer.log" "${TMPDIR:-/tmp}/infer-failure.log" 2>/dev/null || true
    exit 2
fi

fi

report="$work/infer-out/report.json"
if [ ! -f "$report" ]; then
    echo "infer-test: Infer produced no report - CANNOT RUN. Log: $work/infer.log" >&2
    cp "$work/infer.log" "${TMPDIR:-/tmp}/infer-failure.log" 2>/dev/null || true
    exit 2
fi

found="$(python3 -c "import json,sys; print(len(json.load(open(sys.argv[1]))))" "$report")"
echo "infer-test: $found finding(s) across all enabled checkers (infer exit $rc)"
cp "$report" "${TMPDIR:-/tmp}/infer-report.json"
echo "infer-test: full report copied to ${TMPDIR:-/tmp}/infer-report.json"

python3 -c "
import json,sys
for i in json.load(open(sys.argv[1])):
    print('  %s:%s  %s' % (i.get('file','?').split('/')[-1], i.get('line','?'), ' '.join(i.get('qualifier','').split())[:120]))
" "$report"

# AN IDENTITY SET, not a count. config/infer-known-findings.txt records which races are known, keyed on
# bug type plus class.method. A bare ceiling could not tell "one race fixed" from "one race swapped
# for a different one" - the total is unchanged either way - which an independent review caught and
# which is the same reports-green-while-it-changed class this lane exists to police.
#
# Fails BOTH ways on purpose: an identity that is new means a race was introduced, and one that no
# longer fires means somebody fixed a race and did not ratchet, which is how a set quietly stops
# meaning anything. Unset INFER_KNOWN to get report-only, which is what a local exploratory run
# wants.
known="${INFER_KNOWN:-$repo_root/config/infer-known-findings.txt}"
if [ -f "$known" ]; then
    current="$work/current.txt"
    python3 -c "
import json,re,sys,collections
c=collections.Counter()
for i in json.load(open(sys.argv[1])):
    proc=i.get('procedure','')
    m=re.search(r'([A-Za-z0-9_\$]+)\.([A-Za-z0-9_\$<>]+)\(', proc+'(')
    c[(i.get('bug_type','?'), m.group(1)+'.'+m.group(2) if m else proc)] += 1
for (bt,sig),n in sorted(c.items()): print(n, bt, sig)
" "$report" | LC_ALL=C sort > "$current"
    # BOTH sides sorted the same way, with the same collation. comm compares sorted streams and
    # silently produces nonsense otherwise - Python's tuple order and the shell's lexical order
    # disagree, and the first version of this check reported known findings as new because of it.
    expected="$work/expected.txt"
    grep -vE '^\s*(#|$)' "$known" | LC_ALL=C sort > "$expected" || true

    added="$(comm -13 "$expected" "$current" || true)"
    gone="$(comm -23 "$expected" "$current" || true)"

    if [ -n "$added" ]; then
        echo "infer-test: NEW race(s) - not in config/infer-known-findings.txt:" >&2
        echo "$added" | sed 's/^/    /' >&2
        exit 1
    fi
    if [ -n "$gone" ]; then
        echo "infer-test: these known races no longer fire:" >&2
        echo "$gone" | sed 's/^/    /' >&2
        echo "  If you fixed them, delete those lines from config/infer-known-findings.txt in the same" >&2
        echo "  change. A set nobody shrinks stops meaning anything." >&2
        exit 1
    fi
    echo "infer-test: all $found finding(s) are known, none new"
    exit 0
fi

[ "$found" -eq 0 ]
