#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Guards the test logging harness in the four LIBRARY modules' logback-test.xml.
#
# Why a gate rather than a note: every failure in this class is silent. A committed `debug` default
# does not go red - it floods the CI log and slows the run, and the volume alone can break tests.
# Measured on ParallelEoSStreamProcessorTest (58 tests): `warn` emits 869 lines and passes;
# `-Dpc.log.level=debug` emits 469,202 and three tests fail on a 30s loop-cycle latch, while the same
# three pass at debug when selected alone. The console appender is synchronous, so a whole suite at
# debug starves the control loop. Nobody would attribute that to a logging default.
#
# The levels are driven by the `pc.log.level` system property so the harness is raised from the
# command line and there is nothing to remember to revert:
#
#     ./mvnw test -pl :parallel-consumer-core -am -Dpc.log.level=debug -Dtest=TheOneTest
#
# Editing the file to see output is what this replaces - that is how core drifted to root=info while
# vertx, reactor and mutiny all sat at warn. docs/testing.md, "Seeing test output", owns the how-to.
#
# NOT checked, deliberately: parallel-consumer-examples/*. Those are demonstration apps with tiny
# suites that legitimately run verbose, and one of them (example-core) carries a
# `logback-temp-test.xml` that logback never loads at all.
#
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

MODULES=(
    parallel-consumer-core
    parallel-consumer-vertx
    parallel-consumer-reactor
    parallel-consumer-mutiny
)

# Allow the self-test to point the gate at a fixture tree.
ROOT="${TEST_LOG_CONFIG_ROOT:-.}"

failures=0

fail() {
    printf 'FAIL %s\n' "$1" >&2
    failures=$((failures + 1))
}

# Strip XML comments (which span lines) so a commented-out switch is not read as active.
strip_comments() {
    awk '
        {
            line = $0
            out = ""
            while (length(line) > 0) {
                if (incomment) {
                    i = index(line, "-->")
                    if (i == 0) { line = ""; break }
                    line = substr(line, i + 3)
                    incomment = 0
                } else {
                    i = index(line, "<!--")
                    if (i == 0) { out = out line; line = ""; break }
                    out = out substr(line, 1, i - 1)
                    line = substr(line, i + 4)
                    incomment = 1
                }
            }
            print out
        }
    ' "$1"
}

for module in "${MODULES[@]}"; do
    file="$ROOT/$module/src/test/resources/logback-test.xml"

    if [[ ! -f $file ]]; then
        fail "$module: no src/test/resources/logback-test.xml"
        continue
    fi

    # NORMALIZE BEFORE MATCHING: logback accepts single-quoted attributes and spaces around '=',
    # and both evaded the literal level="..." greps below while still switching debug on
    # (astubbs#324 review, proven green-through). Attribute values here never legitimately carry
    # quotes or ' = ', so the rewrite is safe for this file class.
    active=$(strip_comments "$file" | sed "s/'/\"/g; s/[[:space:]]*=[[:space:]]*/=/g")

    # 1. The root logger must read the property, defaulting to warn.
    root_levels=$(grep -oE '<root[[:space:]]+level="[^"]*"' <<<"$active" | sed -E 's/.*level="([^"]*)"/\1/') || true
    if [[ -z $root_levels ]]; then
        fail "$module: no active <root level=...>"
    elif [[ $(wc -l <<<"$root_levels") -ne 1 ]]; then
        fail "$module: expected exactly one active <root>, found $(wc -l <<<"$root_levels")"
    elif [[ $root_levels != '${pc.log.level:-warn}' ]]; then
        fail "$module: <root> level is '$root_levels', expected \${pc.log.level:-warn} - raise the level with -Dpc.log.level, do not edit the default"
    fi

    # 2. The product logger must read the same property.
    pc_level=$(grep -oE '<logger[[:space:]]+name="bz\.stub\.parallelconsumer"[[:space:]]+level="[^"]*"' <<<"$active" | sed -E 's/.*level="([^"]*)"/\1/') || true
    if [[ -z $pc_level ]]; then
        fail "$module: no active <logger name=\"bz.stub.parallelconsumer\">"
    elif [[ $pc_level != '${pc.log.level:-warn}' ]]; then
        fail "$module: bz.stub.parallelconsumer level is '$pc_level', expected \${pc.log.level:-warn}"
    fi

    # 3. Nothing may be left switched on at debug/trace - that is a local diagnostic that escaped.
    # The property form with a debug/trace DEFAULT counts too: level="${pc.log.level:-debug}" is
    # debug on every run that does not pass the flag, which is exactly the committed-diagnostic
    # this gate exists to catch (astubbs#324 review).
    while IFS= read -r offender; do
        [[ -z $offender ]] && continue
        fail "$module: active logger left at debug/trace - revert before committing: $offender"
    done < <(grep -oiE '<(root|logger)[^>]*level="((debug|trace)|\$\{[^"]*:-(debug|trace)\})"' <<<"$active" || true)
done

if (( failures > 0 )); then
    printf '\n%d problem(s). See docs/testing.md, "Seeing test output".\n' "$failures" >&2
    exit 1
fi

printf 'test log config OK (%d modules)\n' "${#MODULES[@]}"
