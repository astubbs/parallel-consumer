#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Self-test for bin/check-test-log-config.sh. Every negative case must go RED against the gate; a
# regression test that has never failed proves nothing (bin/AGENTS.md).
#
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
GATE="$PWD/bin/check-test-log-config.sh"

MODULES=(parallel-consumer-core parallel-consumer-vertx parallel-consumer-reactor parallel-consumer-mutiny)

pass=0
fail=0

# A conforming file. $1 is appended inside <configuration> so a case can inject a violation.
good_config() {
    cat <<XML
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender"/>
    <root level="\${pc.log.level:-warn}">
        <appender-ref ref="STDOUT"/>
    </root>
    <logger name="bz.stub.parallelconsumer" level="\${pc.log.level:-warn}"/>
    <logger name="org.apache.kafka" level="warn"/>
${1:-}
</configuration>
XML
}

# build_tree <dir> [extra-for-core] [replacement-body-for-core]
build_tree() {
    local dir=$1 extra=${2:-} body=${3:-}
    local m
    for m in "${MODULES[@]}"; do
        mkdir -p "$dir/$m/src/test/resources"
        if [[ $m == parallel-consumer-core && -n $body ]]; then
            printf '%s\n' "$body" > "$dir/$m/src/test/resources/logback-test.xml"
        elif [[ $m == parallel-consumer-core ]]; then
            good_config "$extra" > "$dir/$m/src/test/resources/logback-test.xml"
        else
            good_config > "$dir/$m/src/test/resources/logback-test.xml"
        fi
    done
}

# check <name> <expected: pass|fail> <extra> [body]
check() {
    local name=$1 expect=$2 extra=${3:-} body=${4:-}
    local dir status
    dir=$(mktemp -d)
    build_tree "$dir" "$extra" "$body"

    set +e
    TEST_LOG_CONFIG_ROOT="$dir" "$GATE" >/dev/null 2>&1
    status=$?
    set -e
    rm -rf "$dir"

    local got="pass"
    (( status != 0 )) && got="fail"

    if [[ $got == "$expect" ]]; then
        printf 'ok   %s (expected %s)\n' "$name" "$expect"
        pass=$((pass + 1))
    else
        printf 'FAIL %s: expected %s, got %s (exit %d)\n' "$name" "$expect" "$got" "$status" >&2
        fail=$((fail + 1))
    fi
}

# --- positive control -------------------------------------------------------
check "conforming tree" pass

# --- the drift this gate exists for ----------------------------------------
check "root pinned to info (the original drift)" fail "" \
    "$(good_config | sed 's/<root level="${pc.log.level:-warn}">/<root level="info">/')"
check "root pinned to debug" fail "" \
    "$(good_config | sed 's/<root level="${pc.log.level:-warn}">/<root level="debug">/')"
check "root default is not warn" fail "" \
    "$(good_config | sed 's/pc.log.level:-warn}">/pc.log.level:-debug}">/')"
check "product logger pinned to info" fail "" \
    "$(good_config | sed 's|<logger name="bz.stub.parallelconsumer" level="${pc.log.level:-warn}"/>|<logger name="bz.stub.parallelconsumer" level="info"/>|')"

# --- a local diagnostic that escaped ---------------------------------------
check "leftover active debug logger" fail '    <logger name="bz.stub.parallelconsumer.state.WorkManager" level="debug"/>'
check "leftover active trace logger" fail '    <logger name="bz.stub.parallelconsumer.state.ShardManager" level="TRACE"/>'

# --- shapes logback honors but a literal-minded grep missed (astubbs#324 review) ---
# All three were verified green against the unnormalized gate: single-quoted attributes, spaces
# around =, and a property switch whose DEFAULT is debug are all valid XML that logback applies.
check "single-quoted debug logger" fail "    <logger name='bz.stub.parallelconsumer.state.WorkManager' level='debug'/>"
check "spaced-equals debug logger" fail '    <logger name="bz.stub.parallelconsumer.state.WorkManager" level = "debug"/>'
check "property default of debug on an extra logger" fail '    <logger name="bz.stub.parallelconsumer.state.WorkManager" level="${pc.log.level:-debug}"/>'

# --- comment handling: these must NOT trip the gate ------------------------
check "commented-out debug logger" pass '    <!--    <logger name="bz.stub.parallelconsumer.state.WorkManager" level="debug"/>-->'
check "debug switch inside a multi-line comment" pass '    <!-- turn this on when hunting a stall:
         <root level="debug"/>
         <logger name="bz.stub.parallelconsumer" level="trace"/>
    -->'

# --- structural -------------------------------------------------------------
check "missing product logger" fail "" \
    "$(good_config | grep -v '<logger name="bz.stub.parallelconsumer" ')"
check "two active roots" fail '    <root level="${pc.log.level:-warn}"/>'

missing_dir=$(mktemp -d)
build_tree "$missing_dir"
rm -f "$missing_dir/parallel-consumer-mutiny/src/test/resources/logback-test.xml"
set +e
TEST_LOG_CONFIG_ROOT="$missing_dir" "$GATE" >/dev/null 2>&1
missing_status=$?
set -e
rm -rf "$missing_dir"
if (( missing_status != 0 )); then
    printf 'ok   missing module config (expected fail)\n'; pass=$((pass + 1))
else
    printf 'FAIL missing module config: expected fail, got pass\n' >&2; fail=$((fail + 1))
fi

# --- fixture big enough to reach a buffering failure (bin/AGENTS.md) -------
# A violation at the very END of a file with >64KiB of preceding content: the case that catches a
# `printf | grep -q` style early-exit bug, which small fixtures survive.
big_filler=$(for i in $(seq 1 3000); do
    printf '    <!--    <logger name="bz.stub.parallelconsumer.padding.Filler%05d" level="debug"/>-->\n' "$i"
done)
check "violation after >64KiB of commented padding" fail "$big_filler
    <logger name=\"bz.stub.parallelconsumer.state.WorkManager\" level=\"debug\"/>"
check "clean file with >64KiB of commented padding" pass "$big_filler"

printf '\n%d passed, %d failed\n' "$pass" "$fail"
(( fail == 0 ))
