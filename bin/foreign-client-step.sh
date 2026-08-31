#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Runs ONE step of ONE foreign-language proxy-client module, and decides what an ABSENT toolchain
# means (astubbs#242).
#
# Usage: bin/foreign-client-step.sh --tool <executable> [--hello <language>] -- <command> [args...]
#
# THE COMMAND IS NOT DECLARED HERE, AND THAT IS THE POINT. Each client module's pom declares its own
# build and test commands in its four pc.foreign.* properties, and the clients aggregator's
# foreign-clients profile binds them to the compile and test phases. This script is the wrapper
# those commands are passed THROUGH, never a second copy of them - the same rule bin/build-client.sh
# states on feats/proxy-requirements, from the other end. A language command written in two places
# is a language command that drifts, and the version a developer runs then differs from the version
# that gates.
#
# WHAT AN ABSENT TOOLCHAIN MEANS, which is the whole reason this file exists. `-Dpc.foreignClients`
# opts the eight foreign modules into the reactor; toolchain ABSENCE is the normal state of every
# machine that is not that language's CI runner, and a developer who has Go but not Swift must be
# able to build the Go client without installing Swift. So:
#
#   - toolchain present  -> run the step; its exit code is this script's exit code
#   - toolchain absent   -> print a banner naming the language, the missing executable and the flag
#                           that would have made this red, and exit 0
#   - toolchain absent, and PC_FOREIGN_CLIENTS_STRICT is set -> exit 1
#
# STRICT IS WHAT KEEPS THE LENIENT DEFAULT HONEST. On a CI row the toolchain is provisioned by the
# row itself, so its absence is a provisioning bug and must be red, not a green skip - which is why
# .github/workflows/clients.yml sets PC_FOREIGN_CLIENTS_STRICT. The lenient default exists for the
# developer box only. Both halves are needed: lenient everywhere is
# docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md again; strict
# everywhere makes `-Dpc.foreignClients` unusable outside CI.
#
# WHAT A SKIP STILL COSTS, stated rather than hidden: Maven's reactor summary prints SUCCESS for a
# module whose step was skipped, because a skip is not a build failure and nothing in Maven's model
# says otherwise. The banner is what tells a reader; strict is what stops CI relying on the banner
# being read.
#
# --hello IS THE FIXTURE ASSERTION. With it, the command's stdout must be exactly the one line this
# script derives for that language, so "the program ran" and "the program produced the right bytes"
# are one check rather than a run whose output nobody looks at. The expected line is derived here,
# once, for all eleven languages - each module's program hardcodes its own copy, and this is what
# holds the eleven copies to one definition.
#
# Exit codes:
#   0  the step ran and passed, or the toolchain is absent and strict mode is off
#   1  the step FAILED - the command exited non-zero, or its output did not match the fixture
#   2  the toolchain is absent and PC_FOREIGN_CLIENTS_STRICT is set
#   3  usage error

set -euo pipefail

# THE ONE DEFINITION OF THE FIXTURE. Eleven programs print this line; nothing but this function
# decides what it says. Changing the wording here fails every language at once, which is the
# property that makes it a fixture rather than eleven unrelated print statements.
hello_fixture_line() { # <language>
    printf 'parallel-consumer-proxy-client hello fixture: %s' "$1"
}

die() { # <exit code> <message...>
    local code="$1"
    shift
    printf 'foreign-client-step: %s\n' "$*" >&2
    exit "$code"
}

usage() {
    printf 'Usage: bin/foreign-client-step.sh --tool <executable> [--hello <language>] -- <command> [args...]\n'
}

strict_mode() {
    case "${PC_FOREIGN_CLIENTS_STRICT:-}" in
        "" | 0 | false | FALSE | no | NO) return 1 ;;
        *) return 0 ;;
    esac
}

# The banner. Deliberately several lines and deliberately bracketed: it has to survive being read in
# a Maven log where every other line is also INFO, and it has to name the fix rather than only the
# fault.
report_absent() { # <tool> <label>
    local tool="$1" label="$2" verdict
    if strict_mode; then
        verdict='FAILING, because PC_FOREIGN_CLIENTS_STRICT is set - a CI row provisions its own toolchain, so absence here is a provisioning bug'
    else
        verdict='SKIPPED - set PC_FOREIGN_CLIENTS_STRICT=1 to make this red instead'
    fi
    printf '\n'
    printf '========================================================================\n'
    printf '  FOREIGN CLIENT STEP %s\n' "$verdict"
    printf '  module/step : %s\n' "$label"
    printf '  missing     : %s (not on PATH)\n' "$tool"
    printf '  nothing was built, compiled or asserted for this module\n'
    printf '========================================================================\n'
    printf '\n'
    # A GitHub job summary, when there is one. Absent locally, so the banner above is the only
    # channel there; on a runner this puts the same sentence where a reader looks first.
    if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
        printf '- **%s**: `%s` not on PATH - %s\n' "$label" "$tool" "$verdict" \
            >> "$GITHUB_STEP_SUMMARY"
    fi
}

TOOL=""
HELLO=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --tool)
            [ "$#" -ge 2 ] || { usage >&2; die 3 "--tool needs a value"; }
            TOOL="$2"
            shift 2
            ;;
        --hello)
            [ "$#" -ge 2 ] || { usage >&2; die 3 "--hello needs a language"; }
            HELLO="$2"
            shift 2
            ;;
        --)
            shift
            break
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            usage >&2
            die 3 "unexpected argument '$1' - the command goes after --"
            ;;
    esac
done

[ -n "$TOOL" ] || { usage >&2; die 3 "no --tool given; without it an absent toolchain cannot be told from a broken build"; }
[ "$#" -gt 0 ] || { usage >&2; die 3 "no command after --"; }

LABEL="${HELLO:-$TOOL}"

# EVERY MODULE WRITES UNDER target/, AND SEVERAL TOOLCHAINS WILL NOT CREATE IT. `go build -o
# target/hello` and `c++ -o target/hello` both fail with a bare "no such file or directory" naming
# the OUTPUT rather than the missing directory, which reads as a broken compile. Maven only creates
# ${project.build.directory} for packagings that produce something, and these wrapper modules are
# `pom`. One mkdir here rather than a prefix on eight declared commands: keeping target/ as the one
# output root is what lets `mvn clean` be enough for seven of the eight modules.
mkdir -p target

if ! command -v "$TOOL" > /dev/null 2>&1; then
    report_absent "$TOOL" "$LABEL"
    # Spelled as an `if` rather than `strict_mode && exit 2`: in an AND-list the guard's own failure
    # is exempt from `set -e` but the list's non-zero status is not reliably so, and the lenient
    # path would then exit 1 - the exact outcome it exists to avoid.
    if strict_mode; then
        exit 2
    fi
    exit 0
fi

# exec, so the step's own exit code IS this script's with nothing in between to reinterpret it.
if [ -z "$HELLO" ]; then
    exec "$@"
fi

EXPECTED="$(hello_fixture_line "$HELLO")"

# The command's stderr is left attached so a crashing program still explains itself; only stdout is
# the fixture. Failing the run here rather than letting `set -e` take it, so the message names the
# language rather than leaving a bare non-zero exit in the Maven log.
if ! ACTUAL="$("$@")"; then
    die 1 "$HELLO: the hello fixture program exited non-zero"
fi

if [ "$ACTUAL" != "$EXPECTED" ]; then
    printf 'foreign-client-step: %s: the hello fixture does not match\n' "$HELLO" >&2
    printf '  expected: %s\n' "$EXPECTED" >&2
    printf '  actual  : %s\n' "$ACTUAL" >&2
    exit 1
fi

printf '%s: hello fixture matched: %s\n' "$HELLO" "$ACTUAL"
