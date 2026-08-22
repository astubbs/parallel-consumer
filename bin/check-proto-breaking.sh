#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Breaking-change gate for the frozen proxy protocol schema (the language-proxy plan's freeze
# unit, astubbs#242 - the plan is docs/plans/2026-08-14-001-feat-language-proxy-plan.md).
#
# After the freeze the wire may only GAIN, never change: `buf breaking` in the FILE category
# forbids deleting, renumbering, retyping or renaming anything in proxy.proto. FILE rather than
# WIRE, deliberately: WIRE tolerates source-breaking changes (a renamed field is wire-identical),
# and the compatibility promise here covers the generated APIs ten client languages sit on, not
# only the bytes. The category is configured in parallel-consumer-proxy-protocol/buf.yaml; this
# script only points buf at the right before/after pair.
#
# Compares the working tree's schema against the frozen one on origin/master. Before the freeze
# commit reaches master there is nothing frozen to compare against; the check says so and passes,
# arming itself the moment the frozen schema lands on master.
#
# BOTH SIDES ARE FOUND BY SEARCHING, NOT BY A HARDCODED PATH. The first version looked for the
# proto at one path on master and took its absence as "not frozen yet" - so a post-freeze PR that
# renamed the module, updated the constant and deleted a frozen field re-entered the grace branch
# and passed green. Searching the baseline for the file wherever it lives means a rename cannot
# disarm the gate: only the file being genuinely absent from the baseline can, which is the one
# case the grace branch is for. Anything else - two copies, or a layout the module root cannot be
# derived from - exits 2 rather than passing, because a gate that cannot locate what it guards has
# not run. (The repo's green-without-having-run class:
# docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md.)
#
# Overrides, both for the self-test (bin/test-check-proto-breaking.sh):
#   PROTO_BREAKING_AGAINST      a complete buf input to compare against, replacing the discovery
#                               below. NOT a bare git ref: buf reads a bare "master" as a PATH and
#                               fails with 'had no .proto files'. The working forms are a directory
#                               holding a copy of the module, or the git form
#                               '.git#ref=<ref>,subdir=parallel-consumer-proxy-protocol'.
#   PROTO_BREAKING_BASELINE_REF the ref to search for the frozen schema (default origin/master).
#
# Requires buf (https://buf.build). CI installs it via bufbuild/buf-action; locally it is a
# managed toolchain - this script never downloads anything.
#
# Usage: bin/check-proto-breaking.sh
# Exit codes: 0 = compatible (or nothing frozen to compare against), 1 = breaking change,
#             2 = cannot run (buf missing, no usable baseline ref, or the schema cannot be located).

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# The schema's path INSIDE its module - the layout, not the module's own name, so the search below
# survives the module being renamed but reports loudly if the layout itself moves.
PROTO_IN_MODULE=src/main/proto/parallelconsumer/proxy/v1/proxy.proto
BASELINE_REF=${PROTO_BREAKING_BASELINE_REF:-origin/master}

# Module roots holding the schema, one per line. `sed -n s///p` rather than `grep -q`: the guard in
# bin/check-shell-sigpipe.sh, and a reader that exits early would take EPIPE under `pipefail`.
module_roots_in_worktree() {
    git ls-files | sed -n "s|^\(.*\)/${PROTO_IN_MODULE}\$|\1|p"
}
module_roots_in_ref() {
    git ls-tree -r --name-only "$1" | sed -n "s|^\(.*\)/${PROTO_IN_MODULE}\$|\1|p"
}

if ! command -v buf >/dev/null 2>&1; then
    echo "check-proto-breaking: buf is not installed, so the frozen schema cannot be verified." >&2
    echo "CI installs it via bufbuild/buf-action; locally it is an Ansible-managed toolchain." >&2
    exit 2
fi

mapfile -t here < <(module_roots_in_worktree)
if [[ ${#here[@]} -ne 1 ]]; then
    echo "check-proto-breaking: expected exactly one tracked ${PROTO_IN_MODULE}, found ${#here[@]}" \
        "(${here[*]:-none}); cannot tell which module to check, so this gate has NOT run." >&2
    exit 2
fi
MODULE=${here[0]}

AGAINST=${PROTO_BREAKING_AGAINST:-}
if [[ -z "$AGAINST" ]]; then
    if ! git rev-parse --verify --quiet "$BASELINE_REF" >/dev/null; then
        echo "check-proto-breaking: $BASELINE_REF is not available (shallow clone?); cannot pick a baseline." >&2
        exit 2
    fi
    mapfile -t frozen < <(module_roots_in_ref "$BASELINE_REF")
    if [[ ${#frozen[@]} -eq 0 ]]; then
        echo "check-proto-breaking: $BASELINE_REF carries no ${PROTO_IN_MODULE} at any path -" \
            "nothing frozen to compare against; passing. The gate arms when the freeze lands there."
        exit 0
    fi
    if [[ ${#frozen[@]} -gt 1 ]]; then
        echo "check-proto-breaking: $BASELINE_REF holds ${#frozen[@]} copies of the schema" \
            "(${frozen[*]}); the baseline is ambiguous, so this gate has NOT run." >&2
        exit 2
    fi
    if [[ ${frozen[0]} != "$MODULE" ]]; then
        echo "check-proto-breaking: the module moved since $BASELINE_REF (${frozen[0]} -> $MODULE);" \
            "comparing against it where it lives - a rename does not disarm the freeze."
    fi
    AGAINST=".git#ref=${BASELINE_REF},subdir=${frozen[0]}"
fi

echo "check-proto-breaking: buf breaking $MODULE --against $AGAINST"
# The status is captured from buf ITSELF, not read out of `$?` after an `if`: a compound `if` whose
# condition fails and which has no `else` returns 0, so the obvious spelling reports every breaking
# schema as "buf exited 0" and lands in the cannot-run branch below.
status=0
buf breaking "$MODULE" --against "$AGAINST" || status=$?

if [[ $status -eq 0 ]]; then
    echo "check-proto-breaking: OK - the schema only gained; nothing frozen was changed."
    exit 0
fi

# buf's own codes, mapped onto this gate's contract: 100 is "breaking changes found", and anything
# else is buf refusing to answer the question at all (a baseline it cannot read is the live case -
# a bare ref in PROTO_BREAKING_AGAINST exits 1 with "had no .proto files"). Those are not a verdict
# of compatible OR breaking, and reporting one as the other is how a gate lies in both directions.
if [[ $status -eq 100 ]]; then
    exit 1
fi
echo "check-proto-breaking: buf could not complete the comparison (exit $status), so this gate has" \
    "NOT run - the schema is neither confirmed compatible nor found breaking." >&2
exit 2
