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
# Compares the working tree's schema against the frozen one on origin/master. Override the
# baseline with PROTO_BREAKING_AGAINST (any buf input: a git ref spec or a directory holding a
# copy of the module) - that override is also how the gate is proven able to say no, by comparing
# a deliberately mutilated working tree against a pristine copy. Before the freeze commit reaches
# master there is nothing frozen to compare against; the check says so and passes, arming itself
# the moment the frozen schema lands on master.
#
# Requires buf (https://buf.build). CI installs it via bufbuild/buf-action; locally it is a
# managed toolchain - this script never downloads anything.
#
# Usage: bin/check-proto-breaking.sh
# Exit codes: 0 = compatible (or nothing frozen to compare against), 1 = breaking change,
#             2 = cannot run (buf missing, or no usable baseline ref).

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

MODULE=parallel-consumer-proxy-protocol
PROTO_PATH=$MODULE/src/main/proto/parallelconsumer/proxy/v1/proxy.proto

if ! command -v buf >/dev/null 2>&1; then
    echo "check-proto-breaking: buf is not installed, so the frozen schema cannot be verified." >&2
    echo "CI installs it via bufbuild/buf-action; locally it is an Ansible-managed toolchain." >&2
    exit 2
fi

AGAINST=${PROTO_BREAKING_AGAINST:-}
if [[ -z "$AGAINST" ]]; then
    if ! git rev-parse --verify --quiet origin/master >/dev/null; then
        echo "check-proto-breaking: origin/master is not available (shallow clone?); cannot pick a baseline." >&2
        exit 2
    fi
    if ! git cat-file -e "origin/master:$PROTO_PATH" 2>/dev/null; then
        echo "check-proto-breaking: no frozen schema at origin/master:$PROTO_PATH yet -" \
            "nothing to compare against; passing. The gate arms when the freeze lands on master."
        exit 0
    fi
    AGAINST=".git#ref=origin/master,subdir=$MODULE"
fi

echo "check-proto-breaking: buf breaking $MODULE --against $AGAINST"
buf breaking "$MODULE" --against "$AGAINST"
echo "check-proto-breaking: OK - the schema only gained; nothing frozen was changed."
