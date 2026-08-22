#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Static analysis for the proxy protocol schema itself (astubbs#242; the per-language policy is in
# docs/client-static-analysis.md).
#
# WHY THE .PROTO IS A STATIC-ANALYSIS TARGET IN ITS OWN RIGHT: it is the one artifact every client
# is generated FROM. A defect here is not one language's bug, it is eleven - and it arrives in each
# of them as generated code nobody reads, wearing whatever shape protoc chose. `buf lint` catches
# the classes no downstream analyser can: an enum whose zero value is not the UNSPECIFIED default
# (so "unset" and "the first real value" become indistinguishable on the wire), a package that
# disagrees with its directory (so generated import paths differ per language), RPC request and
# response types shared between methods, field-presence conventions.
#
# THE RULE SET AND ITS TWO EXCEPTIONS LIVE IN parallel-consumer-proxy-protocol/buf.yaml, with the
# justification written beside them - STANDARD, less RPC_REQUEST_STANDARD_NAME and
# RPC_RESPONSE_STANDARD_NAME, because the single bidirectional stream reuses the session envelopes
# rather than wrapping them. This script does not restate that list; it points buf at the module
# and lets the config be the single authority. Do not add exceptions here.
#
# The schema lints clean today, and that is exactly why this gate exists: the freeze wave achieved
# it and nothing held it. A clean state nobody checks is a clean state with a date on it.
#
# Severity policy: any lint finding fails. There is no warn tier - see the doc.
#
# Requires buf (https://buf.build). CI installs it via bufbuild/buf-action; locally it is a managed
# toolchain (mise). This script never downloads anything: the module has no BSR dependencies, so
# the lint is entirely local.
#
# Usage: bin/check-proto-lint.sh
# Exit codes: 0 = clean, 1 = lint findings, 2 = cannot run (buf missing).

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

MODULE=parallel-consumer-proxy-protocol

if ! command -v buf >/dev/null 2>&1; then
    echo "check-proto-lint: buf is not installed, so the schema cannot be linted." >&2
    echo "CI installs it via bufbuild/buf-action; locally it is a managed toolchain (mise use -g buf)." >&2
    exit 2
fi

echo "check-proto-lint: buf lint $MODULE (rules and exceptions from $MODULE/buf.yaml)"
# Normalised to 1 rather than propagated: buf exits 100 on findings, and this script's caller reads
# 2 as "could not run". Letting 100 through would be harmless today and wrong the day someone tests
# for a specific code. Findings go to stderr/stdout uncaptured, so nothing is swallowed.
if ! buf lint "$MODULE"; then
    echo "check-proto-lint: FAILED - fix the schema, or justify an exception in $MODULE/buf.yaml" \
        "beside the two that are already there. Do not add exceptions to this script." >&2
    exit 1
fi
echo "check-proto-lint: OK - the schema every client is generated from lints clean."
