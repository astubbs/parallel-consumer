#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Static analysis for the Go client - the ONE recipe, run identically by a developer and by the
# `clients` workflow's Go row (astubbs#242). Bug and bad-pattern detection only; formatting is not
# this script's job and `gofmt` is not run here.
#
# TWO TOOLS, because they catch different things and neither subsumes the other:
#
#   go vet       ships with the toolchain, no install, no pin to rot. Catches misuse the compiler
#                accepts: printf/format-arg mismatches, lost struct tags, copied locks, unreachable
#                code, loop-variable capture in closures.
#   staticcheck  honnef.co/go/tools - the mature third-party analyser, pinned as a `tool` directive
#                in go.mod exactly like the protobuf generators, so `go tool staticcheck` builds the
#                pinned version and NOTHING is installed globally. Catches the classes vet does not
#                look for at all: dead stores (SA4006), unused unexported code (U1000), nil-deref
#                and error-handling mistakes (SA5xxx), impossible conditions, misused stdlib APIs.
#
# Verified rather than assumed (astubbs#242): a dead store injected into Client.Session() left
# `go vet` exit 0 and made staticcheck exit 1 with SA4006 - which is why staticcheck is here and
# not merely nice to have.
#
# Severity policy: EVERYTHING here fails the build. There is no warn-only tier - a finding nobody
# has to act on is a finding everybody learns to scroll past. Suppress an individual false positive
# at the site with a `//lint:ignore <check> <reason>` comment, which forces the reason into the
# diff; do not widen the exclusion in this script.

set -euo pipefail

MODULE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$MODULE_DIR"

echo "==> go vet ./..."
go vet ./...

# `go tool` (Go 1.24+) builds the version pinned in go.mod. `go install` would resolve whatever is
# newest, which is how a lane starts failing on an unrelated day.
echo "==> staticcheck ./... (pinned in go.mod)"
go tool staticcheck ./...

echo "==> static analysis clean"
