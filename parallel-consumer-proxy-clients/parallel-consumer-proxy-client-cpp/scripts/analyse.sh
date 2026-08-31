#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Static analysis for the C++ proxy client (astubbs#242): ONE recipe, run by the developer, by the
# image build, and by the clients workflow's cpp row.
#
# It is a script rather than a command line in .github/workflows/clients.yml because that file's own
# header asks for one - "PREFER THE MODULE'S OWN RECIPE over an invocation spelled out here: a
# command written twice is a command that drifts, and the version a developer runs then differs from
# the version that gates". The row's cpp entry started life as a spelled-out invocation because the
# module had no source yet; it now names this file.
#
# --check-level=exhaustive is NOT a flourish. Without it cppcheck emits
# `information: Limiting analysis of branches` on several of these files, and --error-exitcode=1
# fires on an information message as readily as on a finding - so the default level makes this check
# fail for a reason that is not a defect. Exhaustive analyses the branches instead of announcing
# that it did not, which is the honest way to make the message go away. The codebase is small enough
# that it costs under a second.
#
# Findings fail. There is no warning-only mode: a check whose output nobody has to act on is a check
# nobody reads.

set -euo pipefail

cd "$(dirname "$0")/.."

exec cppcheck \
    --enable=warning,style,performance \
    --check-level=exhaustive \
    --error-exitcode=1 \
    --inline-suppr \
    src
