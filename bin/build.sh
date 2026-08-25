#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Local development build - compile and run unit tests
# Usage: bin/build.sh [extra-maven-args...]
# Example: bin/build.sh -pl parallel-consumer-core
#          bin/build.sh --fail-at-end        # every module's verdict, not just the first failure
#
# DELIBERATELY FAIL-FAST, unlike the CI lanes (bin/ci-build.sh and friends now pass --fail-at-end).
# The two loops optimise different things. CI runs once per push and the round trip is minutes, so
# learning one problem per run is the expensive mistake; here the build is in front of you and the
# expensive thing is the wait, so stopping at the first failure gets you back to an editor sooner -
# and the first failure is usually the cause of the ones behind it. It is one flag away when you
# want the whole picture, because every argument is forwarded to Maven.

# WARNING: `clean` below wipes the whole reactor's output. Scope it with -pl when other sessions
# are building - a bare `bin/build.sh -am` at the repo root has deleted work out from under
# concurrent agents.

set -euo pipefail

./mvnw --batch-mode clean package "$@"
