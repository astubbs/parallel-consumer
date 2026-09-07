#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Local development build - compile and run unit tests
# Usage: bin/build.sh [extra-maven-args...]
# Example: bin/build.sh -pl parallel-consumer-core -am
#
# NB the `-am` is not optional. A bare `-pl <module>` builds a subset of the reactor, which fails the
# enforcer's reactorModuleConvergence rule and skips the `generate-test-sources` phase that produces
# the Google Truth assertion classes (ManagedTruth and friends) - see docs/building.md, and
# astubbs#180 / confluentinc#861:
#   https://github.com/astubbs/parallel-consumer/issues/180
#   https://github.com/confluentinc/parallel-consumer/issues/861

set -euo pipefail

./mvnw --batch-mode clean package "$@"
