#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Local development build - compile and run unit tests
# Usage: bin/build.sh [extra-maven-args...]
# Example: bin/build.sh -pl parallel-consumer-core

set -euo pipefail

./mvnw --batch-mode clean package "$@"
