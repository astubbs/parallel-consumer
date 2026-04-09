#!/usr/bin/env bash
#
# Copyright (C) 2020-2026 Confluent, Inc.
#

# Local development build - compile and run unit tests
# Usage: bin/build.sh [extra-maven-args...]
# Example: bin/build.sh -pl parallel-consumer-core

set -euo pipefail

./mvnw --batch-mode clean package -Dlicense.skip "$@"
