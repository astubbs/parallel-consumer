#!/usr/bin/env bash
#
# Copyright (C) 2020-2026 Confluent, Inc.
#

# CI build script - run the full build and test suite
# Usage: bin/ci-build.sh [kafka-version]
# Example: bin/ci-build.sh 3.9.1

set -euo pipefail

KAFKA_VERSION="${1:-3.9.1}"

echo "Building with Kafka version: $KAFKA_VERSION"

./mvnw --batch-mode \
  -Pci \
  clean verify \
  -Dkafka.version="$KAFKA_VERSION" \
  -Dlicense.skip
