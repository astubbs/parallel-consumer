#!/usr/bin/env bash
#
# Copyright (C) 2020-2026 Confluent, Inc.
#

# CI build script - run the full build and test suite
# Usage: bin/ci-build.sh [kafka-version]
# Example: bin/ci-build.sh 3.9.1
# If no version is specified, uses the default from pom.xml

set -euo pipefail

KAFKA_VERSION_ARG=""
if [ $# -ge 1 ]; then
  KAFKA_VERSION_ARG="-Dkafka.version=$1"
  echo "Building with Kafka version: $1"
else
  echo "Building with default Kafka version from pom.xml"
fi

./mvnw --batch-mode \
  -Pci \
  clean verify \
  $KAFKA_VERSION_ARG \
  -Dlicense.skip \
  -Dexcluded.groups=performance \
  -Dsurefire.rerunFailingTestsCount=2
