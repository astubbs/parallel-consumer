#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
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

# Group exclusions are HARDCODED here, not inherited from the pom default - enforced by
# QuarantinedAnnotationContractTest (pom inheritance once made the quarantine exclusion a
# silent no-op for unit tests). Keep in sync with the pom excluded.groups default.
./mvnw --batch-mode \
  -Pci \
  clean verify \
  ${KAFKA_VERSION_ARG:+"$KAFKA_VERSION_ARG"} \
  -Dexcluded.groups=performance,chaos,quarantined \
  -Dsurefire.rerunFailingTestsCount=2
