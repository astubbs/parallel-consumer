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

# --fail-at-end: report EVERY independent branch of the reactor in one run.
#
# The default stops at the first failing module and never builds the rest, so a 30-module reactor
# teaches you one problem per run - fix, push, wait, discover the next. `-fae` keeps going through
# everything that does not depend on the failure, and the reactor summary at the end lists each
# module's verdict. Only modules DOWNSTREAM of a failure are skipped, which is right: they could
# not have been built anyway.
#
# It does not soften the verdict. Maven still exits non-zero, and this script has `pipefail` set
# and pipes nothing - the shape that would lose the status is `mvn ... | tee`, where the pipeline
# takes tee's exit code unless pipefail is set. The workflows invoke these scripts directly
# (`run: bin/ci-unit-test.sh`, `run: time ${{ matrix.cmd }}`), with no pipe anywhere.
#
# Group exclusions are HARDCODED here, not inherited from the pom default - enforced by
# QuarantinedAnnotationContractTest (pom inheritance once made the quarantine exclusion a
# silent no-op for unit tests). Keep in sync with the pom excluded.groups default.
./mvnw --batch-mode \
  -Pci \
  --fail-at-end \
  clean verify \
  ${KAFKA_VERSION_ARG:+"$KAFKA_VERSION_ARG"} \
  -Dexcluded.groups=performance,chaos,quarantined
