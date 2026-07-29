#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run PIT mutation testing (advisory / non-gating). PIT is CPU-bound and runs mutation
# analysis across parallel minion JVMs, so it scales with cores - which is why it belongs
# on the self-hosted multi-core runner, not GitHub's 2-core gate.
#
# Threads default to the machine's CPU core count (1 minion per core - optimal for CPU-bound
# work; oversubscribing hurts). Override with PIT_THREADS=N. NOTE ON RAM: each minion gets
# -Xmx2g (the coverage minion OOMs at 1g), so peak heap is roughly threads x 2g - make sure the
# runner has the RAM (e.g. 12 cores => ~24g). Lower PIT_THREADS if the box is RAM-constrained.
#
# Usage: bin/ci-mutation-test.sh [extra-maven-args...]   (e.g. -Dverbose=true, -DtargetClasses=...)

set -euo pipefail

# Cross-platform core count (Linux nproc / macOS sysctl), default 2.
if command -v nproc >/dev/null 2>&1; then
  CORES=$(nproc)
elif command -v sysctl >/dev/null 2>&1; then
  CORES=$(sysctl -n hw.ncpu)
else
  CORES=2
fi
THREADS="${PIT_THREADS:-$CORES}"
echo "PIT: using ${THREADS} thread(s) (cores=${CORES}); minion heap -Xmx2g => ~$((THREADS * 2))g peak"

./mvnw --batch-mode -Pci test-compile org.pitest:pitest-maven:mutationCoverage \
  -Dlicense.skip \
  -Djacoco.skip=true \
  -DtargetClasses="io.confluent.parallelconsumer.internal.*" \
  -DtargetTests="io.confluent.parallelconsumer.*" \
  -DexcludedTestClasses="io.confluent.parallelconsumer.integrationTests.*" \
  -DjvmArgs=-Xmx2g \
  -DtimeoutConstant=30000 -DtimeoutFactor=3.0 \
  -Dthreads="${THREADS}" \
  -pl parallel-consumer-core -am \
  "$@"
