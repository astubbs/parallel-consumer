#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Static analysis for the Kotlin client - detekt, run LOCALLY.
#
# Usage: parallel-consumer-proxy-clients/parallel-consumer-proxy-client-kotlin/detekt.sh
#        (from anywhere; it resolves its own module directory)
#
# IT RUNS THE SAME VERSION AND THE SAME FLAGS AS THE MODULE'S CI ROW, deliberately: the row in
# .github/workflows/clients.yml fetches this exact jar from Maven Central, verifies this exact
# sha256, and runs `--input src --build-upon-default-config` from the module directory. A local
# green here is therefore evidence about the row, not merely about this machine - which is the
# point, because "CI covers it" is not evidence of anything.
#
# NO --config FILE, AND THAT IS THE CONSTRAINT, NOT AN OMISSION. The CI row passes no config, so
# detekt's DEFAULT ruleset is the contract this module's code satisfies. A local detekt.yml would
# make local and CI disagree in the one direction that matters - green here, red there. Where a
# default rule is genuinely wrong for a piece of code, the code carries an @Suppress with its
# reason, which is reviewable; a config file that quietly disables the rule everywhere is not.
#
# The version and hash are pinned together: Central artifacts are immutable once published, and
# the sha256 makes the download tamper-evident on top of that. Bumping the version means
# recomputing the hash with sha256sum on the freshly fetched jar, HERE and in the CI row - they
# are two copies on purpose (a workflow cannot source a shell variable from a module script), and
# they must move together.
#
# Negative control: this check has been proven able to fail. Introducing an unused private
# function into ParallelConsumerClient.kt turned it red (UnusedPrivateMember) and reverting turned
# it green again - see docs/inflight/clients/kotlin.md.

set -euo pipefail

DETEKT_VERSION="1.23.7"
DETEKT_SHA256="84beded283012cb2b38bcaef4996452fcd6069d2e9ca74b50eaa79e0ad21897e"

module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$module_dir"

jar="target/detekt-cli-${DETEKT_VERSION}-all.jar"
mkdir -p target

if [ ! -f "$jar" ]; then
    echo "Fetching detekt ${DETEKT_VERSION} from Maven Central..."
    curl -sSL --fail -o "$jar" \
        "https://repo1.maven.org/maven2/io/gitlab/arturbosch/detekt/detekt-cli/${DETEKT_VERSION}/detekt-cli-${DETEKT_VERSION}-all.jar"
fi

echo "${DETEKT_SHA256}  ${jar}" | sha256sum -c -

java="java"
if [ -n "${JAVA_HOME:-}" ] && [ -x "${JAVA_HOME}/bin/java" ]; then
    java="${JAVA_HOME}/bin/java"
fi

exec "$java" -jar "$jar" --input src --build-upon-default-config
