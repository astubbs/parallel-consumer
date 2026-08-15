#!/bin/sh
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Static analysis for the Scala client, as one command - the same one the module's CI row runs.
#
# THE ANALYSER IS THE COMPILER, and that is the mature option rather than a shortcut. Scala's
# standalone analysers were weighed against the "only if mature, and it must run locally" filter
# every other client applied: scapegoat and WartRemover are compiler plugins that must be published
# for the exact Scala patch version in use, and neither tracks releases reliably enough to gate a
# build on - a Scala bump would then be blocked on someone else's release. Scalafix is a rewriter
# whose useful rule set here is mostly formatting. What is left is the compiler's own lint, which is
# maintained by definition, and -Werror is what turns it from advice into a gate.
#
# The flags live in the module's pom, in the scala-maven-plugin's <args>, so there is one copy of
# them: this script only runs the compile that applies them. That is the opposite arrangement from
# the Kotlin row's detekt invocation, and for a good reason - a compiler flag cannot be passed from a
# workflow without the build agreeing to it, so there is no local/CI skew to guard against here.
#
# Proven able to fail: an unused private method and an unused import each turned it red, and removing
# them turned it green.

set -eu

here="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$here/../../.." && pwd)"

# cd rather than -f alone: the root project's asciidoc-template step resolves
# src/docs/README_TEMPLATE.adoc against the PROCESS's working directory, so a Maven run started from
# a module directory fails on a file that is right where it always was. The suite runs this script
# with the module as its working directory, so the cd belongs here rather than in the caller.
cd "$repo_root"

exec ./mvnw --batch-mode \
    -pl :parallel-consumer-proxy-client-scala -am \
    -DskipTests \
    test-compile
