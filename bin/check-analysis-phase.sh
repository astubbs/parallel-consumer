#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# ANALYSIS THAT CLAIMS TEST CODE MUST RUN AFTER TEST CODE IS COMPILED.
#
# The root pom sets SpotBugs' `includeTests` to true - test code is main code for a concurrency
# library - but that setting is inert unless the analysis runs late enough to see it. Maven's
# lifecycle puts `process-classes` BEFORE `test-compile`, so a `spotbugs:check` bound there analyses
# `target/classes` and finds `target/test-classes` empty on a clean build. It prints
# "BugInstance size is 0" for test code it never read, and exits 0.
#
# Every CI build here is clean (`bin/ci-unit-test.sh` runs `clean test`, `bin/ci-build.sh` runs
# `clean verify`), so that was every CI build. Incrementally it was worse: the directory still held
# the PREVIOUS compilation, so the verdict described the source as it was one edit ago - a fixed
# finding kept being reported at its old line number, which is how this was found.
#
# MEASURED BOTH WAYS, on astubbs/parallel-consumer#333, before the phase moved. With an ignored
# `CountDownLatch.await(timeout)` restored in SessionEndTest: a clean build with the gate at
# `process-classes` reported 0 findings and BUILD SUCCESS; the same source with the gate at
# `process-test-classes` reported the finding and failed. The write-up is
# docs/solutions/workflow-issues/an-inert-analysis-config-reads-as-a-clean-codebase.md, which owns
# this class - a configuration that is present, correct-looking, and never reached.
#
# WHY A GATE RATHER THAN A COMMENT. That write-up's own status line says the effective-pom check
# "is a habit, not a gate", and this instance is what a missing gate costs: the setting was added
# deliberately, reviewed, and silently did nothing. A phase is a single greppable token, so the
# invariant is cheap to assert - and the failure it prevents is invisible by construction.
#
# Usage: bin/check-analysis-phase.sh
# Exit codes: 0 = conformant, 1 = violations found, 2 = cannot run.

set -euo pipefail

PY="$(for c in python3 python; do command -v "$c" >/dev/null 2>&1 && "$c" -c '' >/dev/null 2>&1 && { echo "$c"; break; }; done)"
if [ -z "$PY" ]; then
    echo "check-analysis-phase: no working Python 3 interpreter on PATH - cannot parse poms" >&2
    exit 2
fi

ROOT="${ANALYSIS_PHASE_ROOT:-$(git rev-parse --show-toplevel 2>/dev/null || echo .)}"

"$PY" - "$ROOT" <<'PY'
import sys, os, xml.etree.ElementTree as ET

root = sys.argv[1]
NS = '{http://maven.apache.org/POM/4.0.0}'

# Phases at or after test-compile in Maven's default lifecycle. A binding outside this set cannot
# have seen target/test-classes on a clean build, whatever the plugin's configuration says.
AFTER_TEST_COMPILE = {'test-compile', 'process-test-classes', 'test',
                      'prepare-package', 'package', 'pre-integration-test', 'integration-test',
                      'post-integration-test', 'verify', 'install', 'deploy'}

# Plugins whose analysis is configured to include test sources. Extend deliberately: an entry here
# is a claim that the plugin reads target/test-classes.
TEST_INCLUDING = {'spotbugs-maven-plugin'}

violations = []
checked = 0

for dirpath, dirnames, filenames in os.walk(root):
    dirnames[:] = [d for d in dirnames if d not in ('.git', 'target', 'node_modules')]
    if 'pom.xml' not in filenames:
        continue
    path = os.path.join(dirpath, 'pom.xml')
    try:
        tree = ET.parse(path)
    except ET.ParseError as e:
        print("check-analysis-phase: %s is not parseable XML (%s)" % (path, e), file=sys.stderr)
        sys.exit(2)
    for plugin in tree.iter(NS + 'plugin'):
        artifact = plugin.find(NS + 'artifactId')
        if artifact is None or artifact.text not in TEST_INCLUDING:
            continue
        for execution in plugin.iter(NS + 'execution'):
            goals = [g.text for g in execution.iter(NS + 'goal')]
            if 'check' not in goals:
                continue
            checked += 1
            phase = execution.find(NS + 'phase')
            eid = execution.find(NS + 'id')
            name = eid.text if eid is not None else '(unnamed)'
            if phase is None:
                violations.append("%s: execution '%s' binds no phase - `check` then runs at the "
                                  "goal's default, which is `verify` for spotbugs but is not "
                                  "stated here. Name the phase." % (path, name))
            elif phase.text not in AFTER_TEST_COMPILE:
                violations.append("%s: execution '%s' is bound to `%s`, which runs BEFORE "
                                  "test-compile - so target/test-classes is empty on a clean build "
                                  "and the analysis reports 0 findings for test code it never read. "
                                  "Use `process-test-classes`." % (path, name, phase.text))

if violations:
    for v in violations:
        print("ANALYSIS-PHASE: " + v)
    print("check-analysis-phase: %d violation(s) across %d execution(s)." % (len(violations), checked))
    sys.exit(1)

# Nothing in scope is a real answer, but it is not this repo's - the executions exist, so zero of
# them means the walk found nothing and the gate measured nothing.
if checked == 0:
    print("check-analysis-phase: found no test-including analysis executions to check - the walk "
          "measured nothing, which is not the same as a pass.", file=sys.stderr)
    sys.exit(2)

print("check-analysis-phase: %d analysis execution(s) run after test-compile." % checked)
PY
