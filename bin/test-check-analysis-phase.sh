#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Self-test for bin/check-analysis-phase.sh.
#
# THE NEGATIVE CONTROL IS THE POINT. The bug this gate exists for produced a GREEN build, so a
# self-test that only proves the gate passes on a good tree proves nothing about it: the broken
# state also looked like a pass. Case 2 reconstructs the exact pom shape that shipped - a
# `spotbugs:check` bound to `process-classes` - and asserts the gate goes red on it.
#
# Usage: bin/test-check-analysis-phase.sh
# Exit codes: 0 = all cases pass, 1 = a case failed.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GATE="$SCRIPT_DIR/check-analysis-phase.sh"
FIXTURES="$(mktemp -d)"
trap 'rm -rf "$FIXTURES"' EXIT

failures=0

pom() { # <dir> <phase-element-or-empty> <artifactId>
    local dir="$1" phase="$2" artifact="$3"
    mkdir -p "$dir"
    cat > "$dir/pom.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>
    <groupId>fixture</groupId>
    <artifactId>fixture</artifactId>
    <version>1</version>
    <build>
        <plugins>
            <plugin>
                <groupId>com.github.spotbugs</groupId>
                <artifactId>$artifact</artifactId>
                <executions>
                    <execution>
                        <id>spotbugs-gate</id>
                        $phase
                        <goals>
                            <goal>check</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
EOF
}

expect() { # <label> <expected-exit> <fixture-root>
    local label="$1" want="$2" root="$3" got=0
    ANALYSIS_PHASE_ROOT="$root" bash "$GATE" >/dev/null 2>&1 || got=$?
    if [ "$got" -eq "$want" ]; then
        echo "ok:   $label (exit $got)"
    else
        echo "FAIL: $label - expected exit $want, got $got"
        failures=$((failures + 1))
    fi
}

# 1. The fixed shape: bound after test-compile, so target/test-classes exists when it runs.
pom "$FIXTURES/good" "<phase>process-test-classes</phase>" spotbugs-maven-plugin
expect "process-test-classes passes" 0 "$FIXTURES/good"

# 2. THE NEGATIVE CONTROL - the shape that shipped and reported "BugInstance size is 0" for test
#    code it had never compiled. Red against this is the only evidence the gate works.
pom "$FIXTURES/bad" "<phase>process-classes</phase>" spotbugs-maven-plugin
expect "process-classes is refused" 1 "$FIXTURES/bad"

# 3. `compile` is earlier still, and must fail for the same reason rather than by name-matching one
#    known-bad phase.
pom "$FIXTURES/earlier" "<phase>compile</phase>" spotbugs-maven-plugin
expect "an even earlier phase is refused" 1 "$FIXTURES/earlier"

# 4. No phase at all: the binding then depends on the goal's default, which is not stated where a
#    reader looks. Refused, so the phase is always written down.
pom "$FIXTURES/nophase" "" spotbugs-maven-plugin
expect "an unstated phase is refused" 1 "$FIXTURES/nophase"

# 5. A tree with nothing in scope must exit 2, not 0 - a walk that measured nothing is not a pass,
#    and that distinction is what bin/check-all.sh counts in its own CANNOT column.
mkdir -p "$FIXTURES/empty"
expect "nothing in scope cannot run" 2 "$FIXTURES/empty"

# 6. A different plugin's execution is not this gate's business, so an early phase on one is not a
#    finding - the gate must key on the plugin that reads test classes, not on any `check` goal.
pom "$FIXTURES/other" "<phase>process-classes</phase>" some-other-maven-plugin
expect "another plugin at an early phase is out of scope" 2 "$FIXTURES/other"

# 7. `verify` is later than test-compile and legal - the rule is an ordering, not a single blessed
#    phase name.
pom "$FIXTURES/verify" "<phase>verify</phase>" spotbugs-maven-plugin
expect "verify passes" 0 "$FIXTURES/verify"

if [ "$failures" -gt 0 ]; then
    echo "check-analysis-phase self-test: $failures case(s) failed"
    exit 1
fi
echo "check-analysis-phase self-test: all cases passed"
