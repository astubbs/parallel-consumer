#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-ossindex-audit.sh - the "did the vulnerability scan actually run" guard.
#
# The guard exists because ossindex-maven-plugin reports BUILD SUCCESS when it cannot reach the
# scanner, so a token expiry degrades silently to green-but-not-scanning. A guard for that which
# has never been seen to fire is decoration, so every leg gets a negative control here.
#
# Fixtures are the REAL shapes, taken from actual runs, not invented:
#   * a 401 leaves `{ }` in the exported report and prints "Failed to fetch component-reports"
#   * a successful audit writes {"reports": {...}} plus {"vulnerable": {...}} when it found things
#
# THE TWO REDS ARE DIFFERENT EXIT CODES, and this suite is what holds them apart:
#
#   exit 1 = the scan cannot be proven to have run   (the CHECK is broken)
#   exit 2 = the scan ran and found something        (the TREE has an untriaged advisory)
#
# Asserting the exact code, not merely "non-zero", is the point. A refactor that made findings exit 1
# would satisfy a non-zero assertion while destroying the distinction the job summary - and the
# maintainer's response to it - both depend on.
#
# Cases:
#    1. clean scan, no findings                                  -> pass (0)
#    2. clean scan, findings present                             -> FAIL (2)   <- the gate
#    3. 401: failure line in the log AND `{ }` reports           -> FAIL (1)   <- the real regression
#    4. token expiry shape with the log line ABSENT              -> FAIL (1)   <- leg 2 alone
#    5. no report exported at all                                -> FAIL (1)   <- leg 1+2
#    6. log claims 3 modules checked, only 2 reported            -> FAIL (1)   <- leg 3 alone
#    7. missing log file                                         -> FAIL (1)
#    8. vulnerable component with NO scored advisories           -> FAIL (2)   <- renderer crash
#    9. two advisories carrying no id                            -> FAIL (2), both counted
#   10. findings AND an unprovable scan at once                  -> FAIL (1)   <- precedence
#   11. the only advisory is excluded in the pom                 -> pass (0)   <- the escape hatch
#   12. one excluded advisory beside a live one                  -> FAIL (2), only the live one
#
# Cases 4 and 6 are the ones worth keeping among the fail-closed legs. Case 4 is the future failure
# this guard is really for: if Sonatype ever reworded the warning, leg 1 goes quiet and only the
# structural leg is left. Case 6 proves the coverage leg is load-bearing rather than decorative - it
# fires with no failure line in the log and every present report perfectly well-formed.
#
# Case 10 pins the precedence. Findings recovered from a scan that cannot be proven to have run are
# not evidence of anything, so "did not run" has to win - and must not be dressed up as a finding
# about the tree.
#
# Cases 11 and 12 are the ones the gate itself depends on: `excludeVulnerabilityIds` in the root pom
# is the only way out of a permanent red on a disputed or unfixable finding, and the exported report
# does NOT pre-filter by it. See the comment on case 11 - this was found on a live run, not reasoned
# about.
#
# Run: bin/test-check-ossindex-audit.sh   (CI runs it BEFORE the guard it protects)

set -uo pipefail

GUARD="$(cd "$(dirname "$0")" && pwd)/check-ossindex-audit.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

failures=0

assert() { # <description> <expected-exit> <actual-exit>
    if [ "$2" = "$3" ]; then
        echo "ok:   $1"
    else
        echo "FAIL: $1 (expected exit '$2', got '$3')"
        failures=$((failures + 1))
    fi
}

assert_contains() { # <description> <needle> <haystack>
    if grep -qF "$2" <<< "$3"; then
        echo "ok:   $1"
    else
        echo "FAIL: $1 (output did not contain '$2')"
        failures=$((failures + 1))
    fi
}

# A report as the plugin exports it when the audit succeeded. $1 = module dir, $2 = "vulnerable"
# to include a finding.
write_report() { # <tree> <module> [vulnerable]
    mkdir -p "$1/$2/target"
    if [ "${3:-}" = "vulnerable" ]; then
        cat > "$1/$2/target/ossindex-report.json" <<'JSON'
{
  "reports": {
    "org.example:safe:jar:1.0:compile": { "coordinates": "pkg:maven/org.example/safe@1.0" },
    "com.fasterxml.jackson.core:jackson-databind:jar:2.16.2:runtime": {
      "coordinates": "pkg:maven/com.fasterxml.jackson.core/jackson-databind@2.16.2",
      "vulnerabilities": [ { "id": "CVE-2026-54513", "cvssScore": 9.3 } ]
    }
  },
  "vulnerable": {
    "com.fasterxml.jackson.core:jackson-databind:jar:2.16.2:runtime": {
      "coordinates": "pkg:maven/com.fasterxml.jackson.core/jackson-databind@2.16.2",
      "vulnerabilities": [ { "id": "CVE-2026-54513", "cvssScore": 9.3 } ]
    }
  }
}
JSON
    else
        cat > "$1/$2/target/ossindex-report.json" <<'JSON'
{ "reports": { "org.example:safe:jar:1.0:compile": { "coordinates": "pkg:maven/org.example/safe@1.0" } } }
JSON
    fi
}

# The literal file a 401 leaves behind. Verified against a real expired-token run.
write_empty_report() { # <tree> <module>
    mkdir -p "$1/$2/target"
    printf '{ }' > "$1/$2/target/ossindex-report.json"
}

# $1 = tree, $2 = how many "Checking for vulnerabilities" lines, $3 = "failed" to add the 401 lines.
write_log() { # <tree> <modules-checked> [failed]
    local log="$1/audit.log" i
    : > "$log"
    for ((i = 0; i < $2; i++)); do
        printf '[INFO] --- ossindex:3.2.0:audit (audit-dependencies) @ module-%d ---\n' "$i" >> "$log"
        printf '[INFO] Checking for vulnerabilities; 15 artifacts\n' >> "$log"
        if [ "${3:-}" = "failed" ]; then
            printf '[WARNING] Failed to fetch component-reports\n' >> "$log"
            # shellcheck disable=SC2016  # a literal Java class name, not a shell expansion
            printf 'org.sonatype.ossindex.service.client.transport.Transport$TransportException: Unexpected response; status: HTTP/1.1 401 Unauthorized\n' >> "$log"
        fi
    done
    printf '[INFO] BUILD SUCCESS\n' >> "$log"
    echo "$log"
}

# Sets LAST_EC and LAST_OUT. Deliberately NOT `ec=$(run_guard ...)`: a command substitution runs
# in a subshell, so anything it assigns is lost - which is exactly how the first draft of this file
# failed on an unbound LAST_OUT.
run_guard() { # <log> <tree>
    LAST_EC=0
    LAST_OUT="$("$GUARD" "$1" "$2" 2>/dev/null)" || LAST_EC=$?
}

# ── 1. clean scan, nothing found ──────────────────────────────────────────────
t="$TMP/clean"; mkdir -p "$t"
write_report "$t" core
write_report "$t" vertx
log=$(write_log "$t" 2)
run_guard "$log" "$t"
assert "clean scan with no findings passes" 0 "$LAST_EC"
assert_contains "  ...and says so" "no vulnerable components" "$LAST_OUT"

# ── 2. clean scan WITH findings - the gate ────────────────────────────────────
# Exit 2, not 1: the scan worked, so this is a statement about the TREE. Asserting the exact code
# is what stops a later refactor from collapsing the two reds into one unreadable red.
t="$TMP/findings"; mkdir -p "$t"
write_report "$t" core vulnerable
write_report "$t" vertx
log=$(write_log "$t" 2)
run_guard "$log" "$t"
assert "findings on a proven scan fail the check" 2 "$LAST_EC"
assert_contains "  ...naming the advisory" "CVE-2026-54513" "$LAST_OUT"
assert_contains "  ...and its score" "9.3" "$LAST_OUT"
assert_contains "  ...saying the check itself is fine" "not a broken check" "$LAST_OUT"
if grep -qF "did not run" <<< "$LAST_OUT"; then
    echo "FAIL:   ...and NOT blaming the scan (output claimed the scan did not run)"
    failures=$((failures + 1))
else
    echo "ok:     ...and NOT blaming the scan"
fi

# ── 3. the real regression: 401 on every module ───────────────────────────────
t="$TMP/unauth"; mkdir -p "$t"
write_empty_report "$t" core
write_empty_report "$t" vertx
log=$(write_log "$t" 2 failed)
run_guard "$log" "$t"
assert "401 on every module fails the check" 1 "$LAST_EC"
assert_contains "  ...and says it is not a vulnerability finding" "not a vulnerability finding" "$LAST_OUT"

# ── 4. structural leg ALONE - the log line reworded away ──────────────────────
# If Sonatype renames that warning, leg 1 silently stops matching. The empty `reports` map must
# still be enough on its own, or the guard rots into the very defect it was written against.
t="$TMP/reworded"; mkdir -p "$t"
write_empty_report "$t" core
write_empty_report "$t" vertx
log=$(write_log "$t" 2)
run_guard "$log" "$t"
assert "empty reports fail even with NO failure line in the log" 1 "$LAST_EC"
assert_contains "  ...blaming the empty reports" "zero component reports" "$LAST_OUT"

# ── 5. nothing exported at all ────────────────────────────────────────────────
t="$TMP/nothing"; mkdir -p "$t"
log=$(write_log "$t" 0)
run_guard "$log" "$t"
assert "no exported report at all fails the check" 1 "$LAST_EC"

# ── 6. coverage leg ALONE - a module silently produced nothing ────────────────
t="$TMP/partial"; mkdir -p "$t"
write_report "$t" core
write_report "$t" vertx
log=$(write_log "$t" 3)
run_guard "$log" "$t"
assert "log says 3 modules checked but only 2 reported - fails" 1 "$LAST_EC"
assert_contains "  ...naming the shortfall" "a module produced nothing" "$LAST_OUT"

# ── 7. missing log ────────────────────────────────────────────────────────────
t="$TMP/nolog"; mkdir -p "$t"
run_guard "$t/does-not-exist.log" "$t"
assert "missing Maven log fails rather than passing vacuously" 1 "$LAST_EC"

# ── 8. a vulnerable component with nothing scored on it ───────────────────────
# OSS Index can list a coordinate under `vulnerable` while its `vulnerabilities` array is empty -
# an advisory it has not scored yet. That leaves the component's advisory map empty, which a bare
# `max()` in the renderer cannot take: it raises ValueError and, under `set -euo pipefail`, kills
# the guard with a traceback. A scan that DID run would be reported as an unexplained red - the
# guard failing in exactly the direction it exists to prevent.
t="$TMP/unscored"; mkdir -p "$t/core/target"
cat > "$t/core/target/ossindex-report.json" <<'JSON'
{
  "reports": { "org.example:thing:jar:1.0:compile": { "coordinates": "pkg:maven/org.example/thing@1.0" } },
  "vulnerable": {
    "org.example:thing:jar:1.0:compile": {
      "coordinates": "pkg:maven/org.example/thing@1.0",
      "vulnerabilities": []
    }
  }
}
JSON
log=$(write_log "$t" 1)
run_guard "$log" "$t"
assert "a vulnerable component with no scored advisories does not crash the guard" 2 "$LAST_EC"
assert_contains "  ...and still renders that component" "org.example:thing" "$LAST_OUT"

# ── 9. advisories with no id must not collapse onto one key ───────────────────
# Keyed on `id`, so two unidentified advisories both landed on "?" and the second overwrote the
# first - the component's advisory count, and the run total, silently under-reported real findings.
t="$TMP/unidentified"; mkdir -p "$t/core/target"
cat > "$t/core/target/ossindex-report.json" <<'JSON'
{
  "reports": { "org.example:thing:jar:1.0:compile": { "coordinates": "pkg:maven/org.example/thing@1.0" } },
  "vulnerable": {
    "org.example:thing:jar:1.0:compile": {
      "coordinates": "pkg:maven/org.example/thing@1.0",
      "vulnerabilities": [ { "cvssScore": 4.1 }, { "cvssScore": 7.7 } ]
    }
  }
}
JSON
log=$(write_log "$t" 1)
run_guard "$log" "$t"
assert "two unidentified advisories are counted separately, not collapsed" 2 "$LAST_EC"
assert_contains "  ...reporting both of them" "2 advisory(ies)" "$LAST_OUT"
assert_contains "  ...and keeping the higher score" "7.7" "$LAST_OUT"

# ── 10. both reds at once - "did not run" must win ────────────────────────────
# Findings recovered from a scan that cannot be proven to have happened are not evidence of
# anything. If this ever reported exit 2, a broken lane would be triaged as a dependency problem and
# nobody would go and look at the token.
t="$TMP/both"; mkdir -p "$t"
write_report "$t" core vulnerable
write_empty_report "$t" vertx
log=$(write_log "$t" 2 failed)
run_guard "$log" "$t"
assert "findings AND an unprovable scan report the broken scan, not the findings" 1 "$LAST_EC"
assert_contains "  ...and say so explicitly" "not a vulnerability finding" "$LAST_OUT"

# ── 11. an excluded advisory must not gate ────────────────────────────────────
# The pom's `excludeVulnerabilityIds` is the documented escape hatch, and it is the ONLY thing
# standing between this gate and a permanent red on a disputed or unfixable finding. The plugin
# honours it when choosing which components to call vulnerable, but still writes the excluded
# advisory into the exported `vulnerabilities` array AND into a top-level `excludedVulnerabilities`
# block. Observed on a live authenticated run: CVE-2026-54518 was excluded in the pom, logged under
# "Excluded vulnerabilities:", and present in `vulnerable` all the same. Gating on the raw array
# would go red on exactly the debt that was deliberately triaged, and no pom edit could clear it.
t="$TMP/excluded"; mkdir -p "$t/core/target"
cat > "$t/core/target/ossindex-report.json" <<'JSON'
{
  "reports": { "org.example:thing:jar:1.0:compile": { "coordinates": "pkg:maven/org.example/thing@1.0" } },
  "excludedVulnerabilities": [ { "id": "CVE-2026-14683", "cvssScore": 2.0 } ],
  "vulnerable": {
    "org.example:thing:jar:1.0:compile": {
      "coordinates": "pkg:maven/org.example/thing@1.0",
      "vulnerabilities": [ { "id": "CVE-2026-14683", "cvssScore": 2.0 } ]
    }
  }
}
JSON
log=$(write_log "$t" 1)
run_guard "$log" "$t"
assert "an advisory excluded in the pom does not fail the check" 0 "$LAST_EC"
assert_contains "  ...and the suppression is stated, not silent" "suppressed by" "$LAST_OUT"

# ── 12. exclusions must not swallow the advisory next to them ─────────────────
# The opposite error: filtering by component rather than by advisory id would drop a live finding
# because a *different* advisory on the same component happened to be excluded.
t="$TMP/partly-excluded"; mkdir -p "$t/core/target"
cat > "$t/core/target/ossindex-report.json" <<'JSON'
{
  "reports": { "org.example:thing:jar:1.0:compile": { "coordinates": "pkg:maven/org.example/thing@1.0" } },
  "excludedVulnerabilities": [ { "id": "CVE-2026-14683", "cvssScore": 2.0 } ],
  "vulnerable": {
    "org.example:thing:jar:1.0:compile": {
      "coordinates": "pkg:maven/org.example/thing@1.0",
      "vulnerabilities": [
        { "id": "CVE-2026-14683", "cvssScore": 2.0 },
        { "id": "CVE-2026-99999", "cvssScore": 8.1 }
      ]
    }
  }
}
JSON
log=$(write_log "$t" 1)
run_guard "$log" "$t"
assert "a live advisory beside an excluded one still fails the check" 2 "$LAST_EC"
assert_contains "  ...counting only the live one" "1 advisory(ies)" "$LAST_OUT"
assert_contains "  ...and naming it" "CVE-2026-99999" "$LAST_OUT"
if grep -qF "CVE-2026-14683" <<< "$LAST_OUT"; then
    echo "FAIL:   ...and not re-listing the excluded one"
    failures=$((failures + 1))
else
    echo "ok:     ...and not re-listing the excluded one"
fi

echo
if [ "$failures" -eq 0 ]; then
    echo "All bin/check-ossindex-audit.sh self-tests passed"
    exit 0
fi
echo "$failures self-test(s) FAILED"
exit 1
