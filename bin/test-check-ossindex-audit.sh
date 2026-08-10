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
# Cases:
#    1. clean scan, no findings                                  -> pass (0)
#    2. clean scan, findings present                             -> pass (0), findings rendered
#    3. 401: failure line in the log AND `{ }` reports           -> FAIL (1)   <- the real regression
#    4. token expiry shape with the log line ABSENT              -> FAIL (1)   <- leg 2 alone
#    5. no report exported at all                                -> FAIL (1)   <- leg 1+2
#    6. log claims 3 modules checked, only 2 reported            -> FAIL (1)   <- leg 3 alone
#    7. missing log file                                         -> FAIL (1)
#    8. vulnerable component with NO scored advisories           -> pass (0)   <- renderer crash
#    9. two advisories carrying no id                            -> pass (0), both counted
#
# Cases 4 and 6 are the ones worth keeping. Case 4 is the future failure this guard is really for:
# if Sonatype ever reworded the warning, leg 1 goes quiet and only the structural leg is left. Case 6
# proves the coverage leg is load-bearing rather than decorative - it fires with no failure line in
# the log and every present report perfectly well-formed.
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

# ── 2. clean scan WITH findings - reported, never fatal ───────────────────────
t="$TMP/findings"; mkdir -p "$t"
write_report "$t" core vulnerable
write_report "$t" vertx
log=$(write_log "$t" 2)
run_guard "$log" "$t"
assert "findings are reported, not fatal" 0 "$LAST_EC"
assert_contains "  ...naming the advisory" "CVE-2026-54513" "$LAST_OUT"
assert_contains "  ...and its score" "9.3" "$LAST_OUT"

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
assert "a vulnerable component with no scored advisories does not crash the guard" 0 "$LAST_EC"
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
assert "two unidentified advisories are counted separately, not collapsed" 0 "$LAST_EC"
assert_contains "  ...reporting both of them" "2 advisory(ies)" "$LAST_OUT"
assert_contains "  ...and keeping the higher score" "7.7" "$LAST_OUT"

echo
if [ "$failures" -eq 0 ]; then
    echo "All bin/check-ossindex-audit.sh self-tests passed"
    exit 0
fi
echo "$failures self-test(s) FAILED"
exit 1
