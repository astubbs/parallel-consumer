#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-shell-sigpipe.sh.
#
# Builds throwaway fixture scripts and asserts the guard's verdict for each shape:
#    1. no pipes at all                                          -> pass (0)
#    2. `| grep -q`, combined flags (-qE)                        -> FAIL (1)
#    3. `| grep -Eq`, flags the other way round                  -> FAIL (1)
#    4. `| grep -v -q`, SPLIT flags                              -> FAIL (1)
#    5. `| grep --quiet`, long form                              -> FAIL (1)
#    6. the violation appears only in a COMMENT                  -> pass (0)
#    7. same pipe, but the script has no `pipefail`              -> pass (0)
#    8. herestring instead of a pipe - the prescribed fix        -> pass (0)
#    9. `| grep query` - a bare word, no q-bearing FLAG          -> pass (0)
#   10. the guard skips itself, matched on BASENAME not path     -> pass (0)
#   11. the guard skips its OWN self-test, same reason           -> pass (0)
#   12. the NO-ARG form CI runs exits clean on the real tree     -> pass (0)
#   13. ...and its default scan set names .claude/hooks          -> pass (0)
#
# Cases 4 and 5 are the ones worth keeping: the first version of the guard matched
# `grep -[a-zA-Z]*q`, which a space defeats, so `grep -v -q` slipped through. Case 9 guards the
# other direction - the broader regex must not start flagging words that merely contain a q.
#
# Case 10 matters because the exclusion used to be a hardcoded path. Renaming the guard would have
# silently switched it off; matching on basename survives a rename, and this pins that.
#
# Run: bin/test-check-shell-sigpipe.sh   (CI runs it before the guard it protects)

set -uo pipefail

GUARD="$(cd "$(dirname "$0")" && pwd)/check-shell-sigpipe.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

failures=0

assert() { # <description> <expected> <actual>
    if [ "$2" = "$3" ]; then
        echo "ok:   $1"
    else
        echo "FAIL: $1 (expected exit '$2', got '$3')"
        failures=$((failures + 1))
    fi
}

# Writes a fixture script into a fresh directory and returns the guard's exit code for it.
# $2 overrides the `set` line, so a fixture can be built without pipefail.
run_guard() { # <script-body> [set-line]
    local dir="$TMP/case$RANDOM$RANDOM"
    mkdir -p "$dir"
    printf '#!/usr/bin/env bash\n%s\n%s\n' "${2:-set -euo pipefail}" "$1" > "$dir/fixture.sh"
    local ec=0
    "$GUARD" "$dir" >/dev/null 2>&1 || ec=$?
    echo "$ec"
}

assert "no pipes at all" 0 \
    "$(run_guard 'echo hello')"

assert "pipe into grep -qE (combined flags)" 1 \
    "$(run_guard 'if printf "%s" "$x" | grep -qE "foo"; then :; fi')"

assert "pipe into grep -Eq (flags reversed)" 1 \
    "$(run_guard 'if printf "%s" "$x" | grep -Eq "foo"; then :; fi')"

assert "pipe into grep -v -q (SPLIT flags)" 1 \
    "$(run_guard 'if printf "%s" "$x" | grep -v -q "foo"; then :; fi')"

assert "pipe into grep --quiet (long form)" 1 \
    "$(run_guard 'if printf "%s" "$x" | grep --quiet "foo"; then :; fi')"

assert "violation only inside a comment" 0 \
    "$(run_guard '# printf "%s" "$x" | grep -qE "foo"
echo fine')"

assert "same pipe but no pipefail in the script" 0 \
    "$(run_guard 'if printf "%s" "$x" | grep -qE "foo"; then :; fi' 'set -eu')"

assert "herestring instead of a pipe (the prescribed fix)" 0 \
    "$(run_guard 'if grep -qE "foo" <<<"$x"; then :; fi')"

assert "bare word containing q, not a flag" 0 \
    "$(run_guard 'printf "%s" "$x" | grep query')"

# The guard must skip a file with its OWN basename, wherever that file lives - so a rename cannot
# silently disable the exclusion. Copy the real guard into a fixture dir and confirm it is ignored
# despite containing the anti-pattern in its own failure message.
selfdir="$TMP/self"; mkdir -p "$selfdir"
cp "$GUARD" "$selfdir/$(basename "$GUARD")"
ec=0; "$GUARD" "$selfdir" >/dev/null 2>&1 || ec=$?
assert "guard skips itself by basename, not hardcoded path" 0 "$ec"

# ...and its self-test, for the same reason: this very file's fixtures are the shapes it detects.
testdir="$TMP/selftest"; mkdir -p "$testdir"
cp "$0" "$testdir/test-$(basename "$GUARD")"
ec=0; "$GUARD" "$testdir" >/dev/null 2>&1 || ec=$?
assert "guard skips its own self-test by basename" 0 "$ec"

# THE DEFAULT PATH IS THE PRODUCT: CI invokes the guard with NO arguments, and every case above
# hands it a fixture directory - so widening the default scan to .claude/hooks (the point of the
# astubbs#324 change) ran in no test at all. Invoke it the way CI does, and pin the scanned set by
# the guard's own report line: reverting the widening changes that line, so this goes red.
no_arg_out=$("$GUARD" 2>&1); no_arg_ec=$?
assert "no-arg run (CI's invocation) exits clean on the real tree" 0 "$no_arg_ec"
case "$no_arg_out" in
    *".claude/hooks"*) got=scans_hooks ;;
    *)                 got="hooks_not_in_scan_set" ;;
esac
assert "the default scan set includes .claude/hooks" scans_hooks "$got"

echo
if [ "$failures" -eq 0 ]; then
    echo "All bin/check-shell-sigpipe.sh self-tests passed"
    exit 0
fi
echo "$failures self-test(s) FAILED"
exit 1
