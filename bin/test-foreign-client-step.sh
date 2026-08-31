#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/foreign-client-step.sh.
#
# The wrapper's whole job is to tell three outcomes apart that all look alike in a Maven log - the
# step passed, the step failed, the toolchain was never there - so every arm below pins one of those
# boundaries:
#
#    1. tool present, command succeeds                      -> pass (0)
#    2. tool present, command fails                         -> FAIL (1)
#    3. tool ABSENT, lenient (the developer-box default)    -> pass (0), banner printed
#    4. tool ABSENT, PC_FOREIGN_CLIENTS_STRICT=1            -> exit 2
#    5. tool ABSENT, PC_FOREIGN_CLIENTS_STRICT=0/false      -> pass (0) - the off switch really is off
#    6. --hello, program prints the right line              -> pass (0)
#    7. --hello, program prints something else              -> FAIL (1)
#    8. --hello, program prints nothing                     -> FAIL (1)
#    9. --hello, program exits non-zero                     -> FAIL (1)
#   10. no --tool                                           -> usage error (3)
#   11. no command after --                                 -> usage error (3)
#   12. the fixture line for two languages differs only by the language
#
# ARM 3 IS THE ONE THAT EARNS THIS FILE. A wrapper that silently returned 0 for every absent
# toolchain would pass arms 1, 2, 6 and 7 unchanged and turn every foreign row into a check that
# reports success without having run - so the arm asserts the BANNER is on stdout, not merely that
# the exit code is 0.
#
# Arm 12 pins that the expected line is derived from the language rather than being one constant
# eleven modules could all satisfy by printing the same thing.
#
# Run: bin/test-foreign-client-step.sh   (bin/check-all.sh --with-tests sweeps it; CI runs that)

set -uo pipefail

STEP="$(cd "$(dirname "$0")" && pwd)/foreign-client-step.sh"
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

assert_contains() { # <description> <needle> <haystack>
    case "$3" in
        *"$2"*) echo "ok:   $1" ;;
        *)
            echo "FAIL: $1 (output did not contain '$2')"
            echo "----- output -----"
            echo "$3"
            echo "------------------"
            failures=$((failures + 1))
            ;;
    esac
}

# A stand-in "toolchain": a real executable on PATH whose name nothing else in this repo uses, so
# the present/absent arms differ only in whether it exists.
mkdir -p "$TMP/bin"
cat > "$TMP/bin/pc-fake-toolchain" <<'FAKE'
#!/usr/bin/env bash
exit 0
FAKE
chmod +x "$TMP/bin/pc-fake-toolchain"
PATH_WITH_TOOL="$TMP/bin:$PATH"

# ── the toolchain is present ────────────────────────────────────────────────────────────────────
out="$(PATH="$PATH_WITH_TOOL" bash "$STEP" --tool pc-fake-toolchain -- true 2>&1)"; rc=$?
assert "present toolchain, command succeeds" 0 "$rc"

out="$(PATH="$PATH_WITH_TOOL" bash "$STEP" --tool pc-fake-toolchain -- false 2>&1)"; rc=$?
assert "present toolchain, command fails" 1 "$rc"

# ── the toolchain is absent ─────────────────────────────────────────────────────────────────────
out="$(bash "$STEP" --tool pc-definitely-not-installed --hello ruritanian -- true 2>&1)"; rc=$?
assert "absent toolchain, lenient default" 0 "$rc"
assert_contains "absent toolchain names the missing executable" "pc-definitely-not-installed" "$out"
assert_contains "absent toolchain names the module/step" "ruritanian" "$out"
assert_contains "absent toolchain says nothing was built" "nothing was built" "$out"

out="$(PC_FOREIGN_CLIENTS_STRICT=1 bash "$STEP" --tool pc-definitely-not-installed -- true 2>&1)"; rc=$?
assert "absent toolchain under strict" 2 "$rc"

out="$(PC_FOREIGN_CLIENTS_STRICT=false bash "$STEP" --tool pc-definitely-not-installed -- true 2>&1)"; rc=$?
assert "absent toolchain, strict explicitly off" 0 "$rc"

# ── the hello fixture ───────────────────────────────────────────────────────────────────────────
say() { # <language> <what the fake program prints>
    printf '#!/usr/bin/env bash\nprintf %s\n' "'$2'" > "$TMP/bin/pc-fake-hello"
    chmod +x "$TMP/bin/pc-fake-hello"
}

say ruritanian 'parallel-consumer-proxy-client hello fixture: ruritanian'
out="$(PATH="$PATH_WITH_TOOL" bash "$STEP" --tool pc-fake-toolchain --hello ruritanian -- pc-fake-hello 2>&1)"; rc=$?
assert "hello fixture matches" 0 "$rc"

say ruritanian 'parallel-consumer-proxy-client hello fixture: elsewhere'
out="$(PATH="$PATH_WITH_TOOL" bash "$STEP" --tool pc-fake-toolchain --hello ruritanian -- pc-fake-hello 2>&1)"; rc=$?
assert "hello fixture names the wrong language" 1 "$rc"
assert_contains "mismatch prints what was expected" "expected: parallel-consumer-proxy-client hello fixture: ruritanian" "$out"

say ruritanian ''
out="$(PATH="$PATH_WITH_TOOL" bash "$STEP" --tool pc-fake-toolchain --hello ruritanian -- pc-fake-hello 2>&1)"; rc=$?
assert "hello fixture prints nothing" 1 "$rc"

printf '#!/usr/bin/env bash\nexit 7\n' > "$TMP/bin/pc-fake-hello"
chmod +x "$TMP/bin/pc-fake-hello"
out="$(PATH="$PATH_WITH_TOOL" bash "$STEP" --tool pc-fake-toolchain --hello ruritanian -- pc-fake-hello 2>&1)"; rc=$?
assert "hello fixture program crashes" 1 "$rc"

# The fixture line is DERIVED, not a constant: two languages must not be able to satisfy the same
# expectation. Without this, eleven modules printing one shared string would pass every other arm.
say other 'parallel-consumer-proxy-client hello fixture: ruritanian'
out="$(PATH="$PATH_WITH_TOOL" bash "$STEP" --tool pc-fake-toolchain --hello other -- pc-fake-hello 2>&1)"; rc=$?
assert "one language's line does not satisfy another's" 1 "$rc"

# ── usage ───────────────────────────────────────────────────────────────────────────────────────
out="$(bash "$STEP" -- true 2>&1)"; rc=$?
assert "no --tool is a usage error" 3 "$rc"

out="$(bash "$STEP" --tool pc-fake-toolchain -- 2>&1)"; rc=$?
assert "no command after -- is a usage error" 3 "$rc"

if [ "$failures" -eq 0 ]; then
    echo "test-foreign-client-step: all checks passed"
    exit 0
fi
echo "test-foreign-client-step: $failures check(s) failed"
exit 1
