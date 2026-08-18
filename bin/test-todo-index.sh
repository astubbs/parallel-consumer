#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/todo-index.sh's ARGUMENT PARSING, and for the two allowlist grants that depend
# on it. It is a permission-boundary test, not a test of the index's contents.
#
#    1. `--check` on a current index                   -> pass (0), index untouched
#    2. `--check` on a STALE index                     -> stale (1), index untouched
#    3. bare, no arguments                             -> pass (0), index REGENERATED
#    4. `--check=false`                                -> usage (2), index UNTOUCHED
#    5. `--check=true`                                 -> usage (2), index UNTOUCHED
#    6. `--Check`, wrong case                          -> usage (2)
#    7. `-c`, short form that was never supported      -> usage (2)
#    8. `--check extra`, a second argument             -> usage (2)
#    9. both workflows grant the EXACT command, in BOTH the `bin/...` and `./bin/...` spellings,
#       and carry no wildcard todo-index grant in either
#
# WHAT THIS PROTECTS AND WHY IS OWNED BY bin/todo-index.sh's HEADER COMMENT - read that first; it is
# the parser that enforces the boundary. What matters here is only which shapes are asserted:
#
# CASE 4 IS THE REGRESSION - the exact argument that defeated the first attempt at the fix. A
# regression test that has never failed proves nothing (bin/AGENTS.md), so run it against the
# pre-fix revision and watch it report `0 REWRITTEN`.
#
# CASE 9 GUARDS THE ALLOWLIST SIDE, which no amount of parser strictness can cover. It asserts the
# real property - that NO rule in either allowlist permits the bare command - rather than the proxy
# of "no wildcard rule names this script", because a broader neighbour such as `Bash(bin/*.sh:*)`
# re-covers it without mentioning it. Both command spellings are checked: a rule matches the command
# as written, so `bin/...` and `./bin/...` are separate grants and each needs its own assertion.
#
# Run: bin/test-todo-index.sh
#
# NOTE this test regenerates docs/todo-index.md in the real checkout (case 3 has to, to be worth
# anything) and restores it through a trap. It refuses to run against a dirty index for that reason.

set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT="$ROOT/bin/todo-index.sh"
INDEX="$ROOT/docs/todo-index.md"
CLAUDE_YML="$ROOT/.github/workflows/claude.yml"
DISPATCH_YML="$ROOT/.github/workflows/claude-code-review-dispatch.yml"

if ! git -C "$ROOT" diff --quiet -- "$INDEX"; then
    echo "FAIL: docs/todo-index.md has uncommitted changes; refusing to run so they are not lost" >&2
    exit 1
fi

BACKUP="$(mktemp)"
cp "$INDEX" "$BACKUP"
trap 'cp "$BACKUP" "$INDEX"; rm -f "$BACKUP"' EXIT

failures=0

assert() { # <description> <expected> <actual>
    if [ "$2" = "$3" ]; then
        echo "ok:   $1"
    else
        echo "FAIL: $1 (expected '$2', got '$3')"
        failures=$((failures + 1))
    fi
}

# Runs the script with the given arguments and echoes "<exit-code> <untouched|REWRITTEN>".
# The sentinel is what proves the file was not regenerated: a rewrite drops it.
run_case() { # <args...>
    cp "$BACKUP" "$INDEX"
    printf '\n<!-- self-test sentinel -->\n' >> "$INDEX"
    local ec=0
    ( cd "$ROOT" && "$SCRIPT" "$@" ) >/dev/null 2>&1 || ec=$?
    local state=REWRITTEN
    if grep -q 'self-test sentinel' <<<"$(cat "$INDEX")"; then
        state=untouched
    fi
    echo "$ec $state"
}

# Case 1 needs a genuinely current index, so it runs against the committed file rather than the
# sentinel-bearing copy every other case uses.
cp "$BACKUP" "$INDEX"
current_ec=0
( cd "$ROOT" && "$SCRIPT" --check ) >/dev/null 2>&1 || current_ec=$?
assert "--check on a current index passes" 0 "$current_ec"

assert "--check on a stale index reports stale and does not rewrite" "1 untouched" "$(run_case --check)"
assert "bare invocation regenerates the index"                      "0 REWRITTEN" "$(run_case)"

# THE REGRESSION. Both halves of the expectation matter: the exit code proves the argument was
# refused, and the sentinel proves the refusal happened BEFORE the regenerate branch.
assert "--check=false is refused and does not rewrite"  "2 untouched" "$(run_case --check=false)"
assert "--check=true is refused and does not rewrite"   "2 untouched" "$(run_case --check=true)"
assert "--Check is refused"                             "2 untouched" "$(run_case --Check)"
assert "-c is refused"                                  "2 untouched" "$(run_case -c)"
assert "a second argument is refused"                   "2 untouched" "$(run_case --check extra)"

# Case 9 - the allowlist side. See the header above for what is asserted and why.
for yml in "$CLAUDE_YML" "$DISPATCH_YML"; do
    name="$(basename "$yml")"
    contents="$(cat "$yml")"

    # Only GRANT lines are considered - the comments in both files discuss the old wildcard forms
    # deliberately. A grant line is one carrying the allowlist's `Bash(gh pr view:*)` neighbour.
    grants="$(grep 'Bash(gh pr view:\*)' <<<"$contents")"

    # Every `Bash(...)` rule on the grant line, one per line, body only.
    rules="$(grep -o 'Bash([^)]*)' <<<"$grants" | sed 's/^Bash(//; s/)$//')"

    for spelling in 'bin/todo-index.sh' './bin/todo-index.sh'; do
        exact=absent
        if grep -qF "Bash($spelling --check)" <<<"$grants"; then
            exact=present
        fi
        assert "$name grants the exact '$spelling --check' command" present "$exact"

        # No rule may permit the BARE rewriting command - the property itself, not the proxy.
        permitted_by=""
        while IFS= read -r rule; do
            [ -n "$rule" ] || continue
            if [[ "$rule" == *':*' ]]; then
                # Trailing-wildcard: a prefix grant. Rules glob with `*` at any position, so the
                # pattern is deliberately left unquoted on the right of `==`.
                prefix="${rule%:\*}"
                # shellcheck disable=SC2053
                if [[ "$spelling" == $prefix || "$spelling" == $prefix* ]]; then
                    permitted_by="$rule"; break
                fi
            else
                # shellcheck disable=SC2053
                if [[ "$spelling" == $rule ]]; then
                    permitted_by="$rule"; break
                fi
            fi
        done <<< "$rules"
        assert "$name permits no rule matching the bare '$spelling'" "" "$permitted_by"
    done
done

# The trap restores the index, but CI runs this immediately BEFORE `bin/todo-index.sh --check`, so a
# restore that silently failed would leave that gate inspecting a file this test regenerated - and it
# would then pass by construction rather than on the merits. Prove the restore instead of assuming it.
cp "$BACKUP" "$INDEX"
restored=differs
if cmp -s "$BACKUP" "$INDEX"; then
    restored=identical
fi
assert "the committed index is byte-identical after the run" identical "$restored"

if [ "$failures" -gt 0 ]; then
    echo "$failures assertion(s) failed"
    exit 1
fi
echo "all todo-index argument-parsing assertions passed"
