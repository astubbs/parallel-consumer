#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/lib/quarantine-common.sh - the scan that check-quarantine-registry.sh,
# check-quarantine-owners.sh and quarantined-test.sh all read their file list from - AND, in the
# section at the foot of this file, for what bin/check-quarantine-registry.sh actually SAYS when it
# finds drift. Both halves belong here: the gate's reporting failed inside the shared lookup, not in
# its own message code, so splitting them would have put the fixture and the mechanism in two files.
#
# WHY THIS EXISTS. The exclusion of `.claude` from the scan is a behaviour change with a specific
# cause: `.claude/worktrees/<name>` holds a full checkout of some OTHER branch, so without it the
# scan reports drift from every sibling worktree - annotations that are not on this branch at all,
# against a registry that is. `bin/AGENTS.md` is explicit that a checker fix arrives with a case in
# its self-test, and that the case must go red against the old code; `docs/agent-harness.md` rule 3
# says the same. That is what the last review caught here: the fix shipped without one, so a later
# edit could delete or misspell `--exclude-dir=.claude` and every test would stay green.
#
# THE NEGATIVE CONTROL IS BUILT IN, not a claim in a commit message. `previous_implementation()`
# below is the exact grep this repo shipped before the fix. The fixture is asserted against BOTH:
# the current scan must not see the sibling worktree, and the previous one must - if a future
# refactor makes the fixture stop exercising the exclusion, the old-code assertion fails and says
# so, rather than the suite passing on a fixture that no longer reaches the bug.
#
# Read-only, no network, ~0.1s: safe under the `test-check-*` reviewer grant (bin/AGENTS.md).

set -euo pipefail

failures=0

assert() { # <description> <expected> <actual>
    if [ "$2" = "$3" ]; then
        printf 'ok:   %s\n' "$1"
    else
        printf 'FAIL: %s\n        expected: %s\n        actual:   %s\n' "$1" "$2" "$3" >&2
        failures=$((failures + 1))
    fi
}

repo_root=$(cd "$(dirname "$0")/.." && pwd)
# shellcheck source=bin/lib/quarantine-common.sh
. "$repo_root/bin/lib/quarantine-common.sh"

# The scan as it stood before the worktree-pollution fix: `target` excluded, nothing else. Kept
# verbatim so the fixture is proven to reach the defect rather than assumed to.
previous_implementation() {
    grep -rlE --include='*.java' --exclude-dir=target "$QUARANTINE_ANNOTATION_ERE" . 2>/dev/null || true
}

fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT

annotated() { # <path>
    mkdir -p "$(dirname "$1")"
    cat > "$1" <<'JAVA'
package fixture;

class Sample {
    @Test
    @Quarantined(fixedBy = "astubbs#1234", reason = "fixture")
    void aTest() {}
}
JAVA
}

# On this branch, and so in the registry.
annotated "$fixture/parallel-consumer-core/src/test/java/Ordinary.java"

# A sibling worktree: a full checkout of a DIFFERENT branch, carrying annotations this branch's
# registry has never heard of. This is the file the whole exclusion exists for. BOTH worktree roots
# get one - .gitignore names `.claude/worktrees` and `/.worktrees/`, and the first version of this
# fix excluded only the first, which review caught precisely because no fixture covered the second.
annotated "$fixture/.claude/worktrees/some-other-branch/parallel-consumer-core/src/test/java/Sibling.java"
annotated "$fixture/.worktrees/another-branch/parallel-consumer-core/src/test/java/OtherRoot.java"

# Cheap neighbours of the same shape, pinned so removing either exclusion is a deliberate act.
annotated "$fixture/target/generated-sources/Generated.java"
annotated "$fixture/.git/some-tooling-copy/Stashed.java"

# A string literal is NOT annotation usage - the tooling's own sources contain "@Quarantined(" as
# data, and matching it would block a release. This pins the anchoring described in the lib.
mkdir -p "$fixture/bin/fixtures"
cat > "$fixture/bin/fixtures/Literal.java" <<'JAVA'
class Literal {
    String pattern = "@Quarantined(";
}
JAVA

cd "$fixture"

echo "--- quarantined_files(): what the registry is checked against ---"

found=$(quarantined_files | sed 's|^\./||' | sort)
contains() { # <haystack> <needle> -> YES | NO
    case $'\n'"$1"$'\n' in
        *$'\n'"$2"$'\n'*) echo YES ;;
        *)                echo NO ;;
    esac
}

assert "a file on this branch is scanned" YES \
    "$(contains "$found" "parallel-consumer-core/src/test/java/Ordinary.java")"
assert "a sibling worktree under .claude is NOT scanned" NO \
    "$(contains "$found" ".claude/worktrees/some-other-branch/parallel-consumer-core/src/test/java/Sibling.java")"
assert "a sibling worktree under .worktrees is NOT scanned" NO \
    "$(contains "$found" ".worktrees/another-branch/parallel-consumer-core/src/test/java/OtherRoot.java")"
assert "build output under target is NOT scanned" NO \
    "$(contains "$found" "target/generated-sources/Generated.java")"
assert "a copy under .git is NOT scanned" NO \
    "$(contains "$found" ".git/some-tooling-copy/Stashed.java")"
assert "a string literal is not annotation usage" NO \
    "$(contains "$found" "bin/fixtures/Literal.java")"

echo "--- quarantined_audit(): the human listing, same exclusions ---"

# quarantined_audit greps with -A 4, so its output carries file:line prefixes rather than paths.
audit=$(quarantined_audit || true)
audit_names() { # <substring> -> YES | NO
    case "$audit" in
        *"$1"*) echo YES ;;
        *)      echo NO ;;
    esac
}

assert "the audit lists a file on this branch" YES "$(audit_names "Ordinary.java")"
assert "the audit does NOT list a sibling worktree" NO "$(audit_names "Sibling.java")"
assert "the audit does NOT list the other worktree root" NO "$(audit_names "OtherRoot.java")"
assert "the audit does NOT list build output" NO "$(audit_names "Generated.java")"

echo "--- negative control: the fixture must reach the defect ---"

# If this passes, the fixture has stopped exercising the exclusion and every assertion above is
# vacuous. That is the failure mode bin/AGENTS.md means by "a regression test that has never failed
# proves nothing", so it is asserted rather than trusted.
old=$(previous_implementation | sed 's|^\./||' | sort)
assert "the PREVIOUS implementation does leak the sibling worktree" YES \
    "$(contains "$old" ".claude/worktrees/some-other-branch/parallel-consumer-core/src/test/java/Sibling.java")"
assert "the PREVIOUS implementation does leak the other worktree root" YES \
    "$(contains "$old" ".worktrees/another-branch/parallel-consumer-core/src/test/java/OtherRoot.java")"
assert "the PREVIOUS implementation does leak the .git copy" YES \
    "$(contains "$old" ".git/some-tooling-copy/Stashed.java")"
assert "the PREVIOUS implementation already excluded target" NO \
    "$(contains "$old" "target/generated-sources/Generated.java")"

echo "--- quarantined_occurrences: a file with no match must count 0, on ONE line ---"

# The bug this covers is invisible to a string comparison that happens to fail anyway, so assert the
# exact bytes and then assert that a numeric test survives them - which is how the callers use it.
no_match_file=$(mktemp)
printf 'class NothingQuarantinedHere {}\n' > "$no_match_file"

assert "no match counts exactly 0" "0" "$(quarantined_occurrences "$no_match_file")"

numeric_ok=NO
if [ "$(quarantined_occurrences "$no_match_file")" -eq 0 ] 2>/dev/null; then numeric_ok=YES; fi
assert "the result survives a numeric test" YES "$numeric_ok"

# Negative control, in this file's established style: the shipped-before implementation must fail
# both, or the fixture has stopped reaching the defect and the two assertions above are vacuous.
previous_occurrences() { grep -cE "$QUARANTINE_ANNOTATION_ERE" "$1" 2>/dev/null || echo 0; }

assert "the PREVIOUS implementation returns two lines" "$(printf '0\n0')" \
    "$(previous_occurrences "$no_match_file")"

old_numeric_ok=NO
if [ "$(previous_occurrences "$no_match_file")" -eq 0 ] 2>/dev/null; then old_numeric_ok=YES; fi
assert "the PREVIOUS implementation breaks a numeric test" NO "$old_numeric_ok"

rm -f "$no_match_file"

echo
echo "--- check-quarantine-registry.sh: drift is EXPLAINED, not merely refused ---"

# THE GATE REFUSED SILENTLY IN EXACTLY THE CASE IT EXISTS TO CATCH. Its registry -> code loop ended
# in `f=$(quarantined_files | while read -r qf; do [ ... ] && { echo "$qf"; break; }; done)`. When no
# annotated class matches the entry, the final `[ ... ]` fails, the `while` carries that status out,
# `pipefail` promotes it to the pipeline, the assignment takes it, and `set -e` kills the script
# BEFORE the `DRIFT:` line can print. Exit 1 was right; the explanation was destroyed, so the gate
# said nothing at all about the one thing it had found.
#
# CONTROL ARM, one term changed and everything else identical: `set -euo pipefail` printed nothing
# and exited 1, `set -uo pipefail` printed the full DRIFT line and exited 1 - which is what named
# `set -e` plus the loop status as the mechanism rather than the reporting code.
#
# THE NEGATIVE CONTROL BELOW IS THE SHIPPED PRE-FIX LOOP, patched back into a copy of the gate, so
# the fixture is PROVEN to reach the defect rather than assumed to - the same discipline as
# `previous_implementation()` above. If the anchor line it patches ever stops existing, the control
# fails loudly instead of passing vacuously.

gate_fixture="$(mktemp -d)"
trap 'rm -rf "$fixture" "$gate_fixture"' EXIT
mkdir -p "$gate_fixture/docs" "$gate_fixture/bin/lib"
annotated "$gate_fixture/parallel-consumer-core/src/test/java/Present.java"
cp "$repo_root/bin/lib/quarantine-common.sh" "$gate_fixture/bin/lib/quarantine-common.sh"
cat > "$gate_fixture/docs/quarantined-tests.md" <<'MD'
# Quarantined tests - fixture

- [ ] `Present.aTest` - an entry whose class really is annotated
- [ ] `Vanished.someTest` - an entry naming a class with no @Quarantined anywhere in the tree
MD

run_gate() { # <gate-path> -> prints "<exit>|<output>"
    local out rc=0
    out="$(QUARANTINE_CHECK_ROOT="$gate_fixture" bash "$1" 2>&1)" || rc=$?
    printf '%s|%s' "$rc" "$out"
}

gate_result="$(run_gate "$repo_root/bin/check-quarantine-registry.sh")"
gate_rc="${gate_result%%|*}"
gate_out="${gate_result#*|}"

case "$gate_out" in
    *"DRIFT:"*Vanished.someTest*) got=named_the_stale_entry ;;
    '')                           got=said_nothing_at_all ;;
    *)                            got="$gate_out" ;;
esac
assert "a stale registry entry is NAMED, not just refused" named_the_stale_entry "$got"
assert "and the gate still fails" 1 "$gate_rc"

# The other half of the same run: a genuinely annotated entry must not be reported as drift, or the
# case above would pass on a gate that simply shouts about everything.
case "$gate_out" in *Present*) got=false_positive ;; *) got=quiet_about_the_good_one ;; esac
assert "the entry whose class IS annotated is not reported" quiet_about_the_good_one "$got"

# --- negative control: the fixture must reach the defect ---
previous_gate="$gate_fixture/bin/previous-check-quarantine-registry.sh"
python3 - "$repo_root/bin/check-quarantine-registry.sh" "$previous_gate" <<'PY'
import sys

ANCHOR = '    f=$(quarantined_file_for_class "$cls")\n'
SHIPPED_BEFORE_THE_FIX = (
    '    f=$(quarantined_files | while read -r qf; do\n'
    '            [ "$(basename "$qf" .java)" = "$cls" ] && { echo "$qf"; break; }\n'
    '        done)\n'
)
src = open(sys.argv[1]).read()
if ANCHOR not in src:
    sys.stderr.write("anchor line not found - the negative control no longer patches anything\n")
    sys.exit(1)
open(sys.argv[2], "w").write(src.replace(ANCHOR, SHIPPED_BEFORE_THE_FIX))
PY
prev_result="$(run_gate "$previous_gate")"
prev_rc="${prev_result%%|*}"
prev_out="${prev_result#*|}"
[ -z "$prev_out" ] && got=silent || got="$prev_out"
assert "the PREVIOUS implementation refuses with NO explanation" silent "$got"
assert "...while still exiting 1, which is why nobody noticed" 1 "$prev_rc"

echo
echo "--- quarantined_file_for_class(): the lookup both gates share ---"

cd "$gate_fixture"
assert "finds the file of an annotated class" \
    "./parallel-consumer-core/src/test/java/Present.java" "$(quarantined_file_for_class Present)"
# The whole defect in one line: a miss must be an empty answer, never a failing status. Called
# inside a `$(...)` under this file's own `set -e`, a non-zero return here would kill the suite.
assert "a class with no annotation answers empty" "" "$(quarantined_file_for_class Vanished)"
quarantined_file_for_class Vanished >/dev/null
assert '...and returns 0, so set -e does not kill the caller' 0 "$?"

echo
if [ "$failures" -gt 0 ]; then
    printf '%d quarantine-scan self-test(s) failed\n' "$failures" >&2
    exit 1
fi
echo "All quarantine-scan self-tests passed"
