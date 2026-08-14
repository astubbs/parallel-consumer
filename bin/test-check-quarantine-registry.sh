#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/lib/quarantine-common.sh - the scan that check-quarantine-registry.sh,
# check-quarantine-owners.sh and quarantined-test.sh all read their file list from.
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

echo
if [ "$failures" -gt 0 ]; then
    printf '%d quarantine-scan self-test(s) failed\n' "$failures" >&2
    exit 1
fi
echo "All quarantine-scan self-tests passed"
